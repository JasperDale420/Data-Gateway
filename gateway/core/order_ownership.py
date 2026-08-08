"""Broker-reconciled, non-expiring ownership claims for shared-account orders."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from gateway.core.logger import logger
from gateway.core.symbology import SymbolResolver


def canonical_broker_symbol(symbol: str) -> str:
    """Return Alpaca's canonical equity ticker or full OCC option contract."""
    resolved = SymbolResolver().resolve(symbol)
    if resolved.type not in {"stock", "option"}:
        raise ValueError("Shared-account ownership supports equities and full OCC option contracts only")
    return resolved.provider_formats.get("alpaca", resolved.symbol).upper()


def owner_from_client_order_id(client_order_id: Any, client_ids: list[str]) -> str | None:
    """Return the longest configured owner prefix, or None for a manual order."""
    if not isinstance(client_order_id, str) or not client_order_id.startswith("c-"):
        return None
    after_prefix = client_order_id[2:]
    for client_id in sorted(client_ids, key=len, reverse=True):
        if after_prefix.startswith(f"{client_id}-"):
            return client_id
    return None


@dataclass(frozen=True)
class BrokerSymbolState:
    """Fresh broker state for one canonical broker symbol."""

    has_position: bool
    order_owners: frozenset[str | None]
    complete: bool = True


class OwnershipConflict(RuntimeError):
    """The shared broker state is ambiguous or belongs to another client."""


class OwnershipStoreUnavailable(RuntimeError):
    """Redis cannot authoritatively record a safety-critical claim."""


class OrderOwnershipGuard:
    """Atomically owns one broker symbol without Redis expiration semantics."""

    _PREFIX = "gateway:order-ownership"
    _FENCE_TTL_MS = 120_000
    _CLAIM_AND_INDEX = """
if redis.call('SET', KEYS[1], ARGV[1], 'NX') then
  redis.call('SADD', KEYS[2], ARGV[2])
  return 1
end
return 0
"""
    # Compares the decoded owner rather than the raw stored bytes. The claim is
    # rewritten by cjson.encode inside the mutation scripts, whose key order is
    # not the key order Python's sort_keys dump produces, so a byte comparison
    # would stop matching after the first completed mutation and the claim could
    # never be released. Refusing to release a frozen or mid-mutation claim
    # preserves what the byte comparison was there to guarantee.
    _COMPARE_AND_DELETE = """
-- release_claim
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local claim = cjson.decode(raw)
if claim['owner'] ~= ARGV[1] then return 0 end
if claim['mutation_pending'] or claim['frozen_reason'] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[2])
return 1
"""
    _COMPARE_AND_DELETE_FENCE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""
    _BEGIN_MUTATION = """
-- begin_mutation
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local claim = cjson.decode(raw)
if claim['mutation_pending'] then return 0 end
claim['mutation_pending'] = ARGV[1]
redis.call('SET', KEYS[1], cjson.encode(claim))
return 1
"""
    _COMPLETE_MUTATION = """
-- complete_mutation
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local claim = cjson.decode(raw)
if claim['mutation_pending'] ~= ARGV[1] then return 0 end
claim['mutation_pending'] = nil
redis.call('SET', KEYS[1], cjson.encode(claim))
return 1
"""

    def __init__(self, redis: Any) -> None:
        self._redis = redis

    @classmethod
    def claim_key(cls, symbol: str) -> str:
        return f"{cls._PREFIX}:claim:{symbol}"

    @classmethod
    def owner_index_key(cls, client_id: str) -> str:
        return f"{cls._PREFIX}:owner:{client_id}"

    @classmethod
    def fence_key(cls, symbol: str) -> str:
        return f"{cls._PREFIX}:fence:{symbol}"

    async def acquire_fence(self, symbol: str) -> str:
        """Serialize Gateway mutations for one symbol; the durable claim never expires."""
        token = uuid.uuid4().hex
        try:
            acquired = await self._redis.set(self.fence_key(symbol), token, nx=True, px=self._FENCE_TTL_MS)
        except Exception as exc:
            raise self._store_error(symbol, "fence_acquire", exc) from exc
        if not acquired:
            self._reject(symbol, "concurrent_gateway_reconciliation")
        return token

    async def renew_fence(self, symbol: str, token: str) -> None:
        try:
            renewed = await self._redis.eval(
                """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
""",
                1,
                self.fence_key(symbol),
                token,
                self._FENCE_TTL_MS,
            )
        except Exception as exc:
            raise self._store_error(symbol, "fence_renew", exc) from exc
        if not renewed:
            self._reject(symbol, "gateway_fence_lost")

    async def release_fence(self, symbol: str, token: str) -> None:
        """Best-effort delete; deliberately does not raise like acquire/renew.

        The mutation this fence guarded has already completed by the time we
        get here, so there is no caller left to usefully hand a failure to.
        The TTL set at acquire/renew time is the real safety net: a failed
        delete just means the key self-expires up to ``_FENCE_TTL_MS`` later
        instead of immediately.
        """
        # nosemgrep: empire-no-bare-exception -- best-effort release: the fence has a bounded TTL and will self-expire, so a failed delete here is not fatal
        try:
            await self._redis.eval(self._COMPARE_AND_DELETE_FENCE, 1, self.fence_key(symbol), token)
        except Exception:
            logger.error("order_ownership_fence_release_failed", symbol=symbol, exc_info=True)

    async def authorize_submission(
        self,
        *,
        client_id: str,
        symbol: str,
        broker_state: BrokerSymbolState,
    ) -> None:
        """Reconcile the supplied broker state, then atomically claim the symbol."""
        claim = await self._load_claim(symbol)
        self._assert_broker_state_is_unambiguous(symbol=symbol, claim=claim, broker_state=broker_state)
        await self._release_if_clear(symbol=symbol, claim=claim, broker_state=broker_state)
        claim = await self._load_claim(symbol)
        self._assert_broker_state_is_unambiguous(symbol=symbol, claim=claim, broker_state=broker_state)

        if claim is not None:
            if claim["owner"] != client_id:
                self._reject(symbol, "owned_by_another_gateway_client")
            await self._ensure_index(claim["owner"], symbol)
            logger.info("order_ownership_claim_reused", client_id=client_id, symbol=symbol)
            return

        created = self._serialize_claim(client_id)
        try:
            acquired = await self._redis.eval(
                self._CLAIM_AND_INDEX,
                2,
                self.claim_key(symbol),
                self.owner_index_key(client_id),
                created,
                symbol,
            )
        except Exception as exc:
            raise self._store_error(symbol, "claim", exc) from exc
        if acquired:
            logger.info("order_ownership_claim_acquired", client_id=client_id, symbol=symbol)
            return

        winner = await self._load_claim(symbol)
        if winner is None:
            raise OwnershipStoreUnavailable(f"claim_race_unresolved:{symbol}")
        if winner["owner"] != client_id:
            self._reject(symbol, "concurrent_claim_by_another_gateway_client")
        await self._ensure_index(client_id, symbol)
        logger.info("order_ownership_claim_reused", client_id=client_id, symbol=symbol)

    async def authorize_close(
        self,
        *,
        client_id: str,
        symbol: str,
        broker_state: BrokerSymbolState,
    ) -> None:
        """Permit a close only for the recorded owner of a broker-held position."""
        claim = await self._load_claim(symbol)
        self._assert_broker_state_is_unambiguous(symbol=symbol, claim=claim, broker_state=broker_state)
        if not broker_state.has_position:
            self._reject(symbol, "broker_position_not_open")
        if broker_state.order_owners:
            self._reject(symbol, "open_orders_prevent_close")
        if claim is None:
            self._reject(symbol, "unowned_broker_position")
        assert claim is not None
        if claim["owner"] != client_id:
            self._reject(symbol, "owned_by_another_gateway_client")
        logger.info("order_ownership_close_authorized", client_id=client_id, symbol=symbol)

    async def begin_mutation(self, *, symbol: str, operation: str) -> str:
        """Durably mark a broker write before it can leave the Gateway."""
        token = f"{operation}:{uuid.uuid4().hex}"
        try:
            started = await self._redis.eval(
                self._BEGIN_MUTATION,
                1,
                self.claim_key(symbol),
                token,
            )
        except Exception as exc:
            raise self._store_error(symbol, "mutation_begin", exc) from exc
        if not started:
            claim = await self._load_claim(symbol)
            if claim is None:
                raise OwnershipStoreUnavailable(f"missing_claim_to_begin_mutation:{symbol}")
            self._reject(symbol, "mutation_pending_reconciliation")
        logger.info("order_ownership_mutation_started", symbol=symbol, operation=operation)
        return token

    async def complete_mutation(self, *, symbol: str, token: str) -> None:
        """Clear the pending marker only after post-write reconciliation passes."""
        try:
            completed = await self._redis.eval(
                self._COMPLETE_MUTATION,
                1,
                self.claim_key(symbol),
                token,
            )
        except Exception as exc:
            raise self._store_error(symbol, "mutation_complete", exc) from exc
        if not completed:
            self._reject(symbol, "mutation_completion_token_lost")
        logger.info("order_ownership_mutation_reconciled", symbol=symbol)

    async def _release_if_clear(
        self,
        *,
        symbol: str,
        claim: dict[str, str] | None,
        broker_state: BrokerSymbolState,
    ) -> None:
        if claim is None or broker_state.has_position or broker_state.order_owners:
            return
        try:
            released = await self._redis.eval(
                self._COMPARE_AND_DELETE,
                2,
                self.claim_key(symbol),
                self.owner_index_key(claim["owner"]),
                claim["owner"],
                symbol,
            )
        except Exception as exc:
            raise self._store_error(symbol, "release", exc) from exc
        if released:
            logger.info("order_ownership_claim_released", client_id=claim["owner"], symbol=symbol)

    async def _load_claim(self, symbol: str) -> dict[str, str] | None:
        try:
            raw = await self._redis.get(self.claim_key(symbol))
        except Exception as exc:
            raise self._store_error(symbol, "read", exc) from exc
        if raw is None:
            return None
        try:
            claim = json.loads(raw)
            # A claim that decoded to a list/number/string is corrupt state, not
            # a claim; check the container type before reading fields so it is
            # rejected as an ownership conflict rather than an AttributeError.
            if not isinstance(claim, dict):
                raise ValueError("claim is not a JSON object")
            if not isinstance(claim.get("owner"), str) or not claim["owner"]:
                raise ValueError("missing owner")
            return claim
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.error("order_ownership_claim_corrupt", symbol=symbol, exc_info=True)
            raise OwnershipConflict(f"corrupt_claim:{symbol}") from exc

    @staticmethod
    def _serialize_claim(client_id: str) -> str:
        return json.dumps(
            {"owner": client_id, "claimed_at": datetime.now(UTC).isoformat()},
            separators=(",", ":"),
            sort_keys=True,
        )

    def _assert_broker_state_is_unambiguous(
        self,
        *,
        symbol: str,
        claim: dict[str, str] | None,
        broker_state: BrokerSymbolState,
        allow_pending_mutation: bool = False,
    ) -> None:
        if not broker_state.complete:
            self._reject(symbol, "incomplete_broker_reconciliation")
        if claim is not None and claim.get("frozen_reason"):
            self._reject(symbol, "claim_frozen_after_ambiguous_broker_mutation")
        if claim is not None and claim.get("mutation_pending") and not allow_pending_mutation:
            self._reject(symbol, "mutation_pending_reconciliation")
        owners = broker_state.order_owners
        if None in owners or len(owners) > 1:
            self._reject(symbol, "manual_or_mixed_broker_orders")
        if broker_state.has_position and claim is None:
            self._reject(symbol, "unowned_broker_position")
        if claim is None and owners and next(iter(owners)) is not None:
            self._reject(symbol, "unclaimed_broker_order")
        if claim is not None and owners and claim["owner"] not in owners:
            self._reject(symbol, "claim_disagrees_with_broker_order_owner")

    async def verify_reconciliation(
        self,
        *,
        client_id: str,
        symbol: str,
        broker_state: BrokerSymbolState,
    ) -> None:
        """Confirm the post-mutation state without releasing the durable claim."""
        claim = await self._load_claim(symbol)
        self._assert_broker_state_is_unambiguous(
            symbol=symbol,
            claim=claim,
            broker_state=broker_state,
            allow_pending_mutation=True,
        )
        if claim is None or claim["owner"] != client_id:
            self._reject(symbol, "claim_missing_or_owned_by_another_gateway_client")

    async def freeze(self, symbol: str, reason: str) -> None:
        """Keep a claim non-expiring after an ambiguous broker mutation outcome."""
        claim = await self._load_claim(symbol)
        if claim is None:
            raise OwnershipStoreUnavailable(f"missing_claim_to_freeze:{symbol}")
        claim["frozen_reason"] = reason
        try:
            await self._redis.set(
                self.claim_key(symbol),
                json.dumps(claim, separators=(",", ":"), sort_keys=True),
            )
        except Exception as exc:
            raise self._store_error(symbol, "freeze", exc) from exc
        logger.warning("order_ownership_frozen", symbol=symbol, reason=reason)

    async def _ensure_index(self, client_id: str, symbol: str) -> None:
        try:
            await self._redis.sadd(self.owner_index_key(client_id), symbol)
        except Exception as exc:
            raise self._store_error(symbol, "index_repair", exc) from exc

    @staticmethod
    def _reject(symbol: str, reason: str) -> None:
        logger.warning("order_ownership_rejected", symbol=symbol, reason=reason)
        raise OwnershipConflict(f"{reason}:{symbol}")

    @staticmethod
    def _store_error(symbol: str, operation: str, exc: Exception) -> OwnershipStoreUnavailable:
        logger.error("order_ownership_redis_failure", symbol=symbol, operation=operation, exc_info=True)
        return OwnershipStoreUnavailable(f"redis_{operation}_failed:{symbol}")
