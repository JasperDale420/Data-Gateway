"""GlobalRateLimitMiddleware exposes a process singleton so admin blocklist
adds are actually ENFORCED (previously they wrote to a dead dict)."""

from gateway.api.middleware.global_ratelimit import (
    GlobalRateLimitMiddleware,
    get_global_ratelimiter,
)


def test_singleton_registration_and_enforced_block():
    mw = GlobalRateLimitMiddleware(app=lambda scope, receive, send: None)
    assert get_global_ratelimiter() is mw  # middleware registers itself

    mw.block_ip("203.0.113.7", duration_seconds=3600)
    assert "203.0.113.7" in mw.get_blocked_ips()  # actually enforced

    mw.unblock_ip("203.0.113.7")
    assert "203.0.113.7" not in mw.get_blocked_ips()
