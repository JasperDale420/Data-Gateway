"""Symbology API endpoints.

Symbol resolution and format conversion as specified in PRD (lines 459-523).
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from gateway.core.symbology import get_symbol_resolver
from gateway.schemas import SuccessResponse

router = APIRouter(prefix="/symbology", tags=["symbology"])


# ─────────────────────────────────────────────────────────────────────────────
# Request/Response Models
# ─────────────────────────────────────────────────────────────────────────────


class SymbolResolveResponse(BaseModel):
    """Response for symbol resolution."""

    success: bool
    data: dict


class SymbolValidateResponse(BaseModel):
    """Response for symbol validation."""

    valid: bool
    symbol: str
    error: str | None = None


class BatchResolveRequest(BaseModel):
    """Request for batch symbol resolution."""

    symbols: list[str]


class BatchResolveResponse(BaseModel):
    """Response for batch symbol resolution."""

    success: bool
    results: list[dict]
    errors: list[dict]


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/resolve", response_model=SymbolResolveResponse)
async def resolve_symbol(
    symbol: str = Query(..., description="Symbol in any supported format"),
) -> SymbolResolveResponse:
    """Resolve a symbol to its normalized components.

    Supports:
    - Stock symbols (AAPL)
    - OCC option format (AAPL250117C00200000)
    - Human-readable options (AAPL 2025-01-17 $200 C)
    - Crypto pairs (BTC/USD)
    - Forex pairs (EUR/USD)

    Returns normalized data and provider-specific formats.
    """
    resolver = get_symbol_resolver()
    result = resolver.resolve(symbol)

    if result.type == "unknown":
        raise HTTPException(
            status_code=400,
            detail={"code": "GW-E8001", "message": f"Unknown symbol format: {symbol}"},
        )

    return SymbolResolveResponse(
        success=True,
        data={
            "input": result.input,
            "normalized": {
                "symbol": result.symbol,
                "type": result.type,
                "underlying": result.underlying,
                "expiration": result.expiration.isoformat() if result.expiration else None,
                "strike": float(result.strike) if result.strike is not None else None,
                "option_type": result.option_type,
            },
            "provider_formats": result.provider_formats,
        },
    )


@router.get("/validate", response_model=SymbolValidateResponse)
async def validate_symbol(
    symbol: str = Query(..., description="Symbol to validate"),
) -> SymbolValidateResponse:
    """Validate a symbol format.

    Returns whether the symbol is valid and any error message.
    """
    resolver = get_symbol_resolver()
    is_valid, error = resolver.validate(symbol)

    return SymbolValidateResponse(
        valid=is_valid,
        symbol=symbol,
        error=error,
    )


@router.post("/batch", response_model=BatchResolveResponse)
async def batch_resolve_symbols(
    request: BatchResolveRequest,
) -> BatchResolveResponse:
    """Resolve multiple symbols at once.

    Returns results for valid symbols and errors for invalid ones.
    """
    resolver = get_symbol_resolver()
    results = []
    errors = []

    for symbol in request.symbols:
        try:
            result = resolver.resolve(symbol)
            if result.type == "unknown":
                errors.append(
                    {
                        "symbol": symbol,
                        "error": f"Unknown symbol format: {symbol}",
                    }
                )
            else:
                results.append(result.to_dict())
        except Exception as e:
            errors.append(
                {
                    "symbol": symbol,
                    "error": str(e),
                }
            )

    return BatchResolveResponse(
        success=len(errors) == 0,
        results=results,
        errors=errors,
    )


@router.get("/convert", response_model=SuccessResponse)
async def convert_symbol(
    symbol: str = Query(..., description="Symbol to convert"),
    provider: str = Query(..., description="Target provider (alpaca, uw, yfinance)"),
) -> dict:
    """Convert a symbol to provider-specific format.

    Useful for getting the correct symbol format for API calls.
    """
    resolver = get_symbol_resolver()
    result = resolver.resolve(symbol)

    provider_symbol = result.provider_formats.get(provider)
    if not provider_symbol:
        provider_symbol = result.symbol

    return {
        "input": symbol,
        "provider": provider,
        "output": provider_symbol,
    }
