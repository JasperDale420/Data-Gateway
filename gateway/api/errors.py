"""Shared API error normalization.

Converts FastAPI HTTPException payloads into a stable gateway error envelope.
"""

from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

_STATUS_DEFAULT_CODES: dict[int, str] = {
    400: "GW-E4000",
    401: "GW-E2001",
    403: "GW-E2003",
    404: "GW-E4004",
    409: "GW-E4009",
    422: "GW-E8001",
    429: "GW-E4001",
    500: "GW-E5000",
    502: "GW-E5002",
    503: "GW-E5003",
    504: "GW-E5004",
}


def _default_error_code(status_code: int) -> str:
    return _STATUS_DEFAULT_CODES.get(status_code, f"GW-E{status_code}")


def normalize_http_exception(
    status_code: int,
    detail: Any,
) -> tuple[dict[str, Any], Any]:
    """Normalize exception detail into gateway error shape.

    Returns:
        Tuple of (error_obj, legacy_detail)
    """
    error: dict[str, Any] = {
        "code": _default_error_code(status_code),
        "message": "Request failed",
    }

    legacy_detail: Any = detail

    if isinstance(detail, str):
        error["message"] = detail
        return error, legacy_detail

    if isinstance(detail, dict):
        if isinstance(detail.get("code"), str) and detail["code"]:
            error["code"] = detail["code"]
        if isinstance(detail.get("message"), str) and detail["message"]:
            error["message"] = detail["message"]
        elif isinstance(detail.get("detail"), str) and detail["detail"]:
            error["message"] = detail["detail"]
        else:
            error["message"] = "Request failed"

        extra = {k: v for k, v in detail.items() if k not in {"code", "message"}}
        if extra:
            error["details"] = extra
        return error, legacy_detail

    if isinstance(detail, list):
        error["message"] = "Validation error" if status_code == 422 else "Request failed"
        error["details"] = {"issues": detail}
        return error, legacy_detail

    if detail is not None:
        error["message"] = str(detail)
    return error, legacy_detail


async def gateway_http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Render HTTPExceptions with the gateway's stable error contract."""
    if isinstance(exc, HTTPException):
        status_code = exc.status_code
        detail = exc.detail
        headers = exc.headers
    else:
        status_code = 500
        detail = "Internal server error"
        headers = None

    error, legacy_detail = normalize_http_exception(status_code, detail)
    payload: dict[str, Any] = {
        "success": False,
        "error": error,
        # Backward compatibility for callers that still read `detail`.
        "detail": legacy_detail if legacy_detail is not None else error["message"],
    }

    return JSONResponse(
        status_code=status_code,
        content=payload,
        headers=headers,
    )
