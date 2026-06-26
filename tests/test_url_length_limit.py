"""InputValidationMiddleware must reject over-long request URLs with 414."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from gateway.api.middleware.validation import InputValidationMiddleware
from gateway.core.security import PARAM_LIMITS


def _client() -> TestClient:
    app = FastAPI()
    app.add_middleware(InputValidationMiddleware)

    @app.get("/api/v1/echo")
    def _echo():
        return {"ok": True}

    return TestClient(app)


def test_oversized_url_rejected_with_414():
    client = _client()
    big = "x" * (PARAM_LIMITS["url_max_bytes"] + 100)
    resp = client.get(f"/api/v1/echo?q={big}")
    assert resp.status_code == 414


def test_normal_url_passes():
    client = _client()
    assert client.get("/api/v1/echo?q=AAPL").status_code == 200
