"""Tests for HTTP exception normalization into gateway error contract."""

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from gateway.api.errors import gateway_http_exception_handler


def _build_test_app() -> FastAPI:
    app = FastAPI()
    app.add_exception_handler(HTTPException, gateway_http_exception_handler)

    @app.get("/api/v1/test/plain")
    async def plain_error():
        raise HTTPException(status_code=503, detail="Provider unavailable")

    @app.get("/api/v1/test/coded")
    async def coded_error():
        raise HTTPException(
            status_code=404,
            detail={
                "code": "GW-E4004",
                "message": "Not found",
                "provider": "finnhub",
            },
        )

    @app.get("/api/v1/test/list")
    async def list_error():
        raise HTTPException(status_code=422, detail=[{"loc": ["query", "symbol"], "msg": "bad value"}])

    return app


def test_http_exception_plain_string_becomes_gateway_error() -> None:
    client = TestClient(_build_test_app())

    response = client.get("/api/v1/test/plain")
    body = response.json()

    assert response.status_code == 503
    assert body["success"] is False
    assert body["error"]["code"] == "GW-E5003"
    assert body["error"]["message"] == "Provider unavailable"
    assert body["detail"] == "Provider unavailable"


def test_http_exception_keeps_existing_code_and_message() -> None:
    client = TestClient(_build_test_app())

    response = client.get("/api/v1/test/coded")
    body = response.json()

    assert response.status_code == 404
    assert body["success"] is False
    assert body["error"]["code"] == "GW-E4004"
    assert body["error"]["message"] == "Not found"
    assert body["error"]["details"]["provider"] == "finnhub"


def test_http_exception_list_detail_maps_to_validation_shape() -> None:
    client = TestClient(_build_test_app())

    response = client.get("/api/v1/test/list")
    body = response.json()

    assert response.status_code == 422
    assert body["success"] is False
    assert body["error"]["code"] == "GW-E8001"
    assert body["error"]["message"] == "Validation error"
    assert body["error"]["details"]["issues"][0]["loc"] == ["query", "symbol"]
