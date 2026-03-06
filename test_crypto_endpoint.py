from fastapi.testclient import TestClient

from gateway.api.deps import require_api_key
from gateway.main import app


async def override_require_api_key():
    return {"id": "test", "name": "test"}


app.dependency_overrides[require_api_key] = override_require_api_key


def test_crypto_bars_invalid_pair():
    with TestClient(app) as client:
        response = client.get("/crypto/INVALID_PAIR/bars", headers={"x-api-key": "test_key"})
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")


if __name__ == "__main__":
    test_crypto_bars_invalid_pair()
