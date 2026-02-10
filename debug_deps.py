from gateway.api.alpaca.common import require_api_key as alpaca_key
from gateway.api.deps import require_api_key
from gateway.api.market import router

print(f"Deps ID: {id(require_api_key)}")
print(f"Alpaca ID: {id(alpaca_key)}")

found = False
for route in router.routes:
    if route.path == "/api/v1/market/stocks/{symbol}/bars":
        print(f"Found route: {route.path}")
        for dep in route.dependencies:
            print(f"Dependency: {dep.dependency}")
            print(f"Dependency ID: {id(dep.dependency)}")
            if dep.dependency == require_api_key:
                print("MATCHES deps.require_api_key")
            if dep.dependency == alpaca_key:
                print("MATCHES alpaca.require_api_key")
        found = True
        break

if not found:
    print("Route not found!")
