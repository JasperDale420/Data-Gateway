import sys
# Hack to avoid importing local 'unusualwhales' stub from Kairos if present in sys.path
paths_to_remove = [p for p in sys.path if "Kairos" in p]
for p in paths_to_remove:
    sys.path.remove(p)

from unusualwhales.api.endpoints import Endpoints

print("Endpoints attributes:")
print(dir(Endpoints))

print("\nEndpoints.market() attributes:")
try:
    print(dir(Endpoints.market()))
except Exception as e:
    print(e)

print("\nEndpoints.congress() attributes (if exists):")
try:
    print(dir(Endpoints.congress()))
except Exception as e:
    print(e)

# Check for other potential endpoint groups
potential_groups = ['insider', 'news', 'shorts', 'seasonality', 'etfs', 'stock', 'option']
for group in potential_groups:
    print(f"\nEndpoints.{group}() attributes (if exists):")
    try:
        if hasattr(Endpoints, group):
            print(dir(getattr(Endpoints, group)()))
        else:
            print(f"Endpoints has no attribute '{group}'")
    except Exception as e:
        print(e)
