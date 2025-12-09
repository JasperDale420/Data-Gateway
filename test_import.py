import sys
import os

# Remove Kairos from sys.path if present
paths_to_remove = [p for p in sys.path if "Kairos" in p]
for p in paths_to_remove:
    sys.path.remove(p)

try:
    import unusualwhales
    from unusualwhales.client import UnusualWhalesClient
    # Check if it's the real client (should have more than just stub methods)
    print(f"UnusualWhalesClient dir: {dir(UnusualWhalesClient)}")
    print(f"Package file: {unusualwhales.__file__}")
except ImportError as e:
    print(f"ImportError: {e}")
except Exception as e:
    print(f"Error: {e}")
