import sys
import os

# Add the any-llm directory to the path so we can import it
sys.path.append(os.path.abspath("any-llm/src"))

try:
    import any_llm
    print("SUCCESS: `any_llm` imported successfully.")
    print(f"Location: {any_llm.__file__}")
except ImportError as e:
    print(f"FAILURE: Could not import `any_llm`. Error: {e}")
    sys.exit(1)

# Verify specific providers
print("\n--- Verifying Providers ---")
try:
    from any_llm import AnyLLM
    print("SUCCESS: `AnyLLM` class imported.")
    
    # Check if we can load the provider classes (not calling API, just loading definitions)
    # Note: We won't actually make calls since we might not have keys
    print("Checking providers...")
    try:
       # DeepSeek (uses OpenAI client usually, or HTTP)
       # We just check if valid provider string
        deepseek = AnyLLM.create("deepseek", api_key="dummy")
        print("SUCCESS: DeepSeek provider initialized (dummy key).")
    except Exception as e:
        print(f"WARNING: DeepSeek provider init failed: {e}")

    try:
        # Ollama
        ollama = AnyLLM.create("ollama", api_key="dummy") 
        print("SUCCESS: Ollama provider initialized.")
    except Exception as e:
         # It might fail if it tries to connect immediately, but usually it's lazy or just checks import
        print(f"WARNING: Ollama provider init failed: {e}")

    try:
        # OpenRouter
        openrouter = AnyLLM.create("openrouter", api_key="dummy")
        print("SUCCESS: OpenRouter provider initialized (dummy key).")
    except Exception as e:
        print(f"WARNING: OpenRouter provider init failed: {e}")

except Exception as e:
    print(f"FAILURE: Could not import or use AnyLLM: {e}")
