from dataingestion.shared.codex_adapter import CodexAdapter
import logging

# Configure logging to see warnings
logging.basicConfig(level=logging.INFO)

def main():
    print("Initializing Codex Adapter...")
    client = CodexAdapter()
    
    messages = [
        {"role": "user", "content": "Hello, what is 2 + 2?"}
    ]
    
    print(f"Sending request: {messages}")
    try:
        response = client.chat.completions.create(
            model="gpt-4o", # Should map to gpt-5.1
            messages=messages
        )
        
        print("\n--- Response ---")
        print(f"Model used: {response.model}")
        print(f"Content: {response.choices[0].message.content}")
        print("----------------")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
