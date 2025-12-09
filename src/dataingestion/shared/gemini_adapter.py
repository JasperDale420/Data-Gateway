import subprocess
import logging
from types import SimpleNamespace
from typing import Any
import time
import random

LOGGER = logging.getLogger("gemini_client")


class GeminiAdapter:
    def __init__(self, default_model: str = "gemini-1.5-pro") -> None:
        self._default_model = default_model
        # Mimic OpenAI structure: client.chat.completions.create
        self.chat = SimpleNamespace(
            completions=SimpleNamespace(create=self._create_chat_completion)
        )

    def _create_chat_completion(self, *args: Any, **kwargs: Any) -> Any:
        if args:
            raise ValueError("Gemini adapter expects keyword arguments only.")

        messages = kwargs.get("messages", [])
        requested_model = kwargs.get("model")
        model = requested_model or self._default_model

        # Flatten chat history into a single prompt string
        prompt_parts = []
        for msg in messages:
            role = msg.get("role", "user").capitalize()
            content = msg.get("content", "")
            prompt_parts.append(f"{role}: {content}")

        full_prompt = "\n\n".join(prompt_parts)

        # Build command
        # gemini --model "model_name"
        # Prompt passed via stdin
        cmd = ["gemini"]
        if model:
            cmd.extend(["--model", model])
        # cmd.append(full_prompt) <--- Removed

        # Retry logic with exponential backoff
        max_retries = 5
        base_delay = 2

        for attempt in range(max_retries + 1):
            try:
                # Run the CLI
                LOGGER.info(
                    f"Calling Gemini CLI (Model: {model})... Prompt Size: {len(full_prompt)} chars (Attempt {attempt + 1}/{max_retries + 1})"
                )
                result = subprocess.run(
                    cmd,
                    input=full_prompt,  # Pass prompt here
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=120,  # 2 minute timeout
                    cwd="/tmp",  # Prevent scanning current directory
                )

                if result.returncode != 0:
                    # Check for usage limits in stderr/stdout
                    error_msg = (result.stderr or "") + (result.stdout or "")
                    if (
                        "429" in error_msg
                        or "quota" in error_msg.lower()
                        or "resource exhausted" in error_msg.lower()
                    ):
                        if attempt < max_retries:
                            delay = base_delay * (2**attempt) + random.uniform(0, 1)
                            LOGGER.warning(
                                f"Gemini Usage Limit Reached. Retrying in {delay:.2f}s..."
                            )
                            time.sleep(delay)
                            continue
                        else:
                            LOGGER.error(
                                "Gemini Usage Limit Reached. Max retries exceeded."
                            )
                            raise RuntimeError("Gemini Usage Limit Reached")

                    # Only fail if it truly crashed
                    raise RuntimeError(
                        f"Gemini CLI failed (code {result.returncode}): {result.stderr or result.stdout}"
                    )

                LOGGER.info("Gemini CLI returned.")
                output = result.stdout
                break  # Success

            except subprocess.TimeoutExpired:
                if attempt < max_retries:
                    LOGGER.warning("Gemini CLI timed out. Retrying...")
                    continue
                LOGGER.error("Gemini CLI timed out after 120s.")
                raise RuntimeError("Gemini CLI timed out.")
            except FileNotFoundError:
                raise RuntimeError(
                    "Gemini CLI not found. Ensure it is installed and in your PATH."
                )

        # Parse the response
        response_content = self._parse_output(output)

        # Return OpenAI-compatible object
        return SimpleNamespace(
            id="gemini-cli",
            created=0,
            model=model,
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        role="assistant", content=response_content, tool_calls=None
                    ),
                    finish_reason="stop",
                    index=0,
                )
            ],
            usage=SimpleNamespace(prompt_tokens=0, completion_tokens=0, total_tokens=0),
        )

    def _parse_output(self, output: str) -> str:
        """Clean up CLI output."""
        # Remove "Loaded cached credentials." if present
        lines = output.splitlines()
        cleaned_lines = [
            line for line in lines if "Loaded cached credentials" not in line
        ]
        return "\n".join(cleaned_lines).strip()
