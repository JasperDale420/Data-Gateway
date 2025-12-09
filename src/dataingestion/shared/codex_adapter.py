import subprocess
import logging
from types import SimpleNamespace
from typing import Any

LOGGER = logging.getLogger("codex_client")


class CodexAdapter:
    def __init__(self, default_model: str = "gpt-5.1") -> None:
        self._default_model = default_model
        # Mimic OpenAI structure: client.chat.completions.create
        self.chat = SimpleNamespace(
            completions=SimpleNamespace(create=self._create_chat_completion)
        )

    def _create_chat_completion(self, *args: Any, **kwargs: Any) -> Any:
        if args:
            raise ValueError("Codex adapter expects keyword arguments only.")

        messages = kwargs.get("messages", [])
        requested_model = kwargs.get("model")

        # Map unsupported models to gpt-5.1
        if requested_model in ("gpt-4o", "gpt-4o-mini", "gpt-3.5-turbo"):
            model = "gpt-5.1"
        else:
            model = requested_model or self._default_model

        # Flatten chat history into a single prompt string
        prompt_parts = []
        for msg in messages:
            role = msg.get("role", "user").capitalize()
            content = msg.get("content", "")
            prompt_parts.append(f"{role}: {content}")

        full_prompt = "\n\n".join(prompt_parts)

        # Build command - NO config overrides, use local defaults
        cmd = ["codex", "exec", "--skip-git-repo-check", "-m", model, full_prompt]

        try:
            # Run the CLI
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=120,  # 2 minute timeout
            )
        except subprocess.TimeoutExpired:
            LOGGER.error("Codex CLI timed out after 120s.")
            raise RuntimeError("Codex CLI timed out.")
        except FileNotFoundError:
            raise RuntimeError(
                "Codex CLI not found. Ensure it is installed and in your PATH."
            )

        output = result.stdout

        # Log MCP errors as warnings, don't crash
        if "ERROR:" in output:
            if "usage limit" in output.lower():
                LOGGER.error("Codex Usage Limit Reached. Stopping execution.")
                raise RuntimeError("Codex Usage Limit Reached")
            LOGGER.warning(f"Codex CLI reported errors (proceeding): {output}")

        if result.returncode != 0:
            # Only fail if it truly crashed
            raise RuntimeError(
                f"Codex CLI failed (code {result.returncode}): {result.stderr or output}"
            )

        # Parse the response
        response_content = self._parse_output(output)

        # Return OpenAI-compatible object
        return SimpleNamespace(
            id="codex-cli",
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
        """Extract response text between '] codex' and '] tokens used:' markers."""
        response_content = output

        codex_marker = "] codex"
        start_idx = output.rfind(codex_marker)
        if start_idx != -1:
            response_content = output[start_idx + len(codex_marker) :]

        tokens_marker = "] tokens used:"
        end_idx = response_content.rfind(tokens_marker)
        if end_idx != -1:
            # Strip the timestamp line before the token usage
            last_newline = response_content.rfind("\n", 0, end_idx)
            if last_newline != -1:
                response_content = response_content[:last_newline]
            else:
                response_content = response_content[:end_idx]

        return response_content.strip()
