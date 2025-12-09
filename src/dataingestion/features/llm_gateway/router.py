from fastapi import APIRouter, HTTPException
from dataingestion.features.llm_gateway.schemas import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    ChatCompletionChoice,
    ChatCompletionUsage,
    ChatMessage,
)
from dataingestion.shared.codex_adapter import CodexAdapter
import time
import asyncio

from dataingestion.shared.gemini_adapter import GeminiAdapter

router = APIRouter(prefix="/v1", tags=["LLM Gateway"])

# Initialize adapters
codex_adapter = CodexAdapter()
gemini_adapter = GeminiAdapter()


@router.post("/chat/completions", response_model=ChatCompletionResponse)
async def chat_completions(request: ChatCompletionRequest):
    """
    OpenAI-compatible chat completion endpoint.
    Delegates to local Codex CLI or Gemini CLI based on model.
    """
    try:
        # Run adapter in threadpool since it uses subprocess (blocking)
        loop = asyncio.get_running_loop()

        # Convert Pydantic models to dicts for the adapter
        messages_dicts = [m.dict() for m in request.messages]

        # Select adapter
        if request.model.startswith("gemini"):
            adapter = gemini_adapter
        else:
            adapter = codex_adapter

        # We wrap the call in a lambda to pass arguments
        result = await loop.run_in_executor(
            None,
            lambda: adapter.chat.completions.create(
                model=request.model, messages=messages_dicts
            ),
        )

        # The adapter returns a SimpleNamespace structure mimicking OpenAI
        # We need to convert it back to our Pydantic response model

        # Extract content from the first choice
        content = result.choices[0].message.content

        return ChatCompletionResponse(
            id=result.id,
            created=int(time.time()),
            model=result.model,
            choices=[
                ChatCompletionChoice(
                    index=0,
                    message=ChatMessage(role="assistant", content=content),
                    finish_reason="stop",
                )
            ],
            usage=ChatCompletionUsage(
                prompt_tokens=0, completion_tokens=0, total_tokens=0
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
