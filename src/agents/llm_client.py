import json
import os
from typing import TypeVar

from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)


class LLMClient:
    """Optional Groq client; all returned JSON is validated locally."""

    def __init__(self, model: str = "openai/gpt-oss-20b") -> None:
        try:
            from groq import Groq
        except ImportError as exc:
            raise RuntimeError("Install groq to use the LLM client") from exc
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError("GROQ_API_KEY is not set")
        self.client = Groq(api_key=api_key)
        self.model = model

    def structured(self, system_prompt: str, user_prompt: str, output_model: type[ModelT]) -> ModelT:
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = response.choices[0].message.content
        if not content:
            raise ValueError("LLM returned an empty response")
        return output_model.model_validate(json.loads(content))
