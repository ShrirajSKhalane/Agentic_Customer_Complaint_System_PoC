from __future__ import annotations

import os


OPENAI_MODEL = "gpt-4o-mini"


def llm_stub_enabled() -> bool:
    # Unset or 0/false/off: live OpenAI. Only 1/true/yes/on enables fixtures.
    return os.getenv("LLM_STUB", "0").strip().lower() in {"1", "true", "yes", "on"}
