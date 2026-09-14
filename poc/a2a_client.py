from __future__ import annotations

import json
import os
from typing import Any

import httpx
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.helpers import new_text_message
from a2a.types import Role, SendMessageRequest

from poc.policy_agent import DEFAULT_QUESTION, policy_agent_url


def _json_from_parts(parts: Any) -> dict[str, Any] | None:
    for part in parts:
        text = getattr(part, "text", None)
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "eligible" in payload:
            return payload
    return None


def assessment_from_stream(chunks: list[Any]) -> dict[str, Any]:
    found: dict[str, Any] | None = None
    for chunk in chunks:
        name = chunk.WhichOneof("payload")
        if name == "task":
            for artifact in chunk.task.artifacts:
                found = _json_from_parts(artifact.parts) or found
        elif name == "artifact_update":
            found = _json_from_parts(chunk.artifact_update.artifact.parts) or found
        elif name == "message":
            found = _json_from_parts(chunk.message.parts) or found
    if found is None:
        raise RuntimeError("Policy Agent returned no structured assessment")
    return found


def _rewrite_card_url(card: Any, base_url: str) -> None:
    for interface in card.supported_interfaces:
        interface.url = base_url


async def assess_policy(
    *,
    customer_id: str,
    complaint_type: str,
    amount: float | int | None,
    question: str = DEFAULT_QUESTION,
    complaint_id: str | None = None,
    base_url: str | None = None,
) -> dict[str, Any]:
    url = (base_url or policy_agent_url()).rstrip("/")
    context: dict[str, Any] = {
        "customerId": customer_id,
        "complaintType": complaint_type,
        "amount": amount,
    }
    if complaint_id:
        context["complaintId"] = complaint_id
    payload = {
        "task": "ASSESS_POLICY",
        "context": context,
        "question": question,
    }
    async with httpx.AsyncClient(timeout=30.0) as httpx_client:
        resolver = A2ACardResolver(httpx_client=httpx_client, base_url=url)
        card = await resolver.get_agent_card()
        _rewrite_card_url(card, url)
        client = await create_client(
            agent=card,
            client_config=ClientConfig(streaming=False, httpx_client=httpx_client),
        )
        try:
            request = SendMessageRequest(
                message=new_text_message(
                    json.dumps(payload),
                    role=Role.ROLE_USER,
                    media_type="application/json",
                )
            )
            chunks = [chunk async for chunk in client.send_message(request)]
        finally:
            await client.close()
    return assessment_from_stream(chunks)


def policy_agent_base_url() -> str:
    return os.getenv("POLICY_AGENT_URL", "http://localhost:8001")
