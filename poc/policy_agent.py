from __future__ import annotations

import json
import logging
import os
from collections.abc import Awaitable, Callable
from typing import Any

import uvicorn
from a2a.helpers import (
    get_message_text,
    new_task_from_user_message,
    new_text_message,
    new_text_part,
)
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
    TaskState,
)
from openai import OpenAI
from pydantic import BaseModel, Field
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from poc.db import write_trace
from poc.llm import OPENAI_MODEL, llm_stub_enabled
from poc.mcp_client import call_tool


logger = logging.getLogger(__name__)
AGENT_NAME = "policy-agent"
DEFAULT_QUESTION = "Is the customer eligible for an automatic refund?"
PolicyLoader = Callable[[str], Awaitable[dict[str, Any]]]


class PolicyAssessment(BaseModel):
    eligible: bool
    action: str
    reason: str
    confidence: float = Field(ge=0, le=1)

    def as_payload(self) -> dict[str, Any]:
        confidence = round(float(self.confidence), 2)
        return {
            "eligible": self.eligible,
            "action": self.action,
            "reason": self.reason,
            "confidence": confidence,
        }


STUB_ASSESSMENTS: dict[tuple[str, str], PolicyAssessment] = {
    ("CUST-1001", "DUPLICATE_CHARGE"): PolicyAssessment(
        eligible=True,
        action="REFUND",
        reason="Duplicate payment qualifies for automatic refund.",
        confidence=0.94,
    ),
    ("CUST-1002", "UNRECOGNIZED_TRANSACTION"): PolicyAssessment(
        eligible=False,
        action="HUMAN_REVIEW",
        reason="Unrecognized transactions require human authorization and must not be automatically refunded.",
        confidence=0.91,
    ),
    ("CUST-1003", "DUPLICATE_CHARGE"): PolicyAssessment(
        eligible=True,
        action="REFUND",
        reason="Duplicate payment qualifies for automatic refund.",
        confidence=0.94,
    ),
}


def policy_agent_url() -> str:
    return os.getenv("POLICY_AGENT_URL", "http://localhost:8001").rstrip("/")


def build_agent_card(url: str | None = None) -> AgentCard:
    public_url = url or policy_agent_url()
    card = AgentCard(
        name="Policy Agent",
        description=(
            "Assesses automatic refund eligibility from complaint context "
            "and MCP policy text."
        ),
        version="0.1.0",
        default_input_modes=["application/json", "text/plain"],
        default_output_modes=["application/json"],
        capabilities=AgentCapabilities(streaming=False, extended_agent_card=False),
    )
    interface = card.supported_interfaces.add()
    interface.url = public_url
    interface.protocol_binding = "JSONRPC"
    interface.protocol_version = "1.0"
    skill = card.skills.add()
    skill.id = "ASSESS_POLICY"
    skill.name = "Assess Policy"
    skill.description = (
        "Return eligible, action, reason, and confidence for automatic refund."
    )
    skill.tags.extend(["policy", "refund", "eligibility"])
    skill.examples.append(
        json.dumps(
            {
                "task": "ASSESS_POLICY",
                "context": {
                    "customerId": "CUST-1001",
                    "complaintType": "DUPLICATE_CHARGE",
                    "amount": 5000,
                },
                "question": DEFAULT_QUESTION,
            }
        )
    )
    skill.input_modes.append("application/json")
    skill.output_modes.append("application/json")
    return card


def parse_assess_request(raw: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("ASSESS_POLICY request must be JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("ASSESS_POLICY request must be a JSON object")
    task = str(payload.get("task") or "ASSESS_POLICY")
    if task != "ASSESS_POLICY":
        raise ValueError(f"Unsupported A2A task {task}")
    context = payload.get("context") or {}
    if not isinstance(context, dict):
        raise ValueError("context must be an object")
    return {
        "task": task,
        "context": context,
        "question": str(payload.get("question") or DEFAULT_QUESTION),
    }


def stub_assessment(
    customer_id: str, complaint_type: str
) -> PolicyAssessment | None:
    direct = STUB_ASSESSMENTS.get((customer_id, complaint_type))
    if direct is not None:
        return direct
    for (stub_customer, stub_type), assessment in STUB_ASSESSMENTS.items():
        if customer_id == stub_customer or complaint_type == stub_type:
            return assessment
    return None


def assess_with_openai(
    *,
    customer_id: str,
    complaint_type: str,
    amount: float | int | None,
    question: str,
    policy: dict[str, Any],
    client: OpenAI | None = None,
) -> PolicyAssessment:
    openai_client = client or OpenAI()
    response = openai_client.chat.completions.parse(
        model=OPENAI_MODEL,
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": (
                    "Assess automatic refund eligibility using the supplied policy. "
                    "Return eligible, action (REFUND or HUMAN_REVIEW), reason, "
                    "and confidence between 0 and 1. Duplicate charges at or below "
                    "the policy auto-refund limit that are not already refunded "
                    "should be eligible with action REFUND."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "customerId": customer_id,
                        "complaintType": complaint_type,
                        "amount": amount,
                        "question": question,
                        "policy": policy,
                    }
                ),
            },
        ],
        response_format=PolicyAssessment,
    )
    parsed = response.choices[0].message.parsed
    if parsed is None:
        raise RuntimeError("OpenAI returned an empty policy assessment")
    return parsed


async def load_policy(
    policy_type: str, loader: PolicyLoader | None = None
) -> dict[str, Any]:
    if loader is not None:
        return await loader(policy_type)
    result = await call_tool("get_policy", {"policy_type": policy_type})
    policy = result.get("policy", result)
    if not isinstance(policy, dict):
        raise RuntimeError(f"Unexpected get_policy result: {result}")
    return policy


async def assess_policy(
    customer_id: str,
    complaint_type: str,
    amount: float | int | None,
    question: str = DEFAULT_QUESTION,
    *,
    stub: bool | None = None,
    client: OpenAI | None = None,
    policy_loader: PolicyLoader | None = None,
) -> PolicyAssessment:
    policy = await load_policy(complaint_type, policy_loader)
    use_stub = llm_stub_enabled() if stub is None else stub
    if use_stub:
        fixture = stub_assessment(customer_id, complaint_type)
        if fixture is None:
            raise RuntimeError(
                "LLM_STUB is enabled but no policy fixture matches this complaint"
            )
        logger.info("Assessed %s/%s from stub after MCP get_policy", customer_id, complaint_type)
        return fixture
    return assess_with_openai(
        customer_id=customer_id,
        complaint_type=complaint_type,
        amount=amount,
        question=question,
        policy=policy,
        client=client,
    )


def record_assessment(
    context: dict[str, Any], assessment: dict[str, Any]
) -> None:
    complaint_id = str(
        context.get("complaintId") or context.get("correlationId") or ""
    )
    if not complaint_id:
        return
    try:
        write_trace(
            complaint_id,
            AGENT_NAME,
            "ASSESS_POLICY",
            detail=assessment,
        )
    except Exception:
        logger.exception("Failed to write policy assessment trace")


class PolicyAgentExecutor(AgentExecutor):
    def __init__(
        self,
        *,
        stub: bool | None = None,
        client: OpenAI | None = None,
        policy_loader: PolicyLoader | None = None,
    ) -> None:
        self.stub = stub
        self.client = client
        self.policy_loader = policy_loader

    async def execute(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        if context.current_task:
            task = context.current_task
        else:
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)

        updater = TaskUpdater(
            event_queue=event_queue, task_id=task.id, context_id=task.context_id
        )
        await updater.start_work(new_text_message("Assessing policy..."))
        try:
            request = parse_assess_request(get_message_text(context.message))
            ctx = request["context"]
            customer_id = str(ctx.get("customerId") or "")
            complaint_type = str(ctx.get("complaintType") or "")
            amount = ctx.get("amount")
            assessment = await assess_policy(
                customer_id,
                complaint_type,
                amount,
                request["question"],
                stub=self.stub,
                client=self.client,
                policy_loader=self.policy_loader,
            )
            payload = assessment.as_payload()
            record_assessment(ctx, payload)
            await updater.add_artifact(
                parts=[
                    new_text_part(
                        json.dumps(payload), media_type="application/json"
                    )
                ],
                name="policy-assessment",
            )
            await updater.complete()
        except Exception as exc:
            logger.exception("Policy assessment failed")
            await updater.failed(new_text_message(str(exc)))

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        task = context.current_task
        if task is None:
            return
        updater = TaskUpdater(
            event_queue=event_queue, task_id=task.id, context_id=task.context_id
        )
        await updater.cancel()


async def health(_request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


def create_app(
    *,
    stub: bool | None = None,
    client: OpenAI | None = None,
    policy_loader: PolicyLoader | None = None,
    card_url: str | None = None,
) -> Starlette:
    card = build_agent_card(card_url)
    handler = DefaultRequestHandler(
        agent_executor=PolicyAgentExecutor(
            stub=stub, client=client, policy_loader=policy_loader
        ),
        task_store=InMemoryTaskStore(),
        agent_card=card,
    )
    routes = [
        Route("/health", health, methods=["GET"]),
        *create_agent_card_routes(card),
        *create_jsonrpc_routes(handler, "/"),
    ]
    return Starlette(routes=routes)


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    logger.info("Starting Policy Agent A2A server on 0.0.0.0:8001")
    uvicorn.run(create_app(), host="0.0.0.0", port=8001)


if __name__ == "__main__":
    main()
