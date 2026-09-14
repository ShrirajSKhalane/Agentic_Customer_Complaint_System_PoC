from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from collections.abc import Callable
from typing import Any, TypedDict
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, Interrupt, interrupt
from openai import OpenAI
from pydantic import BaseModel, Field
from psycopg_pool import ConnectionPool

from poc.a2a_client import assess_policy as a2a_assess_policy
from poc.db import DATABASE_URL, claim_event, connection, fetch_one, write_trace
from poc.events import COMPLAINTS_TOPIC, EventEnvelope, INVESTIGATIONS_TOPIC
from poc.kafka import publish, run_consumer
from poc.llm import OPENAI_MODEL, llm_stub_enabled
from poc.mcp_client import call_tool


logger = logging.getLogger(__name__)
CONSUMER_NAME = "investigation-agent"
AGENT_NAME = CONSUMER_NAME
HTTP_PORT = int(os.getenv("INVESTIGATION_AGENT_PORT", "8002"))
LOOKUP_TOOLS = ("get_transactions", "get_customer")
Publisher = Callable[[EventEnvelope], None]
ToolCaller = Callable[[str, dict[str, Any]], dict[str, Any]]
PolicyCaller = Callable[..., dict[str, Any]]
Planner = Callable[..., "InvestigationPlan"]


class InvestigationState(TypedDict, total=False):
    complaint_id: str
    customer_id: str
    category: str
    complaint_type: str
    amount: float | int | None
    reviewer_note: str
    graph_thread_id: str
    fact_answer: str
    transactions: list[dict[str, Any]]
    refund_lookups: list[dict[str, Any]]
    policy_assessment: dict[str, Any]
    finding: str
    refund_status: str
    policy_eligible: bool | None
    confidence: float
    transaction_id: str | None
    fraud_flag: bool
    started: bool
    completed: bool
    mcp_calls: list[str]
    planned_tools: list[str]
    ask_which_charge: bool | None
    plan_reason: str


class FactAnswer(BaseModel):
    answer: str = Field(min_length=1)


class InvestigationPlan(BaseModel):
    tools: list[str] = Field(default_factory=list)
    ask_which_charge: bool | None = Field(default=None, alias="askWhichCharge")
    reason: str = ""

    model_config = {"populate_by_name": True}


STUB_PLANS: dict[str, InvestigationPlan] = {
    "CUST-1001": InvestigationPlan(
        tools=["get_transactions", "get_customer"],
        ask_which_charge=False,
        reason="The complaint names one duplicate amount; fetch the ledger and do not ask which charge.",
    ),
    "CUST-1002": InvestigationPlan(
        tools=["get_transactions", "get_customer"],
        ask_which_charge=False,
        reason="The complaint names one unrecognized amount; fetch the ledger and do not ask which charge.",
    ),
    "CUST-1003": InvestigationPlan(
        tools=["get_transactions", "get_customer"],
        ask_which_charge=True,
        reason="The complaint does not name an amount and more than one charge may match; fetch the ledger and ask which charge.",
    ),
}


def normalize_lookup_tools(tools: list[str] | None) -> list[str]:
    chosen: list[str] = []
    for name in tools or []:
        if name in LOOKUP_TOOLS and name not in chosen:
            chosen.append(name)
    if "get_transactions" not in chosen:
        chosen.insert(0, "get_transactions")
    return chosen


def stub_investigation_plan(customer_id: str) -> InvestigationPlan:
    fixture = STUB_PLANS.get(customer_id)
    if fixture is not None:
        return fixture
    return InvestigationPlan(
        tools=["get_transactions", "get_customer"],
        ask_which_charge=None,
        reason="No seeded plan; the ledger decides whether to ask which charge.",
    )


def plan_with_openai(
    *,
    customer_id: str,
    message: str,
    category: str = "",
    complaint_type: str = "",
    amount: float | int | None = None,
    reviewer_note: str = "",
    client: OpenAI | None = None,
) -> InvestigationPlan:
    openai_client = client or OpenAI()
    response = openai_client.chat.completions.parse(
        model=OPENAI_MODEL,
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": (
                    "Plan the first lookups for a banking complaint investigation. "
                    "Return tools drawn only from get_transactions and get_customer. "
                    "Always include get_transactions. Set askWhichCharge true when the "
                    "wording does not name one amount, merchant, or transaction. Do not "
                    "set it false to skip a human choice; duplicate matching stays in "
                    "code. Do not choose get_refund_status or initiate_refund."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "customerId": customer_id,
                        "message": message,
                        "category": category,
                        "complaintType": complaint_type,
                        "amount": amount,
                        "reviewerNote": reviewer_note or None,
                    }
                ),
            },
        ],
        response_format=InvestigationPlan,
    )
    parsed = response.choices[0].message.parsed
    if parsed is None:
        raise RuntimeError("OpenAI returned an empty investigation plan")
    return InvestigationPlan(
        tools=normalize_lookup_tools(parsed.tools),
        ask_which_charge=parsed.ask_which_charge,
        reason=parsed.reason,
    )


def plan_investigation(
    *,
    customer_id: str,
    message: str = "",
    category: str = "",
    complaint_type: str = "",
    amount: float | int | None = None,
    reviewer_note: str = "",
    stub: bool | None = None,
    client: OpenAI | None = None,
) -> InvestigationPlan:
    use_stub = llm_stub_enabled() if stub is None else stub
    if use_stub:
        return stub_investigation_plan(customer_id)
    if client is None and not os.getenv("OPENAI_API_KEY", "").strip():
        raise RuntimeError("OPENAI_API_KEY is required when LLM_STUB is disabled")
    return plan_with_openai(
        customer_id=customer_id,
        message=message,
        category=category,
        complaint_type=complaint_type,
        amount=amount,
        reviewer_note=reviewer_note,
        client=client,
    )


def sync_call_tool(name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
    return asyncio.run(call_tool(name, arguments or {}))


def sync_assess_policy(**kwargs: Any) -> dict[str, Any]:
    return asyncio.run(a2a_assess_policy(**kwargs))


def thread_config(complaint_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": complaint_id}}


def txn_date(transaction: dict[str, Any]) -> str:
    raw = str(transaction.get("transactionTime") or transaction.get("transaction_time") or "")
    return raw[:10]


def txn_id(transaction: dict[str, Any]) -> str:
    return str(transaction.get("transactionId") or transaction.get("transaction_id") or "")


def txn_amount(transaction: dict[str, Any]) -> float:
    return float(transaction.get("amount") or 0)


def duplicate_groups(
    transactions: list[dict[str, Any]], amount: float | int | None = None
) -> list[list[dict[str, Any]]]:
    buckets: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for item in transactions:
        key = (txn_amount(item), item.get("merchant"), txn_date(item))
        buckets.setdefault(key, []).append(item)
    groups = [group for group in buckets.values() if len(group) >= 2]
    if amount is None:
        return groups
    matching = [group for group in groups if txn_amount(group[0]) == float(amount)]
    return matching or groups


def fact_candidates(state: InvestigationState) -> list[dict[str, Any]]:
    transactions = list(state.get("transactions") or [])
    groups = duplicate_groups(transactions, state.get("amount"))
    shortlist = [group[-1] for group in groups] if groups else transactions
    return [
        {
            "transactionId": txn_id(item),
            "amount": txn_amount(item),
            "merchant": str(item.get("merchant") or ""),
            "transactionTime": str(
                item.get("transactionTime") or item.get("transaction_time") or ""
            ),
        }
        for item in shortlist
        if txn_id(item)
    ]


def fact_prompt(state: InvestigationState) -> dict[str, Any]:
    return {
        "prompt": "Which charge is this complaint about?",
        "candidates": fact_candidates(state),
    }


def ledger_needs_fact(state: InvestigationState) -> bool:
    transactions = state.get("transactions") or []
    if len(transactions) < 2:
        return False
    groups = duplicate_groups(transactions, state.get("amount"))
    if not groups:
        # Several charges and no duplicate pair: the complaint does not point at one.
        return True
    return len(groups) > 1


def should_interrupt(state: InvestigationState) -> bool:
    if state.get("fact_answer") or state.get("transaction_id"):
        return False
    transactions = state.get("transactions") or []
    if len(transactions) < 2:
        return False
    # Duplicate matching stays in code: a live plan cannot suppress the pause
    # when the ledger still has more than one candidate charge.
    if ledger_needs_fact(state):
        return True
    ask = state.get("ask_which_charge")
    if ask is True:
        return len(fact_candidates(state)) > 1
    return False


def eligibility_matters(state: InvestigationState) -> bool:
    complaint_type = str(state.get("complaint_type") or "")
    category = str(state.get("category") or "")
    finding = str(state.get("finding") or "")
    return (
        category == "PAYMENT_DISPUTE"
        or complaint_type in {"DUPLICATE_CHARGE", "UNRECOGNIZED_TRANSACTION"}
        or finding == "DUPLICATE_TRANSACTION"
    )


def checkpoint_exists(graph: Any, complaint_id: str) -> bool:
    snapshot = graph.get_state(thread_config(complaint_id))
    values = getattr(snapshot, "values", None) or {}
    nxt = getattr(snapshot, "next", ()) or ()
    return bool(values) or bool(nxt)


def investigation_is_settled(graph: Any, complaint_id: str) -> bool:
    snapshot = graph.get_state(thread_config(complaint_id))
    values = getattr(snapshot, "values", None) or {}
    if values.get("completed"):
        return True
    return bool(interrupt_values(graph, complaint_id))


def reset_incomplete_checkpoint(graph: Any, complaint_id: str) -> None:
    if not checkpoint_exists(graph, complaint_id):
        return
    if investigation_is_settled(graph, complaint_id):
        return
    checkpointer = getattr(graph, "checkpointer", None)
    if checkpointer is None or not hasattr(checkpointer, "delete_thread"):
        logger.warning("Cannot reset incomplete checkpoint for %s", complaint_id)
        return
    checkpointer.delete_thread(complaint_id)
    logger.info("Reset incomplete investigation checkpoint for %s", complaint_id)


def interrupt_values(graph: Any, complaint_id: str) -> list[Any]:
    snapshot = graph.get_state(thread_config(complaint_id))
    found: list[Any] = []
    for task in getattr(snapshot, "tasks", ()) or ():
        for item in getattr(task, "interrupts", ()) or ():
            found.append(item.value if isinstance(item, Interrupt) else item)
    return found


class InvestigationRuntime:
    def __init__(
        self,
        *,
        checkpointer: BaseCheckpointSaver,
        publisher: Publisher = publish,
        tool_caller: ToolCaller | None = None,
        policy_caller: PolicyCaller | None = None,
        persist_pending: Callable[[str, str, dict[str, Any]], None] | None = None,
        clear_pending: Callable[[str], None] | None = None,
        planner: Planner | None = None,
    ) -> None:
        self.publisher = publisher
        self.tool_caller = tool_caller or sync_call_tool
        self.policy_caller = policy_caller or sync_assess_policy
        self.planner = planner or plan_investigation
        self.pending: dict[str, dict[str, Any]] = {}
        self._persist_pending = persist_pending or self._default_persist_pending
        self._clear_pending = clear_pending or self._default_clear_pending
        self.graph = build_graph(self).compile(checkpointer=checkpointer)

    def mcp(self, complaint_id: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = self.tool_caller(name, arguments)
        write_trace(
            complaint_id,
            AGENT_NAME,
            f"MCP:{name}",
            detail={"arguments": arguments, "tool": name},
        )
        return result

    def start_investigation(self, state: InvestigationState) -> dict[str, Any]:
        if state.get("started"):
            return {}
        complaint_id = state["complaint_id"]
        payload = {
            "complaintId": complaint_id,
            "customerId": state.get("customer_id"),
            "complaintType": state.get("complaint_type"),
            "reviewerNote": state.get("reviewer_note") or None,
        }
        event = EventEnvelope.create("InvestigationStarted", complaint_id, payload)
        write_trace(complaint_id, AGENT_NAME, "InvestigationStarted", detail=payload)
        self.publisher(event)
        self._upsert_run(complaint_id, "STARTED")
        return {"started": True}

    def _complaint_message(self, complaint_id: str) -> str:
        try:
            row = fetch_one(
                "SELECT message FROM complaints WHERE complaint_id = %s",
                (complaint_id,),
            )
        except Exception:
            logger.exception("Could not load complaint message for %s", complaint_id)
            return ""
        if row is None:
            return ""
        return str(row.get("message") or "")

    def plan_investigation_node(self, state: InvestigationState) -> dict[str, Any]:
        complaint_id = state["complaint_id"]
        plan = self.planner(
            customer_id=state["customer_id"],
            message=self._complaint_message(complaint_id),
            category=str(state.get("category") or ""),
            complaint_type=str(state.get("complaint_type") or ""),
            amount=state.get("amount"),
            reviewer_note=str(state.get("reviewer_note") or ""),
        )
        if not isinstance(plan, InvestigationPlan):
            plan = InvestigationPlan.model_validate(plan)
        tools = normalize_lookup_tools(plan.tools)
        detail = {
            "tools": tools,
            "askWhichCharge": plan.ask_which_charge,
            "reason": plan.reason,
        }
        write_trace(complaint_id, AGENT_NAME, "LLM:PLAN_INVESTIGATION", detail=detail)
        return {
            "planned_tools": tools,
            "ask_which_charge": plan.ask_which_charge,
            "plan_reason": plan.reason,
        }

    def gather_transactions(self, state: InvestigationState) -> dict[str, Any]:
        complaint_id = state["complaint_id"]
        customer_id = state["customer_id"]
        planned = list(state.get("planned_tools") or ["get_transactions", "get_customer"])
        if "get_transactions" not in planned:
            planned = ["get_transactions", *planned]
        listed = self.mcp(complaint_id, "get_transactions", {"customer_id": customer_id})
        transactions = listed.get("transactions", listed)
        if not isinstance(transactions, list):
            raise RuntimeError(f"Unexpected get_transactions result: {listed}")
        calls = list(state.get("mcp_calls") or [])
        calls.append("get_transactions")
        fraud_flag = False
        if "get_customer" in planned:
            try:
                loaded = self.mcp(complaint_id, "get_customer", {"customer_id": customer_id})
                customer = loaded.get("customer", loaded)
                calls.append("get_customer")
                fraud_flag = bool(customer.get("fraudFlag") or customer.get("fraud_flag"))
            except Exception:
                logger.exception("get_customer failed for %s", customer_id)
        return {
            "transactions": transactions,
            "mcp_calls": calls,
            "fraud_flag": fraud_flag,
        }

    def maybe_interrupt(self, state: InvestigationState) -> dict[str, Any]:
        if not should_interrupt(state):
            return {}
        prompt = fact_prompt(state)
        answer = interrupt(prompt)
        if isinstance(answer, dict):
            text = str(answer.get("answer") or answer)
        else:
            text = str(answer)
        return {"fact_answer": text}

    def analyze_and_refund(self, state: InvestigationState) -> dict[str, Any]:
        complaint_id = state["complaint_id"]
        transactions = list(state.get("transactions") or [])
        amount = state.get("amount")
        groups = duplicate_groups(transactions, amount)
        finding = "NO_DUPLICATE"
        targets: list[dict[str, Any]] = []
        ambiguous = ledger_needs_fact(state) and not state.get("fact_answer")
        if groups:
            finding = "DUPLICATE_TRANSACTION"
            if not ambiguous:
                targets = groups[0]
        elif str(state.get("complaint_type") or "") == "UNRECOGNIZED_TRANSACTION":
            finding = "UNRECOGNIZED_TRANSACTION"
            if not ambiguous:
                targets = transactions[:1]
        elif not ambiguous:
            targets = transactions[:1]
        if state.get("fact_answer"):
            chosen = str(state["fact_answer"])
            matching = [item for item in transactions if txn_id(item) == chosen]
            if matching:
                targets = matching
        refund_lookups: list[dict[str, Any]] = []
        calls = list(state.get("mcp_calls") or [])
        for item in targets:
            identifier = txn_id(item)
            if not identifier:
                continue
            status = self.mcp(
                complaint_id, "get_refund_status", {"transaction_id": identifier}
            )
            refund_lookups.append(status)
            calls.append("get_refund_status")
        refunded = any(bool(item.get("refunded")) for item in refund_lookups)
        target_id = txn_id(targets[-1]) if targets else None
        target_amount = amount
        if target_amount is None and targets:
            target_amount = txn_amount(targets[-1])
            if float(target_amount).is_integer():
                target_amount = int(target_amount)
        return {
            "finding": finding,
            "transaction_id": target_id,
            "amount": target_amount,
            "refund_lookups": refund_lookups,
            "refund_status": "REFUNDED" if refunded else "NOT_REFUNDED",
            "mcp_calls": calls,
        }

    def assess_policy_node(self, state: InvestigationState) -> dict[str, Any]:
        if not eligibility_matters(state):
            return {"policy_eligible": None}
        complaint_id = state["complaint_id"]
        assessment = self.policy_caller(
            customer_id=state["customer_id"],
            complaint_type=str(state.get("complaint_type") or ""),
            amount=state.get("amount"),
            complaint_id=complaint_id,
        )
        write_trace(
            complaint_id,
            AGENT_NAME,
            "A2A:ASSESS_POLICY",
            detail={"task": "ASSESS_POLICY", "result": assessment},
        )
        confidence = float(assessment.get("confidence") or 0)
        if state.get("finding") == "DUPLICATE_TRANSACTION":
            confidence = max(confidence, 0.90)
        return {
            "policy_assessment": assessment,
            "policy_eligible": bool(assessment.get("eligible")),
            "confidence": round(confidence, 2),
        }

    def complete_investigation(self, state: InvestigationState) -> dict[str, Any]:
        if state.get("completed"):
            return {}
        complaint_id = state["complaint_id"]
        payload = {
            "complaintId": complaint_id,
            "customerId": state.get("customer_id"),
            "finding": state.get("finding"),
            "amount": state.get("amount"),
            "refundStatus": state.get("refund_status"),
            "policyEligible": state.get("policy_eligible"),
            "confidence": state.get("confidence"),
            "transactionId": state.get("transaction_id"),
            "fraudFlag": bool(state.get("fraud_flag")),
            "category": state.get("category"),
            "complaintType": state.get("complaint_type"),
            "reviewerNote": state.get("reviewer_note") or None,
        }
        event = EventEnvelope.create("InvestigationCompleted", complaint_id, payload)
        write_trace(complaint_id, AGENT_NAME, "InvestigationCompleted", detail=payload)
        self.publisher(event)
        self._upsert_run(complaint_id, "COMPLETED")
        return {"completed": True}

    def handle_complaint_classified(self, event: EventEnvelope) -> bool:
        if event.event_type != "ComplaintClassified":
            return False
        payload = event.payload
        complaint_id = str(payload.get("complaintId") or event.correlation_id)
        if investigation_is_settled(self.graph, complaint_id):
            logger.info("Skipping investigation; already settled for %s", complaint_id)
            with connection() as conn:
                claim_event(conn, CONSUMER_NAME, event.event_id)
                conn.commit()
            return False
        reset_incomplete_checkpoint(self.graph, complaint_id)
        entities = payload.get("entities") or {}
        amount = entities.get("amount") if isinstance(entities, dict) else None
        initial: InvestigationState = {
            "complaint_id": complaint_id,
            "customer_id": str(payload.get("customerId") or ""),
            "category": str(payload.get("category") or ""),
            "complaint_type": str(payload.get("subCategory") or ""),
            "amount": amount,
            "mcp_calls": [],
        }
        self.graph.invoke(initial, thread_config(complaint_id))
        self._capture_interrupt(complaint_id, complaint_id)
        with connection() as conn:
            claim_event(conn, CONSUMER_NAME, event.event_id)
            conn.commit()
        return True

    def handle_investigation_requested(self, event: EventEnvelope) -> bool:
        if event.event_type != "InvestigationRequested":
            return False
        payload = event.payload
        complaint_id = str(payload.get("complaintId") or event.correlation_id)
        with connection() as conn:
            if not claim_event(conn, CONSUMER_NAME, event.event_id):
                logger.info("Skipping duplicate investigation request %s", event.event_id)
                return False
            conn.commit()
        reviewer_note = str(payload.get("reviewerNote") or payload.get("note") or "")
        run_count = self._begin_follow_up(complaint_id, reviewer_note)
        thread_id = f"{complaint_id}:r{run_count}"
        entities = payload.get("entities") or {}
        amount = entities.get("amount") if isinstance(entities, dict) else None
        if amount is None:
            amount = payload.get("amount")
        initial: InvestigationState = {
            "complaint_id": complaint_id,
            "customer_id": str(payload.get("customerId") or ""),
            "category": str(payload.get("category") or "PAYMENT_DISPUTE"),
            "complaint_type": str(
                payload.get("subCategory") or payload.get("complaintType") or ""
            ),
            "amount": amount,
            "reviewer_note": reviewer_note,
            "graph_thread_id": thread_id,
            "mcp_calls": [],
        }
        self.graph.invoke(initial, thread_config(thread_id))
        self._capture_interrupt(complaint_id, thread_id)
        return True

    def handle_event(self, event: EventEnvelope) -> bool:
        if event.event_type == "ComplaintClassified":
            return self.handle_complaint_classified(event)
        if event.event_type == "InvestigationRequested":
            return self.handle_investigation_requested(event)
        return False

    def resume(self, complaint_id: str, answer: str) -> dict[str, Any]:
        pending = self.pending.get(complaint_id) or self._load_pending(complaint_id)
        thread_id = self._thread_id_for(complaint_id, pending)
        if pending is None and not interrupt_values(self.graph, thread_id):
            raise KeyError(complaint_id)
        self.graph.invoke(Command(resume=answer), thread_config(thread_id))
        self._capture_interrupt(complaint_id, thread_id)
        if not interrupt_values(self.graph, thread_id):
            self._clear_pending(complaint_id)
            self.pending.pop(complaint_id, None)
        snapshot = self.graph.get_state(thread_config(thread_id))
        values = getattr(snapshot, "values", {}) or {}
        return {
            "status": "completed" if values.get("completed") else "resumed",
            "complaintId": complaint_id,
            "finding": values.get("finding"),
        }

    def pending_prompt(self, complaint_id: str) -> dict[str, Any] | None:
        return self.pending.get(complaint_id) or self._load_pending(complaint_id)

    def _thread_id_for(self, complaint_id: str, pending: dict[str, Any] | None) -> str:
        if not pending:
            return complaint_id
        detail = pending.get("detail") if isinstance(pending.get("detail"), dict) else {}
        return str(
            pending.get("threadId")
            or (detail or {}).get("threadId")
            or complaint_id
        )

    def _capture_interrupt(self, complaint_id: str, thread_id: str) -> None:
        values = interrupt_values(self.graph, thread_id)
        if not values:
            return
        prompt = values[0]
        text = prompt.get("prompt") if isinstance(prompt, dict) else str(prompt)
        detail = prompt if isinstance(prompt, dict) else {"prompt": text}
        if isinstance(detail, dict):
            detail = {**detail, "threadId": thread_id}
        record = {
            "complaintId": complaint_id,
            "prompt": text,
            "detail": detail,
            "threadId": thread_id,
        }
        self.pending[complaint_id] = record
        self._persist_pending(
            complaint_id, str(text), detail if isinstance(detail, dict) else record
        )
        write_trace(
            complaint_id,
            AGENT_NAME,
            "FactInterrupt",
            status="PENDING",
            detail=record,
        )
        self._upsert_run(complaint_id, "INTERRUPTED")

    def _default_persist_pending(
        self, complaint_id: str, prompt: str, state: dict[str, Any]
    ) -> None:
        try:
            with connection() as conn:
                conn.execute(
                    """
                    INSERT INTO pending_facts (complaint_id, prompt, state)
                    VALUES (%s, %s, %s::jsonb)
                    ON CONFLICT (complaint_id) DO UPDATE
                    SET prompt = EXCLUDED.prompt,
                        state = EXCLUDED.state,
                        created_at = NOW()
                    """,
                    (complaint_id, prompt, json.dumps(state)),
                )
                conn.commit()
        except Exception:
            logger.exception("Could not persist pending fact for %s", complaint_id)

    def _default_clear_pending(self, complaint_id: str) -> None:
        try:
            with connection() as conn:
                conn.execute(
                    "DELETE FROM pending_facts WHERE complaint_id = %s",
                    (complaint_id,),
                )
                conn.commit()
        except Exception:
            logger.exception("Could not clear pending fact for %s", complaint_id)

    def _load_pending(self, complaint_id: str) -> dict[str, Any] | None:
        try:
            row = fetch_one(
                """
                SELECT complaint_id AS "complaintId", prompt, state
                FROM pending_facts
                WHERE complaint_id = %s
                """,
                (complaint_id,),
            )
        except Exception:
            return None
        if row is None:
            return None
        state = row.get("state") or {}
        return {
            "complaintId": row["complaintId"],
            "prompt": row["prompt"],
            "detail": state,
            "threadId": (state or {}).get("threadId") if isinstance(state, dict) else None,
        }

    def _begin_follow_up(self, complaint_id: str, reviewer_note: str) -> int:
        try:
            with connection() as conn:
                row = conn.execute(
                    """
                    INSERT INTO investigation_runs
                        (complaint_id, status, run_count, reviewer_note)
                    VALUES (%s, 'REQUESTED', 2, %s)
                    ON CONFLICT (complaint_id) DO UPDATE
                    SET status = 'REQUESTED',
                        run_count = investigation_runs.run_count + 1,
                        reviewer_note = EXCLUDED.reviewer_note,
                        updated_at = NOW()
                    RETURNING run_count, reviewer_note
                    """,
                    (complaint_id, reviewer_note),
                ).fetchone()
                conn.commit()
            if row is not None:
                return int(row["run_count"])
        except Exception:
            logger.exception("Could not increment investigation run for %s", complaint_id)
        return 2

    def _upsert_run(self, complaint_id: str, status: str) -> None:
        try:
            with connection() as conn:
                conn.execute(
                    """
                    INSERT INTO investigation_runs (complaint_id, status)
                    VALUES (%s, %s)
                    ON CONFLICT (complaint_id) DO UPDATE
                    SET status = EXCLUDED.status,
                        updated_at = NOW()
                    """,
                    (complaint_id, status),
                )
                conn.commit()
        except Exception:
            logger.exception("Could not update investigation_runs for %s", complaint_id)


def build_graph(runtime: InvestigationRuntime) -> StateGraph:
    graph = StateGraph(InvestigationState)
    graph.add_node("start", runtime.start_investigation)
    graph.add_node("plan_investigation", runtime.plan_investigation_node)
    graph.add_node("gather_transactions", runtime.gather_transactions)
    graph.add_node("maybe_interrupt", runtime.maybe_interrupt)
    graph.add_node("analyze_and_refund", runtime.analyze_and_refund)
    graph.add_node("assess_policy", runtime.assess_policy_node)
    graph.add_node("complete", runtime.complete_investigation)
    graph.add_edge(START, "start")
    graph.add_edge("start", "plan_investigation")
    graph.add_edge("plan_investigation", "gather_transactions")
    graph.add_edge("gather_transactions", "maybe_interrupt")
    graph.add_edge("maybe_interrupt", "analyze_and_refund")
    graph.add_edge("analyze_and_refund", "assess_policy")
    graph.add_edge("assess_policy", "complete")
    graph.add_edge("complete", END)
    return graph


def create_postgres_checkpointer() -> tuple[ConnectionPool, BaseCheckpointSaver]:
    from langgraph.checkpoint.postgres import PostgresSaver

    pool = ConnectionPool(
        conninfo=DATABASE_URL,
        max_size=10,
        kwargs={"autocommit": True, "prepare_threshold": 0},
    )
    saver = PostgresSaver(pool)
    saver.setup()
    return pool, saver


def create_app(runtime: InvestigationRuntime) -> FastAPI:
    app = FastAPI(title="Investigation Agent")
    app.state.runtime = runtime

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/pending/{complaint_id}")
    def get_pending(complaint_id: str) -> dict[str, Any]:
        pending = runtime.pending_prompt(complaint_id)
        if pending is None:
            raise HTTPException(status_code=404, detail="Pending fact not found")
        return pending

    @app.post("/resume/{complaint_id}")
    def resume(complaint_id: str, submission: FactAnswer) -> dict[str, Any]:
        try:
            return runtime.resume(complaint_id, submission.answer)
        except KeyError:
            raise HTTPException(status_code=404, detail="Pending fact not found") from None

    return app


def classified_event(
    *,
    customer_id: str,
    complaint_type: str,
    amount: float | int | None,
    complaint_id: str | None = None,
    category: str = "PAYMENT_DISPUTE",
) -> EventEnvelope:
    complaint_id = complaint_id or f"CMP-{uuid4().hex[:8].upper()}"
    return EventEnvelope.create(
        "ComplaintClassified",
        complaint_id,
        {
            "complaintId": complaint_id,
            "customerId": customer_id,
            "category": category,
            "subCategory": complaint_type,
            "entities": {"amount": amount},
            "priority": "MEDIUM",
            "requiredCapabilities": ["TRANSACTION_LOOKUP", "REFUND_STATUS"],
        },
    )


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    _pool, checkpointer = create_postgres_checkpointer()
    runtime = InvestigationRuntime(checkpointer=checkpointer)
    app = create_app(runtime)
    thread = threading.Thread(
        target=lambda: uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="info"),
        daemon=True,
        name="investigation-http",
    )
    thread.start()
    logger.info("Starting Investigation Agent HTTP on 0.0.0.0:%s", HTTP_PORT)
    run_consumer(
        name=CONSUMER_NAME,
        topics=[COMPLAINTS_TOPIC, INVESTIGATIONS_TOPIC],
        handler=runtime.handle_event,
    )


if __name__ == "__main__":
    main()
