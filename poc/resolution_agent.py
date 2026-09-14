from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Callable
from typing import Any

from poc.db import claim_event, connection, write_trace
from poc.events import EventEnvelope, HUMAN_REVIEW_TOPIC, INVESTIGATIONS_TOPIC
from poc.kafka import publish, run_consumer
from poc.mcp_client import call_tool


logger = logging.getLogger(__name__)
CONSUMER_NAME = "resolution-agent"
AUTO_REFUND_LIMIT_INR = float(os.getenv("AUTO_REFUND_LIMIT_INR", "10000"))

Publisher = Callable[[EventEnvelope], None]
ToolCaller = Callable[[str, dict[str, Any]], dict[str, Any]]
EventClaimer = Callable[[EventEnvelope], bool]
Tracer = Callable[[str, str, str, str, dict[str, Any]], None]
ReviewPersister = Callable[[str, str, dict[str, Any]], None]


def sync_call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return asyncio.run(call_tool(name, arguments))


def claim_once(event: EventEnvelope) -> bool:
    with connection() as conn:
        claimed = claim_event(conn, CONSUMER_NAME, event.event_id)
        conn.commit()
        return claimed


def trace(
    complaint_id: str,
    agent: str,
    action: str,
    status: str,
    detail: dict[str, Any],
) -> None:
    write_trace(complaint_id, agent, action, status=status, detail=detail)


def persist_review(
    complaint_id: str, reason: str, proposed_action: dict[str, Any]
) -> None:
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO human_reviews
                (complaint_id, reason, proposed_action, status)
            VALUES (%s, %s, %s::jsonb, 'PENDING')
            ON CONFLICT (complaint_id) DO UPDATE
            SET reason = EXCLUDED.reason,
                proposed_action = EXCLUDED.proposed_action,
                status = 'PENDING',
                decision = NULL,
                note = NULL,
                decided_at = NULL,
                created_at = NOW()
            """,
            (complaint_id, reason, json.dumps(proposed_action)),
        )
        conn.commit()


def complaint_type_from_payload(payload: dict[str, Any]) -> str:
    explicit = payload.get("complaintType") or payload.get("subCategory")
    if explicit:
        return str(explicit)
    finding = str(payload.get("finding") or "")
    if finding == "DUPLICATE_TRANSACTION":
        return "DUPLICATE_CHARGE"
    if finding == "UNRECOGNIZED_TRANSACTION":
        return "UNRECOGNIZED_TRANSACTION"
    return finding or "UNRECOGNIZED_TRANSACTION"


def refund_gate_failures(
    payload: dict[str, Any], limit: float = AUTO_REFUND_LIMIT_INR
) -> list[str]:
    failures: list[str] = []
    amount = payload.get("amount")
    try:
        numeric_amount = float(amount)
    except (TypeError, ValueError):
        numeric_amount = -1
        failures.append("refund amount is missing or invalid")
    if float(payload.get("confidence") or 0) < 0.90:
        failures.append("confidence is below 0.90")
    if payload.get("policyEligible") is not True:
        failures.append("policy does not authorize automatic refund")
    if numeric_amount > limit:
        failures.append(f"amount exceeds automatic refund limit of INR {limit:g}")
    if bool(payload.get("fraudFlag")):
        failures.append("fraud flag is present")
    if not payload.get("transactionId"):
        failures.append("transaction id is missing")
    if str(payload.get("refundStatus") or "NOT_REFUNDED") != "NOT_REFUNDED":
        failures.append("transaction is already refunded or refund status is ambiguous")
    return failures


class ResolutionRuntime:
    def __init__(
        self,
        *,
        publisher: Publisher = publish,
        tool_caller: ToolCaller = sync_call_tool,
        event_claimer: EventClaimer = claim_once,
        tracer: Tracer = trace,
        review_persister: ReviewPersister = persist_review,
        auto_refund_limit: float = AUTO_REFUND_LIMIT_INR,
    ) -> None:
        self.publisher = publisher
        self.tool_caller = tool_caller
        self.event_claimer = event_claimer
        self.tracer = tracer
        self.review_persister = review_persister
        self.auto_refund_limit = auto_refund_limit

    def emit(
        self, event_type: str, complaint_id: str, payload: dict[str, Any]
    ) -> EventEnvelope:
        event = EventEnvelope.create(event_type, complaint_id, payload)
        self.publisher(event)
        self.tracer(complaint_id, CONSUMER_NAME, event_type, "COMPLETED", payload)
        return event

    def handle_investigation_completed(self, event: EventEnvelope) -> bool:
        if event.event_type != "InvestigationCompleted":
            return False
        if not self.event_claimer(event):
            logger.info("Skipping duplicate investigation event %s", event.event_id)
            return False

        payload = dict(event.payload)
        complaint_id = str(payload.get("complaintId") or event.correlation_id)
        proposed_action = {
            "action": "REFUND",
            "transactionId": payload.get("transactionId"),
            "amount": payload.get("amount"),
            "finding": payload.get("finding"),
            "customerId": payload.get("customerId"),
            "category": payload.get("category") or "PAYMENT_DISPUTE",
            "complaintType": complaint_type_from_payload(payload),
        }
        failures = refund_gate_failures(payload, self.auto_refund_limit)
        if failures:
            reason = "; ".join(failures)
            review_payload = {
                "complaintId": complaint_id,
                "customerId": payload.get("customerId"),
                "reason": reason,
                "gateFailures": failures,
                "proposedAction": proposed_action,
            }
            self.review_persister(complaint_id, reason, proposed_action)
            self.emit("HumanReviewRequired", complaint_id, review_payload)
            return True

        self._execute_refund(
            complaint_id,
            proposed_action,
            resolution="AUTO_REFUND",
        )
        return True

    def handle_human_decision(self, event: EventEnvelope) -> bool:
        if event.event_type != "HumanDecisionSubmitted":
            return False
        if not self.event_claimer(event):
            logger.info("Skipping duplicate human decision %s", event.event_id)
            return False

        payload = dict(event.payload)
        complaint_id = str(payload.get("complaintId") or event.correlation_id)
        decision = str(payload.get("decision") or "").upper()
        note = str(payload.get("note") or "")
        proposed = payload.get("proposedAction") or {}
        if not isinstance(proposed, dict):
            proposed = {}

        if decision == "APPROVE":
            self._execute_refund(
                complaint_id,
                proposed,
                resolution="HUMAN_APPROVED",
            )
            return True
        if decision == "REJECT":
            self.emit(
                "ComplaintResolved",
                complaint_id,
                {
                    "complaintId": complaint_id,
                    "customerId": proposed.get("customerId") or payload.get("customerId"),
                    "transactionId": proposed.get("transactionId"),
                    "amount": proposed.get("amount"),
                    "resolution": "HUMAN_REJECT",
                    "status": "RESOLVED",
                    "note": note,
                },
            )
            return True
        if decision == "REQUEST_MORE_INFORMATION":
            requested_payload = {
                "complaintId": complaint_id,
                "customerId": proposed.get("customerId") or payload.get("customerId"),
                "category": proposed.get("category") or "PAYMENT_DISPUTE",
                "subCategory": proposed.get("complaintType")
                or complaint_type_from_payload(proposed),
                "entities": {"amount": proposed.get("amount")},
                "reviewerNote": note,
                "priorFinding": proposed.get("finding"),
                "transactionId": proposed.get("transactionId"),
            }
            self.emit("InvestigationRequested", complaint_id, requested_payload)
            return True
        logger.warning("Ignored human decision %s for %s", decision, complaint_id)
        return False

    def _execute_refund(
        self,
        complaint_id: str,
        proposed: dict[str, Any],
        *,
        resolution: str,
    ) -> None:
        refund_payload = {
            "action": "REFUND",
            "transactionId": proposed.get("transactionId"),
            "amount": proposed.get("amount"),
            "finding": proposed.get("finding"),
            "customerId": proposed.get("customerId"),
            "complaintId": complaint_id,
            "currency": "INR",
        }
        self.emit("RefundRequired", complaint_id, refund_payload)
        result = self.tool_caller(
            "initiate_refund",
            {
                "transaction_id": str(proposed["transactionId"]),
                "amount": float(proposed["amount"]),
            },
        )
        self.tracer(
            complaint_id,
            CONSUMER_NAME,
            "MCP:initiate_refund",
            "COMPLETED",
            {"tool": "initiate_refund", "result": result},
        )
        refund = result.get("refund") if isinstance(result, dict) else {}
        refund = refund if isinstance(refund, dict) else {}
        completed_payload = {
            **refund_payload,
            "refundId": refund.get("refundId") or refund.get("refund_id"),
            "refundStatus": refund.get("status") or "INITIATED",
            "created": bool(result.get("created", True)),
        }
        self.emit("RefundCompleted", complaint_id, completed_payload)
        self.emit(
            "ComplaintResolved",
            complaint_id,
            {
                **completed_payload,
                "resolution": resolution,
                "status": "RESOLVED",
            },
        )

    def handle_event(self, event: EventEnvelope) -> bool:
        if event.event_type == "InvestigationCompleted":
            return self.handle_investigation_completed(event)
        if event.event_type == "HumanDecisionSubmitted":
            return self.handle_human_decision(event)
        return False


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    runtime = ResolutionRuntime()
    run_consumer(
        name=CONSUMER_NAME,
        topics=[INVESTIGATIONS_TOPIC, HUMAN_REVIEW_TOPIC],
        handler=runtime.handle_event,
    )


if __name__ == "__main__":
    main()
