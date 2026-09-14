from __future__ import annotations

from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from poc.complaint_api import complaint_trace, review_queues
from poc.db import connection, write_trace
from poc.events import EVENT_TOPICS, EventEnvelope
from poc.investigation_agent import InvestigationRuntime, stub_investigation_plan
from poc.notification_service import NotificationRuntime, write_notification
from poc.resolution_agent import ResolutionRuntime, persist_review
from poc.triage_agent import handle_complaint_received
from tests.test_investigation_agent import FakeTools


def postgres_available() -> bool:
    try:
        with connection() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not postgres_available(), reason="PostgreSQL is not available"
)


def complaint_id(label: str) -> str:
    return f"CMP-{label}-{uuid4().hex[:6].upper()}"


def insert_complaint(
    identifier: str, customer_id: str, message: str
) -> EventEnvelope:
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO complaints (complaint_id, customer_id, message)
            VALUES (%s, %s, %s)
            """,
            (identifier, customer_id, message),
        )
        write_trace(
            identifier,
            "complaint-api",
            "ComplaintReceived",
            detail={"customerId": customer_id, "message": message},
            conn=conn,
        )
        conn.commit()
    return EventEnvelope.create(
        "ComplaintReceived",
        identifier,
        {
            "complaintId": identifier,
            "customerId": customer_id,
            "message": message,
        },
    )


def cleanup(identifier: str) -> None:
    with connection() as conn:
        conn.execute("DELETE FROM notifications WHERE complaint_id = %s", (identifier,))
        conn.execute("DELETE FROM pending_facts WHERE complaint_id = %s", (identifier,))
        conn.execute("DELETE FROM human_reviews WHERE complaint_id = %s", (identifier,))
        conn.execute("DELETE FROM investigation_runs WHERE complaint_id = %s", (identifier,))
        conn.execute("DELETE FROM event_trace WHERE correlation_id = %s", (identifier,))
        conn.execute("DELETE FROM complaints WHERE complaint_id = %s", (identifier,))
        conn.commit()


def trace(
    identifier: str,
    agent: str,
    action: str,
    status: str,
    detail: dict[str, object],
) -> None:
    write_trace(identifier, agent, action, status=status, detail=detail)


def policy(
    *,
    complaint_type: str,
    **_kwargs: object,
) -> dict[str, object]:
    eligible = complaint_type == "DUPLICATE_CHARGE"
    return {
        "eligible": eligible,
        "action": "REFUND" if eligible else "HUMAN_REVIEW",
        "reason": "Seeded demo policy assessment.",
        "confidence": 0.94,
    }


def run_to_investigation(
    received: EventEnvelope,
    tools: FakeTools,
) -> tuple[EventEnvelope, list[EventEnvelope]]:
    triage_events: list[EventEnvelope] = []
    assert handle_complaint_received(received, triage_events.append, stub=True)
    investigation_events: list[EventEnvelope] = []
    runtime = InvestigationRuntime(
        checkpointer=InMemorySaver(),
        publisher=investigation_events.append,
        tool_caller=tools,
        policy_caller=policy,
        planner=lambda **kwargs: stub_investigation_plan(str(kwargs["customer_id"])),
    )
    assert runtime.handle_complaint_classified(triage_events[0])
    completed = [
        event
        for event in investigation_events
        if event.event_type == "InvestigationCompleted"
    ]
    assert len(completed) == 1
    return completed[0], triage_events + investigation_events


def notification_runtime(published: list[EventEnvelope]) -> NotificationRuntime:
    claimed: set[object] = set()

    def claim(event: EventEnvelope) -> bool:
        if event.event_id in claimed:
            return False
        claimed.add(event.event_id)
        return True

    return NotificationRuntime(
        publisher=published.append,
        event_claimer=claim,
        notification_writer=write_notification,
        tracer=trace,
    )


def test_cust_1001_walkthrough_populates_complete_portal_trace() -> None:
    identifier = complaint_id("AUTO")
    received = insert_complaint(
        identifier,
        "CUST-1001",
        "I was charged ₹5,000 twice for the same payment.",
    )
    try:
        completed, lifecycle = run_to_investigation(received, FakeTools())
        resolution_events: list[EventEnvelope] = []
        refund_calls: list[tuple[str, dict[str, object]]] = []

        def refund(name: str, arguments: dict[str, object]) -> dict[str, object]:
            refund_calls.append((name, arguments))
            return {
                "created": True,
                "refund": {"refundId": "REF-DEMO", "status": "INITIATED"},
            }

        resolution = ResolutionRuntime(
            publisher=resolution_events.append,
            tool_caller=refund,
            event_claimer=lambda _event: True,
            tracer=trace,
            review_persister=persist_review,
        )
        assert resolution.handle_investigation_completed(completed)
        lifecycle.extend(resolution_events)

        notification_events: list[EventEnvelope] = []
        notifications = notification_runtime(notification_events)
        for event in resolution_events:
            notifications.handle_terminal_event(event)
        lifecycle.extend(notification_events)

        portal = complaint_trace(identifier)
        actions = [row["action"] for row in portal["trace"]]
        assert actions[0] == "ComplaintReceived"
        for expected in (
            "ComplaintClassified",
            "LLM:PLAN_INVESTIGATION",
            "MCP:get_transactions",
            "MCP:get_refund_status",
            "A2A:ASSESS_POLICY",
            "InvestigationCompleted",
            "RefundRequired",
            "MCP:initiate_refund",
            "RefundCompleted",
            "ComplaintResolved",
            "NotificationSent",
        ):
            assert expected in actions
        assert refund_calls == [
            ("initiate_refund", {"transaction_id": "TXN-1001-B", "amount": 5000.0})
        ]
        messages = [row["message"] for row in portal["notifications"]]
        assert any(
            "duplicate charge of ₹5,000" in message
            and "initiated a refund" in message
            for message in messages
        )
        assert all(event.event_type in EVENT_TOPICS for event in lifecycle)
    finally:
        cleanup(identifier)


def test_cust_1002_waits_for_human_approval_before_refund() -> None:
    identifier = complaint_id("REVIEW")
    received = insert_complaint(
        identifier,
        "CUST-1002",
        "I do not recognize this ₹50,000 transaction.",
    )
    tools = FakeTools(
        [
            {
                "transactionId": "TXN-1002-A",
                "amount": 50000.0,
                "merchant": "Global Travel",
                "transactionTime": "2026-08-31T15:30:00Z",
            }
        ]
    )
    try:
        completed, _lifecycle = run_to_investigation(received, tools)
        resolution_events: list[EventEnvelope] = []
        refund_calls: list[tuple[str, dict[str, object]]] = []

        def refund(name: str, arguments: dict[str, object]) -> dict[str, object]:
            refund_calls.append((name, arguments))
            return {
                "created": True,
                "refund": {"refundId": "REF-APPROVED", "status": "INITIATED"},
            }

        resolution = ResolutionRuntime(
            publisher=resolution_events.append,
            tool_caller=refund,
            event_claimer=lambda _event: True,
            tracer=trace,
            review_persister=persist_review,
        )
        assert resolution.handle_investigation_completed(completed)
        assert [event.event_type for event in resolution_events] == [
            "HumanReviewRequired"
        ]
        assert refund_calls == []
        queues = review_queues()
        queued_ids = {
            str(item["complaintId"]) for item in queues["authorization"]
        }
        assert identifier in queued_ids

        proposed = resolution_events[0].payload["proposedAction"]
        decision = EventEnvelope.create(
            "HumanDecisionSubmitted",
            identifier,
            {
                "complaintId": identifier,
                "decision": "APPROVE",
                "note": "Approved after customer verification.",
                "proposedAction": proposed,
            },
        )
        resolution_events.clear()
        assert resolution.handle_human_decision(decision)
        assert refund_calls == [
            ("initiate_refund", {"transaction_id": "TXN-1002-A", "amount": 50000.0})
        ]
        assert [event.event_type for event in resolution_events] == [
            "RefundRequired",
            "RefundCompleted",
            "ComplaintResolved",
        ]
    finally:
        cleanup(identifier)


def test_mcp_and_a2a_hops_are_trace_only_not_kafka_event_types() -> None:
    assert all(not event_type.startswith(("MCP:", "A2A:")) for event_type in EVENT_TOPICS)
    assert set(EVENT_TOPICS.values()) == {
        "complaints",
        "investigations",
        "resolutions",
        "human-review",
        "notifications",
    }
    with pytest.raises(ValueError, match="Unknown lifecycle event type"):
        EventEnvelope.create("MCP:get_transactions", "CMP-BOUNDARY", {})
    with pytest.raises(ValueError, match="Unknown lifecycle event type"):
        EventEnvelope.create("A2A:ASSESS_POLICY", "CMP-BOUNDARY", {})
