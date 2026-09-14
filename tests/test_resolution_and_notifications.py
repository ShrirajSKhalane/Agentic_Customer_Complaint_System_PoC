from __future__ import annotations

from uuid import uuid4

import pytest

from poc.complaint_api import complaint_trace
from poc.db import connection
from poc.events import EventEnvelope
from poc.notification_service import (
    NotificationRuntime,
    customer_message,
    write_notification,
)
from poc.resolution_agent import ResolutionRuntime, refund_gate_failures


def investigation_completed(
    *,
    complaint_id: str = "CMP-TEST",
    customer_id: str = "CUST-1001",
    amount: int = 5000,
    confidence: float = 0.94,
    policy_eligible: bool = True,
    transaction_id: str = "TXN-1001-B",
    finding: str = "DUPLICATE_TRANSACTION",
) -> EventEnvelope:
    return EventEnvelope.create(
        "InvestigationCompleted",
        complaint_id,
        {
            "complaintId": complaint_id,
            "customerId": customer_id,
            "finding": finding,
            "amount": amount,
            "refundStatus": "NOT_REFUNDED",
            "policyEligible": policy_eligible,
            "confidence": confidence,
            "transactionId": transaction_id,
            "fraudFlag": False,
        },
    )


def set_claimer() -> tuple[set[object], object]:
    claimed: set[object] = set()

    def claim(event: EventEnvelope) -> bool:
        if event.event_id in claimed:
            return False
        claimed.add(event.event_id)
        return True

    return claimed, claim


def no_trace(
    _complaint_id: str,
    _agent: str,
    _action: str,
    _status: str,
    _detail: dict[str, object],
) -> None:
    return None


def test_cust_1001_auto_refunds_once_and_publishes_resolution_events() -> None:
    published: list[EventEnvelope] = []
    tool_calls: list[tuple[str, dict[str, object]]] = []
    _, claimer = set_claimer()

    def tool(name: str, arguments: dict[str, object]) -> dict[str, object]:
        tool_calls.append((name, arguments))
        return {
            "created": True,
            "refund": {
                "refundId": "REF-1001",
                "transactionId": arguments["transaction_id"],
                "amount": arguments["amount"],
                "status": "INITIATED",
            },
        }

    runtime = ResolutionRuntime(
        publisher=published.append,
        tool_caller=tool,
        event_claimer=claimer,
        tracer=no_trace,
        review_persister=lambda *_args: None,
    )
    event = investigation_completed()

    assert runtime.handle_investigation_completed(event) is True
    assert runtime.handle_investigation_completed(event) is False
    assert tool_calls == [
        (
            "initiate_refund",
            {"transaction_id": "TXN-1001-B", "amount": 5000.0},
        )
    ]
    assert [item.event_type for item in published] == [
        "RefundRequired",
        "RefundCompleted",
        "ComplaintResolved",
    ]
    assert all(item.correlation_id == event.correlation_id for item in published)


def test_cust_1002_high_value_requires_review_and_never_calls_refund() -> None:
    published: list[EventEnvelope] = []
    reviews: list[tuple[str, str, dict[str, object]]] = []
    tool_calls: list[tuple[str, dict[str, object]]] = []
    _, claimer = set_claimer()
    runtime = ResolutionRuntime(
        publisher=published.append,
        tool_caller=lambda name, arguments: tool_calls.append((name, arguments)) or {},
        event_claimer=claimer,
        tracer=no_trace,
        review_persister=lambda complaint_id, reason, proposed: reviews.append(
            (complaint_id, reason, proposed)
        ),
    )
    event = investigation_completed(
        complaint_id="CMP-HIGH",
        customer_id="CUST-1002",
        amount=50000,
        policy_eligible=False,
        transaction_id="TXN-1002-A",
        finding="UNRECOGNIZED_TRANSACTION",
    )

    assert runtime.handle_investigation_completed(event) is True
    assert tool_calls == []
    assert [item.event_type for item in published] == ["HumanReviewRequired"]
    assert "exceeds automatic refund limit" in published[0].payload["reason"]
    assert reviews[0][0] == "CMP-HIGH"


def test_all_automatic_refund_gates_are_enforced() -> None:
    payload = dict(investigation_completed().payload)
    assert refund_gate_failures(payload) == []
    payload.update(
        confidence=0.89,
        policyEligible=False,
        amount=10001,
        fraudFlag=True,
    )
    failures = refund_gate_failures(payload)
    assert len(failures) == 4


def test_duplicate_refund_notification_is_customer_visible_and_idempotent() -> None:
    published: list[EventEnvelope] = []
    written: list[tuple[str, str]] = []
    _, claimer = set_claimer()
    runtime = NotificationRuntime(
        publisher=published.append,
        event_claimer=claimer,
        notification_writer=lambda complaint_id, message: (
            written.append((complaint_id, message)) or 7
        ),
        tracer=no_trace,
    )
    event = EventEnvelope.create(
        "RefundCompleted",
        "CMP-NOTIFY",
        {
            "complaintId": "CMP-NOTIFY",
            "finding": "DUPLICATE_TRANSACTION",
            "amount": 5000,
            "refundStatus": "INITIATED",
        },
    )

    assert runtime.handle_terminal_event(event) is True
    assert runtime.handle_terminal_event(event) is False
    assert len(written) == 1
    assert "duplicate charge of ₹5,000" in written[0][1]
    assert "initiated a refund" in written[0][1]
    assert [item.event_type for item in published] == ["NotificationSent"]


def postgres_available() -> bool:
    try:
        with connection() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_notification_appears_in_portal_notification_pane_data() -> None:
    complaint_id = f"CMP-{uuid4().hex[:8].upper()}"
    message = customer_message(
        EventEnvelope.create(
            "RefundCompleted",
            complaint_id,
            {
                "complaintId": complaint_id,
                "finding": "DUPLICATE_TRANSACTION",
                "amount": 5000,
            },
        )
    )
    assert message is not None
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO complaints (complaint_id, customer_id, message)
            VALUES (%s, 'CUST-1001', 'test notification')
            """,
            (complaint_id,),
        )
        conn.commit()
    try:
        write_notification(complaint_id, message)
        pane = complaint_trace(complaint_id)
        notifications = pane["notifications"]
        assert isinstance(notifications, list)
        assert "duplicate charge of ₹5,000" in notifications[0]["message"]
        assert "initiated a refund" in notifications[0]["message"]
    finally:
        with connection() as conn:
            conn.execute(
                "DELETE FROM notifications WHERE complaint_id = %s",
                (complaint_id,),
            )
            conn.execute(
                "DELETE FROM complaints WHERE complaint_id = %s",
                (complaint_id,),
            )
            conn.commit()
