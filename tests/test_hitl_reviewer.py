from __future__ import annotations

import json
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from poc.complaint_api import (
    DecisionSubmission,
    app,
    record_human_decision,
    review_queues,
)
from poc.db import connection, fetch_one
from poc.events import EventEnvelope
from poc.investigation_agent import classified_event
from poc.resolution_agent import ResolutionRuntime
from tests.test_investigation_agent import (
    CUST_1003_TXNS,
    FakeTools,
    insert_complaint,
    make_runtime,
)
from tests.test_resolution_and_notifications import no_trace, set_claimer


def postgres_available() -> bool:
    try:
        with connection() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:8].upper()}"


def insert_pending_fact(complaint_id: str, prompt: str = "Which transaction?") -> None:
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO pending_facts (complaint_id, prompt, state)
            VALUES (%s, %s, '{}'::jsonb)
            """,
            (complaint_id, prompt),
        )
        conn.commit()


def insert_human_review(
    complaint_id: str,
    *,
    reason: str = "amount exceeds automatic refund limit",
    proposed: dict[str, object] | None = None,
) -> None:
    proposed = proposed or {
        "action": "REFUND",
        "transactionId": "TXN-1002-A",
        "amount": 50000,
        "finding": "UNRECOGNIZED_TRANSACTION",
        "customerId": "CUST-1002",
        "complaintType": "UNRECOGNIZED_TRANSACTION",
    }
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO human_reviews (complaint_id, reason, proposed_action, status)
            VALUES (%s, %s, %s::jsonb, 'PENDING')
            """,
            (complaint_id, reason, json.dumps(proposed)),
        )
        conn.commit()


def cleanup(*complaint_ids: str) -> None:
    with connection() as conn:
        for complaint_id in complaint_ids:
            conn.execute("DELETE FROM pending_facts WHERE complaint_id = %s", (complaint_id,))
            conn.execute("DELETE FROM human_reviews WHERE complaint_id = %s", (complaint_id,))
            conn.execute(
                "DELETE FROM investigation_runs WHERE complaint_id = %s", (complaint_id,)
            )
            conn.execute("DELETE FROM event_trace WHERE correlation_id = %s", (complaint_id,))
            conn.execute("DELETE FROM complaints WHERE complaint_id = %s", (complaint_id,))
        conn.commit()


def ids(rows: list[dict[str, object]], key: str = "complaintId") -> set[str]:
    return {str(row[key]) for row in rows}


def decision_event(
    *,
    complaint_id: str,
    decision: str,
    note: str = "",
    proposed: dict[str, object] | None = None,
) -> EventEnvelope:
    proposed = proposed or {
        "action": "REFUND",
        "transactionId": "TXN-1002-A",
        "amount": 50000,
        "finding": "UNRECOGNIZED_TRANSACTION",
        "customerId": "CUST-1002",
        "complaintType": "UNRECOGNIZED_TRANSACTION",
        "category": "PAYMENT_DISPUTE",
    }
    return EventEnvelope.create(
        "HumanDecisionSubmitted",
        complaint_id,
        {
            "complaintId": complaint_id,
            "decision": decision,
            "note": note,
            "proposedAction": proposed,
        },
    )


def refund_tool(calls: list[tuple[str, dict[str, object]]]):
    def tool(name: str, arguments: dict[str, object]) -> dict[str, object]:
        calls.append((name, arguments))
        return {
            "created": True,
            "refund": {
                "refundId": "REF-HITL",
                "transactionId": arguments["transaction_id"],
                "amount": arguments["amount"],
                "status": "INITIATED",
            },
        }

    return tool


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_human_review_required_appears_only_on_authorization_queue() -> None:
    fact_id = unique_id("CMP-FACT")
    auth_id = unique_id("CMP-AUTH")
    insert_complaint(fact_id, "CUST-1001")
    insert_complaint(auth_id, "CUST-1002")
    insert_pending_fact(fact_id)
    insert_human_review(auth_id)
    try:
        queues = review_queues()
        needs = ids(queues["needsInformation"])
        auth = ids(queues["authorization"])
        assert auth_id in auth
        assert auth_id not in needs
        assert fact_id in needs
        assert fact_id not in auth
    finally:
        cleanup(fact_id, auth_id)


def test_approve_refunds_via_mcp_and_reject_resolves_without_refund() -> None:
    published: list[EventEnvelope] = []
    tool_calls: list[tuple[str, dict[str, object]]] = []
    _, claimer = set_claimer()
    runtime = ResolutionRuntime(
        publisher=published.append,
        tool_caller=refund_tool(tool_calls),
        event_claimer=claimer,
        tracer=no_trace,
        review_persister=lambda *_args: None,
    )

    approve = decision_event(complaint_id="CMP-APPROVE", decision="APPROVE")
    assert runtime.handle_human_decision(approve) is True
    assert runtime.handle_human_decision(approve) is False
    assert tool_calls == [
        ("initiate_refund", {"transaction_id": "TXN-1002-A", "amount": 50000.0})
    ]
    assert [item.event_type for item in published] == [
        "RefundRequired",
        "RefundCompleted",
        "ComplaintResolved",
    ]
    assert published[-1].payload["resolution"] == "HUMAN_APPROVED"

    published.clear()
    tool_calls.clear()
    reject = decision_event(complaint_id="CMP-REJECT", decision="REJECT", note="Not our charge")
    assert runtime.handle_human_decision(reject) is True
    assert tool_calls == []
    assert [item.event_type for item in published] == ["ComplaintResolved"]
    assert published[0].payload["resolution"] == "HUMAN_REJECT"


def test_request_more_information_does_not_refund_and_asks_for_investigation() -> None:
    published: list[EventEnvelope] = []
    tool_calls: list[tuple[str, dict[str, object]]] = []
    _, claimer = set_claimer()
    runtime = ResolutionRuntime(
        publisher=published.append,
        tool_caller=refund_tool(tool_calls),
        event_claimer=claimer,
        tracer=no_trace,
        review_persister=lambda *_args: None,
    )
    note = "Confirm the merchant with the customer"
    event = decision_event(
        complaint_id="CMP-MORE",
        decision="REQUEST_MORE_INFORMATION",
        note=note,
    )
    assert runtime.handle_human_decision(event) is True
    assert tool_calls == []
    assert [item.event_type for item in published] == ["InvestigationRequested"]
    assert published[0].payload["reviewerNote"] == note
    assert published[0].correlation_id == "CMP-MORE"


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_follow_up_investigation_includes_reviewer_note() -> None:
    runtime, _tools, published = make_runtime()
    complaint_id = unique_id("CMP-NOTE")
    insert_complaint(complaint_id, "CUST-1002")
    event = EventEnvelope.create(
        "InvestigationRequested",
        complaint_id,
        {
            "complaintId": complaint_id,
            "customerId": "CUST-1002",
            "category": "PAYMENT_DISPUTE",
            "subCategory": "UNRECOGNIZED_TRANSACTION",
            "entities": {"amount": 50000},
            "reviewerNote": "Ask which card was used",
        },
    )
    try:
        assert runtime.handle_investigation_requested(event) is True
        assert runtime.handle_investigation_requested(event) is False
        completed = [item for item in published if item.event_type == "InvestigationCompleted"]
        started = [item for item in published if item.event_type == "InvestigationStarted"]
        assert started and completed
        assert started[0].payload["reviewerNote"] == "Ask which card was used"
        assert completed[0].payload["reviewerNote"] == "Ask which card was used"
        row = fetch_one(
            "SELECT run_count, reviewer_note FROM investigation_runs WHERE complaint_id = %s",
            (complaint_id,),
        )
        assert row is not None
        assert row["reviewer_note"] == "Ask which card was used"
        assert int(row["run_count"]) >= 2
    finally:
        cleanup(complaint_id)


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_ui_decision_publishes_human_decision_and_leaves_authorization_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[EventEnvelope] = []
    monkeypatch.setattr("poc.complaint_api.publish", published.append)
    complaint_id = unique_id("CMP-DECIDE")
    insert_complaint(complaint_id, "CUST-1002")
    insert_human_review(complaint_id)
    try:
        result = record_human_decision(
            complaint_id,
            DecisionSubmission(decision="REJECT", note="Customer confirmed travel"),
        )
        assert result["decision"] == "REJECT"
        assert [item.event_type for item in published] == ["HumanDecisionSubmitted"]
        assert published[0].payload["decision"] == "REJECT"
        queues = review_queues()
        assert complaint_id not in ids(queues["authorization"])
    finally:
        cleanup(complaint_id)


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_fact_queue_answer_resumes_paused_investigation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _tools, published = make_runtime(FakeTools(CUST_1003_TXNS))
    event = classified_event(
        customer_id="CUST-1003",
        complaint_type="DUPLICATE_CHARGE",
        amount=None,
    )
    insert_complaint(event.correlation_id, "CUST-1003")
    assert runtime.handle_complaint_classified(event) is True
    assert [item.event_type for item in published] == ["InvestigationStarted"]

    queues = review_queues()
    assert event.correlation_id in ids(queues["needsInformation"])
    assert event.correlation_id not in ids(queues["authorization"])

    class ResumeClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

        async def __aenter__(self) -> ResumeClient:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def post(self, url: str, json: dict[str, str] | None = None) -> object:
            complaint_id = url.rsplit("/", 1)[-1]
            result = runtime.resume(complaint_id, (json or {})["answer"])

            class Response:
                status_code = 200

                def raise_for_status(self) -> None:
                    return None

                def json(self) -> dict[str, object]:
                    return result

            return Response()

    monkeypatch.setattr("poc.complaint_api.httpx.AsyncClient", ResumeClient)
    monkeypatch.setattr("poc.complaint_api.initialize_kafka", lambda: None)
    try:
        with TestClient(app) as client:
            response = client.post(
                f"/api/facts/{event.correlation_id}/answer",
                json={"answer": "TXN-1003-B"},
            )
        assert response.status_code == 200
        assert [item.event_type for item in published] == [
            "InvestigationStarted",
            "InvestigationCompleted",
        ]
        queues = review_queues()
        assert event.correlation_id not in ids(queues["needsInformation"])
    finally:
        cleanup(event.correlation_id)
