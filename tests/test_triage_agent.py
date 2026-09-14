from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from poc.db import connection, fetch_all
from poc.events import EventEnvelope
from poc.llm import OPENAI_MODEL
from poc.triage_agent import (
    AMBIGUOUS_DUPLICATE_MESSAGE,
    CONSUMER_NAME,
    DUPLICATE_MESSAGE,
    UNRECOGNIZED_MESSAGE,
    classify_complaint,
    classify_with_openai,
    handle_complaint_received,
)


class FakeOpenAI:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(parse=self._parse))

    def _parse(self, **kwargs: object) -> SimpleNamespace:
        self.calls.append(kwargs)
        parsed = classify_complaint("CUST-1001", DUPLICATE_MESSAGE, stub=True)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(parsed=parsed))]
        )


def received_event(
    customer_id: str,
    message: str,
    *,
    complaint_id: str | None = None,
) -> EventEnvelope:
    complaint_id = complaint_id or f"CMP-{uuid4().hex[:8].upper()}"
    return EventEnvelope.create(
        "ComplaintReceived",
        complaint_id,
        {
            "complaintId": complaint_id,
            "customerId": customer_id,
            "message": message,
        },
    )


def postgres_available() -> bool:
    try:
        with connection() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def test_duplicate_charge_message_classifies_to_payment_dispute() -> None:
    result = classify_complaint("CUST-1001", DUPLICATE_MESSAGE, stub=True)
    payload = result.as_payload("CMP-1001", "CUST-1001")
    assert payload["category"] == "PAYMENT_DISPUTE"
    assert payload["subCategory"] == "DUPLICATE_CHARGE"
    assert payload["entities"]["amount"] == 5000


def test_ambiguous_duplicate_message_does_not_name_an_amount() -> None:
    result = classify_complaint("CUST-1003", AMBIGUOUS_DUPLICATE_MESSAGE, stub=True)
    payload = result.as_payload("CMP-1003", "CUST-1003")
    assert payload["category"] == "PAYMENT_DISPUTE"
    assert payload["subCategory"] == "DUPLICATE_CHARGE"
    assert payload["entities"]["amount"] is None


def test_openai_structured_classification_uses_gpt_4o_mini() -> None:
    client = FakeOpenAI()
    result = classify_with_openai("CUST-1001", DUPLICATE_MESSAGE, client=client)
    assert client.calls
    assert client.calls[0]["model"] == OPENAI_MODEL
    assert client.calls[0]["response_format"].__name__ == "ComplaintClassification"
    payload = result.as_payload("CMP-1001", "CUST-1001")
    assert payload["category"] == "PAYMENT_DISPUTE"
    assert payload["subCategory"] == "DUPLICATE_CHARGE"
    assert payload["entities"]["amount"] == 5000


def test_stub_mode_does_not_call_openai() -> None:
    client = FakeOpenAI()
    classify_complaint(
        "CUST-1001", DUPLICATE_MESSAGE, stub=True, client=client
    )
    classify_complaint(
        "CUST-1002", UNRECOGNIZED_MESSAGE, stub=True, client=client
    )
    assert client.calls == []


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_stub_handler_publishes_classified_without_openai() -> None:
    published: list[EventEnvelope] = []
    client = FakeOpenAI()
    event = received_event("CUST-1001", DUPLICATE_MESSAGE)
    handled = handle_complaint_received(
        event, published.append, stub=True, client=client
    )
    assert handled is True
    assert client.calls == []
    assert len(published) == 1
    classified = published[0]
    assert classified.event_type == "ComplaintClassified"
    assert classified.correlation_id == event.correlation_id
    assert classified.topic == "complaints"
    assert classified.payload["category"] == "PAYMENT_DISPUTE"
    assert classified.payload["subCategory"] == "DUPLICATE_CHARGE"
    assert classified.payload["entities"]["amount"] == 5000


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_duplicate_event_id_does_not_publish_second_classification() -> None:
    published: list[EventEnvelope] = []
    event = received_event("CUST-1002", UNRECOGNIZED_MESSAGE)
    assert handle_complaint_received(event, published.append, stub=True) is True
    assert handle_complaint_received(event, published.append, stub=True) is False
    assert len(published) == 1
    traces = fetch_all(
        """
        SELECT action FROM event_trace
        WHERE correlation_id = %s AND agent = %s
        """,
        (event.correlation_id, CONSUMER_NAME),
    )
    assert [row["action"] for row in traces] == ["ComplaintClassified"]
    with connection() as conn:
        claimed = conn.execute(
            """
            SELECT COUNT(*) AS n FROM processed_events
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, event.event_id),
        ).fetchone()
    assert claimed is not None
    assert claimed["n"] == 1
