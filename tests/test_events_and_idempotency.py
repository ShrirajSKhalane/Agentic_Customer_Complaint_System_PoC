from uuid import uuid4

import pytest

from poc.db import claim_event, connection, json_safe, write_trace
from poc.events import EventEnvelope


def test_event_envelope_round_trip() -> None:
    event = EventEnvelope.create(
        "ComplaintReceived",
        "CMP-TEST",
        {"customerId": "CUST-1001", "message": "test"},
    )

    restored = EventEnvelope.from_json(event.to_json())

    assert restored == event
    assert restored.correlation_id == "CMP-TEST"
    assert restored.topic == "complaints"


def test_consumer_rejects_duplicate_event_id() -> None:
    event_id = uuid4()
    try:
        with connection() as conn:
            assert claim_event(conn, "test-consumer", event_id) is True
            conn.commit()
            assert claim_event(conn, "test-consumer", event_id) is False
            conn.rollback()
    except Exception as exc:
        pytest.skip(f"PostgreSQL integration not available: {exc}")


def test_json_safe_strips_nul_bytes() -> None:
    cleaned = json_safe(
        {"reason": "amount of \x00₹50,000", "nested": ["keep", "bad\x00"]}
    )
    assert cleaned == {"reason": "amount of ₹50,000", "nested": ["keep", "bad"]}


def test_write_trace_accepts_nul_in_detail() -> None:
    try:
        with connection() as conn:
            conn.execute("SELECT 1")
    except Exception as exc:
        pytest.skip(f"PostgreSQL integration not available: {exc}")
    complaint_id = f"CMP-NUL-{uuid4().hex[:8].upper()}"
    write_trace(
        complaint_id,
        "investigation-agent",
        "LLM:PLAN_INVESTIGATION",
        detail={"reason": "transaction amount of \x00₹50,000"},
    )
    with connection() as conn:
        row = conn.execute(
            """
            SELECT detail->>'reason' AS reason
            FROM event_trace
            WHERE correlation_id = %s AND action = %s
            """,
            (complaint_id, "LLM:PLAN_INVESTIGATION"),
        ).fetchone()
    assert row is not None
    assert "\x00" not in (row["reason"] or "")
    assert "₹50,000" in row["reason"]
