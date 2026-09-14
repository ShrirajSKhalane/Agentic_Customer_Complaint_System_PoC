from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

from poc.db import claim_event, connection, write_trace
from poc.events import EventEnvelope, HUMAN_REVIEW_TOPIC, RESOLUTIONS_TOPIC
from poc.kafka import publish, run_consumer


logger = logging.getLogger(__name__)
CONSUMER_NAME = "notification-service"

Publisher = Callable[[EventEnvelope], None]
EventClaimer = Callable[[EventEnvelope], bool]
NotificationWriter = Callable[[str, str], int | None]
Tracer = Callable[[str, str, str, str, dict[str, Any]], None]


def claim_once(event: EventEnvelope) -> bool:
    with connection() as conn:
        claimed = claim_event(conn, CONSUMER_NAME, event.event_id)
        conn.commit()
        return claimed


def write_notification(complaint_id: str, message: str) -> int:
    with connection() as conn:
        row = conn.execute(
            """
            INSERT INTO notifications (complaint_id, message)
            VALUES (%s, %s)
            RETURNING id
            """,
            (complaint_id, message),
        ).fetchone()
        conn.commit()
    if row is None:
        raise RuntimeError("Notification could not be recorded")
    return int(row["id"])


def trace(
    complaint_id: str,
    agent: str,
    action: str,
    status: str,
    detail: dict[str, Any],
) -> None:
    write_trace(complaint_id, agent, action, status=status, detail=detail)


def display_amount(value: Any) -> str:
    amount = float(value)
    return f"{amount:,.0f}" if amount.is_integer() else f"{amount:,.2f}"


def customer_message(event: EventEnvelope) -> str | None:
    payload = event.payload
    if event.event_type == "RefundCompleted":
        amount = display_amount(payload.get("amount") or 0)
        if payload.get("finding") == "DUPLICATE_TRANSACTION":
            return (
                f"We identified a duplicate charge of ₹{amount} and initiated a refund. "
                "The credit will appear according to your bank's processing time."
            )
        return f"We initiated a refund of ₹{amount} for your complaint."
    if event.event_type == "ComplaintResolved":
        if payload.get("resolution") == "AUTO_REFUND":
            return "Your complaint has been resolved and the approved refund is in progress."
        return "Your complaint has been resolved. Thank you for contacting us."
    if event.event_type == "CustomerExplanationRequired":
        return str(
            payload.get("message")
            or "We need additional information before we can complete your complaint."
        )
    if event.event_type == "HumanDecisionSubmitted":
        decision = str(payload.get("decision") or "").upper()
        if decision == "REJECT":
            note = str(payload.get("note") or "").strip()
            suffix = f" Reason: {note}" if note else ""
            return f"Your complaint was reviewed and closed without a refund.{suffix}"
        if decision == "REQUEST_MORE_INFORMATION":
            note = str(payload.get("note") or "").strip()
            suffix = f" Requested information: {note}" if note else ""
            return f"We need more information to continue reviewing your complaint.{suffix}"
    return None


class NotificationRuntime:
    def __init__(
        self,
        *,
        publisher: Publisher = publish,
        event_claimer: EventClaimer = claim_once,
        notification_writer: NotificationWriter = write_notification,
        tracer: Tracer = trace,
    ) -> None:
        self.publisher = publisher
        self.event_claimer = event_claimer
        self.notification_writer = notification_writer
        self.tracer = tracer

    def handle_terminal_event(self, event: EventEnvelope) -> bool:
        message = customer_message(event)
        if message is None:
            return False
        if not self.event_claimer(event):
            logger.info("Skipping duplicate terminal event %s", event.event_id)
            return False

        complaint_id = str(event.payload.get("complaintId") or event.correlation_id)
        notification_id = self.notification_writer(complaint_id, message)
        payload = {
            "complaintId": complaint_id,
            "message": message,
            "notificationId": notification_id,
            "sourceEventType": event.event_type,
        }
        sent = EventEnvelope.create("NotificationSent", complaint_id, payload)
        self.publisher(sent)
        self.tracer(
            complaint_id,
            CONSUMER_NAME,
            "NotificationSent",
            "COMPLETED",
            payload,
        )
        return True


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    runtime = NotificationRuntime()
    run_consumer(
        name=CONSUMER_NAME,
        topics=[RESOLUTIONS_TOPIC, HUMAN_REVIEW_TOPIC],
        handler=runtime.handle_terminal_event,
    )


if __name__ == "__main__":
    main()
