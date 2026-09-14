from __future__ import annotations

import logging
import os
from typing import Any, Callable

from openai import OpenAI
from pydantic import BaseModel, Field

from poc.db import claim_event, connection, write_trace
from poc.events import COMPLAINTS_TOPIC, EventEnvelope
from poc.kafka import publish, run_consumer
from poc.llm import OPENAI_MODEL, llm_stub_enabled


logger = logging.getLogger(__name__)
CONSUMER_NAME = "triage-agent"
DUPLICATE_MESSAGE = "I was charged ₹5,000 twice for the same payment."
UNRECOGNIZED_MESSAGE = "I do not recognize this ₹50,000 transaction."
AMBIGUOUS_DUPLICATE_MESSAGE = "I have been charged twice for the same payment."
Publisher = Callable[[EventEnvelope], None]


class ExtractedEntities(BaseModel):
    amount: float | int | None = None


class ComplaintClassification(BaseModel):
    category: str
    sub_category: str = Field(alias="subCategory")
    entities: ExtractedEntities = Field(default_factory=ExtractedEntities)
    priority: str
    required_capabilities: list[str] = Field(alias="requiredCapabilities")

    def as_payload(self, complaint_id: str, customer_id: str) -> dict[str, Any]:
        amount = self.entities.amount
        if isinstance(amount, float) and amount.is_integer():
            amount = int(amount)
        return {
            "complaintId": complaint_id,
            "customerId": customer_id,
            "category": self.category,
            "subCategory": self.sub_category,
            "entities": {"amount": amount},
            "priority": self.priority,
            "requiredCapabilities": self.required_capabilities,
        }


STUB_CLASSIFICATIONS: dict[str, ComplaintClassification] = {
    "CUST-1001": ComplaintClassification(
        category="PAYMENT_DISPUTE",
        subCategory="DUPLICATE_CHARGE",
        entities=ExtractedEntities(amount=5000),
        priority="MEDIUM",
        requiredCapabilities=["TRANSACTION_LOOKUP", "REFUND_STATUS"],
    ),
    "CUST-1002": ComplaintClassification(
        category="PAYMENT_DISPUTE",
        subCategory="UNRECOGNIZED_TRANSACTION",
        entities=ExtractedEntities(amount=50000),
        priority="HIGH",
        requiredCapabilities=["TRANSACTION_LOOKUP", "REFUND_STATUS"],
    ),
    # The customer states no amount, so nothing here narrows the complaint to one charge.
    "CUST-1003": ComplaintClassification(
        category="PAYMENT_DISPUTE",
        subCategory="DUPLICATE_CHARGE",
        entities=ExtractedEntities(amount=None),
        priority="MEDIUM",
        requiredCapabilities=["TRANSACTION_LOOKUP", "REFUND_STATUS"],
    ),
}


def stub_classification(
    customer_id: str, message: str
) -> ComplaintClassification | None:
    text = message.strip()
    if text == DUPLICATE_MESSAGE:
        return STUB_CLASSIFICATIONS["CUST-1001"]
    if text == UNRECOGNIZED_MESSAGE:
        return STUB_CLASSIFICATIONS["CUST-1002"]
    if text == AMBIGUOUS_DUPLICATE_MESSAGE:
        return STUB_CLASSIFICATIONS["CUST-1003"]
    return STUB_CLASSIFICATIONS.get(customer_id)


def classify_with_openai(
    customer_id: str,
    message: str,
    client: OpenAI | None = None,
) -> ComplaintClassification:
    openai_client = client or OpenAI()
    response = openai_client.chat.completions.parse(
        model=OPENAI_MODEL,
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": (
                    "Classify a banking customer complaint. "
                    "Return category, subCategory, entities.amount when present, "
                    "priority (LOW, MEDIUM, or HIGH), and requiredCapabilities "
                    "drawn from TRANSACTION_LOOKUP, REFUND_STATUS, and POLICY_ASSESSMENT. "
                    "Use PAYMENT_DISPUTE / DUPLICATE_CHARGE for duplicate charges "
                    "and PAYMENT_DISPUTE / UNRECOGNIZED_TRANSACTION for unrecognized payments."
                ),
            },
            {
                "role": "user",
                "content": f"customerId={customer_id}\nmessage={message}",
            },
        ],
        response_format=ComplaintClassification,
    )
    parsed = response.choices[0].message.parsed
    if parsed is None:
        raise RuntimeError("OpenAI returned an empty classification")
    return parsed


def classify_complaint(
    customer_id: str,
    message: str,
    *,
    stub: bool | None = None,
    client: OpenAI | None = None,
) -> ComplaintClassification:
    use_stub = llm_stub_enabled() if stub is None else stub
    if use_stub:
        fixture = stub_classification(customer_id, message)
        if fixture is None:
            raise RuntimeError(
                "LLM_STUB is enabled but no fixture matches this complaint"
            )
        return fixture
    return classify_with_openai(customer_id, message, client)


def handle_complaint_received(
    event: EventEnvelope,
    publisher: Publisher = publish,
    *,
    stub: bool | None = None,
    client: OpenAI | None = None,
) -> bool:
    if event.event_type != "ComplaintReceived":
        return False

    payload = event.payload
    customer_id = str(payload.get("customerId") or "")
    message = str(payload.get("message") or "")
    complaint_id = str(payload.get("complaintId") or event.correlation_id)

    with connection() as conn:
        if not claim_event(conn, CONSUMER_NAME, event.event_id):
            logger.info("Skipping duplicate event %s", event.event_id)
            return False
        classification = classify_complaint(
            customer_id, message, stub=stub, client=client
        )
        classified_payload = classification.as_payload(complaint_id, customer_id)
        classified = EventEnvelope.create(
            "ComplaintClassified",
            event.correlation_id,
            classified_payload,
        )
        write_trace(
            event.correlation_id,
            CONSUMER_NAME,
            "ComplaintClassified",
            detail=classified_payload,
            conn=conn,
        )
        try:
            publisher(classified)
        except Exception:
            conn.rollback()
            raise
        conn.commit()
    logger.info(
        "Classified %s as %s/%s",
        complaint_id,
        classified_payload["category"],
        classified_payload["subCategory"],
    )
    return True


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    run_consumer(
        name=CONSUMER_NAME,
        topics=[COMPLAINTS_TOPIC],
        handler=handle_complaint_received,
    )


if __name__ == "__main__":
    main()
