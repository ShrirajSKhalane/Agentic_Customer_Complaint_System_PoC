from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


COMPLAINTS_TOPIC = "complaints"
INVESTIGATIONS_TOPIC = "investigations"
RESOLUTIONS_TOPIC = "resolutions"
HUMAN_REVIEW_TOPIC = "human-review"
NOTIFICATIONS_TOPIC = "notifications"

TOPICS = (
    COMPLAINTS_TOPIC,
    INVESTIGATIONS_TOPIC,
    RESOLUTIONS_TOPIC,
    HUMAN_REVIEW_TOPIC,
    NOTIFICATIONS_TOPIC,
)

EVENT_TOPICS = {
    "ComplaintReceived": COMPLAINTS_TOPIC,
    "ComplaintClassified": COMPLAINTS_TOPIC,
    "InvestigationStarted": INVESTIGATIONS_TOPIC,
    "InvestigationCompleted": INVESTIGATIONS_TOPIC,
    "InvestigationRequested": INVESTIGATIONS_TOPIC,
    "RefundRequired": RESOLUTIONS_TOPIC,
    "RefundCompleted": RESOLUTIONS_TOPIC,
    "ComplaintResolved": RESOLUTIONS_TOPIC,
    "CustomerExplanationRequired": RESOLUTIONS_TOPIC,
    "HumanReviewRequired": HUMAN_REVIEW_TOPIC,
    "HumanDecisionSubmitted": HUMAN_REVIEW_TOPIC,
    "NotificationRequested": NOTIFICATIONS_TOPIC,
    "NotificationSent": NOTIFICATIONS_TOPIC,
}


class EventEnvelope(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: UUID = Field(default_factory=uuid4, alias="eventId")
    event_type: str = Field(alias="eventType")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: str = Field(alias="correlationId")
    payload: dict[str, Any]

    @classmethod
    def create(
        cls, event_type: str, correlation_id: str, payload: dict[str, Any]
    ) -> "EventEnvelope":
        if event_type not in EVENT_TOPICS:
            raise ValueError(f"Unknown lifecycle event type: {event_type}")
        return cls(
            eventType=event_type,
            correlationId=correlation_id,
            payload=payload,
        )

    @property
    def topic(self) -> str:
        return EVENT_TOPICS[self.event_type]

    def to_json(self) -> str:
        return self.model_dump_json(by_alias=True)

    @classmethod
    def from_json(cls, value: str | bytes) -> "EventEnvelope":
        return cls.model_validate_json(value)
