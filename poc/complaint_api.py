from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import httpx
import uvicorn
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from poc.db import connection, fetch_all, fetch_one, write_trace
from poc.events import EventEnvelope
from poc.kafka import ensure_topics, publish


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
INVESTIGATION_AGENT_URL = os.getenv(
    "INVESTIGATION_AGENT_URL", "http://localhost:8002"
)
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


def initialize_kafka() -> None:
    last_error: Exception | None = None
    for _ in range(30):
        try:
            ensure_topics()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError("Kafka topic initialization failed") from last_error


@asynccontextmanager
async def lifespan(_app: FastAPI):
    initialize_kafka()
    yield


app = FastAPI(title="Complaint Portal", lifespan=lifespan)


class ComplaintSubmission(BaseModel):
    customer_id: str = Field(alias="customerId")
    message: str = Field(min_length=3)


class DecisionSubmission(BaseModel):
    decision: str
    note: str = ""


class FactSubmission(BaseModel):
    answer: str = Field(min_length=1)


def accept_complaint(submission: ComplaintSubmission) -> str:
    complaint_id = f"CMP-{uuid4().hex[:8].upper()}"
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO complaints
                (complaint_id, customer_id, message)
            VALUES (%s, %s, %s)
            """,
            (
                complaint_id,
                submission.customer_id,
                submission.message,
            ),
        )
        write_trace(
            complaint_id,
            "complaint-api",
            "ComplaintReceived",
            detail={
                "customerId": submission.customer_id,
                "message": submission.message,
            },
            conn=conn,
        )
        conn.commit()

    event = EventEnvelope.create(
        "ComplaintReceived",
        complaint_id,
        {
            "complaintId": complaint_id,
            "customerId": submission.customer_id,
            "message": submission.message,
        },
    )
    try:
        publish(event)
    except Exception:
        # A successful response is forbidden unless the event was published.
        with connection() as conn:
            conn.execute(
                "DELETE FROM event_trace WHERE correlation_id = %s",
                (complaint_id,),
            )
            conn.execute(
                "DELETE FROM complaints WHERE complaint_id = %s",
                (complaint_id,),
            )
            conn.commit()
        raise
    return complaint_id


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def list_customers() -> list[dict[str, object]]:
    try:
        return fetch_all(
            """
            SELECT customer_id AS "customerId", name
            FROM customers
            ORDER BY name
            """
        )
    except Exception:
        logger.exception("Could not load customers for the complaint form")
        return []


@app.get("/", response_class=HTMLResponse)
def home(request: Request, complaint_id: str | None = None) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"complaint_id": complaint_id, "customers": list_customers()},
    )


@app.post("/api/complaints", status_code=201)
def submit_complaint(submission: ComplaintSubmission) -> dict[str, str]:
    return {"complaintId": accept_complaint(submission)}


@app.post("/complaints")
def submit_complaint_form(
    customer_id: str = Form(alias="customerId"),
    message: str = Form(),
) -> RedirectResponse:
    complaint_id = accept_complaint(
        ComplaintSubmission(customerId=customer_id, message=message)
    )
    return RedirectResponse(f"/?complaint_id={complaint_id}", status_code=303)


@app.get("/api/complaints/{complaint_id}/trace")
def complaint_trace(complaint_id: str) -> dict[str, object]:
    rows = fetch_all(
        """
        SELECT created_at AS timestamp, agent, action, status, detail
        FROM event_trace
        WHERE correlation_id = %s
        ORDER BY created_at, id
        """,
        (complaint_id,),
    )
    notifications = fetch_all(
        """
        SELECT created_at AS timestamp, message
        FROM notifications
        WHERE complaint_id = %s
        ORDER BY created_at, id
        """,
        (complaint_id,),
    )
    return {
        "complaintId": complaint_id,
        "trace": rows,
        "notifications": notifications,
    }


@app.get("/api/review-queues")
def review_queues() -> dict[str, object]:
    facts = fetch_all(
        """
        SELECT p.complaint_id AS "complaintId", c.customer_id AS "customerId",
               cu.name AS "customerName", p.prompt,
               p.state -> 'candidates' AS candidates,
               p.created_at AS "createdAt"
        FROM pending_facts p
        JOIN complaints c USING (complaint_id)
        LEFT JOIN customers cu ON cu.customer_id = c.customer_id
        ORDER BY p.created_at
        """
    )
    authorizations = fetch_all(
        """
        SELECT h.complaint_id AS "complaintId", c.customer_id AS "customerId",
               cu.name AS "customerName", h.reason,
               h.proposed_action AS "proposedAction",
               h.created_at AS "createdAt"
        FROM human_reviews h
        JOIN complaints c USING (complaint_id)
        LEFT JOIN customers cu ON cu.customer_id = c.customer_id
        WHERE h.status = 'PENDING'
        ORDER BY h.created_at
        """
    )
    return {"needsInformation": facts, "authorization": authorizations}


def record_human_decision(complaint_id: str, submission: DecisionSubmission) -> dict[str, str]:
    decision = submission.decision.upper()
    if decision not in {"APPROVE", "REJECT", "REQUEST_MORE_INFORMATION"}:
        raise HTTPException(status_code=422, detail="Invalid decision")
    review = fetch_one(
        """
        SELECT proposed_action, status
        FROM human_reviews
        WHERE complaint_id = %s
        """,
        (complaint_id,),
    )
    if review is None:
        raise HTTPException(status_code=404, detail="Review not found")
    if str(review["status"]) != "PENDING":
        raise HTTPException(status_code=409, detail="Review already decided")
    proposed = review["proposed_action"]
    publish(
        EventEnvelope.create(
            "HumanDecisionSubmitted",
            complaint_id,
            {
                "complaintId": complaint_id,
                "decision": decision,
                "note": submission.note,
                "proposedAction": proposed,
            },
        )
    )
    with connection() as conn:
        conn.execute(
            """
            UPDATE human_reviews
            SET status = 'DECIDED',
                decision = %s,
                note = %s,
                decided_at = NOW()
            WHERE complaint_id = %s AND status = 'PENDING'
            """,
            (decision, submission.note, complaint_id),
        )
        write_trace(
            complaint_id,
            "complaint-api",
            "HumanDecisionSubmitted",
            detail={"decision": decision, "note": submission.note},
            conn=conn,
        )
        conn.commit()
    return {"status": "submitted", "decision": decision}


@app.post("/api/reviews/{complaint_id}/decision")
def submit_decision(
    complaint_id: str, submission: DecisionSubmission
) -> dict[str, str]:
    return record_human_decision(complaint_id, submission)


@app.post("/api/facts/{complaint_id}/answer")
async def submit_fact(
    complaint_id: str, submission: FactSubmission
) -> dict[str, object]:
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{INVESTIGATION_AGENT_URL}/resume/{complaint_id}",
            json={"answer": submission.answer},
        )
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Pending fact not found")
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
