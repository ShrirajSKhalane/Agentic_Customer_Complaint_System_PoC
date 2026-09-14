# Event-Driven Agentic Customer Complaint PoC

This proof of concept resolves seeded banking complaints while making each
integration boundary visible:

- Kafka carries asynchronous complaint lifecycle events between applications.
- LangGraph runs the stateful investigation workflow inside `investigation-agent`.
- MCP provides banking data, policy data, and the refund operation.
- A2A connects `investigation-agent` to the specialist `policy-agent`.
- Deterministic Python rules—not an LLM—authorize automatic refunds.
- Human review uses either a LangGraph fact interrupt or Kafka authorization,
  depending on why human input is required.

The stack contains seven application processes (`complaint-api`, `triage-agent`,
`investigation-agent`, `policy-agent`, `resolution-agent`,
`notification-service`, and `mcp-server`) plus Kafka and PostgreSQL.

## Prerequisites

- Docker Desktop with Docker Compose
- Ports `8000`, `8001`, `8002`, `8003`, `9092`, and `5432` available
- An OpenAI API key (`OPENAI_API_KEY`) unless you enable stub mode

## Start the stack

Live OpenAI is the default. Set a key, then start Compose:

```powershell
$env:OPENAI_API_KEY = "your-key"
docker compose up --build -d
docker compose ps
```

Wait until Kafka, PostgreSQL, `complaint-api`, `investigation-agent`,
`policy-agent`, and `mcp-server` report healthy. Open
<http://localhost:8000>.

For deterministic seeded fixtures (no live model calls):

```powershell
$env:LLM_STUB = "1"
docker compose up --build -d
```

Useful configuration:

- `LLM_STUB` defaults to `0`. Classification, investigation plans, and
  policy assessments use OpenAI `gpt-4o-mini`. Duplicate matching and refund
  authorization stay in Python.
- `LLM_STUB=1` uses fixtures for the seeded customers. Use this for a
  recorded talk that must not depend on model drift.
- `OPENAI_API_KEY` is required when `LLM_STUB` is not enabled.
- `AUTO_REFUND_LIMIT_INR` defaults to `10000`.

Follow service output with:

```powershell
docker compose logs -f complaint-api triage-agent investigation-agent policy-agent resolution-agent notification-service
```

Stop the stack with `docker compose down`. To reset all demo data, including
refunds and LangGraph checkpoints, use `docker compose down -v` and then start
the stack again.

## Demo 1: Ananya Rao automatic refund

1. Open **New complaint**.
2. Select **Ananya Rao**. Type `I was charged ₹5,000 twice for the same
   payment.`
3. Submit and copy the displayed complaint reference.
4. Open **Case history**. The reference is pre-filled; the view refreshes
   every two seconds.
5. Confirm the chronological trace contains:
   `ComplaintReceived`, `ComplaintClassified`, `InvestigationStarted`,
   `LLM:PLAN_INVESTIGATION`, `MCP:get_transactions`, `MCP:get_customer`,
   `MCP:get_refund_status`, `A2A:ASSESS_POLICY`, `InvestigationCompleted`,
   `RefundRequired`, `MCP:initiate_refund`, `RefundCompleted`,
   `ComplaintResolved`, and `NotificationSent`.
6. Confirm the customer message says a duplicate charge of ₹5,000 was
   identified and a refund was initiated.

This path passes the deterministic gates: confidence is at least 0.90, policy
permits the action, ₹5,000 is within the ₹10,000 limit, and no fraud flag is
present.

## Demo 2: Vikram Singh human review

1. Open **New complaint**, select **Vikram Singh**, and type `I do not
   recognize this ₹50,000 transaction.`
2. Submit and copy the complaint reference.
3. Open **Review** and wait for the case under **Needs authorization**.
4. Before deciding, inspect **Case history**. It contains
   `HumanReviewRequired` but no `RefundRequired`, `MCP:initiate_refund`, or
   `RefundCompleted`. The ₹50,000 amount exceeds the automatic limit and the
   seeded policy requires human authorization.
5. Return to **Review**, optionally add a note, then choose:
   - **Approve** to initiate the refund through MCP and resolve the complaint.
   - **Reject** to resolve without a refund.
   - **Request more information** to publish `InvestigationRequested` and run
     a new investigation carrying the reviewer note; that decision alone never
     refunds.
6. Reopen the case history to confirm the selected disposition and message.

## Demo 3: Rohit Desai missing charge

Rohit has two separate duplicate pairs (₹4,500 at Skyline Foods and ₹8,900 at
Trailhead Outdoors). If the complaint does not name an amount, investigation
cannot tell which pair is meant and asks a person. There is no demo checkbox;
the pause comes from the data.

1. Select **Rohit Desai** and type `I have been charged twice for the same
   payment.`
2. Open **Review**. The case appears only under **Needs information**.
3. Choose the ₹4,500 Skyline Foods charge and submit the answer.
4. The same investigation continues with its prior evidence and completes.

## Architecture and protocol boundaries

Kafka topics contain only lifecycle envelopes:

- `complaints`: `ComplaintReceived`, `ComplaintClassified`
- `investigations`: `InvestigationStarted`, `InvestigationCompleted`,
  `InvestigationRequested`
- `resolutions`: `RefundRequired`, `RefundCompleted`, `ComplaintResolved`,
  `CustomerExplanationRequired`
- `human-review`: `HumanReviewRequired`, `HumanDecisionSubmitted`
- `notifications`: `NotificationRequested`, `NotificationSent`

MCP calls and A2A tasks never become Kafka messages. The portal reads
PostgreSQL `event_trace`, where protocol hops are deliberately labeled
`MCP:<tool>`, `A2A:ASSESS_POLICY`, and `LLM:PLAN_INVESTIGATION`. This makes
direct network calls visible without mixing their payloads into the event
backbone.

A full architecture thesis (decisions, rejected alternatives, and trade-offs, in the same narrative shape as a production write-up) is in
[`docs/thesis-architectural-decisions.md`](docs/thesis-architectural-decisions.md).
Shorter briefings: [`docs/architecture-briefing.md`](docs/architecture-briefing.md) and
[`docs/architecture-briefing-explained.md`](docs/architecture-briefing-explained.md).

The complete request path is:

```text
Browser -> complaint-api -> Kafka complaints -> triage-agent
        -> Kafka complaints -> investigation-agent (LangGraph)
             -> mcp-server (MCP tools)
             -> policy-agent (A2A) -> mcp-server (MCP policy lookup)
        -> Kafka investigations -> resolution-agent
             -> mcp-server (MCP refund, only after deterministic gates/approval)
        -> Kafka resolutions -> notification-service
        -> Kafka notifications + PostgreSQL trace -> Browser
```

## Verification

Run the test suite in the application image:

```powershell
docker compose run --rm --no-deps --build complaint-api sh -c "pip install -q pytest pytest-asyncio && PYTHONPATH=/app pytest -q"
```

The end-to-end hardening tests drive both seeded journeys, query the same trace
and queue functions used by the portal, assert no CUST-1002 refund before human
approval, and reject MCP/A2A names as Kafka lifecycle event types.
