## Context

Greenfield repo: OpenSpec plus the architecture source `event_driven_agentic_customer_complaint_poc.md`. No application code yet. See proposal.md for motivation. Constraints: one-week PoC, Docker Compose only, mock data, OpenAI `gpt-4o-mini`, official MCP and A2A SDKs, seven application processes, dual HITL, basic HTML in complaint-api.

## Goals / Non-Goals

**Goals:**

- Make Kafka vs LangGraph vs MCP vs A2A vs deterministic code visible in logs and the portal trace.
- Run two scripted journeys from the UI: auto-refund (CUST-1001) and Kafka escalation (CUST-1002).
- Keep LangGraph interrupt as a distinct “needs a fact” path, with at least one seeded or forced interrupt for the demo.
- Survive Kafka redelivery (idempotent consumers) and investigation-container restart (Postgres checkpoints).

**Non-Goals:**

- Kubernetes, authn/authz, real payments, SMTP, production UI, advanced fraud, multi-region Kafka.
- Putting MCP or A2A payloads on Kafka.
- Letting the LLM execute refunds without the deterministic gates.

## Decisions

### 1. Seven application containers (literal topology)

**Choice:** One container per process: `complaint-api`, `triage-agent`, `investigation-agent`, `policy-agent`, `resolution-agent`, `notification-service`, `mcp-server`, plus Kafka (KRaft) and PostgreSQL.

**Why:** Independently deployable components is the Kafka story; MCP and A2A must be real network hops. Alternatives: modular monolith (weaker demo) or collapsing Policy into Investigation (A2A becomes a function call).

Shared `common/` Python package for event envelope, Kafka helpers, idempotency store, and trace writer. Each service is a thin consumer or HTTP server.

### 2. Kafka topics and envelope

Topics: `complaints`, `investigations`, `resolutions`, `human-review`, `notifications`. JSON envelope `{eventId, eventType, timestamp, correlationId, payload}`. `correlationId` = complaint id.

Idempotency: Postgres table `(consumer_name, event_id)` unique; process-once wrapper around every consumer handler.

### 3. MCP: official SDK, Streamable HTTP

**Choice:** Python MCP SDK FastMCP (or equivalent) served over Streamable HTTP from `mcp-server`. Investigation, Policy, and Resolution are MCP clients.

**Why:** Cross-process tools; stdio is awkward from Kafka consumers. Alternatives: stdio sidecar (ops heavy) or REST pretending to be MCP (fails fidelity).

Tools backed by PostgreSQL. Duplicate detection and amount math stay in Investigation as deterministic Python after `get_transactions` returns.

`initiate_refund` is the only refund side effect. Resolution Agent calls it after gates pass (or after APPROVE). Auto-refund limit default: **₹10,000** so ₹5,000 auto-qualifies and ₹50,000 cannot.

### 4. A2A: official SDK, Policy as server

**Choice:** Policy Agent exposes an A2A Agent Card + task API. Investigation submits `ASSESS_POLICY` and waits. Policy uses MCP `get_policy` then `gpt-4o-mini` structured output `{eligible, action, reason, confidence}`.

**Why:** A2A must not look like another Kafka topic. Alternative: JSON HTTP labeled A2A (weaker claim).

### 5. LangGraph inside Investigation only

**Choice:** Investigation Agent is the only LangGraph app. Checkpointer: Postgres. Thread id = complaint id.

Conditional edges: fetch transactions → deterministic duplicate check → refund status → A2A policy → confidence → complete or `interrupt()` for missing facts.

Triage and Resolution are LLM-optional but not graphs: triage is classify-and-publish; resolution is rules (+ optional LLM recommendation that cannot authorize).

### 6. Dual HITL split

| Mechanism | Job | Resume |
|---|---|---|
| LangGraph `interrupt()` | Missing *facts* | complaint-api proxies POST to investigation-agent `/resume` |
| Kafka `HumanReviewRequired` | Authorization | complaint-api publishes `HumanDecisionSubmitted` |

`REQUEST_MORE_INFORMATION` republishes a new investigation trigger (e.g. `ComplaintClassified` or `InvestigationRequested` on `investigations`) with the reviewer note in payload so LangGraph starts a follow-up thread or continues with extra state.

Demo interrupt: if entity extraction lacks `transaction_id` and more than one candidate exists, interrupt once with “which transaction?”. Shortcut scenarios should still complete without interrupt unless the reviewer demo is explicitly started (e.g. a third canned “ambiguous merchant” submit, or a “force interrupt” flag on the form). Default happy path MUST NOT stall on interrupt.

### 7. Portal in complaint-api

Jinja (or equivalent) HTML, no SPA framework. Tabs: Customer, Review, Trace. Poll `GET /api/complaints/{id}/trace`. Reviewer page lists both queues. No login.

Trace rows are written by every service to a Postgres `event_trace` table (correlationId, timestamp, agent, action, status, detail) so the UI does not parse Kafka.

### 8. LLM

OpenAI `gpt-4o-mini`, structured outputs, low temperature. `OPENAI_API_KEY` in Compose env. `LLM_STUB=1` returns fixtures for CUST-1001 / CUST-1002 classify and policy so a recording cannot fail on model drift.

### 9. Seed data

- CUST-1001: two ₹5,000 payments, same merchant/day, `refund_status=NOT_REFUNDED`.
- CUST-1002: one ₹50,000 payment, no duplicate, `fraudFlag=false` (escalation via amount and/or low confidence, not fraud models).
- Policy row: duplicate charge eligible for automatic refund at or below limit.

## Risks / Trade-offs

- [Seven-process local ops] → Healthchecks, explicit depends_on, Day 1 “ping Kafka round-trip” before LLM. Shared Compose network aliases.
- [A2A/MCP SDK churn] → Pin versions; isolate adapters in `common/` so protocol wiring is one module per SDK.
- [Dual HITL confusion] → Two UI queues with copy that names the mechanism; happy path never uses interrupt.
- [LLM non-determinism] → Structured outputs + stub mode + deterministic duplicate/refund math.
- [LangGraph interrupt + Kafka redelivery] → Idempotent start: if a checkpoint exists for complaint id, do not start a second graph from a duplicate `ComplaintClassified`.
- [Refund double-apply] → MCP refund unique on `(transaction_id, amount)` plus consumer idempotency.

## Migration Plan

Greenfield: `docker compose up --build` is the deploy. Rollback is `docker compose down -v` (wipes Kafka + Postgres). No production migration. Env: `OPENAI_API_KEY`, optional `LLM_STUB`, `AUTO_REFUND_LIMIT_INR=10000`.

## Open Questions

None that affect specs or task breakdown. Kafka image tag and exact MCP/A2A package versions can be chosen at implementation from current stable releases.
