## Why

We need a one-week proof of concept that makes the architecture split obvious: Kafka between components, LangGraph inside an agent, MCP for tools/data, A2A for agent collaboration, LLMs for reasoning, and deterministic code for safety-critical actions. A greenfield complaint-resolution demo is the smallest business slice that still exercises that split, including human-in-the-loop.

## What Changes

- Add a Docker Compose stack with Kafka, PostgreSQL, and **seven application processes**: complaint-api, triage-agent, investigation-agent, policy-agent, resolution-agent, notification-service, mcp-server.
- Add a complaint lifecycle on Kafka (`complaints`, `investigations`, `resolutions`, `human-review`, `notifications`) with a shared event envelope and stable `correlationId`.
- Add LLM-based triage (`gpt-4o-mini`) and a LangGraph investigation agent that calls official MCP tools and the Policy Agent over official A2A.
- Add deterministic resolution gates (auto-refund vs escalate) and MCP-based refund execution.
- Add dual HITL: LangGraph interrupt for missing facts, Kafka `HumanReviewRequired` / `HumanDecisionSubmitted` for authorization; `REQUEST_MORE_INFORMATION` re-enters investigation.
- Add a basic HTML demo UI in complaint-api (customer submit, two reviewer queues, correlation trace, notification pane).
- Seed mock customer/transaction/policy data. No real banking, auth, Kubernetes, or production UI.

## Capabilities

### New Capabilities

- `complaint-event-model`: Shared event envelope, topics, correlation, idempotent consumers.
- `complaint-portal`: Ingest API, customer submit UI, reviewer UI, trace/notification panes; proxies LangGraph resume.
- `triage-agent`: Classify complaints with OpenAI and publish `ComplaintClassified`.
- `business-tools-mcp`: Official MCP server over HTTP for customer, account, transaction, refund, and policy tools on PostgreSQL.
- `investigation-agent`: LangGraph investigation with MCP + A2A, Postgres checkpoints, fact-level interrupt/resume.
- `policy-agent`: Official A2A server that retrieves policy via MCP and returns structured eligibility.
- `complaint-resolution`: Deterministic authorization, auto-refund, Kafka HITL disposition, notifications.

### Modified Capabilities

- (none — no existing specs)

## Impact

- New Python services and `docker-compose.yml`; no existing application code.
- External: Kafka, PostgreSQL, OpenAI API (`gpt-4o-mini`), official MCP and A2A SDKs.
- Operators need `OPENAI_API_KEY` (optional `LLM_STUB=1` for canned demo replay).
- Demo-only: mock data, log/UI notifications, no production identity or payment rails.
