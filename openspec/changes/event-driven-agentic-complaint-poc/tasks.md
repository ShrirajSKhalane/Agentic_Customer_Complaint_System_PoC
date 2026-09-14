## 1. Infrastructure and event backbone

- [x] 1.1 Add `docker-compose.yml` with Kafka (KRaft), PostgreSQL, and seven app service stubs (complaint-api, triage-agent, investigation-agent, policy-agent, resolution-agent, notification-service, mcp-server) and verify `docker compose config` succeeds and all nine service names are listed
- [x] 1.2 Add shared `common/` event envelope, topic constants, Kafka produce/consume helpers, and Postgres idempotency + `event_trace` schema; verify a unit test round-trips envelope JSON and rejects a duplicate `(consumer_name, event_id)`
- [x] 1.3 Implement complaint-api health plus Kafka topic auto-create (or compose init) and verify compose up reaches healthy Kafka, Postgres, and complaint-api `/health`

## 2. Portal ingest and customer UI

- [x] 2.1 Implement POST ingest that persists a complaint and publishes `ComplaintReceived` on `complaints` before returning; verify the response includes complaint id and a consumer can read the event with matching `correlationId`
- [x] 2.2 Add customer HTML form with CUST-1001 / CUST-1002 dropdown, message field, and duplicate ₹5,000 plus unrecognized ₹50,000 shortcuts; verify both shortcuts submit and display a complaint id
- [x] 2.3 Add trace API + journey panel polling `event_trace` by complaint id; verify a manually inserted trace row appears in the UI

## 3. MCP server and seed data

- [x] 3.1 Implement official MCP HTTP server with tools `get_customer`, `get_account`, `get_transaction`, `get_transactions`, `get_refund_status`, `initiate_refund`, `get_policy` on PostgreSQL; verify an MCP client `list_tools` returns all seven names
- [x] 3.2 Seed CUST-1001 duplicate ₹5,000 pair (not refunded), CUST-1002 ₹50,000 with no duplicate, and duplicate-charge policy; verify `get_transactions` for each customer matches those facts
- [x] 3.3 Make `initiate_refund` unique on `(transaction_id, amount)`; verify a second identical call does not create a second refund row

## 4. Triage agent

- [x] 4.1 Consume `ComplaintReceived`, call OpenAI `gpt-4o-mini` structured classification, publish `ComplaintClassified`; verify the duplicate-charge message yields PAYMENT_DISPUTE / DUPLICATE_CHARGE / amount 5000
- [x] 4.2 Implement `LLM_STUB=1` fixtures for both seeded scenarios; verify stub mode publishes `ComplaintClassified` with no OpenAI HTTP call
- [x] 4.3 Write trace rows for triage and honor idempotency; verify duplicate `eventId` does not publish a second classification

## 5. Policy agent (A2A)

- [x] 5.1 Expose Policy Agent with official A2A Agent Card and `ASSESS_POLICY` task; verify a sample A2A client can submit a task and receive `{eligible, action, reason, confidence}`
- [x] 5.2 Policy Agent loads policy via MCP `get_policy` (not Kafka); verify ASSESS_POLICY for CUST-1001 DUPLICATE_CHARGE 5000 returns eligible true and action REFUND
- [x] 5.3 Add stub-mode policy assessment for the two seeded scenarios; verify stub returns structured JSON without OpenAI

## 6. Investigation agent (LangGraph + MCP + A2A)

- [x] 6.1 LangGraph workflow with Postgres checkpointer, thread id = complaint id, MCP client, A2A client; on `ComplaintClassified` publish `InvestigationStarted` then gather transactions/refund via MCP; verify CUST-1001 path calls those tools and records them in `event_trace`
- [x] 6.2 Invoke Policy Agent over A2A when eligibility matters and publish `InvestigationCompleted` (finding, amount, refundStatus, policyEligible, confidence); verify CUST-1001 completes with DUPLICATE_TRANSACTION, not refunded, policyEligible true, confidence >= 0.90
- [x] 6.3 Skip starting a second graph if a checkpoint already exists for the complaint id; verify redelivered `ComplaintClassified` does not duplicate MCP refund lookups as a new investigation
- [x] 6.4 Fact interrupt: pause when a required fact is missing, expose pending prompt via investigation-agent API, complaint-api proxies resume; verify no `InvestigationCompleted` until resume, then prior MCP evidence is still in state
- [x] 6.5 Add a demo path that can force one fact interrupt without stalling the default ₹5,000 shortcut; verify the shortcut happy path still completes InvestigationCompleted without a reviewer

## 7. Resolution, refund, and notifications

- [x] 7.1 Resolution Agent consumes `InvestigationCompleted` and applies gates (confidence >= 0.90, policyEligible, amount <= AUTO_REFUND_LIMIT_INR default 10000, fraudFlag false); verify CUST-1001 calls MCP `initiate_refund` once and publishes refund/resolved events
- [x] 7.2 When gates fail, publish `HumanReviewRequired` and do not refund; verify CUST-1002 ₹50,000 produces human-review and zero refund rows
- [x] 7.3 Notification service consumes refund/resolved/closing human decisions, writes customer-visible copy and `NotificationSent` / trace; verify happy-path pane text mentions duplicate ₹5,000 and refund initiated

## 8. Kafka HITL and reviewer UI

- [x] 8.1 Reviewer UI with two queues (agent needs information vs authorization); verify a `HumanReviewRequired` complaint appears only on the authorization queue
- [x] 8.2 APPROVE / REJECT / REQUEST_MORE_INFORMATION publish `HumanDecisionSubmitted`; verify APPROVE then refunds via MCP, REJECT resolves without refund
- [x] 8.3 REQUEST_MORE_INFORMATION re-triggers investigation with the reviewer note; verify a new investigation run includes the note and does not refund from that decision alone
- [x] 8.4 Wire fact-queue answers to investigation `/resume`; verify a paused investigation completes after UI submit

## 9. End-to-end demo and hardening

- [x] 9.1 Script or documented UI walkthrough for CUST-1001 auto-refund; verify portal trace lists Received, classified, MCP tools, A2A Policy, InvestigationCompleted, refund, notification
- [x] 9.2 Script or documented UI walkthrough for CUST-1002 human review then APPROVE or REJECT; verify no auto-refund before the decision
- [x] 9.3 Confirm Kafka never carries MCP payloads or A2A tasks (topic contents / trace actions); verify tool and policy hops appear as MCP/A2A trace lines only
- [x] 9.4 README: compose up, `OPENAI_API_KEY`, `LLM_STUB`, two demo paths, architecture split; verify a new reader can start the stack from the README alone
