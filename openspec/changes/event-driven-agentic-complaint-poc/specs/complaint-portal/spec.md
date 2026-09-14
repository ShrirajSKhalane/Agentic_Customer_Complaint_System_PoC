## Purpose

Lets a customer submit a complaint in the browser and lets a reviewer act on paused cases, while showing the correlated event journey and customer-facing notification copy.

## ADDED Requirements

### Requirement: Customer can submit a complaint
The portal MUST provide an HTML form to submit `customerId` and a free-text complaint message. The portal MUST offer shortcuts that fill the duplicate ₹5,000 scenario (seeded customer CUST-1001) and the unrecognized ₹50,000 scenario (seeded customer CUST-1002). On success the portal MUST display the new complaint id.

#### Scenario: Happy-path shortcut
- **WHEN** the user activates the duplicate ₹5,000 shortcut and submits
- **THEN** the system accepts the complaint for CUST-1001 and shows a complaint id

#### Scenario: Escalation shortcut
- **WHEN** the user activates the unrecognized ₹50,000 shortcut and submits
- **THEN** the system accepts the complaint for CUST-1002 and shows a complaint id

### Requirement: Ingest publishes ComplaintReceived
Accepting a complaint MUST persist it and MUST publish `ComplaintReceived` on Kafka before the submit response completes successfully.

#### Scenario: Submit publishes event
- **WHEN** a valid complaint is submitted
- **THEN** a `ComplaintReceived` event exists for that complaint id with the submitted customer id and message

### Requirement: Customer journey view
After submit, the portal MUST show a chronological event trace for that `correlationId`, including agent actions, MCP tool names, A2A Policy Agent calls, resolution outcome, and notification copy when present. The portal MUST expose a notification pane listing messages that would be sent to the customer.

#### Scenario: Trace after auto-refund
- **WHEN** the duplicate-charge happy path completes
- **THEN** the journey view lists Received, classified, investigation, MCP and A2A steps, refund, and notification text

### Requirement: Reviewer queues
The portal MUST show two labeled reviewer queues: (1) agent needs information (LangGraph fact interrupt) and (2) authorization / disposition (Kafka human review). Submitting a fact answer MUST resume the paused investigation for that complaint. Submitting APPROVE, REJECT, or REQUEST_MORE_INFORMATION MUST publish `HumanDecisionSubmitted` for that complaint.

#### Scenario: Fact resume from UI
- **WHEN** a reviewer submits an answer on the agent-needs-information queue
- **THEN** investigation continues for that complaint without requiring a new customer submit

#### Scenario: Disposition from UI
- **WHEN** a reviewer submits APPROVE on the authorization queue
- **THEN** `HumanDecisionSubmitted` is published with decision APPROVE for that correlation id
