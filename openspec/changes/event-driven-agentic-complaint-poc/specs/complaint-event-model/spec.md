## Purpose

Defines the Kafka-backed complaint lifecycle events, shared envelope, correlation, and idempotent consumption that all services rely on.

## ADDED Requirements

### Requirement: Event envelope
Every published complaint-lifecycle event MUST include `eventId`, `eventType`, `timestamp`, `correlationId`, and `payload`. The `correlationId` MUST equal the complaint id and MUST remain constant for the entire complaint journey.

#### Scenario: Complaint received envelope
- **WHEN** a new complaint is accepted
- **THEN** the system publishes `ComplaintReceived` with a unique `eventId`, ISO-8601 `timestamp`, and `correlationId` equal to the new complaint id

#### Scenario: Downstream events keep correlation
- **WHEN** later lifecycle events are published for the same complaint
- **THEN** each event uses the same `correlationId` as the original `ComplaintReceived`

### Requirement: Topic mapping
The system MUST publish lifecycle events to these topics, distinguished by `eventType` within a topic: `complaints` (`ComplaintReceived`, `ComplaintClassified`), `investigations` (`InvestigationStarted`, `InvestigationCompleted`), `resolutions` (`RefundRequired`, `RefundCompleted`, `ComplaintResolved`, `CustomerExplanationRequired`), `human-review` (`HumanReviewRequired`, `HumanDecisionSubmitted`), `notifications` (`NotificationRequested`, `NotificationSent`). Kafka MUST NOT carry MCP tool calls or A2A tasks.

#### Scenario: Classified complaint uses complaints topic
- **WHEN** triage finishes classification
- **THEN** `ComplaintClassified` is published on `complaints` and not as an MCP or A2A message

### Requirement: Idempotent consumers
Each consumer MUST ignore a duplicate `eventId` already processed for that consumer, so retried Kafka delivery does not double-execute side effects (including refunds and notifications).

#### Scenario: Duplicate event id
- **WHEN** the same `eventId` is delivered twice to a consumer
- **THEN** the second delivery produces no additional side effect
