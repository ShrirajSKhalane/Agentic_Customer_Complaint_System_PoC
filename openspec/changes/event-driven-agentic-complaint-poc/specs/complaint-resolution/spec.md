## Purpose

Turns investigation findings into a safe automatic refund or a Kafka-mediated human disposition, then notifies the customer and records the outcome.

## ADDED Requirements

### Requirement: Deterministic auto-refund gates
When `InvestigationCompleted` is consumed, automatic refund MUST occur only when all of the following hold: `confidence` >= 0.90, `policyEligible` is true, refund amount is at most the configured automatic limit, and `fraudFlag` is false. Meeting those gates MUST result in MCP `initiate_refund` and events `RefundRequired` then `RefundCompleted` (or equivalent sequenced refund events) and `ComplaintResolved` for the same `correlationId`. The LLM MUST NOT be the sole authority to execute a refund.

#### Scenario: Happy-path auto refund
- **WHEN** investigation reports DUPLICATE_TRANSACTION amount 5000, not refunded, policyEligible true, confidence at least 0.90, fraudFlag false, and amount at or below the auto limit
- **THEN** a refund is initiated once via MCP and the customer journey shows refund completed and complaint resolved

### Requirement: Kafka human review for authorization
When automatic gates fail (including confidence below 0.90, contradictory evidence, amount above the auto limit, or ineligible/ambiguous policy), the system MUST publish `HumanReviewRequired` and MUST NOT initiate a refund until a human decision allows it. APPROVE MUST proceed with the proposed refund or documented action; REJECT MUST resolve without refund; REQUEST_MORE_INFORMATION MUST cause investigation to run again for that complaint with the reviewer’s note.

#### Scenario: Unrecognized high-value transaction
- **WHEN** investigation of the ₹50,000 unrecognized transaction completes with low confidence or failed gates
- **THEN** `HumanReviewRequired` is published and no refund is initiated

#### Scenario: Request more information
- **WHEN** a reviewer submits REQUEST_MORE_INFORMATION with a note
- **THEN** investigation runs again for that correlation id including the note, and no refund occurs from that decision alone

### Requirement: Customer notification
The notification service MUST consume terminal outcomes (`RefundCompleted`, `ComplaintResolved`, and human decisions that close or explain the case) and MUST record a customer-visible message. For the duplicate auto-refund path the message MUST state that a duplicate charge of ₹5,000 was identified and a refund was initiated.

#### Scenario: Duplicate refund notification
- **WHEN** refund completes for the duplicate ₹5,000 complaint
- **THEN** the portal notification pane shows a message that a duplicate ₹5,000 charge was identified and a refund was initiated
