## Purpose

Runs a stateful investigation per complaint: gather evidence via MCP, ask the Policy Agent via A2A, pause for missing facts, and publish findings with confidence.

## ADDED Requirements

### Requirement: Investigate classified complaints
When `ComplaintClassified` is consumed, the investigation agent MUST start a stateful workflow, publish `InvestigationStarted`, use MCP tools as needed (including transaction and refund lookup), request a policy assessment via A2A when eligibility matters, and publish `InvestigationCompleted` with findings, `policyEligible` when assessed, `refundStatus`, amount, and `confidence`, using the same `correlationId`.

#### Scenario: Duplicate charge investigation
- **WHEN** a DUPLICATE_CHARGE classification for CUST-1001 is consumed
- **THEN** investigation calls MCP transaction and refund tools, calls the Policy Agent over A2A, and publishes `InvestigationCompleted` with finding DUPLICATE_TRANSACTION, refund not completed, policyEligible true, and high confidence (at least 0.90)

### Requirement: Investigation plan chooses lookups, not money
When investigation starts, it MUST record a plan of lookup tools (`get_transactions` and optionally `get_customer`) and whether to ask which charge. Duplicate grouping and refund lookup MUST remain in code. When `LLM_STUB` is enabled, the plan MUST be a fixture for the seeded customers so Ananya and Vikram do not pause and Rohit does.

#### Scenario: Seeded Rohit plan asks which charge
- **WHEN** a DUPLICATE_CHARGE classification for CUST-1003 is consumed in stub mode
- **THEN** investigation writes `LLM:PLAN_INVESTIGATION` with ask-which-charge true, fetches transactions, and pauses until a reviewer names a charge

### Requirement: Fact-level human interrupt
When investigation cannot continue without a customer or reviewer fact, the workflow MUST pause and MUST make the pending prompt visible on the portal agent-needs-information queue. After a fact answer is submitted, the same investigation MUST resume from the pause without dropping prior collected evidence.

#### Scenario: Pause then resume
- **WHEN** the workflow determines a required fact is missing
- **THEN** no `InvestigationCompleted` is published until a reviewer answer is submitted, after which investigation continues with prior state intact

### Requirement: Kafka is not used for tools or policy questions
MCP tool invocations and A2A policy tasks MUST go to the MCP server and Policy Agent respectively. Investigation MUST persist workflow checkpoints so a process restart can resume an interrupted investigation for the same complaint.

#### Scenario: Policy via A2A not Kafka
- **WHEN** policy assessment is required
- **THEN** the Policy Agent is invoked over A2A and no A2A task is published as a Kafka lifecycle event
