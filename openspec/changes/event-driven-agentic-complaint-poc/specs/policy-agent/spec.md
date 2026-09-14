## Purpose

Provides a specialized Policy Agent over A2A that loads policy through MCP and returns a structured eligibility assessment for the investigation.

## ADDED Requirements

### Requirement: A2A policy assessment
The Policy Agent MUST accept an A2A task `ASSESS_POLICY` with complaint context (including customer id, complaint type, and amount) and a question about automatic refund eligibility. It MUST retrieve policy via MCP `get_policy` and MUST return a structured result including `eligible`, `action`, `reason`, and `confidence`.

#### Scenario: Duplicate charge eligible
- **WHEN** Investigation sends ASSESS_POLICY for CUST-1001 DUPLICATE_CHARGE amount 5000
- **THEN** the result has eligible true, action REFUND, and a reason that the duplicate payment qualifies for automatic refund

### Requirement: Not a Kafka consumer for the question
Policy assessment MUST be served over A2A. The Policy Agent MUST NOT consume the complaint lifecycle Kafka topics to answer ASSESS_POLICY.

#### Scenario: Direct A2A
- **WHEN** an ASSESS_POLICY task is submitted over A2A
- **THEN** the assessment is returned on that A2A interaction without requiring a Kafka policy topic
