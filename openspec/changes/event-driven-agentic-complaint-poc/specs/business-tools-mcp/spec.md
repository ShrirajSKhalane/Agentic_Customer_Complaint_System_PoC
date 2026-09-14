## Purpose

Exposes mock banking and policy data as MCP tools so agents retrieve customer, transaction, refund, and policy information through a standard tool interface rather than ad-hoc APIs.

## ADDED Requirements

### Requirement: MCP tool surface
The system MUST expose an MCP server that provides tools `get_customer`, `get_account`, `get_transaction`, `get_transactions`, `get_refund_status`, `initiate_refund`, and `get_policy`. Agents MUST use these tools for data and refund execution; those operations MUST NOT be performed over A2A or as Kafka payloads.

#### Scenario: List tools
- **WHEN** an MCP client lists tools on the server
- **THEN** the seven named tools are available

### Requirement: Seeded demo data
PostgreSQL MUST contain mock data such that CUST-1001 has two matching ₹5,000 payments (duplicate, not yet refunded) and CUST-1002 has a ₹50,000 transaction with no duplicate. Policy records MUST exist so duplicate-charge automatic refund can be assessed.

#### Scenario: Duplicate transactions for CUST-1001
- **WHEN** `get_transactions` is invoked for CUST-1001
- **THEN** the result includes two ₹5,000 charges that a deterministic duplicate check can treat as duplicates, and refund status is not refunded

#### Scenario: Unrecognized amount for CUST-1002
- **WHEN** `get_transactions` is invoked for CUST-1002
- **THEN** the result includes a ₹50,000 transaction and does not include a duplicate pair for that amount

### Requirement: Refund tool is idempotent
`initiate_refund` MUST record a refund for the given transaction and amount. Repeating the same transaction and amount MUST NOT create a second refund.

#### Scenario: Repeat initiate_refund
- **WHEN** `initiate_refund` is called twice with the same transaction id and amount
- **THEN** only one refund exists and the second call reports the existing refund
