## Purpose

Classifies inbound complaints with an LLM so downstream investigation receives a structured category, entities, priority, and required capabilities.

## ADDED Requirements

### Requirement: Classify ComplaintReceived
When `ComplaintReceived` is consumed, the triage agent MUST produce a structured classification including `category`, `subCategory`, extracted entities (including amount when present), `priority`, and `requiredCapabilities`, then MUST publish `ComplaintClassified` with the same `correlationId`.

#### Scenario: Duplicate charge classification
- **WHEN** the message is "I was charged ₹5,000 twice for the same payment."
- **THEN** `ComplaintClassified` is published with category PAYMENT_DISPUTE, subCategory DUPLICATE_CHARGE, and amount 5000

### Requirement: OpenAI default with stub fallback
Classification MUST use OpenAI model `gpt-4o-mini` when `LLM_STUB` is not enabled. When `LLM_STUB` is enabled, the agent MUST still publish `ComplaintClassified` for the two seeded demo scenarios without calling the live API.

#### Scenario: Stub mode seeded duplicate
- **WHEN** `LLM_STUB` is enabled and the duplicate ₹5,000 complaint is received
- **THEN** `ComplaintClassified` is still published for that complaint without a live LLM call
