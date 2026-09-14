CREATE TABLE IF NOT EXISTS complaints (
    complaint_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_name TEXT NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE IF NOT EXISTS event_trace (
    id BIGSERIAL PRIMARY KEY,
    correlation_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    agent TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS event_trace_correlation_idx
    ON event_trace (correlation_id, created_at, id);

CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    risk_level TEXT NOT NULL DEFAULT 'LOW',
    fraud_flag BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES customers(customer_id),
    account_type TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    customer_id TEXT NOT NULL REFERENCES customers(customer_id),
    amount NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL DEFAULT 'INR',
    merchant TEXT NOT NULL,
    transaction_time TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'SETTLED'
);
CREATE INDEX IF NOT EXISTS transactions_customer_idx
    ON transactions (customer_id, transaction_time);

CREATE TABLE IF NOT EXISTS refunds (
    refund_id UUID PRIMARY KEY,
    transaction_id TEXT NOT NULL REFERENCES transactions(transaction_id),
    amount NUMERIC(12,2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'INITIATED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (transaction_id, amount)
);

CREATE TABLE IF NOT EXISTS policies (
    policy_key TEXT PRIMARY KEY,
    complaint_type TEXT NOT NULL,
    text TEXT NOT NULL,
    auto_refund_limit NUMERIC(12,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_facts (
    complaint_id TEXT PRIMARY KEY REFERENCES complaints(complaint_id),
    prompt TEXT NOT NULL,
    state JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS human_reviews (
    complaint_id TEXT PRIMARY KEY REFERENCES complaints(complaint_id),
    reason TEXT NOT NULL,
    proposed_action JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    decision TEXT,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS notifications (
    id BIGSERIAL PRIMARY KEY,
    complaint_id TEXT NOT NULL REFERENCES complaints(complaint_id),
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS investigation_runs (
    complaint_id TEXT PRIMARY KEY REFERENCES complaints(complaint_id),
    status TEXT NOT NULL,
    run_count INTEGER NOT NULL DEFAULT 1,
    reviewer_note TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO customers (customer_id, name, risk_level, fraud_flag) VALUES
    ('CUST-1001', 'Ananya Rao', 'LOW', FALSE),
    ('CUST-1002', 'Vikram Singh', 'LOW', FALSE),
    ('CUST-1003', 'Rohit Desai', 'LOW', FALSE)
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO accounts (account_id, customer_id, account_type, status) VALUES
    ('ACC-1001', 'CUST-1001', 'CURRENT', 'ACTIVE'),
    ('ACC-1002', 'CUST-1002', 'CURRENT', 'ACTIVE'),
    ('ACC-1003', 'CUST-1003', 'CURRENT', 'ACTIVE')
ON CONFLICT (account_id) DO NOTHING;

INSERT INTO transactions
    (transaction_id, account_id, customer_id, amount, merchant, transaction_time)
VALUES
    ('TXN-1001-A', 'ACC-1001', 'CUST-1001', 5000, 'Metro Electronics', '2026-08-30T10:00:00Z'),
    ('TXN-1001-B', 'ACC-1001', 'CUST-1001', 5000, 'Metro Electronics', '2026-08-30T10:02:00Z'),
    ('TXN-1002-A', 'ACC-1002', 'CUST-1002', 50000, 'Global Travel', '2026-08-31T15:30:00Z'),
    ('TXN-1003-A', 'ACC-1003', 'CUST-1003', 4500, 'Skyline Foods', '2026-09-02T13:10:00Z'),
    ('TXN-1003-B', 'ACC-1003', 'CUST-1003', 4500, 'Skyline Foods', '2026-09-02T13:12:00Z'),
    ('TXN-1003-C', 'ACC-1003', 'CUST-1003', 8900, 'Trailhead Outdoors', '2026-09-06T18:40:00Z'),
    ('TXN-1003-D', 'ACC-1003', 'CUST-1003', 8900, 'Trailhead Outdoors', '2026-09-06T18:43:00Z')
ON CONFLICT (transaction_id) DO NOTHING;

INSERT INTO policies (policy_key, complaint_type, text, auto_refund_limit) VALUES
    (
        'DUPLICATE_CHARGE',
        'DUPLICATE_CHARGE',
        'A settled duplicate payment is eligible for refund when it has not already been refunded, fraud is not indicated, and the amount does not exceed INR 10,000.',
        10000
    ),
    (
        'UNRECOGNIZED_TRANSACTION',
        'UNRECOGNIZED_TRANSACTION',
        'Unrecognized transactions require human authorization and must not be automatically refunded.',
        0
    )
ON CONFLICT (policy_key) DO NOTHING;
