CREATE TABLE IF NOT EXISTS automation_executions(
    id SERIAL PRIMARY KEY,
    po_number VARCHAR(50),
    workflow_name VARCHAR(100) NOT NULL,
    outcome VARCHAR(50) NOT NULL,
    error_type VARCHAR(200),
    message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
