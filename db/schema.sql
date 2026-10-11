CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    po_number VARCHAR(50) UNIQUE NOT NULL,
    retailer_name VARCHAR(255) NOT NULL,
    sku VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    status VARCHAR(50) NOT NULL DEFAULT 'received',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_pipeline_state (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_processed_at TIMESTAMPTZ,
    last_processed_change_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS data_quality_results (
    id BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    status VARCHAR(10) NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    failure_count BIGINT NOT NULL CHECK (failure_count >= 0),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_data_quality_results_checked_at
    ON data_quality_results (checked_at DESC);

ALTER TABLE analytics_pipeline_state
    ADD COLUMN IF NOT EXISTS last_processed_change_id BIGINT;

CREATE TABLE IF NOT EXISTS orders_change_log (
    change_id BIGSERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    affected_sale_date DATE NOT NULL,
    operation VARCHAR(20) NOT NULL CHECK (
        operation IN ('BOOTSTRAP', 'INSERT', 'UPDATE', 'DELETE')
    ),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_orders_change_log_date
    ON orders_change_log (affected_sale_date, change_id);

CREATE INDEX IF NOT EXISTS idx_orders_change_log_order
    ON orders_change_log (order_id);

CREATE OR REPLACE FUNCTION record_order_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO orders_change_log (
            order_id,
            affected_sale_date,
            operation
        )
        VALUES (OLD.id, OLD.created_at::date, 'DELETE');
        RETURN OLD;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        INSERT INTO orders_change_log (
            order_id,
            affected_sale_date,
            operation
        )
        VALUES (NEW.id, NEW.created_at::date, 'UPDATE');

        IF OLD.created_at::date IS DISTINCT FROM NEW.created_at::date THEN
            INSERT INTO orders_change_log (
                order_id,
                affected_sale_date,
                operation
            )
            VALUES (OLD.id, OLD.created_at::date, 'UPDATE');
        END IF;

        RETURN NEW;
    END IF;

    INSERT INTO orders_change_log (
        order_id,
        affected_sale_date,
        operation
    )
    VALUES (NEW.id, NEW.created_at::date, 'INSERT');
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER orders_change_capture
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW
EXECUTE FUNCTION record_order_change();

-- Seed pre-existing orders once so the first change-ID run reconciles history.
INSERT INTO orders_change_log (
    order_id,
    affected_sale_date,
    operation
)
SELECT
    o.id,
    o.created_at::date,
    'BOOTSTRAP'
FROM orders o
WHERE NOT EXISTS (
    SELECT 1
    FROM orders_change_log c
    WHERE c.order_id = o.id
);

INSERT INTO analytics_pipeline_state (
    pipeline_name,
    last_processed_at,
    last_processed_change_id
)
VALUES (
    'retail_daily_analytics',
    NULL,
    NULL
)
ON CONFLICT (pipeline_name) DO NOTHING;
