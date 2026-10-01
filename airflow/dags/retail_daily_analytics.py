from datetime import timedelta

import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


PIPELINE_NAME = "retail_daily_analytics"


def _affected_dates_cte():
    return """
        SELECT DISTINCT affected_sale_date AS sale_date
        FROM orders_change_log
        WHERE change_id > %(lower_change_id)s
          AND change_id <= %(upper_change_id)s
    """


@dag(
    dag_id="retail_daily_analytics",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retail", "analytics"],
)
def retail_daily_analytics():

    @task
    def check_source_orders():
        hook = PostgresHook(postgres_conn_id="retail_postgres")
        order_count = hook.get_first("SELECT COUNT(*) FROM orders;")[0]

        # An empty source is valid after deletions; change capture will remove
        # any now-stale aggregate rows for the affected dates.
        print(f"Source contains {order_count} orders")


    @task
    def get_processing_window():
        hook = PostgresHook(postgres_conn_id="retail_postgres")

        # Keep this initialization defensive so deploying the DAG before
        # applying schema.sql does not leave changes invisible.
        hook.run([
            """
                CREATE TABLE IF NOT EXISTS analytics_pipeline_state (
                    pipeline_name VARCHAR(100) PRIMARY KEY,
                    last_processed_at TIMESTAMPTZ,
                    last_processed_change_id BIGINT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """,
            """
                ALTER TABLE analytics_pipeline_state
                    ADD COLUMN IF NOT EXISTS last_processed_change_id BIGINT;
            """,
            """
                CREATE TABLE IF NOT EXISTS orders_change_log (
                    change_id BIGSERIAL PRIMARY KEY,
                    order_id INTEGER NOT NULL,
                    affected_sale_date DATE NOT NULL,
                    operation VARCHAR(20) NOT NULL CHECK (
                        operation IN (
                            'BOOTSTRAP', 'INSERT', 'UPDATE', 'DELETE'
                        )
                    ),
                    changed_at TIMESTAMPTZ NOT NULL
                        DEFAULT clock_timestamp()
                );
            """,
            """
                CREATE INDEX IF NOT EXISTS idx_orders_change_log_date
                    ON orders_change_log (affected_sale_date, change_id);
            """,
            """
                CREATE INDEX IF NOT EXISTS idx_orders_change_log_order
                    ON orders_change_log (order_id);
            """,
            """
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
                        VALUES (
                            OLD.id,
                            OLD.created_at::date,
                            'DELETE'
                        );
                        RETURN OLD;
                    END IF;

                    IF TG_OP = 'UPDATE' THEN
                        INSERT INTO orders_change_log (
                            order_id,
                            affected_sale_date,
                            operation
                        )
                        VALUES (
                            NEW.id,
                            NEW.created_at::date,
                            'UPDATE'
                        );

                        IF OLD.created_at::date IS DISTINCT FROM
                                NEW.created_at::date THEN
                            INSERT INTO orders_change_log (
                                order_id,
                                affected_sale_date,
                                operation
                            )
                            VALUES (
                                OLD.id,
                                OLD.created_at::date,
                                'UPDATE'
                            );
                        END IF;

                        RETURN NEW;
                    END IF;

                    INSERT INTO orders_change_log (
                        order_id,
                        affected_sale_date,
                        operation
                    )
                    VALUES (
                        NEW.id,
                        NEW.created_at::date,
                        'INSERT'
                    );
                    RETURN NEW;
                END;
                $$;
            """,
            """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_trigger
                        WHERE tgname = 'orders_change_capture'
                          AND tgrelid = 'orders'::regclass
                          AND NOT tgisinternal
                    ) THEN
                        CREATE TRIGGER orders_change_capture
                        AFTER INSERT OR UPDATE OR DELETE ON orders
                        FOR EACH ROW
                        EXECUTE FUNCTION record_order_change();
                    END IF;
                END;
                $$;
            """,
            """
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
            """,
        ])

        hook.run("""
            INSERT INTO analytics_pipeline_state (
                pipeline_name,
                last_processed_at,
                last_processed_change_id
            )
            VALUES (%s, NULL, NULL)
            ON CONFLICT (pipeline_name) DO NOTHING;
        """, parameters=(PIPELINE_NAME,))

        state = hook.get_first("""
            SELECT
                COALESCE(last_processed_change_id, 0),
                last_processed_at
            FROM analytics_pipeline_state
            WHERE pipeline_name = %s;
        """, parameters=(PIPELINE_NAME,))

        lower_change_id = state[0]
        upper = hook.get_first("""
            SELECT change_id, changed_at
            FROM orders_change_log
            ORDER BY change_id DESC
            LIMIT 1;
        """)

        if upper is None:
            upper_change_id = 0
            upper_changed_at = None
        else:
            upper_change_id, upper_changed_at = upper

        if upper_change_id < lower_change_id:
            raise ValueError(
                "orders_change_log is behind the stored watermark; "
                "restore the missing change history or reset the pipeline "
                "state intentionally"
            )

        has_changes = upper_change_id > lower_change_id

        print(f"Lower change ID: {lower_change_id}")
        print(f"Upper change ID: {upper_change_id}")
        print(f"Pending source changes: {has_changes}")

        return {
            "lower_change_id": lower_change_id,
            "upper_change_id": upper_change_id,
            "upper_changed_at": (
                upper_changed_at.isoformat()
                if upper_changed_at is not None
                else None
            ),
            "has_changes": has_changes,
        }


    @task
    def build_daily_sales(window):
        if not window["has_changes"]:
            print("No source changes. daily_sales unchanged.")
            return

        hook = PostgresHook(postgres_conn_id="retail_postgres")
        affected_dates = _affected_dates_cte()

        hook.run("""
            CREATE TABLE IF NOT EXISTS daily_sales (
                sale_date DATE PRIMARY KEY,
                order_count INTEGER NOT NULL,
                total_units INTEGER NOT NULL,
                revenue NUMERIC(14,2) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        hook.run([
            f"""
                WITH affected_dates AS ({affected_dates})
                DELETE FROM daily_sales d
                USING affected_dates a
                WHERE d.sale_date = a.sale_date;
            """,
            f"""
                WITH affected_dates AS ({affected_dates})
                INSERT INTO daily_sales (
                    sale_date,
                    order_count,
                    total_units,
                    revenue,
                    updated_at
                )
                SELECT
                    o.created_at::date,
                    COUNT(*)::integer,
                    SUM(o.quantity)::integer,
                    SUM(o.quantity * o.unit_price),
                    NOW()
                FROM orders o
                JOIN affected_dates d
                    ON o.created_at::date = d.sale_date
                GROUP BY o.created_at::date;
            """,
        ], parameters=window)

        print("daily_sales refreshed for all changed dates")


    @task
    def build_retailer_metrics(window):
        if not window["has_changes"]:
            print("No source changes. retailer metrics unchanged.")
            return

        hook = PostgresHook(postgres_conn_id="retail_postgres")
        affected_dates = _affected_dates_cte()

        hook.run("""
            CREATE TABLE IF NOT EXISTS retailer_daily_metrics (
                sale_date DATE NOT NULL,
                retailer_name VARCHAR(255) NOT NULL,
                order_count INTEGER NOT NULL,
                total_units INTEGER NOT NULL,
                revenue NUMERIC(14,2) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (sale_date, retailer_name)
            );
        """)

        hook.run([
            f"""
                WITH affected_dates AS ({affected_dates})
                DELETE FROM retailer_daily_metrics r
                USING affected_dates a
                WHERE r.sale_date = a.sale_date;
            """,
            f"""
                WITH affected_dates AS ({affected_dates})
                INSERT INTO retailer_daily_metrics (
                    sale_date,
                    retailer_name,
                    order_count,
                    total_units,
                    revenue,
                    updated_at
                )
                SELECT
                    o.created_at::date,
                    o.retailer_name,
                    COUNT(*)::integer,
                    SUM(o.quantity)::integer,
                    SUM(o.quantity * o.unit_price),
                    NOW()
                FROM orders o
                JOIN affected_dates d
                    ON o.created_at::date = d.sale_date
                GROUP BY o.created_at::date, o.retailer_name;
            """,
        ], parameters=window)

        print("retailer_daily_metrics refreshed for all changed dates")


    @task
    def build_sku_metrics(window):
        if not window["has_changes"]:
            print("No source changes. SKU metrics unchanged.")
            return

        hook = PostgresHook(postgres_conn_id="retail_postgres")
        affected_dates = _affected_dates_cte()

        hook.run("""
            CREATE TABLE IF NOT EXISTS sku_daily_metrics (
                sale_date DATE NOT NULL,
                sku VARCHAR(100) NOT NULL,
                order_count INTEGER NOT NULL,
                total_units INTEGER NOT NULL,
                revenue NUMERIC(14,2) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (sale_date, sku)
            );
        """)

        hook.run([
            f"""
                WITH affected_dates AS ({affected_dates})
                DELETE FROM sku_daily_metrics s
                USING affected_dates a
                WHERE s.sale_date = a.sale_date;
            """,
            f"""
                WITH affected_dates AS ({affected_dates})
                INSERT INTO sku_daily_metrics (
                    sale_date,
                    sku,
                    order_count,
                    total_units,
                    revenue,
                    updated_at
                )
                SELECT
                    o.created_at::date,
                    o.sku,
                    COUNT(*)::integer,
                    SUM(o.quantity)::integer,
                    SUM(o.quantity * o.unit_price),
                    NOW()
                FROM orders o
                JOIN affected_dates d
                    ON o.created_at::date = d.sale_date
                GROUP BY o.created_at::date, o.sku;
            """,
        ], parameters=window)

        print("sku_daily_metrics refreshed for all changed dates")


    @task
    def validate_analytics(window):
        if not window["has_changes"]:
            print("No source changes to validate.")
            return

        hook = PostgresHook(postgres_conn_id="retail_postgres")
        affected_dates = _affected_dates_cte()

        daily_mismatches = hook.get_first(f"""
            WITH affected_dates AS ({affected_dates}),
            expected AS (
                SELECT
                    o.created_at::date AS sale_date,
                    COUNT(*)::integer AS order_count,
                    SUM(o.quantity)::integer AS total_units,
                    SUM(o.quantity * o.unit_price)::numeric(14,2) AS revenue
                FROM orders o
                JOIN affected_dates d
                    ON o.created_at::date = d.sale_date
                GROUP BY o.created_at::date
            ),
            actual AS (
                SELECT d.sale_date, d.order_count, d.total_units, d.revenue
                FROM daily_sales d
                JOIN affected_dates a USING (sale_date)
            )
            SELECT COUNT(*)
            FROM (
                (SELECT * FROM expected EXCEPT SELECT * FROM actual)
                UNION ALL
                (SELECT * FROM actual EXCEPT SELECT * FROM expected)
            ) differences;
        """, parameters=window)[0]

        retailer_mismatches = hook.get_first(f"""
            WITH affected_dates AS ({affected_dates}),
            expected AS (
                SELECT
                    o.created_at::date AS sale_date,
                    o.retailer_name,
                    COUNT(*)::integer AS order_count,
                    SUM(o.quantity)::integer AS total_units,
                    SUM(o.quantity * o.unit_price)::numeric(14,2) AS revenue
                FROM orders o
                JOIN affected_dates d
                    ON o.created_at::date = d.sale_date
                GROUP BY o.created_at::date, o.retailer_name
            ),
            actual AS (
                SELECT
                    r.sale_date,
                    r.retailer_name,
                    r.order_count,
                    r.total_units,
                    r.revenue
                FROM retailer_daily_metrics r
                JOIN affected_dates a USING (sale_date)
            )
            SELECT COUNT(*)
            FROM (
                (SELECT * FROM expected EXCEPT SELECT * FROM actual)
                UNION ALL
                (SELECT * FROM actual EXCEPT SELECT * FROM expected)
            ) differences;
        """, parameters=window)[0]

        sku_mismatches = hook.get_first(f"""
            WITH affected_dates AS ({affected_dates}),
            expected AS (
                SELECT
                    o.created_at::date AS sale_date,
                    o.sku,
                    COUNT(*)::integer AS order_count,
                    SUM(o.quantity)::integer AS total_units,
                    SUM(o.quantity * o.unit_price)::numeric(14,2) AS revenue
                FROM orders o
                JOIN affected_dates d
                    ON o.created_at::date = d.sale_date
                GROUP BY o.created_at::date, o.sku
            ),
            actual AS (
                SELECT
                    s.sale_date,
                    s.sku,
                    s.order_count,
                    s.total_units,
                    s.revenue
                FROM sku_daily_metrics s
                JOIN affected_dates a USING (sale_date)
            )
            SELECT COUNT(*)
            FROM (
                (SELECT * FROM expected EXCEPT SELECT * FROM actual)
                UNION ALL
                (SELECT * FROM actual EXCEPT SELECT * FROM expected)
            ) differences;
        """, parameters=window)[0]

        mismatches = {
            "daily_sales": daily_mismatches,
            "retailer_daily_metrics": retailer_mismatches,
            "sku_daily_metrics": sku_mismatches,
        }
        failed = {name: count for name, count in mismatches.items() if count}

        if failed:
            raise ValueError(f"Aggregate/source mismatches: {failed}")

        print("Changed-date aggregate validation passed")


    @task
    def advance_watermark(window):
        if not window["has_changes"]:
            print("No source changes. Watermark unchanged.")
            return

        hook = PostgresHook(postgres_conn_id="retail_postgres")
        hook.run("""
            INSERT INTO analytics_pipeline_state (
                pipeline_name,
                last_processed_at,
                last_processed_change_id,
                updated_at
            )
            VALUES (
                %(pipeline_name)s,
                %(upper_changed_at)s::timestamptz,
                %(upper_change_id)s,
                NOW()
            )
            ON CONFLICT (pipeline_name)
            DO UPDATE SET
                last_processed_at = EXCLUDED.last_processed_at,
                last_processed_change_id = EXCLUDED.last_processed_change_id,
                updated_at = NOW();
        """, parameters={
            **window,
            "pipeline_name": PIPELINE_NAME,
        })

        print(
            "Watermark advanced to change ID "
            f"{window['upper_change_id']}"
        )


    source_check = check_source_orders()
    window = get_processing_window()

    daily_sales = build_daily_sales(window)
    retailer_metrics = build_retailer_metrics(window)
    sku_metrics = build_sku_metrics(window)
    quality_check = validate_analytics(window)
    watermark = advance_watermark(window)

    source_check >> window
    daily_sales >> [retailer_metrics, sku_metrics]
    [retailer_metrics, sku_metrics] >> quality_check >> watermark


retail_daily_analytics()
