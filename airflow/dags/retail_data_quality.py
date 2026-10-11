import pendulum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


QUALITY_CHECKS = {
    "invalid_orders": """
        SELECT COUNT(*)
        FROM orders
        WHERE po_number IS NULL
           OR BTRIM(po_number) = ''
           OR retailer_name IS NULL
           OR BTRIM(retailer_name) = ''
           OR sku IS NULL
           OR BTRIM(sku) = ''
           OR quantity <= 0
           OR unit_price < 0
           OR created_at IS NULL;
    """,
    "daily_sales_mismatches": """
        WITH expected AS (
            SELECT
                created_at::date AS sale_date,
                COUNT(*)::integer AS order_count,
                SUM(quantity)::integer AS total_units,
                SUM(quantity * unit_price)::numeric(14,2) AS revenue
            FROM orders
            GROUP BY created_at::date
        ),
        actual AS (
            SELECT sale_date, order_count, total_units, revenue
            FROM daily_sales
        )
        SELECT COUNT(*)
        FROM (
            (SELECT * FROM expected EXCEPT SELECT * FROM actual)
            UNION ALL
            (SELECT * FROM actual EXCEPT SELECT * FROM expected)
        ) differences;
    """,
    "retailer_metrics_mismatches": """
        WITH expected AS (
            SELECT
                created_at::date AS sale_date,
                retailer_name,
                COUNT(*)::integer AS order_count,
                SUM(quantity)::integer AS total_units,
                SUM(quantity * unit_price)::numeric(14,2) AS revenue
            FROM orders
            GROUP BY created_at::date, retailer_name
        ),
        actual AS (
            SELECT
                sale_date,
                retailer_name,
                order_count,
                total_units,
                revenue
            FROM retailer_daily_metrics
        )
        SELECT COUNT(*)
        FROM (
            (SELECT * FROM expected EXCEPT SELECT * FROM actual)
            UNION ALL
            (SELECT * FROM actual EXCEPT SELECT * FROM expected)
        ) differences;
    """,
    "sku_metrics_mismatches": """
        WITH expected AS (
            SELECT
                created_at::date AS sale_date,
                sku,
                COUNT(*)::integer AS order_count,
                SUM(quantity)::integer AS total_units,
                SUM(quantity * unit_price)::numeric(14,2) AS revenue
            FROM orders
            GROUP BY created_at::date, sku
        ),
        actual AS (
            SELECT
                sale_date,
                sku,
                order_count,
                total_units,
                revenue
            FROM sku_daily_metrics
        )
        SELECT COUNT(*)
        FROM (
            (SELECT * FROM expected EXCEPT SELECT * FROM actual)
            UNION ALL
            (SELECT * FROM actual EXCEPT SELECT * FROM expected)
        ) differences;
    """,
    "unprocessed_order_changes": """
        SELECT COUNT(*)
        FROM orders_change_log c
        WHERE c.change_id > COALESCE(
            (
                SELECT last_processed_change_id
                FROM analytics_pipeline_state
                WHERE pipeline_name = 'retail_daily_analytics'
            ),
            0
        );
    """,
}


@dag(
    dag_id="retail_data_quality",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "data-quality"],
)
def retail_data_quality():

    @task(task_id="blocking_data_quality_gate")
    def run_quality_checks():
        hook = PostgresHook(postgres_conn_id="retail_postgres")

        hook.run([
            """
                CREATE TABLE IF NOT EXISTS data_quality_results (
                    id BIGSERIAL PRIMARY KEY,
                    dag_id VARCHAR(100) NOT NULL,
                    check_name VARCHAR(100) NOT NULL,
                    status VARCHAR(10) NOT NULL CHECK (
                        status IN ('PASS', 'FAIL')
                    ),
                    failure_count BIGINT NOT NULL CHECK (
                        failure_count >= 0
                    ),
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """,
            """
                CREATE INDEX IF NOT EXISTS
                    idx_data_quality_results_checked_at
                ON data_quality_results (checked_at DESC);
            """,
        ])

        results = []
        for check_name, query in QUALITY_CHECKS.items():
            failure_count = int(hook.get_first(query)[0])
            status = "PASS" if failure_count == 0 else "FAIL"
            results.append((
                "retail_data_quality",
                check_name,
                status,
                failure_count,
            ))
            print(
                f"{check_name}: {status} "
                f"({failure_count} failing rows)"
            )

        connection = hook.get_conn()
        try:
            with connection.cursor() as cursor:
                cursor.executemany("""
                    INSERT INTO data_quality_results (
                        dag_id,
                        check_name,
                        status,
                        failure_count
                    )
                    VALUES (%s, %s, %s, %s);
                """, results)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        failures = {
            check_name: failure_count
            for _, check_name, status, failure_count in results
            if status == "FAIL"
        }
        if failures:
            raise ValueError(
                "Blocking data-quality checks failed: "
                f"{failures}"
            )

        print(f"All {len(results)} blocking data-quality checks passed")

    run_quality_checks()


retail_data_quality()
