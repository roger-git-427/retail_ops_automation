import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="retail_ml_features",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "ml", "features"],
)
def retail_ml_features():

    @task
    def check_source_data():
        hook = PostgresHook(postgres_conn_id="retail_postgres")

        row_count = hook.get_first("""
            SELECT COUNT(*)
            FROM sku_daily_metrics;
        """)[0]

        sku_count = hook.get_first("""
            SELECT COUNT(DISTINCT sku)
            FROM sku_daily_metrics;
        """)[0]

        min_date, max_date = hook.get_first("""
            SELECT
                MIN(sale_date),
                MAX(sale_date)
            FROM sku_daily_metrics;
        """)

        if row_count < 100:
            raise ValueError(
                f"Not enough SKU history for ML: {row_count} rows"
            )

        if sku_count == 0:
            raise ValueError("No SKUs found")

        print(f"SKU-day source rows: {row_count}")
        print(f"SKUs: {sku_count}")
        print(f"History: {min_date} -> {max_date}")


    @task
    def build_daily_series():
        hook = PostgresHook(postgres_conn_id="retail_postgres")

        hook.run(sql=["""
            CREATE TABLE IF NOT EXISTS ml_sku_daily_series (
                sale_date DATE NOT NULL,
                sku VARCHAR(100) NOT NULL,

                order_count INTEGER NOT NULL,
                total_units INTEGER NOT NULL,
                revenue NUMERIC(14,2) NOT NULL,

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                PRIMARY KEY (sale_date, sku)
            );
        """,

        """
            TRUNCATE TABLE ml_sku_daily_series;
        """,

        """
            WITH skus AS (
                SELECT DISTINCT sku
                FROM sku_daily_metrics
            ),

            sku_bounds AS (
                SELECT
                    sku,
                    MIN(sale_date) AS first_date
                FROM sku_daily_metrics
                GROUP BY sku
            ),

            date_spine AS (
                SELECT
                    s.sku,
                    d::date AS sale_date

                FROM skus s

                JOIN sku_bounds b
                    ON b.sku = s.sku

                CROSS JOIN LATERAL generate_series(
                    b.first_date,
                    CURRENT_DATE - INTERVAL '1 day',
                    INTERVAL '1 day'
                ) d
            )
            INSERT INTO ml_sku_daily_series (
                sale_date,
                sku,
                order_count,
                total_units,
                revenue
            )
            SELECT
                ds.sale_date,
                ds.sku,

                COALESCE(m.order_count, 0),
                COALESCE(m.total_units, 0),
                COALESCE(m.revenue, 0)

            FROM date_spine ds

            LEFT JOIN sku_daily_metrics m
                ON m.sale_date = ds.sale_date
               AND m.sku = ds.sku

            ORDER BY ds.sku, ds.sale_date;
        """], autocommit=False)

        row_count = hook.get_first("""
            SELECT COUNT(*)
            FROM ml_sku_daily_series;
        """)[0]

        zero_days = hook.get_first("""
            SELECT COUNT(*)
            FROM ml_sku_daily_series
            WHERE total_units = 0;
        """)[0]

        print(f"Dense SKU-day rows: {row_count}")
        print(f"Zero-demand SKU-days: {zero_days}")


    @task
    def build_ml_features():
        hook = PostgresHook(postgres_conn_id="retail_postgres")

        hook.run(sql=["""
            DROP TABLE IF EXISTS ml_sku_features;

            CREATE TABLE ml_sku_features (
                feature_date DATE NOT NULL,
                target_date DATE NOT NULL,
                sku VARCHAR(100) NOT NULL,

                units_today INTEGER NOT NULL,
                orders_today INTEGER NOT NULL,
                revenue_today NUMERIC(14,2) NOT NULL,

                lag_1_units INTEGER NOT NULL,
                lag_7_units INTEGER NOT NULL,
                lag_14_units INTEGER NOT NULL,
                lag_28_units INTEGER NOT NULL,

                rolling_7_avg DOUBLE PRECISION NOT NULL,
                rolling_14_avg DOUBLE PRECISION NOT NULL,
                rolling_28_avg DOUBLE PRECISION NOT NULL,

                rolling_7_std DOUBLE PRECISION NOT NULL,
                rolling_28_std DOUBLE PRECISION NOT NULL,

                zero_days_last_7 INTEGER NOT NULL,

                trend_7_vs_28 DOUBLE PRECISION NOT NULL,

                target_day_of_week INTEGER NOT NULL,
                target_week_of_year INTEGER NOT NULL,
                target_month INTEGER NOT NULL,
                target_day_of_month INTEGER NOT NULL,
                target_is_weekend INTEGER NOT NULL,

                target_dow_sin DOUBLE PRECISION NOT NULL,
                target_dow_cos DOUBLE PRECISION NOT NULL,
                target_month_sin DOUBLE PRECISION NOT NULL,
                target_month_cos DOUBLE PRECISION NOT NULL,

                target_next_day_units INTEGER NOT NULL,

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                PRIMARY KEY (feature_date, sku)
            );
        """,


        """
            WITH windowed AS (
                SELECT
                    sale_date AS feature_date,
                    sale_date + 1 AS target_date,
                    sku,

                    total_units AS units_today,
                    order_count AS orders_today,
                    revenue AS revenue_today,

                    LAG(total_units, 1) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                    ) AS lag_1_units,

                    LAG(total_units, 7) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                    ) AS lag_7_units,

                    LAG(total_units, 14) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                    ) AS lag_14_units,

                    LAG(total_units, 28) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                    ) AS lag_28_units,

                    AVG(total_units) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                        ROWS BETWEEN 7 PRECEDING
                        AND 1 PRECEDING
                    ) AS rolling_7_avg,

                    AVG(total_units) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                        ROWS BETWEEN 14 PRECEDING
                        AND 1 PRECEDING
                    ) AS rolling_14_avg,

                    AVG(total_units) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                        ROWS BETWEEN 28 PRECEDING
                        AND 1 PRECEDING
                    ) AS rolling_28_avg,

                    STDDEV_SAMP(total_units) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                        ROWS BETWEEN 7 PRECEDING
                        AND 1 PRECEDING
                    ) AS rolling_7_std,

                    STDDEV_SAMP(total_units) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                        ROWS BETWEEN 28 PRECEDING
                        AND 1 PRECEDING
                    ) AS rolling_28_std,

                    SUM(
                        CASE
                            WHEN total_units = 0 THEN 1
                            ELSE 0
                        END
                    ) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                        ROWS BETWEEN 7 PRECEDING
                        AND 1 PRECEDING
                    ) AS zero_days_last_7,

                    LEAD(total_units, 1) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                    ) AS target_next_day_units

                FROM ml_sku_daily_series
            ),

            features AS (
                SELECT
                    *,

                    rolling_7_avg
                        - rolling_28_avg
                        AS trend_7_vs_28,

                    EXTRACT(
                        ISODOW FROM target_date
                    )::integer AS target_day_of_week,

                    EXTRACT(
                        WEEK FROM target_date
                    )::integer AS target_week_of_year,

                    EXTRACT(
                        MONTH FROM target_date
                    )::integer AS target_month,

                    EXTRACT(
                        DAY FROM target_date
                    )::integer AS target_day_of_month,

                    CASE
                        WHEN EXTRACT(
                            ISODOW FROM target_date
                        ) IN (6, 7)
                        THEN 1
                        ELSE 0
                    END AS target_is_weekend,

                    SIN(
                        2 * PI()
                        * EXTRACT(ISODOW FROM target_date)
                        / 7.0
                    ) AS target_dow_sin,

                    COS(
                        2 * PI()
                        * EXTRACT(ISODOW FROM target_date)
                        / 7.0
                    ) AS target_dow_cos,

                    SIN(
                        2 * PI()
                        * EXTRACT(MONTH FROM target_date)
                        / 12.0
                    ) AS target_month_sin,

                    COS(
                        2 * PI()
                        * EXTRACT(MONTH FROM target_date)
                        / 12.0
                    ) AS target_month_cos

                FROM windowed
            )

            INSERT INTO ml_sku_features (
                feature_date,
                target_date,
                sku,

                units_today,
                orders_today,
                revenue_today,

                lag_1_units,
                lag_7_units,
                lag_14_units,
                lag_28_units,

                rolling_7_avg,
                rolling_14_avg,
                rolling_28_avg,

                rolling_7_std,
                rolling_28_std,

                zero_days_last_7,

                trend_7_vs_28,

                target_day_of_week,
                target_week_of_year,
                target_month,
                target_day_of_month,
                target_is_weekend,

                target_dow_sin,
                target_dow_cos,
                target_month_sin,
                target_month_cos,

                target_next_day_units
            )

            SELECT
                feature_date,
                target_date,
                sku,

                units_today,
                orders_today,
                revenue_today,

                lag_1_units,
                lag_7_units,
                lag_14_units,
                lag_28_units,

                rolling_7_avg,
                rolling_14_avg,
                rolling_28_avg,

                rolling_7_std,
                rolling_28_std,

                zero_days_last_7,

                trend_7_vs_28,

                target_day_of_week,
                target_week_of_year,
                target_month,
                target_day_of_month,
                target_is_weekend,

                target_dow_sin,
                target_dow_cos,
                target_month_sin,
                target_month_cos,

                target_next_day_units

            FROM features

            WHERE lag_28_units IS NOT NULL
              AND rolling_28_avg IS NOT NULL
              AND rolling_28_std IS NOT NULL
              AND target_next_day_units IS NOT NULL;
        """], autocommit=False)

        row_count = hook.get_first("""
            SELECT COUNT(*)
            FROM ml_sku_features;
        """)[0]

        print(f"Created {row_count} ML feature rows")


    @task
    def validate_ml_features():
        hook = PostgresHook(postgres_conn_id="retail_postgres")

        invalid_rows = hook.get_first("""
            SELECT COUNT(*)
            FROM ml_sku_features
            WHERE
                target_date <> feature_date + 1
                OR lag_1_units IS NULL
                OR lag_7_units IS NULL
                OR lag_14_units IS NULL
                OR lag_28_units IS NULL
                OR rolling_7_avg IS NULL
                OR rolling_14_avg IS NULL
                OR rolling_28_avg IS NULL
                OR rolling_7_std IS NULL
                OR rolling_28_std IS NULL
                OR trend_7_vs_28 IS NULL
                OR target_next_day_units IS NULL;
        """)[0]

        duplicate_rows = hook.get_first("""
            SELECT COUNT(*)
            FROM (
                SELECT
                    feature_date,
                    sku,
                    COUNT(*)
                FROM ml_sku_features
                GROUP BY
                    feature_date,
                    sku
                HAVING COUNT(*) > 1
            ) duplicates;
        """)[0]

        row_count = hook.get_first("""
            SELECT COUNT(*)
            FROM ml_sku_features;
        """)[0]

        if invalid_rows > 0:
            raise ValueError(
                f"Feature validation failed: "
                f"{invalid_rows} invalid rows"
            )

        if duplicate_rows > 0:
            raise ValueError(
                f"Feature validation failed: "
                f"{duplicate_rows} duplicate keys"
            )

        if row_count < 100:
            raise ValueError(
                f"Too few training rows: {row_count}"
            )

        print(
            f"ML feature validation passed: "
            f"{row_count} rows"
        )


    source = check_source_data()
    series = build_daily_series()
    features = build_ml_features()
    validation = validate_ml_features()

    source >> series >> features >> validation


retail_ml_features()
