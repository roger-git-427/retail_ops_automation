from datetime import timedelta

import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


MODEL_NAME = "naive_persistence"
EVALUATION_DAYS = 30


@dag(
    dag_id="retail_ml_baseline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "ml", "forecasting"],
)
def retail_ml_baseline():

    @task
    def get_evaluation_window():
        hook = PostgresHook(postgres_conn_id="retail_postgres")

        max_target_date = hook.get_first("""
            SELECT MAX(target_date)
            FROM ml_sku_features;
        """)[0]

        if max_target_date is None:
            raise ValueError("No feature rows available")

        start_date = max_target_date - timedelta(
            days=EVALUATION_DAYS - 1
        )

        return {
            "start_date": start_date.isoformat(),
            "end_date": max_target_date.isoformat(),
            "evaluation_period": (
                f"{start_date.isoformat()}_to_"
                f"{max_target_date.isoformat()}"
            ),
        }


    @task
    def check_features(window):
        hook = PostgresHook(postgres_conn_id="retail_postgres")

        result = hook.get_first("""
            SELECT COUNT(*)
            FROM ml_sku_features
            WHERE target_date >= %(start_date)s::date
              AND target_date <= %(end_date)s::date;
        """, parameters=window)

        row_count = result[0]

        if row_count == 0:
            raise ValueError("No test rows available")

        print(f"Found {row_count} test rows")


    @task
    def build_baseline_predictions(window):
        hook = PostgresHook(postgres_conn_id="retail_postgres")

        hook.run("""
            CREATE TABLE IF NOT EXISTS ml_forecast_predictions (
                model_name VARCHAR(100) NOT NULL,
                forecast_date DATE NOT NULL,
                sku VARCHAR(100) NOT NULL,

                predicted_units NUMERIC NOT NULL,
                actual_units INTEGER,

                generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                PRIMARY KEY (
                    model_name,
                    forecast_date,
                    sku
                )
            );
        """)

        hook.run(sql=["""
            DELETE FROM ml_forecast_predictions
            WHERE model_name = %(model_name)s;
        """,

        """
            INSERT INTO ml_forecast_predictions (
                model_name,
                forecast_date,
                sku,
                predicted_units,
                actual_units
            )
            SELECT
                %(model_name)s,
                target_date,
                sku,

                -- Baseline:
                -- tomorrow ~= today
                units_today,

                target_next_day_units

            FROM ml_sku_features

            WHERE target_date >= %(start_date)s::date
              AND target_date <= %(end_date)s::date;
        """], parameters={
            **window,
            "model_name": MODEL_NAME,
        }, autocommit=False)

        print("Baseline predictions created")


    @task
    def evaluate_baseline(window):
        hook = PostgresHook(postgres_conn_id="retail_postgres")

        hook.run("""
            CREATE TABLE IF NOT EXISTS ml_model_metrics (
                model_name VARCHAR(100) NOT NULL,
                evaluation_period VARCHAR(50) NOT NULL,

                mae NUMERIC NOT NULL,
                rmse NUMERIC NOT NULL,
                wape NUMERIC,

                evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                PRIMARY KEY (
                    model_name,
                    evaluation_period
                )
            );
        """)

        hook.run("""
            INSERT INTO ml_model_metrics (
                model_name,
                evaluation_period,
                mae,
                rmse,
                wape,
                evaluated_at
            )
            SELECT
                %(model_name)s::varchar(100),
                %(evaluation_period)s::varchar(50),

                AVG(
                    ABS(actual_units - predicted_units)
                ) AS mae,

                SQRT(
                    AVG(
                        POWER(
                            actual_units - predicted_units,
                            2
                        )
                    )
                ) AS rmse,

                100.0
                * SUM(
                    ABS(actual_units - predicted_units)
                )
                / NULLIF(
                    SUM(actual_units),
                    0
                ) AS wape,

                NOW()

            FROM ml_forecast_predictions

            WHERE model_name = %(model_name)s::varchar(100)

            ON CONFLICT (
                model_name,
                evaluation_period
            )
            DO UPDATE SET
                mae = EXCLUDED.mae,
                rmse = EXCLUDED.rmse,
                wape = EXCLUDED.wape,
                evaluated_at = NOW();
        """, parameters={
            **window,
            "model_name": MODEL_NAME,
        })

        result = hook.get_first("""
            SELECT
                mae,
                rmse,
                wape
            FROM ml_model_metrics
            WHERE model_name = %(model_name)s::varchar(100)
              AND evaluation_period =
                    %(evaluation_period)s::varchar(50);
        """, parameters={
            **window,
            "model_name": MODEL_NAME,
        })

        mae, rmse, wape = result

        print(f"Baseline MAE:  {mae}")
        print(f"Baseline RMSE: {rmse}")
        print(f"Baseline WAPE: {wape}%")


    window = get_evaluation_window()
    source = check_features(window)
    predictions = build_baseline_predictions(window)
    evaluation = evaluate_baseline(window)

    window >> source >> predictions >> evaluation


retail_ml_baseline()
