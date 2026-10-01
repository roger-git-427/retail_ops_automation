import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


MODEL_NAME = "xgboost_v1"
MODEL_PATH = "/opt/airflow/models/xgboost_v1.joblib"


NUMERIC_FEATURES = [
    "units_today",
    "orders_today",
    "revenue_today",

    "lag_1_units",
    "lag_7_units",
    "lag_14_units",
    "lag_28_units",

    "rolling_7_avg",
    "rolling_14_avg",
    "rolling_28_avg",

    "rolling_7_std",
    "rolling_28_std",

    "zero_days_last_7",
    "trend_7_vs_28",

    "target_day_of_week",
    "target_week_of_year",
    "target_month",
    "target_day_of_month",
    "target_is_weekend",

    "target_dow_sin",
    "target_dow_cos",
    "target_month_sin",
    "target_month_cos",
]


@dag(
    dag_id="retail_ml_forecast",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "retail",
        "ml",
        "forecasting",
        "inference",
    ],
)
def retail_ml_forecast():

    @task
    def check_model():
        import os
        import joblib

        if not os.path.exists(MODEL_PATH):
            raise FileNotFoundError(
                f"Model not found: {MODEL_PATH}"
            )

        artifact = joblib.load(MODEL_PATH)

        if "model" not in artifact:
            raise ValueError(
                "Model artifact contains no model"
            )

        if "feature_columns" not in artifact:
            raise ValueError(
                "Model artifact contains no feature schema"
            )

        if "known_skus" not in artifact:
            raise ValueError(
                "Model artifact contains no trained SKU set"
            )

        print(
            f"Loaded model: "
            f"{artifact.get('model_name')}"
        )

        print(
            f"Feature count: "
            f"{len(artifact['feature_columns'])}"
        )


    @task
    def generate_forecasts():
        import joblib
        import numpy as np
        import pandas as pd

        hook = PostgresHook(
            postgres_conn_id="retail_postgres"
        )

        rows = hook.get_records("""
            WITH feature_rows AS (
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
                            WHEN total_units = 0
                            THEN 1
                            ELSE 0
                        END
                    ) OVER (
                        PARTITION BY sku
                        ORDER BY sale_date
                        ROWS BETWEEN 7 PRECEDING
                        AND 1 PRECEDING
                    ) AS zero_days_last_7

                FROM ml_sku_daily_series
            ),

            engineered AS (
                SELECT
                    *,

                    rolling_7_avg
                    - rolling_28_avg
                    AS trend_7_vs_28,

                    EXTRACT(
                        ISODOW FROM target_date
                    )::integer
                    AS target_day_of_week,

                    EXTRACT(
                        WEEK FROM target_date
                    )::integer
                    AS target_week_of_year,

                    EXTRACT(
                        MONTH FROM target_date
                    )::integer
                    AS target_month,

                    EXTRACT(
                        DAY FROM target_date
                    )::integer
                    AS target_day_of_month,

                    CASE
                        WHEN EXTRACT(
                            ISODOW FROM target_date
                        ) IN (6, 7)
                        THEN 1
                        ELSE 0
                    END
                    AS target_is_weekend,

                    SIN(
                        2 * PI()
                        * EXTRACT(
                            ISODOW FROM target_date
                        )
                        / 7.0
                    ) AS target_dow_sin,

                    COS(
                        2 * PI()
                        * EXTRACT(
                            ISODOW FROM target_date
                        )
                        / 7.0
                    ) AS target_dow_cos,

                    SIN(
                        2 * PI()
                        * EXTRACT(
                            MONTH FROM target_date
                        )
                        / 12.0
                    ) AS target_month_sin,

                    COS(
                        2 * PI()
                        * EXTRACT(
                            MONTH FROM target_date
                        )
                        / 12.0
                    ) AS target_month_cos

                FROM feature_rows
            ),

            latest AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY sku
                        ORDER BY feature_date DESC
                    ) AS rn

                FROM engineered
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
                target_month_cos

            FROM latest

            WHERE rn = 1
              AND lag_28_units IS NOT NULL
              AND rolling_28_avg IS NOT NULL
              AND rolling_28_std IS NOT NULL

            ORDER BY sku;
        """)

        columns = [
            "feature_date",
            "target_date",
            "sku",

            *NUMERIC_FEATURES,
        ]

        df = pd.DataFrame(
            rows,
            columns=columns,
        )

        if df.empty:
            raise ValueError(
                "No inference rows available"
            )

        for column in NUMERIC_FEATURES:
            df[column] = pd.to_numeric(
                df[column],
                errors="raise",
            )

        artifact = joblib.load(
            MODEL_PATH
        )

        model = artifact["model"]

        feature_columns = artifact[
            "feature_columns"
        ]

        known_skus = set(artifact["known_skus"])
        inference_skus = set(df["sku"].unique())
        unknown_skus = sorted(
            inference_skus - known_skus
        )

        if unknown_skus:
            raise ValueError(
                "Forecast input contains SKUs that were not "
                f"present during training: {unknown_skus}"
            )

        X = df[
            ["sku"] + NUMERIC_FEATURES
        ].copy()

        X = pd.get_dummies(
            X,
            columns=["sku"],
            prefix="sku",
            dtype=float,
        )

        X = X.reindex(
            columns=feature_columns,
            fill_value=0.0,
        )

        predictions = model.predict(X)

        predictions = np.maximum(
            predictions,
            0,
        )

        hook.run("""
            CREATE TABLE IF NOT EXISTS
            sku_demand_forecasts (
                model_name VARCHAR(100)
                    NOT NULL,

                forecast_date DATE
                    NOT NULL,

                sku VARCHAR(100)
                    NOT NULL,

                predicted_units NUMERIC(14,2)
                    NOT NULL,

                source_feature_date DATE
                    NOT NULL,

                generated_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW(),

                PRIMARY KEY (
                    model_name,
                    forecast_date,
                    sku
                )
            );
        """)

        forecast_rows = []

        for position, row in df.iterrows():

            forecast_rows.append(
                (
                    MODEL_NAME,
                    row["target_date"],
                    row["sku"],
                    float(
                        predictions[
                            df.index.get_loc(position)
                        ]
                    ),
                    row["feature_date"],
                )
            )

        connection = hook.get_conn()

        try:
            with connection.cursor() as cursor:

                forecast_dates = sorted({
                    (
                        row["target_date"].date()
                        if hasattr(row["target_date"], "date")
                        else row["target_date"]
                    )
                    for _, row in df.iterrows()
                })

                for forecast_date in forecast_dates:
                    cursor.execute("""
                        DELETE FROM sku_demand_forecasts
                        WHERE model_name = %s
                          AND forecast_date = %s;
                    """, (
                        MODEL_NAME,
                        forecast_date,
                    ))

                cursor.executemany("""
                    INSERT INTO
                    sku_demand_forecasts (
                        model_name,
                        forecast_date,
                        sku,
                        predicted_units,
                        source_feature_date,
                        generated_at
                    )

                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        NOW()
                    )

                    ON CONFLICT (
                        model_name,
                        forecast_date,
                        sku
                    )

                    DO UPDATE SET
                        predicted_units =
                            EXCLUDED.predicted_units,

                        source_feature_date =
                            EXCLUDED.source_feature_date,

                        generated_at = NOW();
                """, forecast_rows)

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        for i, row in df.iterrows():

            prediction = predictions[
                df.index.get_loc(i)
            ]

            print(
                f"{row['sku']} | "
                f"{row['target_date']} | "
                f"{prediction:.2f} units"
            )

        return {
            "forecast_dates": [
                value.isoformat()
                for value in forecast_dates
            ],
            "row_count": len(forecast_rows),
        }


    @task
    def validate_forecasts(forecast_info):
        hook = PostgresHook(
            postgres_conn_id="retail_postgres"
        )

        invalid_rows = hook.get_first("""
            SELECT COUNT(*)
            FROM sku_demand_forecasts
            WHERE model_name = %s
              AND forecast_date = ANY(%s::date[])
              AND (
                    predicted_units < 0
                 OR predicted_units IS NULL
              );
        """, parameters=(
            MODEL_NAME,
            forecast_info["forecast_dates"],
        ))[0]

        if invalid_rows > 0:
            raise ValueError(
                f"{invalid_rows} invalid forecasts"
            )

        persisted_count = hook.get_first("""
            SELECT COUNT(*)
            FROM sku_demand_forecasts
            WHERE model_name = %s
              AND forecast_date = ANY(%s::date[]);
        """, parameters=(
            MODEL_NAME,
            forecast_info["forecast_dates"],
        ))[0]

        if persisted_count != forecast_info["row_count"]:
            raise ValueError(
                "Persisted forecast count does not match "
                f"the current run: {persisted_count} != "
                f"{forecast_info['row_count']}"
            )

        print(
            f"Validated {persisted_count} "
            f"SKU forecasts from the current run"
        )


    model = check_model()
    forecasts = generate_forecasts()
    validation = validate_forecasts(forecasts)

    model >> forecasts >> validation


retail_ml_forecast()
