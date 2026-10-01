from datetime import timedelta

import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


MODEL_NAME = "xgboost_v1"
MODEL_PATH = "/opt/airflow/models/xgboost_v1.joblib"
VALIDATION_DAYS = 31
TEST_DAYS = 30


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


def load_dataset(where_clause, parameters=None):
    import pandas as pd

    hook = PostgresHook(
        postgres_conn_id="retail_postgres"
    )

    columns = [
        "target_date",
        "sku",
        *NUMERIC_FEATURES,
        "target_next_day_units",
    ]

    rows = hook.get_records(f"""
        SELECT
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

        FROM ml_sku_features

        WHERE {where_clause}

        ORDER BY
            target_date,
            sku;
    """, parameters=parameters)

    df = pd.DataFrame(
        rows,
        columns=columns
    )

    if df.empty:
        return df

    df["target_date"] = pd.to_datetime(
        df["target_date"]
    )

    for column in NUMERIC_FEATURES:
        df[column] = pd.to_numeric(
            df[column],
            errors="raise"
        )

    df["target_next_day_units"] = pd.to_numeric(
        df["target_next_day_units"],
        errors="raise"
    )

    return df


def encode_features(df, feature_columns=None):
    import pandas as pd

    X = df[
        ["sku"] + NUMERIC_FEATURES
    ].copy()

    X = pd.get_dummies(
        X,
        columns=["sku"],
        prefix="sku",
        dtype=float,
    )

    if feature_columns is not None:
        X = X.reindex(
            columns=feature_columns,
            fill_value=0.0,
        )

    return X


@dag(
    dag_id="retail_ml_xgboost",
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
        "xgboost",
        "forecasting",
    ],
)
def retail_ml_xgboost():

    @task
    def get_dataset_split():
        hook = PostgresHook(
            postgres_conn_id="retail_postgres"
        )

        max_target_date = hook.get_first("""
            SELECT MAX(target_date)
            FROM ml_sku_features;
        """)[0]

        if max_target_date is None:
            raise ValueError("No ML feature rows available")

        test_end = max_target_date + timedelta(days=1)
        test_start = test_end - timedelta(days=TEST_DAYS)
        validation_start = test_start - timedelta(
            days=VALIDATION_DAYS
        )

        return {
            "validation_start": validation_start.isoformat(),
            "test_start": test_start.isoformat(),
            "test_end": test_end.isoformat(),
            "evaluation_period": (
                f"{test_start.isoformat()}_to_"
                f"{max_target_date.isoformat()}"
            ),
        }


    @task
    def check_dataset(split):
        hook = PostgresHook(
            postgres_conn_id="retail_postgres"
        )

        result = hook.get_first("""
            SELECT
                COUNT(*) FILTER (
                    WHERE target_date < %(validation_start)s::date
                ) AS train_rows,

                COUNT(*) FILTER (
                    WHERE target_date >= %(validation_start)s::date
                      AND target_date < %(test_start)s::date
                ) AS validation_rows,

                COUNT(*) FILTER (
                    WHERE target_date >= %(test_start)s::date
                      AND target_date < %(test_end)s::date
                ) AS test_rows

            FROM ml_sku_features;
        """, parameters=split)

        train_rows, validation_rows, test_rows = result

        print(f"Train rows:      {train_rows}")
        print(f"Validation rows: {validation_rows}")
        print(f"Test rows:       {test_rows}")

        if train_rows < 100:
            raise ValueError(
                "Not enough training data"
            )

        if validation_rows == 0:
            raise ValueError(
                "No validation data"
            )

        if test_rows == 0:
            raise ValueError(
                "No test data"
            )


    @task
    def train_model(split):
        import os
        import tempfile

        import joblib
        import numpy as np

        from sklearn.metrics import (
            mean_absolute_error,
            mean_squared_error,
        )

        from xgboost import XGBRegressor

        df = load_dataset(
            "target_date < %(test_start)s::date",
            parameters=split,
        )

        train_mask = (
            df["target_date"]
            < split["validation_start"]
        )

        validation_mask = (
            (df["target_date"] >= split["validation_start"])
            &
            (df["target_date"] < split["test_start"])
        )

        train_df = df.loc[train_mask].copy()
        validation_df = df.loc[
            validation_mask
        ].copy()

        X_all = encode_features(df)

        X_train = X_all.loc[
            train_df.index
        ]

        X_validation = X_all.loc[
            validation_df.index
        ]

        y_train = train_df[
            "target_next_day_units"
        ].astype(float)

        y_validation = validation_df[
            "target_next_day_units"
        ].astype(float)

        candidates = [
            {
                "n_estimators": 200,
                "max_depth": 3,
                "learning_rate": 0.05,
                "subsample": 0.9,
                "colsample_bytree": 0.9,
            },
            {
                "n_estimators": 300,
                "max_depth": 3,
                "learning_rate": 0.03,
                "subsample": 0.9,
                "colsample_bytree": 0.9,
            },
            {
                "n_estimators": 200,
                "max_depth": 4,
                "learning_rate": 0.05,
                "subsample": 0.9,
                "colsample_bytree": 0.9,
            },
        ]

        best_model = None
        best_params = None
        best_mae = float("inf")

        for params in candidates:

            model = XGBRegressor(
                objective="reg:squarederror",
                random_state=42,
                n_jobs=2,
                **params,
            )

            model.fit(
                X_train,
                y_train,
            )

            predictions = model.predict(
                X_validation
            )

            predictions = np.maximum(
                predictions,
                0
            )

            mae = mean_absolute_error(
                y_validation,
                predictions,
            )

            rmse = np.sqrt(
                mean_squared_error(
                    y_validation,
                    predictions,
                )
            )

            total_actual = y_validation.sum()
            wape = None

            if total_actual != 0:
                wape = (
                    100
                    * np.abs(
                        y_validation.values
                        - predictions
                    ).sum()
                    / total_actual
                )

            print(
                f"Candidate {params}"
            )

            print(
                f"Validation MAE:  {mae:.4f}"
            )

            print(
                f"Validation RMSE: {rmse:.4f}"
            )

            print(
                "Validation WAPE: "
                + (
                    f"{wape:.2f}%"
                    if wape is not None
                    else "N/A (zero actual demand)"
                )
            )

            if mae < best_mae:
                best_mae = mae
                best_params = params
                best_model = model

        print(
            f"Best validation MAE: "
            f"{best_mae:.4f}"
        )

        print(
            f"Best parameters: "
            f"{best_params}"
        )

        # Retrain the selected model using
        # both training + validation data.

        final_model = XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            n_jobs=2,
            **best_params,
        )

        y_all = df[
            "target_next_day_units"
        ].astype(float)

        final_model.fit(
            X_all,
            y_all,
        )

        artifact = {
            "model": final_model,
            "feature_columns": list(
                X_all.columns
            ),
            "best_params": best_params,
            "validation_mae": best_mae,
            "model_name": MODEL_NAME,
            "known_skus": sorted(
                df["sku"].unique().tolist()
            ),
            "dataset_split": split,
        }

        model_directory = os.path.dirname(MODEL_PATH)
        os.makedirs(model_directory, exist_ok=True)

        with tempfile.NamedTemporaryFile(
            prefix=".xgboost_v1.",
            suffix=".joblib.tmp",
            dir=model_directory,
            delete=False,
        ) as temporary_file:
            temporary_path = temporary_file.name

        try:
            joblib.dump(artifact, temporary_path)
            os.replace(temporary_path, MODEL_PATH)
        finally:
            if os.path.exists(temporary_path):
                os.unlink(temporary_path)

        print(
            f"Model saved to {MODEL_PATH}"
        )


    @task
    def evaluate_model(split):
        import joblib
        import numpy as np

        from sklearn.metrics import (
            mean_absolute_error,
            mean_squared_error,
        )

        artifact = joblib.load(
            MODEL_PATH
        )

        model = artifact["model"]

        feature_columns = artifact[
            "feature_columns"
        ]

        df = load_dataset("""
            target_date >= %(test_start)s::date
            AND target_date < %(test_end)s::date
        """, parameters=split)

        X_test = encode_features(
            df,
            feature_columns=feature_columns,
        )

        y_test = df[
            "target_next_day_units"
        ].astype(float)

        predictions = model.predict(
            X_test
        )

        predictions = np.maximum(
            predictions,
            0
        )

        mae = mean_absolute_error(
            y_test,
            predictions,
        )

        rmse = np.sqrt(
            mean_squared_error(
                y_test,
                predictions,
            )
        )

        total_actual = y_test.sum()
        wape = None

        if total_actual != 0:
            wape = (
                100
                * np.abs(
                    y_test.values
                    - predictions
                ).sum()
                / total_actual
            )

        print(f"TEST MAE:  {mae:.4f}")
        print(f"TEST RMSE: {rmse:.4f}")
        print(
            "TEST WAPE: "
            + (
                f"{wape:.2f}%"
                if wape is not None
                else "N/A (zero actual demand)"
            )
        )

        hook = PostgresHook(
            postgres_conn_id="retail_postgres"
        )

        create_predictions_table_sql = """
            CREATE TABLE IF NOT EXISTS
            ml_forecast_predictions (
                model_name VARCHAR(100) NOT NULL,
                forecast_date DATE NOT NULL,
                sku VARCHAR(100) NOT NULL,

                predicted_units NUMERIC NOT NULL,
                actual_units INTEGER,

                generated_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW(),

                PRIMARY KEY (
                    model_name,
                    forecast_date,
                    sku
                )
            );
        """

        delete_predictions_sql = """
            DELETE FROM ml_forecast_predictions
            WHERE model_name = %s;
        """

        prediction_rows = []

        for index, row in df.iterrows():

            prediction_rows.append(
                (
                    MODEL_NAME,
                    row["target_date"].date(),
                    row["sku"],
                    float(
                        predictions[
                            df.index.get_loc(index)
                        ]
                    ),
                    int(
                        row[
                            "target_next_day_units"
                        ]
                    ),
                )
            )

        connection = hook.get_conn()

        try:
            with connection.cursor() as cursor:

                cursor.execute(
                    create_predictions_table_sql
                )

                cursor.execute(
                    delete_predictions_sql,
                    (MODEL_NAME,),
                )

                cursor.executemany("""
                    INSERT INTO
                    ml_forecast_predictions (
                        model_name,
                        forecast_date,
                        sku,
                        predicted_units,
                        actual_units
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    );
                """, prediction_rows)

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        hook.run("""
            CREATE TABLE IF NOT EXISTS
            ml_model_metrics (
                model_name VARCHAR(100) NOT NULL,
                evaluation_period VARCHAR(50)
                    NOT NULL,

                mae NUMERIC NOT NULL,
                rmse NUMERIC NOT NULL,
                wape NUMERIC,

                evaluated_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW(),

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

            VALUES (
                %(model_name)s,
                %(evaluation_period)s,
                %(mae)s,
                %(rmse)s,
                %(wape)s,
                NOW()
            )

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
            "model_name": MODEL_NAME,
            "evaluation_period": split["evaluation_period"],
            "mae": float(mae),
            "rmse": float(rmse),
            "wape": (
                float(wape)
                if wape is not None
                else None
            ),
        })

        print(
            "XGBoost predictions and "
            "metrics persisted"
        )


    split = get_dataset_split()
    dataset = check_dataset(split)
    training = train_model(split)
    evaluation = evaluate_model(split)

    split >> dataset >> training >> evaluation


retail_ml_xgboost()
