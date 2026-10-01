import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import timedelta

import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


MODEL_NAME = "xgboost_v1"
INVENTORY_BASE_URL = "http://inventory-api:8000"


@dag(
    dag_id="retail_inventory_risk",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=[
        "retail",
        "ml",
        "inventory",
        "risk",
    ],
)
def retail_inventory_risk():

    @task
    def check_latest_forecasts():
        hook = PostgresHook(
            postgres_conn_id="retail_postgres"
        )

        result = hook.get_first("""
            SELECT
                MAX(forecast_date),
                COUNT(*)
            FROM sku_demand_forecasts
            WHERE model_name = %s
              AND forecast_date = (
                    SELECT MAX(forecast_date)
                    FROM sku_demand_forecasts
                    WHERE model_name = %s
              );
        """, parameters=(
            MODEL_NAME,
            MODEL_NAME,
        ))

        forecast_date, sku_count = result

        if forecast_date is None:
            raise ValueError(
                "No forecasts available"
            )

        if sku_count == 0:
            raise ValueError(
                "Latest forecast contains no SKUs"
            )

        print(
            f"Latest forecast date: "
            f"{forecast_date}"
        )

        print(
            f"Forecasted SKUs: "
            f"{sku_count}"
        )

        return {
            "forecast_date": forecast_date.isoformat(),
            "sku_count": sku_count,
        }


    @task
    def build_inventory_risk(forecast_info):
        hook = PostgresHook(
            postgres_conn_id="retail_postgres"
        )

        forecasts = hook.get_records("""
            SELECT
                forecast_date,
                sku,
                predicted_units
            FROM sku_demand_forecasts
            WHERE model_name = %s
              AND forecast_date = %s::date
            ORDER BY sku;
        """, parameters=(
            MODEL_NAME,
            forecast_info["forecast_date"],
        ))

        if not forecasts:
            raise ValueError(
                "No latest forecasts found"
            )

        hook.run("""
            CREATE TABLE IF NOT EXISTS
            sku_inventory_risk (
                model_name VARCHAR(100)
                    NOT NULL,

                forecast_date DATE
                    NOT NULL,

                sku VARCHAR(100)
                    NOT NULL,

                current_inventory INTEGER
                    NOT NULL,

                predicted_daily_demand
                    NUMERIC(14,2)
                    NOT NULL,

                days_of_supply
                    NUMERIC(14,2),

                risk_level VARCHAR(20)
                    NOT NULL,

                inventory_checked_at
                    TIMESTAMPTZ
                    NOT NULL,

                generated_at
                    TIMESTAMPTZ
                    NOT NULL
                    DEFAULT NOW(),

                PRIMARY KEY (
                    model_name,
                    forecast_date,
                    sku
                )
            );
        """)

        risk_rows = []

        for (
            forecast_date,
            sku,
            predicted_units,
        ) in forecasts:

            url = (
                f"{INVENTORY_BASE_URL}"
                f"/inventory/"
                f"{urllib.parse.quote(sku, safe='')}"
            )

            try:
                with urllib.request.urlopen(
                    url,
                    timeout=5,
                ) as response:

                    inventory_data = json.loads(
                        response.read().decode(
                            "utf-8"
                        )
                    )

            except urllib.error.HTTPError as exc:
                raise RuntimeError(
                    f"Inventory API returned "
                    f"{exc.code} for {sku}"
                ) from exc

            except urllib.error.URLError as exc:
                raise RuntimeError(
                    f"Inventory API unavailable "
                    f"for {sku}: {exc}"
                ) from exc

            inventory = int(
                inventory_data["available_quantity"]
            )

            demand = float(
                predicted_units
            )

            if demand <= 0:
                days_of_supply = None
                risk_level = "LOW"

            else:
                days_of_supply = (
                    inventory / demand
                )

                if days_of_supply < 1:
                    risk_level = "CRITICAL"

                elif days_of_supply < 3:
                    risk_level = "HIGH"

                elif days_of_supply < 7:
                    risk_level = "MEDIUM"

                else:
                    risk_level = "LOW"

            print(
                f"{sku}: "
                f"inventory={inventory}, "
                f"forecast={demand:.2f}, "
                f"days_of_supply="
                f"{days_of_supply}, "
                f"risk={risk_level}"
            )

            risk_rows.append(
                (
                    MODEL_NAME,
                    forecast_date,
                    sku,
                    inventory,
                    demand,
                    days_of_supply,
                    risk_level,
                )
            )

        connection = hook.get_conn()

        try:
            with connection.cursor() as cursor:

                cursor.execute("""
                    DELETE FROM sku_inventory_risk
                    WHERE model_name = %s
                      AND forecast_date = %s::date;
                """, (
                    MODEL_NAME,
                    forecast_info["forecast_date"],
                ))

                cursor.executemany("""
                    INSERT INTO
                    sku_inventory_risk (
                        model_name,
                        forecast_date,
                        sku,
                        current_inventory,
                        predicted_daily_demand,
                        days_of_supply,
                        risk_level,
                        inventory_checked_at,
                        generated_at
                    )

                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        NOW(),
                        NOW()
                    )

                    ON CONFLICT (
                        model_name,
                        forecast_date,
                        sku
                    )

                    DO UPDATE SET
                        current_inventory =
                            EXCLUDED.current_inventory,

                        predicted_daily_demand =
                            EXCLUDED.predicted_daily_demand,

                        days_of_supply =
                            EXCLUDED.days_of_supply,

                        risk_level =
                            EXCLUDED.risk_level,

                        inventory_checked_at =
                            NOW(),

                        generated_at =
                            NOW();
                """, risk_rows)

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        return forecast_info


    @task
    def validate_inventory_risk(forecast_info):
        hook = PostgresHook(
            postgres_conn_id="retail_postgres"
        )

        invalid_rows = hook.get_first("""
            SELECT COUNT(*)
            FROM sku_inventory_risk

            WHERE model_name = %s
              AND forecast_date = %s::date

              AND (
                    current_inventory < 0

                 OR predicted_daily_demand < 0

                 OR (
                        days_of_supply IS NOT NULL
                        AND days_of_supply < 0
                    )

                 OR risk_level NOT IN (
                        'LOW',
                        'MEDIUM',
                        'HIGH',
                        'CRITICAL'
                    )
              );
        """, parameters=(
            MODEL_NAME,
            forecast_info["forecast_date"],
        ))[0]

        if invalid_rows > 0:
            raise ValueError(
                f"Inventory risk table has "
                f"{invalid_rows} invalid rows"
            )

        rows = hook.get_records("""
            SELECT
                sku,
                current_inventory,
                predicted_daily_demand,
                days_of_supply,
                risk_level

            FROM sku_inventory_risk

            WHERE model_name = %s
              AND forecast_date = %s::date

            ORDER BY
                CASE risk_level
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 3
                    WHEN 'LOW' THEN 4
                END,
                sku;
        """, parameters=(
            MODEL_NAME,
            forecast_info["forecast_date"],
        ))

        if len(rows) != forecast_info["sku_count"]:
            raise ValueError(
                "Inventory-risk row count does not match "
                f"the forecast set: {len(rows)} != "
                f"{forecast_info['sku_count']}"
            )

        for row in rows:
            print(row)

        print(
            f"Validated {len(rows)} "
            f"inventory-risk rows"
        )


    forecast_check = check_latest_forecasts()
    risk = build_inventory_risk(forecast_check)
    validate_inventory_risk(risk)


retail_inventory_risk()
