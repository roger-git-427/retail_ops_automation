# Pipeline reference

## DAGs

All ML DAGs use `schedule=None`: they are manually triggered. The code implements no cross-DAG dependency, trigger, or dataset scheduling, so operators must preserve the logical order below. Every DAG has `max_active_runs=1`.

### `retail_daily_analytics`

- **Schedule:** `@daily`; `catchup=False`; two retries with a two-minute delay.
- **Purpose:** incrementally recompute daily, retailer/day, and SKU/day aggregates.
- **Flow:** `check_source_orders` → `get_processing_window` → `build_daily_sales` → [`build_retailer_metrics`, `build_sku_metrics`] → `validate_analytics` → `advance_watermark`.
- **Input:** `orders`, `orders_change_log`, `analytics_pipeline_state`.
- **Output:** `daily_sales`, `retailer_daily_metrics`, `sku_daily_metrics`, updated change-ID watermark.
- **Reliability:** idempotent change-capture initialization; fixed upper change ID per run; transactional delete/rebuild by affected date; source-to-aggregate validation before watermark advancement; no-op when no new change exists.

### `retail_ml_features`

- **Schedule:** manual.
- **Purpose:** create a gap-free SKU/day series and supervised next-day feature rows.
- **Flow:** `check_source_data` → `build_daily_series` → `build_ml_features` → `validate_ml_features`.
- **Input:** `sku_daily_metrics`.
- **Output:** `ml_sku_daily_series`, `ml_sku_features`.
- **Reliability:** source/output minimum of 100 rows; transactional create/truncate/insert and drop/create/insert operations; duplicate and null validation.

### `retail_ml_baseline`

- **Schedule:** manual.
- **Purpose:** evaluate persistence (`tomorrow ≈ today`) over the latest 30 target dates.
- **Flow:** `get_evaluation_window` → `check_features` → `build_baseline_predictions` → `evaluate_baseline`.
- **Input:** `ml_sku_features`.
- **Output:** `ml_forecast_predictions` under `naive_persistence`; `ml_model_metrics`.
- **Reliability:** dynamic window, atomic delete/insert for this model, metric upsert.

### `retail_ml_xgboost`

- **Schedule:** manual.
- **Purpose:** select, train, persist, and evaluate `XGBRegressor` as `xgboost_v1`.
- **Flow:** `get_dataset_split` → `check_dataset` → `train_model` → `evaluate_model`.
- **Input:** `ml_sku_features`.
- **Output:** `/opt/airflow/models/xgboost_v1.joblib`, `ml_forecast_predictions`, `ml_model_metrics`.
- **Reliability:** chronological dynamic split; row-count gates; non-negative predictions; atomic artifact replacement; transactional prediction replacement; metric upsert.

### `retail_ml_forecast`

- **Schedule:** manual.
- **Purpose:** generate next-day demand forecasts using the persisted model and latest dense-series row per SKU.
- **Flow:** `check_model` → `generate_forecasts` → `validate_forecasts`.
- **Input:** model artifact, `ml_sku_daily_series`.
- **Output:** `sku_demand_forecasts`.
- **Reliability:** artifact/schema/trained-SKU checks; non-negative clipping; current model/date rows deleted and inserted in one transaction; current-run count and value validation.

### `retail_inventory_risk`

- **Schedule:** manual; two retries with a one-minute delay.
- **Purpose:** combine the latest `xgboost_v1` forecast with current simulated inventory.
- **Flow:** `check_latest_forecasts` → `build_inventory_risk` → `validate_inventory_risk`.
- **Input:** `sku_demand_forecasts`, `GET http://inventory-api:8000/inventory/{sku}`.
- **Output:** `sku_inventory_risk`.
- **Reliability:** API timeout; encoded SKU path; exact forecast-date scope; atomic stale-row replacement; invalid-value and row-count validation.

## Logical run order

1. `retail_daily_analytics`
2. `retail_ml_features`
3. `retail_ml_baseline` when a reference evaluation is wanted
4. `retail_ml_xgboost` when training or reevaluation is needed
5. `retail_ml_forecast`
6. `retail_inventory_risk`

Training need not run as often as inference. After a compatible artifact exists, feature refresh → forecast → inventory risk can run without retraining.

## Analytics watermark

Order inserts, updates, and deletes append affected dates to `orders_change_log`. Previous change ID → capture the maximum current change ID → select log rows in `(previous, upper]` → identify affected dates → delete and rebuild complete aggregates for those dates → compare them to source-derived expectations → advance the watermark.

An update that moves an order to another date records both the old and new dates. A deletion records the old date, allowing the rebuild to remove stale groups and even clear a date that no longer has orders. Existing orders receive one bootstrap event when change capture is installed.

The monotonic change ID prevents same-timestamp changes from being skipped. `last_processed_at` remains an operational timestamp, while `last_processed_change_id` is the authoritative processing position.
