# Data model

Application, analytics, and ML tables share the retail PostgreSQL database. Airflow metadata remains in the separate `airflow-postgres` service.

| Table | Purpose | Key | Writer | Consumer |
| --- | --- | --- | --- | --- |
| `orders` | Validated purchase orders | PK `id`; unique `po_number` | n8n order workflow; optional seed SQL | daily analytics; n8n duplicate check |
| `orders_change_log` | Append-only affected dates for order inserts, updates, and deletes | PK `change_id` | database trigger and bootstrap | daily analytics |
| `automation_executions` | Operational workflow outcomes | PK `id` | n8n logging workflow | operational inspection |
| `analytics_pipeline_state` | Per-pipeline watermark | PK `pipeline_name` | schema initialization; daily analytics | daily analytics |
| `data_quality_results` | Append-only outcomes from each blocking quality check | PK `id` | data-quality DAG | operations and incident review |
| `daily_sales` | One aggregate per sale date | PK `sale_date` | daily analytics | reporting/inspection |
| `retailer_daily_metrics` | Retailer/day aggregates | PK (`sale_date`, `retailer_name`) | daily analytics | reporting/inspection |
| `sku_daily_metrics` | SKU/day aggregates | PK (`sale_date`, `sku`) | daily analytics | ML feature DAG |
| `ml_sku_daily_series` | Dense SKU/day demand series | PK (`sale_date`, `sku`) | ML feature DAG | ML feature and forecast DAGs |
| `ml_sku_features` | Supervised next-day feature rows | PK (`feature_date`, `sku`) | ML feature DAG | baseline and XGBoost DAGs |
| `ml_forecast_predictions` | Historical holdout predictions and actuals | PK (`model_name`, `forecast_date`, `sku`) | baseline and XGBoost evaluation DAGs | model evaluation |
| `ml_model_metrics` | MAE, RMSE, and WAPE by evaluation period | PK (`model_name`, `evaluation_period`) | baseline and XGBoost evaluation DAGs | model comparison/inspection |
| `sku_demand_forecasts` | Inference output for a future target date | PK (`model_name`, `forecast_date`, `sku`) | forecast DAG | inventory-risk DAG |
| `sku_inventory_risk` | Inventory/demand ratio and risk category | PK (`model_name`, `forecast_date`, `sku`) | inventory-risk DAG | operational inspection |

## Core records

`orders` stores PO number, retailer, SKU, positive quantity, non-negative unit price, status, and an ingestion timestamp. Database constraints enforce the PO idempotency key and numeric bounds. A trigger writes the affected sale date to `orders_change_log` after every insert, update, or delete; date-moving updates record both dates.

`automation_executions` stores PO number, workflow name, outcome, optional error type, message, and database-generated timestamp. It is an execution history, not a foreign-key child of `orders`.

## Analytics aggregates

- `daily_sales`: order count, total units, and revenue per date.
- `retailer_daily_metrics`: the same measures grouped by retailer and date.
- `sku_daily_metrics`: the same measures grouped by SKU and date.

Each affected date is selected from the change-ID window. Existing aggregates for those dates are deleted and rebuilt from current `orders` in a transaction. This handles updates, deletions, and groups that disappear completely while keeping reruns idempotent.

## ML records

`ml_sku_daily_series` fills every date from each SKU's first observed date through yesterday. Missing SKU/date observations become zero orders, units, and revenue.

`ml_sku_features` records `feature_date`, next-day `target_date`, current and historical predictors, calendar encodings, and `target_next_day_units`. Rows without 28-day history, rolling volatility, or a known next-day target are excluded.

`ml_forecast_predictions` is evaluation data: it contains both `predicted_units` and `actual_units`. `sku_demand_forecasts` is inference data: it records the forecast, source feature date, and generation timestamp without an actual.

## Watermark state

The state row is created idempotently by `db/schema.sql` and again defensively by the daily DAG. `last_processed_change_id` is authoritative; `last_processed_at` records the corresponding change time for operators. A successful run advances both only after all aggregate builders match source-derived expectations.

## Data-quality audit

`retail_data_quality` performs a full reconciliation after incremental analytics completes. It checks source field validity, all three aggregate tables against fresh source-derived results, and whether any order-change IDs remain above the analytics watermark. Each check appends `PASS` or `FAIL`, its failing-row count, and a timestamp to `data_quality_results`. Any nonzero count raises an exception, so the controller cannot start feature generation.
