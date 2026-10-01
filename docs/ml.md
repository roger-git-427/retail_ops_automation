# ML, forecasting, and inventory risk

## Dense SKU/day series

For each SKU, `ml_sku_daily_series` spans its first observed sale date through yesterday. A missing SKU/date combination represents zero demand, not a missing row. This preserves the meaning of fixed-day lags, rolling averages, volatility, and zero-demand frequency.

## Feature groups

| Group | Implemented fields |
| --- | --- |
| Current day | `units_today`, `orders_today`, `revenue_today` |
| Lags | `lag_1_units`, `lag_7_units`, `lag_14_units`, `lag_28_units` |
| Rolling means | `rolling_7_avg`, `rolling_14_avg`, `rolling_28_avg` over preceding rows only |
| Rolling standard deviation | `rolling_7_std`, `rolling_28_std` |
| Zero demand | `zero_days_last_7` |
| Trend | `trend_7_vs_28` = seven-day mean − 28-day mean |
| Target calendar | ISO weekday, ISO week, month, day of month, weekend flag |
| Cyclical encoding | weekday sine/cosine, month sine/cosine |
| Supervised target | `target_next_day_units` |

The feature row for target date *t+1* uses values from date *t* or earlier; rolling windows exclude the current row and no target demand is included among the predictors.

## Evaluation models

### Persistence baseline

`naive_persistence` predicts tomorrow's units as today's `units_today`. It is evaluated over the latest 30 available target dates and provides a reference that a trained model should improve upon.

### XGBoost

- **Class/name:** `xgboost.XGBRegressor`, persisted as `xgboost_v1`.
- **Inputs:** all numeric fields above plus one-hot encoded `sku` columns from `pandas.get_dummies`.
- **Split:** latest 30 target dates are test; preceding 31 are validation; all earlier rows are training.
- **Why chronological:** future observations never select or train a model evaluated on earlier observations; random splitting would mix time periods.
- **Selection:** lowest validation MAE across three configurations: 200/depth 3/rate .05, 300/depth 3/rate .03, and 200/depth 4/rate .05; all use 0.9 row and column subsampling.
- **Retraining:** the selected parameters are fitted again on combined training and validation rows.
- **Output rules:** predictions are clipped at zero; MAE, RMSE, and WAPE are computed, with WAPE left null when total actual demand is zero.
- **Artifact:** `/opt/airflow/models/xgboost_v1.joblib`, backed by `./airflow/models`; joblib writes a temporary file in the same directory and `os.replace` swaps it atomically.
- **Compatibility:** the artifact stores ordered feature columns and trained SKUs; inference fails on a previously unseen SKU instead of silently discarding its identity.

Evaluation predictions go to `ml_forecast_predictions`; metrics go to `ml_model_metrics`.

## Current metrics

Read from the running retail database on 2026-10-01 UTC; values are not copied from earlier documentation.

| Model | Evaluation period | MAE | RMSE | WAPE |
| --- | --- | ---: | ---: | ---: |
| `naive_persistence` | `2026-09-01_to_2026-09-30` | 29.3667 | 83.7139 | 35.8762% |
| `xgboost_v1` | `2026-09-01_to_2026-09-30` | 25.0508 | 66.6344 | 30.6036% |

See [Operations](operations.md#inspection-queries) for the read-only query.

## Evaluation versus inference

Evaluation uses historical unseen rows that already have `target_next_day_units`, allowing comparison with actual demand. Inference loads the persisted artifact, builds the same feature schema from the latest dense-series row per SKU, and predicts its next date without an actual.

`sku_demand_forecasts` stores `model_name`, `forecast_date`, `sku`, `predicted_units`, `source_feature_date`, and `generated_at`. Historical dates remain in the table. For dates produced by a rerun, the DAG deletes that model/date set and inserts the complete current set in one transaction, so removed SKUs do not remain stale. The latest forecast is the maximum `forecast_date` per model.

## Inventory risk

The inventory DAG reads the API's `available_quantity` and computes:

`days_of_supply = current_inventory / predicted_daily_demand`

| Condition | Category |
| --- | --- |
| Demand ≤ 0 | `LOW`, with null days of supply |
| Days < 1 | `CRITICAL` |
| 1 ≤ days < 3 | `HIGH` |
| 3 ≤ days < 7 | `MEDIUM` |
| Days ≥ 7 | `LOW` |

This ratio treats the next-day forecast as representative daily demand. It is an approximation, not a stockout probability.

`sku_inventory_risk` retains historical forecast dates. A rerun deletes and replaces all rows for the exact model/forecast date in one transaction, then verifies that its row count matches the forecast set.
