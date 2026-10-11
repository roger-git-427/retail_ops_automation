# Operations

Commands below target Windows Command Prompt from the repository root. They use credentials from container environment variables and do not place credentials on the command line.

## Service runbook

Start and inspect:

```bat
docker compose up -d
docker compose ps
```

Stop without deleting volumes:

```bat
docker compose stop
```

`docker compose down -v` deletes the retail, n8n, and Airflow PostgreSQL volumes; do not use it when data must be retained.

Restart only Airflow runtime services:

```bat
docker compose restart airflow-api-server airflow-scheduler airflow-dag-processor
```

Inspect DAG registration and parser failures:

```bat
docker compose exec -T airflow-scheduler airflow dags list
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

View recent Airflow logs:

```bat
docker compose logs --tail 100 airflow-scheduler airflow-dag-processor
```

## Schema initialization

Compose does not apply application SQL automatically:

```bat
docker compose exec -T postgres sh -c "psql -U $POSTGRES_USER -d $POSTGRES_DB" < db\schema.sql
docker compose exec -T postgres sh -c "psql -U $POSTGRES_USER -d $POSTGRES_DB" < db\logging.sql
```

Both files use idempotent table creation; `schema.sql` also inserts the watermark row with `ON CONFLICT DO NOTHING`.

Reapply `schema.sql` when upgrading an existing checkout. It adds the order-change log and trigger without replacing order data, then creates one bootstrap change for each order not already represented in the log. The daily DAG also initializes these objects defensively.

## Manual DAG execution

The controller and every child must be unpaused. In the Airflow UI, unpause these DAGs once after deployment: `retail_daily_pipeline`, `retail_daily_analytics`, `retail_data_quality`, `retail_ml_features`, `retail_ml_baseline`, `retail_ml_xgboost`, `retail_ml_forecast`, and `retail_inventory_risk`.

Trigger the complete chain:

```bat
docker compose exec -T airflow-scheduler airflow dags trigger retail_daily_pipeline
```

Only `retail_daily_pipeline` is scheduled. Its trigger tasks wait for each child DAG to finish and stop the chain on failure. Individual child DAGs remain manually triggerable for focused recovery, but the operator then owns dependency safety.

## Backfill demonstration

Airflow backfills the scheduled controller; the controller creates or resets the matching child run for every logical date. Keep `max_active_runs=1` because the aggregate tables and model artifact are shared mutable outputs.

Preview a three-day backfill without creating runs:

```bat
docker compose exec -T airflow-scheduler airflow backfill create --dag-id retail_daily_pipeline --from-date 2026-09-28 --to-date 2026-09-30 --reprocess-behavior failed --max-active-runs 1 --dry-run
```

Run the same backfill:

```bat
docker compose exec -T airflow-scheduler airflow backfill create --dag-id retail_daily_pipeline --from-date 2026-09-28 --to-date 2026-09-30 --reprocess-behavior failed --max-active-runs 1
```

Demonstrate that the quality gate is blocking by changing one aggregate in a disposable environment, triggering `retail_data_quality`, and checking that it fails. Restore correctness by recording the date for an incremental rebuild, rerunning analytics, and rerunning the gate. Never perform this step against data that cannot be rebuilt:

```bat
docker compose exec -T postgres sh -c "psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"WITH changed AS (UPDATE daily_sales SET total_units = total_units + 1 WHERE sale_date = (SELECT MAX(sale_date) FROM daily_sales) RETURNING sale_date) INSERT INTO orders_change_log (order_id, affected_sale_date, operation) SELECT 0, sale_date, 'BOOTSTRAP' FROM changed;\""
docker compose exec -T airflow-scheduler airflow dags trigger retail_data_quality
docker compose exec -T airflow-scheduler airflow dags trigger retail_daily_analytics
docker compose exec -T airflow-scheduler airflow dags trigger retail_data_quality
```

The first quality run records a nonzero `daily_sales_mismatches` result and fails; after analytics rebuilds the recorded date, the second quality run records zero and succeeds.

Verify controller runs in the Airflow metadata database:

```bat
docker compose exec -T airflow-postgres psql -U airflow -d airflow -c "SELECT dag_id, run_id, logical_date, state FROM dag_run WHERE dag_id = 'retail_daily_pipeline' AND logical_date::date BETWEEN DATE '2026-09-28' AND DATE '2026-09-30' ORDER BY logical_date;"
```

This backfill replays the current-state, idempotent pipeline for historical logical dates; it does not recreate point-in-time source snapshots. The CDC watermark means the first run consumes pending changes and later runs can be no-ops at the analytics stage. That limitation is intentional and documented in [Scaling and trade-offs](scaling-and-tradeoffs.md).

## Inspection queries

Watermark:

```bat
echo SELECT pipeline_name, last_processed_change_id, last_processed_at, updated_at FROM analytics_pipeline_state ORDER BY pipeline_name; | docker compose exec -T postgres sh -c "psql -U $POSTGRES_USER -d $POSTGRES_DB -P pager=off"
```

Recent order changes:

```bat
echo SELECT change_id, order_id, affected_sale_date, operation, changed_at FROM orders_change_log ORDER BY change_id DESC LIMIT 20; | docker compose exec -T postgres sh -c "psql -U $POSTGRES_USER -d $POSTGRES_DB -P pager=off"
```

Model metrics:

```bat
echo SELECT model_name, evaluation_period, mae, rmse, wape, evaluated_at FROM ml_model_metrics ORDER BY model_name, evaluation_period; | docker compose exec -T postgres sh -c "psql -U $POSTGRES_USER -d $POSTGRES_DB -P pager=off"
```

Recent data-quality results:

```bat
echo SELECT checked_at, check_name, status, failure_count FROM data_quality_results ORDER BY checked_at DESC, id DESC LIMIT 20; | docker compose exec -T postgres sh -c "psql -U $POSTGRES_USER -d $POSTGRES_DB -P pager=off"
```

Latest forecast date per model:

```bat
echo SELECT f.* FROM sku_demand_forecasts f JOIN ^(SELECT model_name, MAX^(forecast_date^) forecast_date FROM sku_demand_forecasts GROUP BY model_name^) x USING ^(model_name, forecast_date^) ORDER BY f.model_name, f.sku; | docker compose exec -T postgres sh -c "psql -U $POSTGRES_USER -d $POSTGRES_DB -P pager=off"
```

Latest inventory-risk date per model:

```bat
echo SELECT r.* FROM sku_inventory_risk r JOIN ^(SELECT model_name, MAX^(forecast_date^) forecast_date FROM sku_inventory_risk GROUP BY model_name^) x USING ^(model_name, forecast_date^) ORDER BY r.model_name, r.risk_level, r.sku; | docker compose exec -T postgres sh -c "psql -U $POSTGRES_USER -d $POSTGRES_DB -P pager=off"
```

The carets escape parentheses for CMD before the SQL is piped into the container.

## Synthetic data

`db/seeded_ml_history.sql` supplies reproducible history for three SKUs and five retailers from 2025-10-01 through 2026-09-26. It includes weekday effects, annual sinusoidal seasonality, SKU-specific growth, 5% promotion spikes, random noise, randomized daytime timestamps, and deterministic `HIST-*` PO numbers. `setseed(0.42)` makes the random series reproducible, and PO conflicts are ignored.

Load it manually only when synthetic orders are wanted:

```bat
docker compose exec -T postgres sh -c "psql -U $POSTGRES_USER -d $POSTGRES_DB" < db\seeded_ml_history.sql
```

## Reliability mechanisms

- All DAGs limit concurrency with `max_active_runs=1`.
- The scheduled controller waits for child completion and will not release feature generation until the full-reconciliation quality DAG succeeds.
- Order triggers append every affected date to a durable change log, including updates and deletes.
- Daily analytics fixes one upper change ID, transactionally rebuilds complete affected-date aggregates, compares them to the source, then advances state.
- Feature rebuilds and per-model evaluation prediction replacements use database transactions.
- Forecast and inventory-risk reruns transactionally remove stale rows for the current model/date before insertion.
- State-table and state-row initialization are idempotent.
- Compose provisions the Airflow retail connection and gates runtime services on metadata migration.
- Evaluation windows follow the latest feature date rather than fixed calendar literals.
- Model artifacts use same-directory temporary files and atomic replacement.
- Forecasting rejects artifacts without schema/SKU metadata and rejects unseen SKUs.

## Runtime logs

`airflow/logs/` is configured to be ignored by Git because task and parser logs are mutable runtime output. Inspect it through the Airflow UI or `docker compose logs`. Historical failures remain in existing log files by design; a current successful run is the evidence that a corrected implementation works. Do not edit or delete old entries to make run history appear successful.

Ignore rules do not affect paths already added to the Git index. To stop tracking previously indexed logs without deleting local files, review the staged changes first and then run this one-time command intentionally:

```bat
git rm -r --cached airflow\logs
```

## Known limitations

- n8n database credentials and workflow imports require manual configuration.
- Trigger tasks poll while waiting and therefore occupy a LocalExecutor worker slot; the Compose deployment does not run a triggerer for deferrable mode.
- Inventory is static and is not decremented when an order is accepted.
- Inventory risk uses one-day forecast demand as a days-of-supply approximation.

## Future work — not implemented

- AI operations agent for SQL analytics, forecasts, inventory risk, inventory API access, n8n actions, and optional retrieval-augmented context.
- Multi-horizon forecasting, model registry/promotion, drift monitoring, and more realistic replenishment logic.
- Spark or a data lake only if data volume outgrows the current PostgreSQL batch design.
