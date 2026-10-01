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

```bat
docker compose exec -T airflow-scheduler airflow dags trigger retail_daily_analytics
docker compose exec -T airflow-scheduler airflow dags trigger retail_ml_features
docker compose exec -T airflow-scheduler airflow dags trigger retail_ml_baseline
docker compose exec -T airflow-scheduler airflow dags trigger retail_ml_xgboost
docker compose exec -T airflow-scheduler airflow dags trigger retail_ml_forecast
docker compose exec -T airflow-scheduler airflow dags trigger retail_inventory_risk
```

Only `retail_daily_analytics` is scheduled. Trigger the manual DAGs in dependency order; Airflow code does not connect them across DAG boundaries.

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
- ML DAGs are manual and have no cross-DAG scheduling or dependency enforcement.
- Inventory is static and is not decremented when an order is accepted.
- Inventory risk uses one-day forecast demand as a days-of-supply approximation.

## Future work — not implemented

- AI operations agent for SQL analytics, forecasts, inventory risk, inventory API access, n8n actions, and optional retrieval-augmented context.
- Multi-horizon forecasting, scheduled/cross-DAG orchestration, model versioning, drift monitoring, and more realistic replenishment logic.
- Spark or a data lake only if data volume outgrows the current PostgreSQL batch design.
