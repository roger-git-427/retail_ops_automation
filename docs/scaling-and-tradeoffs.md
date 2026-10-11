# Scaling and trade-offs

The current design favors a reproducible, locally runnable system over distributed throughput. These are deliberate boundaries rather than claims that the Docker Compose topology is production scale.

## Orchestration

The scheduled `retail_daily_pipeline` DAG uses `TriggerDagRunOperator` with `wait_for_completion=True`. This gives every stage its own run history while making child failures visible in one controller graph. It also couples the controller to child DAG IDs and occupies a LocalExecutor worker slot while it polls. A larger deployment should run the operators in deferrable mode with an Airflow triggerer, or replace the controller with Asset-aware scheduling when durable dataset updates—not a fixed daily sequence—should release consumers.

All DAGs use `max_active_runs=1`. This protects the shared PostgreSQL tables and single model path from overlapping replacement jobs, but limits throughput. Partitioned outputs, run-scoped artifact paths, and explicit promotion would be required before increasing concurrency.

## Data and compute

Incremental analytics rebuilds only dates named by the append-only change log, which keeps ordinary daily work small and handles updates and deletes. The blocking quality gate deliberately reconciles complete aggregates against `orders`; this is easy to reason about but is O(N) in source history. At higher volume, partition the tables by date and run full reconciliation periodically while retaining per-partition checks in the daily path.

PostgreSQL is appropriate for the present order volume and keeps operational, aggregate, and feature data easy to inspect. Growth would call for retention on `orders_change_log` and `data_quality_results`, read replicas for reporting, and possibly object storage plus a distributed query engine only after measured database pressure justifies the added complexity.

## Model lifecycle

Daily XGBoost retraining keeps the demo self-contained and continuously compares the model with persistence. The cost grows with history, and daily tuning can create unnecessary churn. A production design would separate daily inference from weekly or drift-triggered training, register immutable artifacts, validate a challenger, and promote it explicitly.

The model one-hot encodes known SKUs and fails on unseen SKUs. Failing is safer than silently erasing SKU identity, but new-item forecasting needs a cold-start policy such as category features, a pooled model, or an approved baseline fallback.

## Availability and serving

Compose uses Airflow `LocalExecutor`, one scheduler, one API server, and local bind-mounted model storage. Horizontal execution requires a distributed executor, remote logs, shared artifact storage, highly available metadata PostgreSQL, secret management, and health/alerting outside the Compose host.

The FastAPI inventory service is deterministic test infrastructure. It does not reserve or decrement stock, and days of supply treats one next-day forecast as steady daily demand. Real replenishment decisions need an inventory system of record, lead times, safety stock, open purchase orders, forecast uncertainty, and service-level targets.

## n8n boundary

n8n owns short request/response automation: validation, duplicate detection, inventory lookup, order persistence, and execution logging. Airflow owns repeatable batch computation. Moving long-running ML into n8n would weaken scheduling and retry visibility; using Airflow for synchronous order intake would add avoidable latency. The split does require shared contracts and coordinated schema changes.
