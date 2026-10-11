from datetime import timedelta

import pendulum

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import dag


CHILD_DAGS = (
    "retail_daily_analytics",
    "retail_data_quality",
    "retail_ml_features",
    "retail_ml_baseline",
    "retail_ml_xgboost",
    "retail_ml_forecast",
    "retail_inventory_risk",
)


def _trigger(child_dag_id):
    return TriggerDagRunOperator(
        task_id=f"run_{child_dag_id}",
        trigger_dag_id=child_dag_id,
        trigger_run_id=(
            "retail_daily_pipeline__{{ logical_date | ts_nodash }}"
        ),
        logical_date="{{ logical_date }}",
        conf={
            "orchestrated_by": "retail_daily_pipeline",
            "parent_run_id": "{{ run_id }}",
            "data_interval_start": "{{ data_interval_start }}",
            "data_interval_end": "{{ data_interval_end }}",
        },
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        fail_when_dag_is_paused=True,
    )


@dag(
    dag_id="retail_daily_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["retail", "orchestration"],
)
def retail_daily_pipeline():
    stages = [_trigger(child_dag_id) for child_dag_id in CHILD_DAGS]

    for upstream, downstream in zip(stages, stages[1:]):
        upstream >> downstream


retail_daily_pipeline()
