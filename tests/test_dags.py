import unittest
from pathlib import Path

from airflow.dag_processing.dagbag import DagBag


CONTAINER_DAG_FOLDER = Path("/opt/airflow/dags")
DAG_FOLDER = (
    CONTAINER_DAG_FOLDER
    if CONTAINER_DAG_FOLDER.is_dir()
    else Path(__file__).resolve().parents[1] / "airflow" / "dags"
)


class DagContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(
            dag_folder=str(DAG_FOLDER),
            safe_mode=False,
        )

    def test_all_dags_import_without_errors(self):
        self.assertEqual({}, self.dagbag.import_errors)

    def test_daily_controller_has_the_expected_blocking_chain(self):
        dag = self.dagbag.dags.get("retail_daily_pipeline")
        self.assertIsNotNone(dag)

        child_dags = [
            "retail_daily_analytics",
            "retail_data_quality",
            "retail_ml_features",
            "retail_ml_baseline",
            "retail_ml_xgboost",
            "retail_ml_forecast",
            "retail_inventory_risk",
        ]
        task_ids = [f"run_{dag_id}" for dag_id in child_dags]
        self.assertEqual(set(task_ids), set(dag.task_ids))

        for index, task_id in enumerate(task_ids):
            task = dag.get_task(task_id)
            self.assertEqual(child_dags[index], task.trigger_dag_id)
            expected_downstream = (
                {task_ids[index + 1]}
                if index + 1 < len(task_ids)
                else set()
            )
            self.assertEqual(expected_downstream, task.downstream_task_ids)
            self.assertTrue(task.wait_for_completion)

    def test_data_quality_is_a_single_blocking_gate(self):
        dag = self.dagbag.dags.get("retail_data_quality")
        self.assertIsNotNone(dag)
        self.assertEqual(["blocking_data_quality_gate"], dag.task_ids)

    def test_analytics_validation_blocks_watermark_advancement(self):
        dag = self.dagbag.dags.get("retail_daily_analytics")
        validation = dag.get_task("validate_analytics")
        self.assertIn("advance_watermark", validation.downstream_task_ids)


if __name__ == "__main__":
    unittest.main()
