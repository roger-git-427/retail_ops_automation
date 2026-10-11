import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class RepositoryContractTests(unittest.TestCase):
    def test_n8n_workflows_are_valid_json(self):
        workflow_paths = sorted((ROOT / "n8n" / "workflows").glob("*.json"))
        self.assertGreaterEqual(len(workflow_paths), 2)

        for path in workflow_paths:
            with self.subTest(path=path.name):
                with path.open(encoding="utf-8") as workflow_file:
                    workflow = json.load(workflow_file)
                self.assertIn("nodes", workflow)
                self.assertIn("connections", workflow)

    def test_documented_dag_graph_exists(self):
        graph = ROOT / "docs" / "images" / "retail-daily-pipeline.png"
        self.assertTrue(graph.is_file())
        self.assertIn(
            "docs/images/retail-daily-pipeline.png",
            (ROOT / "README.md").read_text(encoding="utf-8"),
        )

    def test_schema_contains_data_quality_audit_table(self):
        schema = (ROOT / "db" / "schema.sql").read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE IF NOT EXISTS data_quality_results", schema)


if __name__ == "__main__":
    unittest.main()
