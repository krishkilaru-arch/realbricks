import json
import tempfile
import unittest
from pathlib import Path

from realbricks.pipelines.ingest import load_source_configs


class TestIngestConfigValidation(unittest.TestCase):
    def _write(self, payload: dict) -> Path:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        with tmp:
            tmp.write(json.dumps(payload))
        return Path(tmp.name)

    def test_rejects_duplicate_targets(self) -> None:
        p = self._write(
            {
                "sources": [
                    {"name": "a", "source_table": "c.s.t1", "target_table": "c.b.x"},
                    {"name": "b", "source_table": "c.s.t2", "target_table": "c.b.x"},
                ]
            }
        )
        with self.assertRaises(RuntimeError):
            load_source_configs(p)

    def test_rejects_invalid_mode(self) -> None:
        p = self._write(
            {
                "sources": [
                    {
                        "name": "a",
                        "source_table": "c.s.t1",
                        "target_table": "c.b.x",
                        "mode": "truncate",
                    }
                ]
            }
        )
        with self.assertRaises(RuntimeError):
            load_source_configs(p)

    def test_accepts_valid_config(self) -> None:
        p = self._write(
            {
                "sources": [
                    {
                        "name": "a",
                        "source_table": "c.s.t1",
                        "target_table": "c.b.x",
                        "mode": "overwrite",
                    }
                ]
            }
        )
        sources = load_source_configs(p)
        self.assertEqual(len(sources), 1)
        self.assertEqual(sources[0].name, "a")


if __name__ == "__main__":
    unittest.main()

