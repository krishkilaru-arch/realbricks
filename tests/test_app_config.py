import json
import tempfile
import unittest
from pathlib import Path

from realbricks.app_config import load_app_config


class TestAppConfig(unittest.TestCase):
    def _write(self, payload: dict) -> Path:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        with tmp:
            tmp.write(json.dumps(payload))
        return Path(tmp.name)

    def test_valid_config(self) -> None:
        path = self._write(
            {
                "app_title": "x",
                "kpi_table": "c.s.kpi",
                "ranking_table": "c.s.rank",
                "state_table": "c.s.state",
                "page_size_default": 20,
                "page_size_max": 100,
                "allowed_users": ["a@x.com"],
                "allowed_email_domains": ["x.com"],
                "enable_actions": True,
            }
        )
        cfg = load_app_config(path)
        self.assertEqual(cfg.page_size_default, 20)
        self.assertEqual(cfg.allowed_email_domains, ["x.com"])

    def test_invalid_page_limits(self) -> None:
        path = self._write(
            {
                "app_title": "x",
                "kpi_table": "c.s.kpi",
                "ranking_table": "c.s.rank",
                "state_table": "c.s.state",
                "page_size_default": 100,
                "page_size_max": 20,
                "allowed_users": [],
                "allowed_email_domains": [],
                "enable_actions": True,
            }
        )
        with self.assertRaises(RuntimeError):
            load_app_config(path)


if __name__ == "__main__":
    unittest.main()

