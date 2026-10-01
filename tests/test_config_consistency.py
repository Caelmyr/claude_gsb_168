"""Tests for dynamic cluster configuration consistency."""

import inspect
import shutil
import tempfile
import unittest

from backend.common.config import ClusterConfig, ConfigManager
from backend.common.storage import Storage, read_json
from backend.master.scheduler import Scheduler


class ConfigUpdateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.manager = ConfigManager(self.storage)
        self.manager.ensure_seeded()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_partial_cluster_update_keeps_omitted_values(self):
        original = self.manager.load_cluster()
        updated = self.manager.update_cluster({
            "scheduler_tick_sec": 0.1,
            "seed": 42,
        })
        self.assertAlmostEqual(updated.scheduler_tick_sec, 0.1)
        self.assertEqual(updated.seed, 42)
        self.assertEqual(updated.max_attempts, original.max_attempts)

        on_disk = read_json(self.storage.path("config", "cluster.json"))
        self.assertAlmostEqual(on_disk["scheduler_tick_sec"], 0.1)
        self.assertEqual(on_disk["seed"], 42)
        self.assertEqual(on_disk["max_attempts"], original.max_attempts)

    def test_dataclass_merge_allows_every_exposed_field_to_change(self):
        cfg = ClusterConfig()
        fields = set(cfg.to_dict())
        values = {
            "heartbeat_interval_sec": 0.5,
            "heartbeat_timeout_sec": 2.0,
            "task_timeout_sec": 10.0,
            "max_attempts": 4,
            "retry_backoff_base_sec": 0.5,
            "speculative_execution": False,
            "speculation_threshold": 3.0,
            "shuffle_fetch_batch": 8,
            "shuffle_spill_records": 123,
            "map_parallelism_factor": 4.0,
            "reduce_parallelism_factor": 1.5,
            "scheduler_tick_sec": 0.2,
            "metric_interval_sec": 1.0,
            "demo_mode": True,
            "default_input_rows": 99,
            "seed": 7,
        }
        self.assertEqual(set(values), fields)
        merged = cfg.merge(values).validated()
        for key, value in values.items():
            self.assertEqual(getattr(merged, key), value)

    def test_partial_defaults_update_does_not_reset_omitted_values(self):
        original = self.manager.save_defaults(self.manager.load_defaults().merge({
            "mapper": "kv_mapper",
            "reducer": "sum_reducer",
            "num_map_tasks": 7,
            "num_reduce_tasks": 3,
            "input_rows": 1234,
        }))
        updated = self.manager.update_defaults({"input_rows": 4321})
        self.assertEqual(updated.mapper, original.mapper)
        self.assertEqual(updated.num_map_tasks, original.num_map_tasks)
        self.assertEqual(updated.input_rows, 4321)

    def test_scheduler_loop_uses_scheduler_tick(self):
        source = inspect.getsource(Scheduler._loop)
        self.assertIn("scheduler_tick_sec", source)
        self.assertNotIn("metric_interval_sec", source)

    def test_cluster_and_default_input_rows_share_one_saved_value(self):
        updated = self.manager.update_cluster({"default_input_rows": 321})
        self.assertEqual(updated.default_input_rows, 321)
        self.assertEqual(self.manager.load_defaults().input_rows, 321)

        defaults = self.manager.update_defaults({"input_rows": 654}, sync_input_rows=True)
        self.assertEqual(defaults.input_rows, 654)
        self.assertEqual(self.manager.load_cluster().default_input_rows, 654)


if __name__ == "__main__":
    unittest.main()
