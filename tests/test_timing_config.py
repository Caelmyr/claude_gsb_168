"""Tests for runtime timing configuration."""

import shutil
import tempfile
import unittest

from backend.common.config import ClusterConfig
from backend.common.storage import Storage
from backend.master.registry import WorkerRegistry


class TimingConfigTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.config = ClusterConfig(heartbeat_timeout_sec=2.0)
        self.registry = WorkerRegistry(Storage(self.tmp), self.config)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_heartbeat_timeout_uses_configured_seconds(self):
        self.registry.register({"worker_id": "w1", "name": "w1", "host": "127.0.0.1", "port": 1})
        worker = self.registry.get("w1")
        worker.last_heartbeat_ms -= 2500
        self.assertEqual([w.worker_id for w in self.registry.reap()], ["w1"])
        self.assertFalse(self.registry.get("w1").is_alive)

    def test_heartbeat_timeout_changes_are_seen_through_shared_config(self):
        self.registry.register({"worker_id": "w1", "name": "w1", "host": "127.0.0.1", "port": 1})
        worker = self.registry.get("w1")
        self.config.heartbeat_timeout_sec = 1.0
        worker.last_heartbeat_ms -= 1500
        self.assertEqual([w.worker_id for w in self.registry.reap()], ["w1"])


if __name__ == "__main__":
    unittest.main()
