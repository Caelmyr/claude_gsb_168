"""End-to-end tests for configuration consistency.

The bug these tests pin down: a value saved on the config page must be
simultaneously (a) echoed back exactly, (b) persisted to disk, and (c) the
value every live module actually runs with — including the scheduler cadence,
worker-facing settings and fields the form does not render (e.g. ``seed``).
"""

import shutil
import tempfile
import time
import unittest

from backend.common.config import ClusterConfig, ConfigManager, JobDefaults
from backend.common.storage import Storage
from backend.worker.heartbeat import HeartbeatThread

try:
    from backend.master.server import Master
    _HAS_FLASK = True
except ModuleNotFoundError:  # Flask not installed in this environment
    _HAS_FLASK = False
    Master = None

requires_flask = unittest.skipUnless(_HAS_FLASK, "Flask not installed")


class TestConfigModel(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.mgr = ConfigManager(Storage(self.tmp))
        self.mgr.ensure_seeded()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_partial_merge_preserves_unsubmitted_fields(self):
        # The form never submits ``seed``; saving must not reset it.
        seeded = self.mgr.save_cluster(ClusterConfig(seed=4242, max_attempts=5))
        self.assertEqual(seeded.seed, 4242)

        updated = self.mgr.update_cluster({"scheduler_tick_sec": 0.25})
        self.assertEqual(updated.scheduler_tick_sec, 0.25)
        self.assertEqual(updated.seed, 4242)          # preserved
        self.assertEqual(updated.max_attempts, 5)     # preserved

        # The persisted document carries the merged values too.
        reloaded = self.mgr.load_cluster()
        self.assertEqual(reloaded.scheduler_tick_sec, 0.25)
        self.assertEqual(reloaded.seed, 4242)

    def test_values_are_validated_on_merge(self):
        updated = self.mgr.update_cluster({"scheduler_tick_sec": 999})
        self.assertEqual(updated.scheduler_tick_sec, 10.0)  # clamped to hi

    def test_job_defaults_merge_preserves_params(self):
        self.mgr.save_defaults(JobDefaults(params={"k": "v"}))
        updated = self.mgr.update_defaults({"num_map_tasks": 12})
        self.assertEqual(updated.num_map_tasks, 12)
        self.assertEqual(updated.params, {"k": "v"})


@requires_flask
class TestConfigApiPropagation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.master = Master(self.tmp, host="127.0.0.1", port=0)
        self.client = self.master.app.test_client()

    def tearDown(self):
        self.master.scheduler.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _put(self, patch):
        resp = self.client.put("/api/config", json=patch)
        self.assertEqual(resp.status_code, 200)
        return resp.get_json()

    def test_get_initial_is_validated_default(self):
        doc = self.client.get("/api/config").get_json()
        self.assertIn("scheduler_tick_sec", doc)
        self.assertIn("seed", doc)

    def test_scheduler_tick_is_echoed_persisted_and_live(self):
        # The original bug: this field was skipped on PUT and the OLD value was
        # written back to disk.
        echo = self._put({"scheduler_tick_sec": 0.25})
        self.assertEqual(echo["scheduler_tick_sec"], 0.25)

        # live object the scheduler loop reads
        self.assertEqual(self.master.config.scheduler_tick_sec, 0.25)
        # every other shared-reference consumer sees it too
        self.assertEqual(self.master.job_manager.config.scheduler_tick_sec, 0.25)
        self.assertEqual(self.master.fault_tolerance.config.scheduler_tick_sec, 0.25)
        # persisted
        doc = self.client.get("/api/config").get_json()
        self.assertEqual(doc["scheduler_tick_sec"], 0.25)
        on_disk = self.master.config_manager.load_cluster()
        self.assertEqual(on_disk.scheduler_tick_sec, 0.25)

    def test_unrendered_seed_survives_form_save(self):
        original_seed = self.client.get("/api/config").get_json()["seed"]
        # A normal form save contains the 15 rendered fields but never seed.
        form_body = self.client.get("/api/config").get_json()
        form_body.pop("seed", None)
        form_body["task_timeout_sec"] = 42.0
        resp = self.client.put("/api/config", json=form_body)
        self.assertEqual(resp.get_json()["seed"], original_seed)
        self.assertEqual(resp.get_json()["task_timeout_sec"], 42.0)

    def test_all_consumers_share_one_live_config(self):
        self._put({"heartbeat_timeout_sec": 12.0, "max_attempts": 7,
                   "speculation_threshold": 3.5, "shuffle_fetch_batch": 11})
        refs = [
            self.master.config,
            self.master.job_manager.config,
            self.master.job_manager.planner.config,
            self.master.registry.config,
            self.master.fault_tolerance.config,
            self.master.scheduler.config,
        ]
        for r in refs:
            self.assertIs(r, refs[0])
            self.assertEqual(r.heartbeat_timeout_sec, 12.0)
            self.assertEqual(r.max_attempts, 7)
            self.assertEqual(r.shuffle_fetch_batch, 11)

    def test_worker_facing_routes_return_live_config(self):
        self._put({"heartbeat_interval_sec": 0.5, "demo_mode": True})
        # A worker receives the current config on (re)registration...
        reg = self.client.post("/api/workers/register", json={
            "worker_id": "w1", "name": "w1", "host": "127.0.0.1", "port": 9001,
        })
        self.assertEqual(reg.get_json()["config"]["heartbeat_interval_sec"], 0.5)
        self.assertTrue(reg.get_json()["config"]["demo_mode"])
        # ...and on every heartbeat afterwards.
        hb = self.client.post("/api/workers/heartbeat", json={
            "worker_id": "w1", "cpu_percent": 1.0, "mem_percent": 2.0,
        })
        self.assertEqual(hb.get_json()["config"]["heartbeat_interval_sec"], 0.5)


class TestHeartbeatIntervalLiveUpdate(unittest.TestCase):
    def test_interval_change_takes_effect_on_next_beat(self):
        hb = HeartbeatThread(
            "w", "http://127.0.0.1:1", interval_sec=5.0,
            status_provider=dict, client=_NeverClient(),
        )
        self.assertAlmostEqual(hb.interval, 5.0)
        hb.set_interval(0.25)   # what an apply-config call does
        # the wake event is signalled, so the sleeping loop exits immediately
        # (and the stop event must NOT be set by a mere cadence change)
        self.assertTrue(hb._wake.is_set())
        self.assertFalse(hb._stop_event.is_set())
        self.assertAlmostEqual(hb.interval, 0.25)


class _NeverClient:
    """HttpClient stand-in whose POST always fails (no server needed)."""

    def post(self, *a, **k):
        raise RuntimeError("no network")


@requires_flask
class TestSchedulerCadence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.master = Master(self.tmp, host="127.0.0.1", port=0)

    def tearDown(self):
        self.master.scheduler.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_loop_wait_uses_scheduler_tick_not_metric_interval(self):
        waits = []
        self.master.config.metric_interval_sec = 9.0
        self.master.config.scheduler_tick_sec = 0.05
        orig_wait = self.master.scheduler._wake.wait
        def recording_wait(seconds):
            waits.append(seconds)
            return orig_wait(seconds)
        self.master.scheduler._wake.wait = recording_wait

        self.master.scheduler.start()
        deadline = time.time() + 2.0
        while time.time() < deadline and len(waits) < 3:
            time.sleep(0.02)
        self.master.scheduler.stop()
        self.assertTrue(waits, "scheduler loop never waited")
        self.assertTrue(all(abs(w - 0.05) < 1e-6 for w in waits),
                        f"loop waited on {waits}, expected scheduler_tick_sec=0.05")

    def test_config_save_wakes_scheduler_immediately(self):
        self.master.scheduler.start()
        # Long tick currently in effect.
        self.master.config.scheduler_tick_sec = 30.0
        time.sleep(0.15)  # let the loop enter its 30s wait
        self.master.scheduler.notify_config_changed()
        # The wake event is set so the long sleep is interrupted promptly.
        self.assertTrue(self.master.scheduler._wake.is_set())
        self.master.scheduler.stop()


if __name__ == "__main__":
    unittest.main()
