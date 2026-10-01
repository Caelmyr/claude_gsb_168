"""Tests proving the previously-inert config knobs now drive scheduler behavior."""

import shutil
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common import jsonutil
from backend.common import models as _models
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master import fault_tolerance as _ftmod
from backend.master import job_manager as _jmmod
from backend.master import registry as _regmod
from backend.master import scheduler as _schedmod
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.scheduler import Scheduler
from backend.master.shuffle import ShuffleCoordinator


def _install_clock(holder):
    """Point every module's bound ``now_ms`` at one controllable clock."""
    def tick():
        return holder["t"]
    for mod in (jsonutil, _models, _ftmod, _jmmod, _regmod, _schedmod):
        mod.now_ms = tick


class _Handler(BaseHTTPRequestHandler):
    accepted = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        import json
        body = json.loads(self.rfile.read(length) or b"{}")
        if self.path == "/task/execute":
            type(self).accepted.append(body.get("task_id"))
            resp = b'{"accepted": true}'
        else:  # /task/cancel
            resp = b'{"cancelled": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(resp)))
        self.end_headers()
        self.wfile.write(resp)

    def log_message(self, *a):
        pass


class SchedulerHarness(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig()
        self.logbus = LogBus(self.storage)
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.registry = WorkerRegistry(self.storage, self.config)
        self.ft = FaultTolerance(self.storage, self.jm, self.config, self.logbus)
        self.metrics = Metrics(self.storage)
        self.shuffle = ShuffleCoordinator(self.storage, self.jm, self.registry, self.logbus)
        self.scheduler = Scheduler(
            self.storage, self.jm, self.registry, self.shuffle,
            self.ft, self.metrics, self.config, self.logbus,
        )
        self.server = HTTPServer(("127.0.0.1", 0), _Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.registry.register({
            "worker_id": "w1", "name": "w1", "host": "127.0.0.1",
            "port": self.port, "cpu_cores": 2, "mem_total_mb": 1,
        })

    def tearDown(self):
        self.server.shutdown()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _submit(self, maps=4, reduces=1):
        return self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": maps, "num_reduce_tasks": reduces,
            "input_rows": 100, "params": {},
        })


class TestParallelismFactor(SchedulerHarness):
    def test_map_parallelism_factor_caps_inflight_dispatches(self):
        # 1 alive worker * factor 0.5 -> ceil -> global cap of 1 map in flight.
        self.config.map_parallelism_factor = 0.5
        self._submit(maps=4, reduces=1)
        _Handler.accepted.clear()
        self.scheduler.tick()
        self.assertEqual(len(_Handler.accepted), 1)

        # A second tick with the first task still ASSIGNED must not exceed cap.
        self.scheduler.tick()
        self.assertEqual(len(_Handler.accepted), 1)

    def test_larger_factor_allows_more_inflight(self):
        # cap = ceil(1 worker * 3.0) = 3
        self.config.map_parallelism_factor = 3.0
        self._submit(maps=4, reduces=1)
        _Handler.accepted.clear()
        self.scheduler.tick()
        self.assertEqual(len(_Handler.accepted), 3)

    def test_factor_change_takes_effect_on_next_tick(self):
        self.config.map_parallelism_factor = 0.5
        self._submit(maps=4, reduces=1)
        _Handler.accepted.clear()
        self.scheduler.tick()
        self.assertEqual(len(_Handler.accepted), 1)
        # live reconfiguration (as the config PUT does in place)
        self.config.map_parallelism_factor = 5.0
        self.scheduler.tick()
        self.assertEqual(len(_Handler.accepted), 4)


class TestTaskTimeout(SchedulerHarness):
    def setUp(self):
        super().setUp()
        self.clock = {"t": 1_000_000}
        _install_clock(self.clock)

    def test_stuck_task_is_requeued_after_task_timeout_sec(self):
        self.config.task_timeout_sec = 10.0
        self.config.max_attempts = 3
        job = self._submit(maps=1, reduces=1)
        self.scheduler.tick()  # dispatch the map task -> ASSIGNED
        task = self.jm.tasks_for(job.job_id, C.TASK_MAP)[0]
        self.assertEqual(task.status, C.TASK_ASSIGNED)
        self.assertGreater(task.assigned_ms, 0)

        # Still within the timeout: nothing happens.
        self.clock["t"] += 9_000
        self.scheduler._enforce_task_timeouts()
        self.assertEqual(self.jm.get_task(job.job_id, task.task_id).status,
                         C.TASK_ASSIGNED)

        # Past the timeout: routed through the retry path (RETRYING, attempts+1).
        self.clock["t"] += 2_000
        self.scheduler._enforce_task_timeouts()
        refreshed = self.jm.get_task(job.job_id, task.task_id)
        self.assertEqual(refreshed.status, C.TASK_RETRYING)
        self.assertEqual(refreshed.attempts, 1)

    def test_timeout_change_is_honoured(self):
        self.config.task_timeout_sec = 30.0
        job = self._submit(maps=1, reduces=1)
        self.scheduler.tick()
        task = self.jm.tasks_for(job.job_id, C.TASK_MAP)[0]
        self.clock["t"] = task.assigned_ms + 20_000
        self.scheduler._enforce_task_timeouts()
        self.assertEqual(self.jm.get_task(job.job_id, task.task_id).status,
                         C.TASK_ASSIGNED)
        # shrink timeout live -> the same silence now trips it
        self.config.task_timeout_sec = 5.0
        self.scheduler._enforce_task_timeouts()
        self.assertEqual(self.jm.get_task(job.job_id, task.task_id).status,
                         C.TASK_RETRYING)


if __name__ == "__main__":
    unittest.main()
