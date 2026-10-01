"""Worker heartbeat: periodic liveness + resource report to the Master.

The heartbeat is a plain HTTP POST carrying the worker id and a live resource
sample.  The Master treats a heartbeat as proof-of-life and reaps any worker it
has not heard from within ``heartbeat_timeout_sec`` (see ``master.registry``).
"""

from __future__ import annotations

import threading
from typing import Callable, Optional

from backend.common.http_client import HttpClient


class HeartbeatThread(threading.Thread):
    """Daemon thread that POSTs a heartbeat on a fixed cadence."""

    def __init__(
        self,
        worker_id: str,
        master_url: str,
        interval_sec: float,
        status_provider: Callable[[], dict],
        client: Optional[HttpClient] = None,
        on_response: Optional[Callable[[dict], None]] = None,
    ) -> None:
        super().__init__(daemon=True, name=f"heartbeat-{worker_id}")
        self.worker_id = worker_id
        self.master_url = master_url.rstrip("/")
        self.interval = max(0.2, float(interval_sec))
        self.status_provider = status_provider
        self.client = client or HttpClient(timeout=5.0, retries=1)
        self.on_response = on_response
        # NOTE: named ``_stop_event`` (not ``_stop``) because ``threading._after_fork``
        # calls the ``Thread._stop()`` method during fork; shadowing it with an Event
        # would raise inside the forked child.
        self._stop_event = threading.Event()
        # Separate "cadence changed" event so set_interval can interrupt the
        # current wait without colliding with shutdown or being cleared by the
        # loop's own bookkeeping.
        self._wake = threading.Event()

    def set_interval(self, interval_sec: float) -> None:
        """Change the heartbeat cadence; the current wait exits immediately.

        Only an actual change signals the wait — this method is called on
        every heartbeat reply (config reconciliation), and signalling
        unconditionally would busy-loop because each reply arrives right after
        the loop clears the event.
        """
        new_interval = max(0.2, float(interval_sec))
        if abs(new_interval - self.interval) < 1e-9:
            return
        self.interval = new_interval
        self._wake.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            self._wake.clear()
            try:
                payload: dict = {"worker_id": self.worker_id}
                payload.update(self.status_provider())
                resp = self.client.post(
                    f"{self.master_url}/api/workers/heartbeat", payload, timeout=5.0,
                )
                if resp.ok and isinstance(resp.data, dict) and self.on_response is not None:
                    self.on_response(resp.data)
            except Exception:
                # A dropped heartbeat is expected during a Master restart; the
                # next tick retries and the Master's own timeout is generous.
                pass
            self._wake.wait(self.interval)

    def stop(self) -> None:
        self._stop_event.set()
        self._wake.set()
