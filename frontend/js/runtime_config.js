/* Shared runtime configuration. Pages read the same authoritative snapshot and
   immediately react when another page saves a new value. */
const RuntimeConfig = (() => {
  const DEFAULT = {
    heartbeat_interval_sec: 2,
    heartbeat_timeout_sec: 80,
    scheduler_tick_sec: 5,
    metric_interval_sec: 2,
  };
  let current = { ...DEFAULT };
  const listeners = new Set();
  let started = false;
  let timer = null;
  let inflight = null;
  const STORAGE_KEY = 'mr-runtime-config';
  const channel = ('BroadcastChannel' in window) ? new BroadcastChannel('mr-runtime-config') : null;

  function emit() {
    const snapshot = { ...current };
    listeners.forEach(fn => {
      try { fn(snapshot); } catch (e) { /* listener must not break polling */ }
    });
  }

  function apply(next, broadcast = false) {
    const old = current;
    current = { ...DEFAULT, ...(next || {}) };
    current.scheduler_tick_sec = Math.max(0.05, Number(current.scheduler_tick_sec) || DEFAULT.scheduler_tick_sec);
    current.metric_interval_sec = Math.max(0.5, Number(current.metric_interval_sec) || DEFAULT.metric_interval_sec);
    if (JSON.stringify(old) !== JSON.stringify(current)) {
      emit();
      if (broadcast) {
        const message = { ...current, ts: Date.now() };
        try { localStorage.setItem(STORAGE_KEY, JSON.stringify(message)); } catch (e) {}
        if (channel) channel.postMessage(message);
      }
    }
  }

  async function refresh(broadcast = false) {
    if (!inflight) inflight = API.get('/api/config');
    try {
      const cfg = await inflight;
      apply(cfg, broadcast);
      return { ...cfg };
    } finally {
      inflight = null;
    }
  }

  async function save(values) {
    const cfg = await API.put('/api/config', values);
    apply(cfg, true);
    return { ...cfg };
  }

  function start() {
    if (started) return;
    started = true;
    try {
      const cached = JSON.parse(localStorage.getItem(STORAGE_KEY) || 'null');
      if (cached) apply(cached, false);
    } catch (e) {}
    if (channel) {
      channel.addEventListener('message', e => apply(e.data, false));
    }
    window.addEventListener('storage', e => {
      if (e.key === STORAGE_KEY && e.newValue) {
        try { apply(JSON.parse(e.newValue), false); } catch (err) {}
      }
    });
    refresh(false);
    const loop = () => {
      refresh(false).finally(() => { timer = setTimeout(loop, 1000); });
    };
    timer = setTimeout(loop, 1000);
  }

  function get() { return { ...current }; }
  function onChange(fn) { listeners.add(fn); return () => listeners.delete(fn); }

  return { start, get, onChange, refresh, save };
})();
