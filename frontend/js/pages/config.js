/* 配置管理 Config */
Components.init('config');
const C = Components;

const numberField = (key, label, opts) => ({ key, label, type: 'number', min: opts.min, max: opts.max, step: opts.step });

const CLUSTER_FIELDS = [
  numberField('heartbeat_interval_sec', '心跳间隔 Heartbeat interval (s)', { step: 0.5, min: 0.2, max: 60 }),
  numberField('heartbeat_timeout_sec', '心跳超时 Heartbeat timeout (s)', { step: 0.5, min: 1, max: 300 }),
  numberField('task_timeout_sec', '任务超时 Task timeout (s)', { step: 5, min: 5, max: 3600 }),
  numberField('max_attempts', '最大重试次数 Max attempts', { step: 1, min: 1, max: 10 }),
  numberField('retry_backoff_base_sec', '重试退避基数 Backoff base (s)', { step: 0.1, min: 0.1, max: 60 }),
  { key: 'speculative_execution', label: '推测执行 Speculative execution', type: 'checkbox' },
  numberField('speculation_threshold', '推测阈值 Speculation threshold (×median)', { step: 0.1, min: 1, max: 10 }),
  numberField('shuffle_fetch_batch', 'Shuffle 拉取批次 Fetch batch', { step: 1, min: 1, max: 10000 }),
  numberField('shuffle_spill_records', 'Shuffle 溢写阈值 Spill records', { step: 100, min: 100, max: 1000000 }),
  numberField('map_parallelism_factor', 'Map 并行因子 Map parallelism', { step: 0.5, min: 0.5, max: 50 }),
  numberField('reduce_parallelism_factor', 'Reduce 并行因子 Reduce parallelism', { step: 0.5, min: 0.5, max: 50 }),
  numberField('scheduler_tick_sec', '调度周期 Scheduler tick (s)', { step: 0.05, min: 0.05, max: 10 }),
  numberField('metric_interval_sec', '指标采样周期 Metric interval (s)', { step: 0.5, min: 0.5, max: 60 }),
  { key: 'demo_mode', label: '演示模式 Demo mode', type: 'checkbox' },
  numberField('seed', '随机种子 Seed', { step: 1, min: 0, max: 2147483647 }),
];

const DEFAULT_FIELDS = [
  { key: 'mapper', label: '默认 Map 函数 Default mapper', type: 'text' },
  { key: 'reducer', label: '默认 Reduce 函数 Default reducer', type: 'text' },
  { key: 'num_map_tasks', label: '默认 Map 任务数 Map tasks', type: 'number', step: 1, min: 1 },
  { key: 'num_reduce_tasks', label: '默认 Reduce 任务数 Reduce tasks', type: 'number', step: 1, min: 1 },
  { key: 'input_rows', label: '默认输入行数 Default input rows（新作业/sample jobs）', type: 'number', step: 100, min: 10 },
];

function renderForm(hostId, fields, data) {
  const host = document.getElementById(hostId);
  host.innerHTML = fields.map(f => {
    if (f.type === 'checkbox') {
      return `<label style="display:flex;align-items:center;gap:8px;margin:10px 0 3px">
        <input type="checkbox" id="${hostId}-${f.key}" style="width:auto" ${data[f.key] ? 'checked' : ''}>
        ${C.esc(f.label)}
      </label>`;
    }
    return `<label>${C.esc(f.label)}</label>
      <input type="${f.type}" id="${hostId}-${f.key}" value="${C.esc(data[f.key])}"
        ${f.step ? `step="${f.step}"` : ''} ${f.min != null ? `min="${f.min}"` : ''}
        ${f.max != null ? `max="${f.max}"` : ''}>`;
  }).join('');
}

function readForm(hostId, fields) {
  const out = {};
  fields.forEach(f => {
    const el = document.getElementById(`${hostId}-${f.key}`);
    if (f.type === 'checkbox') out[f.key] = el.checked;
    else if (f.type === 'number') out[f.key] = parseFloat(el.value) || 0;
    else out[f.key] = el.value;
  });
  return out;
}

async function load() {
  RuntimeConfig.start();
  const cfg = await API.get('/api/config');
  renderForm('cluster-form', CLUSTER_FIELDS, cfg);
  const defaults = await API.get('/api/config/defaults');
  renderForm('defaults-form', DEFAULT_FIELDS, defaults);
}

document.getElementById('save-cluster').addEventListener('click', async () => {
  const body = readForm('cluster-form', CLUSTER_FIELDS);
  try {
    const saved = await RuntimeConfig.save(body);
    renderForm('cluster-form', CLUSTER_FIELDS, saved);
    C.toast('集群配置已保存并生效 Cluster config saved and applied', 'ok');
  } catch (e) { C.toast('保存失败 ' + e.message, 'error'); }
});

document.getElementById('save-defaults').addEventListener('click', async () => {
  const body = readForm('defaults-form', DEFAULT_FIELDS);
  try {
    const saved = await API.put('/api/config/defaults', body);
    renderForm('defaults-form', DEFAULT_FIELDS, saved);
    const cfg = await RuntimeConfig.refresh(true);
    renderForm('cluster-form', CLUSTER_FIELDS, cfg);
    C.toast('默认值已保存 Defaults saved', 'ok');
  } catch (e) { C.toast('保存失败 ' + e.message, 'error'); }
});

load();
