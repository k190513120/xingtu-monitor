import { bitable } from '@lark-base-open/js-sdk';

const API = '';

// ─── 通用消息提示（替代 alert，Feishu iframe 中 alert 被屏蔽）─────────────────
function showMsg(id: string, text: string, ok = true) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  (el as HTMLElement).style.color = ok ? '#27c346' : '#f54a45';
  setTimeout(() => { el.textContent = ''; }, 5000);
}

function showSaveMsg(text: string, ok = true) { showMsg('saveMsg', text, ok); }
function showRunMsg(text: string, ok = true) { showMsg('runMsg', text, ok); }

// ─── Tab 切换 ────────────────────────────────────────────────────────────────
(window as any).switchTab = function (name: string) {
  ['config', 'run'].forEach((n) => {
    document.getElementById('tab-btn-' + n)!.classList.toggle('active', n === name);
    document.getElementById('tab-' + n)!.classList.toggle('active', n === name);
  });
  if (name === 'run') loadStatus();
};

// ─── 同一多维表格 ─────────────────────────────────────────────────────────────
(window as any).onSameBaseChange = async function () {
  const same = (document.getElementById('sameBase') as HTMLInputElement).checked;
  document.getElementById('targetTokenFields')!.style.display = same ? 'none' : '';
  if (same) await loadTargetTables();
};

function getTargetAppToken(): string {
  if ((document.getElementById('sameBase') as HTMLInputElement).checked)
    return (document.getElementById('sourceAppToken') as HTMLInputElement).value;
  return (document.getElementById('targetAppToken') as HTMLInputElement).value.trim();
}

function getTargetToken(): string {
  if ((document.getElementById('sameBase') as HTMLInputElement).checked)
    return (document.getElementById('sourceToken') as HTMLInputElement).value.trim();
  return (document.getElementById('targetToken') as HTMLInputElement).value.trim();
}

// ─── 博主表：JS SDK 读取 ──────────────────────────────────────────────────────
async function loadSourceTables() {
  // 注意：getSelection().baseId 返回的是飞书内部加密 ID，不是 REST API 用的 appToken
  // appToken 必须由用户从 URL /base/xxx 中手动复制
  try {
    const tableList = await bitable.base.getTableMetaList();
    const sel = document.getElementById('sourceTableId') as HTMLSelectElement;
    sel.innerHTML = tableList
      .map((t) => `<option value="${t.id}">${t.name}</option>`)
      .join('');
    if (tableList.length > 0) await loadSourceFields(tableList[0].id);
  } catch (e: any) {
    (document.getElementById('sourceTableId') as HTMLSelectElement).innerHTML =
      `<option value="">⚠️ 加载失败: ${e?.message || e}</option>`;
  }
}

async function loadSourceFields(tableId: string) {
  if (!tableId) return;
  try {
    const table = await bitable.base.getTableById(tableId);
    const fieldList = await table.getFieldMetaList();
    (document.getElementById('profileUrlFieldName') as HTMLSelectElement).innerHTML = fieldList
      .map((f) => `<option value="${f.name}">${f.name}</option>`)
      .join('');
  } catch (e) {
    console.warn('字段加载失败:', e);
  }
}

(window as any).onSourceTableChange = async function () {
  const tableId = (document.getElementById('sourceTableId') as HTMLSelectElement).value;
  await loadSourceFields(tableId);
  if ((document.getElementById('sameBase') as HTMLInputElement).checked) await loadTargetTables();
};

// ─── 目标表 ───────────────────────────────────────────────────────────────────
async function loadTargetTables() {
  if ((document.getElementById('sameBase') as HTMLInputElement).checked) {
    try {
      const tableList = await bitable.base.getTableMetaList();
      (document.getElementById('targetTableId') as HTMLSelectElement).innerHTML = tableList
        .map((t) => `<option value="${t.id}">${t.name}</option>`)
        .join('');
    } catch (e) {
      console.warn(e);
    }
  } else {
    const appToken = getTargetAppToken();
    const token = getTargetToken();
    if (!appToken || !token) {
      showSaveMsg('❌ 请先填写目标表的 AppToken 和 PersonalBaseToken', false);
      return;
    }
    try {
      const res = await fetch(API + '/api/tables?appToken=' + encodeURIComponent(appToken), {
        headers: { 'x-personal-token': token },
      });
      const data = await res.json();
      if (data.error) { showSaveMsg('❌ 加载表失败：' + data.error, false); return; }
      (document.getElementById('targetTableId') as HTMLSelectElement).innerHTML = data.tables
        .map((t: any) => `<option value="${t.table_id}">${t.name}</option>`)
        .join('');
    } catch (e: any) {
      showSaveMsg('❌ 加载表失败：' + e.message, false);
    }
  }
}
(window as any).loadTargetTables = loadTargetTables;

// ─── 一键初始化目标表 ──────────────────────────────────────────────────────────
(window as any).initTargetTable = async function () {
  const appToken = getTargetAppToken();
  const token = getTargetToken();
  if (!appToken || !token) {
    showSaveMsg('❌ 请先填写目标表信息或勾选"同一多维表格"', false);
    return;
  }
  const btn = document.getElementById('initBtn') as HTMLButtonElement;
  const msg = document.getElementById('initMsg')!;
  btn.disabled = true;
  btn.textContent = '创建中...';
  try {
    const res = await fetch(API + '/api/init', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ appToken, token }),
    });
    const data = await res.json();
    if (!data.success) throw new Error(data.error);
    await loadTargetTables();
    (document.getElementById('targetTableId') as HTMLSelectElement).value = data.tableId;
    msg.textContent = '✅ 表格已创建并自动选中';
    (msg as HTMLElement).style.color = '#27c346';
  } catch (e: any) {
    msg.textContent = '❌ ' + e.message;
    (msg as HTMLElement).style.color = '#f54a45';
  } finally {
    btn.disabled = false;
    btn.textContent = '✨ 一键创建「星图视频数据」表（含所有字段）';
    setTimeout(() => { msg.textContent = ''; }, 4000);
  }
};

// ─── 保存配置 ─────────────────────────────────────────────────────────────────
(window as any).saveConfig = async function () {
  const cfg: Record<string, string> = {
    sourceAppToken: (document.getElementById('sourceAppToken') as HTMLInputElement).value.trim(),
    sourcePersonalBaseToken: (document.getElementById('sourceToken') as HTMLInputElement).value.trim(),
    sourceTableId: (document.getElementById('sourceTableId') as HTMLSelectElement).value,
    profileUrlFieldName: (document.getElementById('profileUrlFieldName') as HTMLSelectElement).value,
    targetAppToken: getTargetAppToken(),
    targetPersonalBaseToken: getTargetToken(),
    targetTableId: (document.getElementById('targetTableId') as HTMLSelectElement).value,
    tikHubApiKey: (document.getElementById('tikHubApiKey') as HTMLInputElement).value.trim(),
  };
  const missing = Object.entries(cfg)
    .filter(([, v]) => !v)
    .map(([k]) => k);
  if (missing.length) {
    showSaveMsg('❌ 请填写所有必填项：' + missing.join(', '), false);
    return;
  }
  try {
    const res = await fetch(API + '/api/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(cfg),
    });
    const text = await res.text();
    if (!text) {
      showSaveMsg('❌ 服务器返回空响应 (status=' + res.status + ')，请刷新重试', false);
      return;
    }
    const data = JSON.parse(text);
    showSaveMsg(data.success ? '✅ 配置已保存' : '❌ ' + data.error, data.success);
  } catch (e: any) {
    showSaveMsg('❌ 请求失败：' + e.message, false);
  }
};

// ─── 立即执行 ─────────────────────────────────────────────────────────────────
(window as any).runNow = async function () {
  const btn = document.getElementById('runBtn') as HTMLButtonElement;
  btn.disabled = true;
  btn.textContent = '启动中...';
  try {
    const res = await fetch(API + '/api/run', { method: 'POST' });
    const text = await res.text();
    if (!text) {
      showRunMsg('❌ 服务器返回空响应 (status=' + res.status + ')，请刷新重试', false);
      return;
    }
    const data = JSON.parse(text);
    if (!data.success) {
      showRunMsg('❌ 启动失败：' + (data.error || '请先保存配置'), false);
      return;
    }
    showRunMsg('✅ 任务已启动', true);
    startPolling();
  } catch (e: any) {
    showRunMsg('❌ 请求失败：' + e.message, false);
  } finally {
    btn.disabled = false;
    btn.textContent = '▶ 立即执行';
  }
};

// ─── 状态轮询 ─────────────────────────────────────────────────────────────────
let pollTimer: ReturnType<typeof setInterval> | null = null;

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  loadStatus();
  pollTimer = setInterval(async () => {
    const state = await loadStatus();
    if (state !== 'running') { clearInterval(pollTimer!); pollTimer = null; }
  }, 2500);
}

async function loadStatus(): Promise<string> {
  try {
    const res = await fetch(API + '/api/status');
    const s = await res.json();
    renderStatus(s);
    return s.state || 'idle';
  } catch (_) { return 'idle'; }
}

function renderStatus(s: any) {
  const cls = ({ running: 'state-running', done: 'state-done', error: 'state-error' } as any)[s.state] || '';
  const msgHtml = (s.message || '空闲').replace(/\n/g, '<br/>');
  let html = `<span class="${cls}">${msgHtml}</span>`;
  if (s.lastRunAt)
    html += `<br/><span class="hint">上次执行：${new Date(s.lastRunAt).toLocaleString('zh-CN')}</span>`;
  if (s.videoCount)
    html += `<br/><span class="hint">视频数据：${s.videoCount} 条</span>`;
  document.getElementById('statusBox')!.innerHTML = html;
  const show = s.state === 'running' && s.totalCount;
  document.getElementById('progressWrap')!.style.display = show ? '' : 'none';
  if (show)
    (document.getElementById('progressBar') as HTMLElement).style.width =
      Math.round((s.processedCount / s.totalCount) * 100) + '%';
}

// ─── 初始化 ───────────────────────────────────────────────────────────────────
async function init() {
  await loadSourceTables();
  // sameBase 默认勾选，立即加载目标表列表
  await loadTargetTables();

  try {
    const res = await fetch(API + '/api/config');
    const cfg = await res.json();
    if (!cfg.sourceAppToken) return;

    (document.getElementById('sourceAppToken') as HTMLInputElement).value = cfg.sourceAppToken;
    (document.getElementById('sourceToken') as HTMLInputElement).value = cfg.sourcePersonalBaseToken || '';
    (document.getElementById('tikHubApiKey') as HTMLInputElement).value = cfg.tikHubApiKey || '';

    if (cfg.sourceTableId) {
      (document.getElementById('sourceTableId') as HTMLSelectElement).value = cfg.sourceTableId;
      await loadSourceFields(cfg.sourceTableId);
    }
    if (cfg.profileUrlFieldName)
      (document.getElementById('profileUrlFieldName') as HTMLSelectElement).value = cfg.profileUrlFieldName;

    const sameBase = cfg.sourceAppToken === cfg.targetAppToken;
    (document.getElementById('sameBase') as HTMLInputElement).checked = sameBase;
    document.getElementById('targetTokenFields')!.style.display = sameBase ? 'none' : '';
    await loadTargetTables();
    if (!sameBase) {
      (document.getElementById('targetAppToken') as HTMLInputElement).value = cfg.targetAppToken || '';
      (document.getElementById('targetToken') as HTMLInputElement).value = cfg.targetPersonalBaseToken || '';
    }
    if (cfg.targetTableId)
      (document.getElementById('targetTableId') as HTMLSelectElement).value = cfg.targetTableId;
  } catch (e) {
    console.warn('恢复配置失败:', e);
  }
}

init();
