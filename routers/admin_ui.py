from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Crime Map 管理パネル</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
  <style>
    :root {
      --bg: #0f1117; --card: #1a1d27; --border: #2a2d3a;
      --text: #e0e0e0; --muted: #888;
    }
    body { background: var(--bg); color: var(--text); }
    .card { background: var(--card); border-color: var(--border); }
    .card-header { background: #232636; border-bottom: 1px solid var(--border); }
    .table { color: var(--text); }
    .table td, .table th { border-color: var(--border); vertical-align: middle; }
    .navbar { background: var(--card) !important; border-bottom: 1px solid var(--border); }
    .form-control {
      background: #111318; color: var(--text);
      border-color: var(--border);
    }
    .form-control:focus {
      background: #111318; color: var(--text);
      border-color: #6366f1; box-shadow: 0 0 0 .2rem rgba(99,102,241,.3);
    }
    .stat-num { font-size: 2.2rem; font-weight: 700; line-height: 1; }
    #loginWrap { min-height: 100vh; }
  </style>
</head>
<body>

<!-- ログイン画面 -->
<div id="loginWrap" class="d-flex align-items-center justify-content-center d-none">
  <div class="card shadow" style="width:380px">
    <div class="card-header py-3 text-center">
      <h5 class="mb-0 fw-bold">Crime Map 管理パネル</h5>
    </div>
    <div class="card-body p-4">
      <label class="form-label small">管理者トークン</label>
      <input type="password" id="tokenInput" class="form-control mb-3"
             placeholder="ADMIN_TOKEN" onkeydown="if(event.key==='Enter')login()">
      <button onclick="login()" class="btn btn-primary w-100">ログイン</button>
      <p id="loginErr" class="text-danger small mt-2 mb-0 d-none">認証に失敗しました</p>
    </div>
  </div>
</div>

<!-- ダッシュボード -->
<div id="dash" class="d-none">
  <nav class="navbar px-3 py-2">
    <span class="fw-bold">Crime Map 管理パネル</span>
    <button onclick="logout()" class="btn btn-outline-secondary btn-sm">ログアウト</button>
  </nav>

  <div class="container-fluid px-4 py-4">

    <!-- サマリーカード -->
    <div class="row g-3 mb-4">
      <div class="col-6 col-lg-3">
        <div class="card p-3 text-center">
          <div class="stat-num" style="color:#6366f1" id="sTotal">-</div>
          <div class="small text-muted mt-1">総投稿数</div>
        </div>
      </div>
      <div class="col-6 col-lg-3">
        <div class="card p-3 text-center">
          <div class="stat-num text-success" id="sApproved">-</div>
          <div class="small text-muted mt-1">承認済み</div>
        </div>
      </div>
      <div class="col-6 col-lg-3">
        <div class="card p-3 text-center">
          <div class="stat-num text-warning" id="sPending">-</div>
          <div class="small text-muted mt-1">審査待ち</div>
        </div>
      </div>
      <div class="col-6 col-lg-3">
        <div class="card p-3 text-center">
          <div class="stat-num text-danger" id="sRejected">-</div>
          <div class="small text-muted mt-1">却下</div>
        </div>
      </div>
    </div>

    <!-- チャート行1 -->
    <div class="row g-3 mb-4">
      <div class="col-lg-8">
        <div class="card h-100">
          <div class="card-header small fw-semibold">月別投稿数（発生日基準）</div>
          <div class="card-body"><canvas id="cMonthly"></canvas></div>
        </div>
      </div>
      <div class="col-lg-4">
        <div class="card h-100">
          <div class="card-header small fw-semibold">種別内訳</div>
          <div class="card-body d-flex align-items-center">
            <canvas id="cType"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- チャート行2 -->
    <div class="row g-3 mb-4">
      <div class="col-12">
        <div class="card">
          <div class="card-header small fw-semibold">国籍別件数</div>
          <div class="card-body">
            <canvas id="cNationality" height="80"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- 承認待ちキュー -->
    <div class="card">
      <div class="card-header small fw-semibold d-flex justify-content-between align-items-center">
        <span>
          承認待ちキュー
          <span id="qBadge" class="badge bg-warning text-dark ms-1">0</span>
        </span>
        <button onclick="loadAll()" class="btn btn-outline-secondary btn-sm">更新</button>
      </div>
      <div class="card-body p-0">
        <div id="qEmpty" class="text-center text-muted py-5 d-none">
          承認待ちの投稿はありません
        </div>
        <div id="qTableWrap" class="table-responsive">
          <table class="table table-dark table-hover mb-0">
            <thead>
              <tr>
                <th class="text-muted fw-normal">#</th>
                <th>タイトル / 説明</th>
                <th>データ</th>
                <th>AIスコア</th>
                <th>AI判定理由</th>
                <th>ソース</th>
                <th>投稿日</th>
                <th></th>
              </tr>
            </thead>
            <tbody id="qBody"></tbody>
          </table>
        </div>
      </div>
    </div>

  </div>
</div>

<script>
const PALETTE = [
  '#6366f1','#06b6d4','#22c55e','#f59e0b','#f43f5e',
  '#a855f7','#ec4899','#84cc16','#14b8a6','#f97316',
  '#0ea5e9','#d946ef','#ef4444','#3b82f6','#10b981'
];
let _charts = {};

// ─── 認証 ────────────────────────────────────────────────────────────────────
function tok() { return localStorage.getItem('adm') || ''; }

async function login() {
  const t = document.getElementById('tokenInput').value.trim();
  const r = await fetch('/api/admin/stats', {
    headers: { 'X-Admin-Token': t }
  });
  if (r.ok) {
    localStorage.setItem('adm', t);
    showDash();
  } else {
    document.getElementById('loginErr').classList.remove('d-none');
  }
}

function logout() {
  localStorage.removeItem('adm');
  document.getElementById('dash').classList.add('d-none');
  document.getElementById('loginWrap').classList.remove('d-none');
  document.getElementById('tokenInput').value = '';
  document.getElementById('loginErr').classList.add('d-none');
}

// ─── API ─────────────────────────────────────────────────────────────────────
async function api(path, method = 'GET') {
  const r = await fetch(path, { method, headers: { 'X-Admin-Token': tok() } });
  if (r.status === 401) { logout(); throw new Error('Unauthorized'); }
  return r.json();
}

// ─── ダッシュボード全体ロード ─────────────────────────────────────────────────
function showDash() {
  document.getElementById('loginWrap').classList.add('d-none');
  document.getElementById('dash').classList.remove('d-none');
  loadAll();
}

async function loadAll() {
  await Promise.all([
    loadStats(),
    loadMonthly(),
    loadBreakdown('crime_type',  'cType',        'doughnut'),
    loadBreakdown('nationality', 'cNationality',  'bar'),
    loadQueue(),
  ]);
}

// ─── サマリー ─────────────────────────────────────────────────────────────────
async function loadStats() {
  const s = await api('/api/admin/stats');
  document.getElementById('sTotal').textContent    = s.total;
  document.getElementById('sApproved').textContent = s.approved;
  document.getElementById('sPending').textContent  = s.pending;
  document.getElementById('sRejected').textContent = s.rejected;
}

// ─── 月別チャート ─────────────────────────────────────────────────────────────
async function loadMonthly() {
  const raw = await api('/api/admin/stats/monthly');
  const data = [...raw].reverse();
  mkChart('cMonthly', {
    type: 'bar',
    data: {
      labels: data.map(d => d.month),
      datasets: [{
        label: '件数',
        data: data.map(d => d.count),
        backgroundColor: 'rgba(99,102,241,.55)',
        borderColor: 'rgba(99,102,241,1)',
        borderWidth: 1,
      }]
    },
    options: chartOpts({ yBegin: true }),
  });
}

// ─── 種別・国籍チャート ───────────────────────────────────────────────────────
async function loadBreakdown(field, canvasId, type) {
  const raw = await api(`/api/admin/stats/breakdown/${field}`);
  if (!raw.length) return;

  const isHoriz = type === 'bar';
  mkChart(canvasId, {
    type,
    data: {
      labels: raw.map(d => d.value || '不明'),
      datasets: [{
        label: '件数',
        data: raw.map(d => d.count),
        backgroundColor: raw.map((_, i) => PALETTE[i % PALETTE.length]),
      }]
    },
    options: type === 'doughnut'
      ? { plugins: { legend: { position: 'bottom', labels: { color: '#e0e0e0', font: { size: 10 } } } } }
      : chartOpts({ horiz: true }),
  });
}

function chartOpts({ yBegin = false, horiz = false } = {}) {
  const base = {
    plugins: { legend: { labels: { color: '#e0e0e0' } } },
    scales: {
      x: { ticks: { color: '#999' }, grid: { color: '#2a2d3a' } },
      y: { ticks: { color: '#999' }, grid: { color: '#2a2d3a' }, beginAtZero: yBegin },
    },
  };
  if (horiz) {
    base.indexAxis = 'y';
    base.scales.x.beginAtZero = true;
    base.plugins.legend = { display: false };
  }
  return base;
}

function mkChart(id, cfg) {
  if (_charts[id]) _charts[id].destroy();
  _charts[id] = new Chart(document.getElementById(id).getContext('2d'), cfg);
}

// ─── 承認待ちキュー ───────────────────────────────────────────────────────────
async function loadQueue() {
  const data = await api('/api/admin/queue');
  document.getElementById('qBadge').textContent = data.length;
  const tbody = document.getElementById('qBody');
  tbody.innerHTML = '';

  if (!data.length) {
    document.getElementById('qEmpty').classList.remove('d-none');
    document.getElementById('qTableWrap').classList.add('d-none');
    return;
  }
  document.getElementById('qEmpty').classList.add('d-none');
  document.getElementById('qTableWrap').classList.remove('d-none');

  for (const r of data) {
    const sc = r.ai_score;
    const scCol = sc == null ? 'text-muted'
                : sc >= 0.8 ? 'text-success'
                : sc >= 0.5 ? 'text-warning' : 'text-danger';
    const scTxt = sc != null
      ? `<span class="${scCol} fw-bold">${(sc * 100).toFixed(0)}%</span>`
      : '<span class="text-muted">-</span>';

    const links = [];
    if (r.source_url)
      links.push(`<a href="${esc(r.source_url)}" target="_blank"
                     class="btn btn-outline-info btn-sm py-0 px-2">元URL</a>`);
    if (r.archive_url)
      links.push(`<a href="${esc(r.archive_url)}" target="_blank"
                     class="btn btn-outline-warning btn-sm py-0 px-2 ms-1">魚拓</a>`);

    const badges = r.data
      ? Object.values(r.data)
          .map(v => `<span class="badge bg-secondary me-1">${esc(v)}</span>`)
          .join('')
      : '';

    tbody.innerHTML += `
<tr>
  <td class="text-muted small">${r.id}</td>
  <td style="max-width:240px">
    <div class="fw-semibold text-truncate">${esc(r.title || '(なし)')}</div>
    <div class="text-muted small"
         style="white-space:pre-wrap;max-height:3.5em;overflow:hidden"
    >${esc((r.description || '').slice(0, 120))}</div>
  </td>
  <td style="max-width:180px">${badges}</td>
  <td>${scTxt}</td>
  <td class="text-muted small" style="max-width:200px">${esc(r.ai_reason || '-')}</td>
  <td class="text-nowrap">${links.join('')}</td>
  <td class="text-muted small text-nowrap">${(r.created_at || '').slice(0, 10)}</td>
  <td class="text-nowrap">
    <button onclick="act(${r.id},'approve')"
            class="btn btn-success btn-sm">承認</button>
    <button onclick="act(${r.id},'reject')"
            class="btn btn-danger btn-sm ms-1">却下</button>
  </td>
</tr>`;
  }
}

async function act(id, action) {
  await api(`/api/admin/${action}/${id}`, 'POST');
  loadQueue();
  loadStats();
}

function esc(s) {
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ─── 起動 ─────────────────────────────────────────────────────────────────────
if (tok()) {
  showDash();
} else {
  document.getElementById('loginWrap').classList.remove('d-none');
}
</script>
</body>
</html>"""


@router.get("/admin", response_class=HTMLResponse, include_in_schema=False)
def admin_dashboard():
    return _HTML
