"use strict";

// === Auth: token in sessionStorage; auto-prompt when server requires it ===

const TOKEN_KEY = "distlog.token";
let authRequired = null; // null until first probe completes

function getToken() {
  return sessionStorage.getItem(TOKEN_KEY) || "";
}
function setToken(t) {
  if (t) sessionStorage.setItem(TOKEN_KEY, t);
  else sessionStorage.removeItem(TOKEN_KEY);
  updateAuthBar();
}

function authHeaders() {
  const t = getToken();
  return t ? { Authorization: "Bearer " + t } : {};
}

// apiFetch wraps fetch with the Authorization header (when set) and
// auto-prompts for a token on 401. The retry-after-login path is
// transparent: caller awaits one promise, login + retry happens
// underneath.
async function apiFetch(input, init = {}) {
  init = Object.assign({}, init);
  init.headers = Object.assign({}, init.headers, authHeaders());
  let resp = await fetch(input, init);
  if (resp.status === 401) {
    authRequired = true;
    updateAuthBar();
    const got = await promptLogin();
    if (!got) return resp;
    init.headers = Object.assign({}, init.headers, authHeaders());
    resp = await fetch(input, init);
  }
  return resp;
}

function updateAuthBar() {
  const status = document.getElementById("auth-status");
  const btn = document.getElementById("auth-toggle");
  const tok = getToken();
  if (authRequired === false) {
    status.textContent = "auth: disabled";
    btn.hidden = true;
    return;
  }
  if (tok) {
    status.textContent = "signed in";
    btn.textContent = "Sign out";
    btn.hidden = false;
    btn.onclick = () => setToken("");
  } else {
    status.textContent = authRequired ? "not signed in" : "auth: checking…";
    btn.textContent = "Sign in";
    btn.hidden = !authRequired;
    btn.onclick = () => promptLogin();
  }
}

function promptLogin() {
  return new Promise((resolve) => {
    const dlg = document.getElementById("login-dialog");
    const input = document.getElementById("login-token");
    input.value = "";
    const submit = document.getElementById("login-submit");
    const cancel = document.getElementById("login-cancel");
    const onSubmit = (e) => {
      e.preventDefault();
      const t = input.value.trim();
      if (!t) return;
      setToken(t);
      dlg.close();
      cleanup();
      resolve(true);
    };
    const onCancel = () => {
      dlg.close();
      cleanup();
      resolve(false);
    };
    const cleanup = () => {
      submit.removeEventListener("click", onSubmit);
      cancel.removeEventListener("click", onCancel);
    };
    submit.addEventListener("click", onSubmit);
    cancel.addEventListener("click", onCancel);
    dlg.showModal();
    input.focus();
  });
}

// On first load, probe /api/healthz with no headers (it's open) and
// then probe /api/ingest with HEAD to see if auth is required. We
// actually probe via /api/query empty (which always needs auth when
// enabled) using a no-op fetch.
async function detectAuthMode() {
  try {
    // Fire a cheap authenticated-required endpoint with no header to
    // distinguish auth-on from auth-off.
    const resp = await fetch("/api/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql: "SELECT * FROM logs LIMIT 0" }),
    });
    authRequired = resp.status === 401;
  } catch {
    authRequired = false;
  }
  updateAuthBar();
}
detectAuthMode();
updateAuthBar();

// === Health panel: poll /api/healthz; rate adapts to tab visibility ===

const HEALTH_FIELDS = [
  "sstable_count",
  "frozen_memtable_count",
  "active_memtable_size",
  "next_doc_id",
  "last_scan_pruned_sstables",
  "corrupted_sstables_quarantined",
  "fatal",
];
const POLL_VISIBLE_MS = 2000;
const POLL_HIDDEN_MS = 30000;
const CHART_MAX_POINTS = 60; // ~2 minutes of history when visible.

const history = []; // { t, activeKiB, writesPerSec, prunedLast }
let prevDocID = null;
let prevSampleTime = null;
let pollTimer = null;
let pollIntervalMs = POLL_VISIBLE_MS;

async function refreshHealth() {
  const badge = document.getElementById("health-status");
  try {
    const resp = await fetch("/api/healthz");
    const body = await resp.json();
    for (const k of HEALTH_FIELDS) {
      const cell = document.getElementById("h-" + k);
      if (cell) cell.textContent = formatStat(k, body[k]);
    }
    badge.textContent = body.status || (resp.ok ? "ok" : "unhealthy");
    badge.className = "badge " + (resp.ok && !body.fatal && !body.closed ? "ok" : "bad");

    // Sample for chart.
    const now = Date.now();
    let writesPerSec = 0;
    if (prevDocID !== null && prevSampleTime !== null) {
      const dt = (now - prevSampleTime) / 1000;
      if (dt > 0) {
        writesPerSec = Math.max(0, (body.next_doc_id - prevDocID) / dt);
      }
    }
    prevDocID = body.next_doc_id;
    prevSampleTime = now;
    history.push({
      t: now,
      activeKiB: body.active_memtable_size / 1024,
      writesPerSec,
      prunedLast: body.last_scan_pruned_sstables,
    });
    if (history.length > CHART_MAX_POINTS) history.shift();
    drawChart();
  } catch (err) {
    badge.textContent = "unreachable";
    badge.className = "badge bad";
  }
}

function formatStat(key, val) {
  if (val === undefined || val === null) return "—";
  if (key === "active_memtable_size") return formatBytes(val);
  return String(val);
}

function formatBytes(n) {
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KiB";
  return (n / 1024 / 1024).toFixed(2) + " MiB";
}

function schedulePoll() {
  if (pollTimer) clearTimeout(pollTimer);
  pollTimer = setTimeout(async () => {
    await refreshHealth();
    schedulePoll();
  }, pollIntervalMs);
}

document.addEventListener("visibilitychange", () => {
  pollIntervalMs = document.hidden ? POLL_HIDDEN_MS : POLL_VISIBLE_MS;
  // Reschedule immediately so a return-to-foreground refreshes promptly.
  if (!document.hidden) {
    if (pollTimer) clearTimeout(pollTimer);
    refreshHealth().then(schedulePoll);
  }
});

refreshHealth().then(schedulePoll);

// === Chart (vanilla Canvas, no library) ===

const CHART_COLORS = {
  active: getCSSVar("--accent"),
  writes: getCSSVar("--ok"),
  pruned: getCSSVar("--warn"),
  axis: getCSSVar("--border"),
  text: getCSSVar("--muted"),
};

function getCSSVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function drawChart() {
  const canvas = document.getElementById("health-chart");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const cssW = canvas.clientWidth || 520;
  const cssH = canvas.clientHeight || 180;
  // Resize backing store to DPR for crisp lines.
  if (canvas.width !== cssW * dpr || canvas.height !== cssH * dpr) {
    canvas.width = cssW * dpr;
    canvas.height = cssH * dpr;
  }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);

  if (history.length < 2) {
    ctx.fillStyle = CHART_COLORS.text;
    ctx.font = "12px " + getCSSVar("--mono");
    ctx.fillText("collecting data…", 12, 24);
    return;
  }

  const padL = 36, padR = 8, padT = 8, padB = 18;
  const plotW = cssW - padL - padR;
  const plotH = cssH - padT - padB;

  // Three series share x-axis (time) but each has its own y-range so
  // they coexist visually. We normalize each to its own max.
  const series = [
    { color: CHART_COLORS.active, get: (p) => p.activeKiB },
    { color: CHART_COLORS.writes, get: (p) => p.writesPerSec },
    { color: CHART_COLORS.pruned, get: (p) => p.prunedLast },
  ];

  // X axis: linear over history index, oldest left.
  const n = history.length;
  const x = (i) => padL + (i / (n - 1)) * plotW;

  // Y baseline.
  ctx.strokeStyle = CHART_COLORS.axis;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padL, padT + plotH);
  ctx.lineTo(padL + plotW, padT + plotH);
  ctx.stroke();

  for (const s of series) {
    const values = history.map(s.get);
    let max = Math.max(...values, 1);
    // Snap to a friendlier max (round up to nearest "nice" number).
    max = niceCeil(max);
    const y = (v) => padT + plotH - (v / max) * plotH;

    ctx.strokeStyle = s.color;
    ctx.lineWidth = 1.6;
    ctx.beginPath();
    for (let i = 0; i < n; i++) {
      const px = x(i);
      const py = y(values[i]);
      if (i === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    }
    ctx.stroke();

    // Latest-value label.
    const last = values[n - 1];
    ctx.fillStyle = s.color;
    ctx.font = "11px " + getCSSVar("--mono");
    ctx.fillText(formatChartValue(last), padL + plotW - 50, y(last) - 4);
  }

  // X label: how many seconds of history.
  const spanSec = Math.round((history[n - 1].t - history[0].t) / 1000);
  ctx.fillStyle = CHART_COLORS.text;
  ctx.font = "10px " + getCSSVar("--mono");
  ctx.fillText("←" + spanSec + "s", padL, padT + plotH + 12);
}

function niceCeil(v) {
  if (v <= 1) return 1;
  const pow = Math.pow(10, Math.floor(Math.log10(v)));
  const m = v / pow;
  let n;
  if (m <= 1) n = 1;
  else if (m <= 2) n = 2;
  else if (m <= 5) n = 5;
  else n = 10;
  return n * pow;
}

function formatChartValue(v) {
  if (v >= 1000) return v.toFixed(0);
  if (v >= 10) return v.toFixed(1);
  return v.toFixed(2);
}

window.addEventListener("resize", drawChart);

// === Ingest panel ===

document.getElementById("ingest-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const form = e.currentTarget;
  const data = new FormData(form);
  const body = {};
  if (data.get("ts"))         body.ts        = data.get("ts");
  if (data.get("tenant_id"))  body.tenant_id = data.get("tenant_id");
  if (data.get("source"))     body.source    = data.get("source");
  if (data.get("message"))    body.message   = data.get("message");
  const fieldsRaw = data.get("fields") || "";
  if (fieldsRaw.trim()) {
    const fields = {};
    for (const line of fieldsRaw.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const idx = trimmed.indexOf("=");
      if (idx < 0) continue;
      fields[trimmed.slice(0, idx).trim()] = trimmed.slice(idx + 1).trim();
    }
    if (Object.keys(fields).length) body.fields = fields;
  }

  const result = document.getElementById("ingest-result");
  try {
    const resp = await apiFetch("/api/ingest", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const json = await resp.json();
    result.textContent = JSON.stringify(json, null, 2);
    result.className = "result " + (resp.ok ? "ok" : "err");
    if (resp.ok) refreshHealth();
  } catch (err) {
    result.textContent = String(err);
    result.className = "result err";
  }
});

// === Query panel: SQL highlighting + sortable results ===

const sqlInput = document.getElementById("sql-input");
const sqlHL = document.getElementById("sql-hl");

const SQL_KEYWORDS = new Set([
  "SELECT", "FROM", "WHERE", "LIMIT", "AND", "OR",
]);

function escapeHTML(s) {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function highlightSQL(text) {
  // Tokenizer: walk char by char, emitting spans for strings, numbers,
  // identifiers (keyword?), operators, whitespace. Simple but correct
  // for our tiny dialect.
  let out = "";
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (ch === "'") {
      // String literal (with simple backslash escape).
      let j = i + 1;
      while (j < text.length && text[j] !== "'") {
        if (text[j] === "\\" && j + 1 < text.length) j++;
        j++;
      }
      if (j < text.length) j++; // include closing quote
      out += '<span class="str">' + escapeHTML(text.slice(i, j)) + "</span>";
      i = j;
    } else if (/[0-9]/.test(ch) || (ch === "-" && /[0-9]/.test(text[i + 1] || ""))) {
      let j = i + 1;
      while (j < text.length && /[0-9]/.test(text[j])) j++;
      out += '<span class="num">' + escapeHTML(text.slice(i, j)) + "</span>";
      i = j;
    } else if (/[a-zA-Z_]/.test(ch)) {
      let j = i + 1;
      while (j < text.length && /[a-zA-Z0-9_.]/.test(text[j])) j++;
      const word = text.slice(i, j);
      if (SQL_KEYWORDS.has(word.toUpperCase())) {
        out += '<span class="kw">' + escapeHTML(word) + "</span>";
      } else {
        out += escapeHTML(word);
      }
      i = j;
    } else if (/[=<>!*,();]/.test(ch)) {
      out += '<span class="op">' + escapeHTML(ch) + "</span>";
      i++;
    } else {
      out += escapeHTML(ch);
      i++;
    }
  }
  // Trailing newline keeps the overlay's height in sync with the textarea
  // when the user ends on a newline (textarea pads, <pre> does not).
  if (text.endsWith("\n")) out += "\n";
  return out;
}

function syncHighlight() {
  sqlHL.innerHTML = highlightSQL(sqlInput.value);
}
sqlInput.addEventListener("input", syncHighlight);
sqlInput.addEventListener("scroll", () => {
  sqlHL.scrollTop = sqlInput.scrollTop;
  sqlHL.scrollLeft = sqlInput.scrollLeft;
});
syncHighlight();

document.querySelectorAll(".sample").forEach((btn) => {
  btn.addEventListener("click", () => {
    sqlInput.value = btn.dataset.sql;
    syncHighlight();
  });
});

let lastQueryResult = null;
let sortState = { col: null, dir: 0 }; // dir: 1 asc, -1 desc, 0 none

document.getElementById("query-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const sql = sqlInput.value;
  const status = document.getElementById("query-status");
  const resultDiv = document.getElementById("query-result");
  status.textContent = "running…";
  resultDiv.innerHTML = "";

  const t0 = performance.now();
  try {
    const resp = await apiFetch("/api/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql }),
    });
    const elapsed = (performance.now() - t0).toFixed(1);
    const body = await resp.json();

    if (!resp.ok) {
      status.textContent = `error (${resp.status}) in ${elapsed}ms`;
      const pre = document.createElement("pre");
      pre.className = "result err";
      pre.textContent = body.error || JSON.stringify(body, null, 2);
      resultDiv.appendChild(pre);
      lastQueryResult = null;
      return;
    }

    status.textContent = `${body.rows.length} row(s) in ${elapsed}ms`;
    lastQueryResult = body;
    sortState = { col: null, dir: 0 };
    resultDiv.appendChild(renderTable(body));
    refreshHealth();
  } catch (err) {
    status.textContent = "fetch error";
    const pre = document.createElement("pre");
    pre.className = "result err";
    pre.textContent = String(err);
    resultDiv.appendChild(pre);
  }
});

function renderTable(body) {
  const cols = body.columns || [];
  const allCols = ["doc_id", ...cols];

  const rows = body.rows.map((r) => {
    const flat = { doc_id: r.doc_id };
    for (const c of cols) flat[c] = r.values ? r.values[c] : undefined;
    return flat;
  });

  if (sortState.col && sortState.dir !== 0) {
    rows.sort((a, b) => cmpValues(a[sortState.col], b[sortState.col]) * sortState.dir);
  }

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  for (const c of allCols) {
    const th = document.createElement("th");
    th.textContent = c;
    const mark = document.createElement("span");
    mark.className = "sort-mark";
    if (sortState.col === c) mark.textContent = sortState.dir === 1 ? "▲" : sortState.dir === -1 ? "▼" : "";
    th.appendChild(mark);
    th.addEventListener("click", () => toggleSort(c));
    headerRow.appendChild(th);
  }
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const c of allCols) {
      const td = document.createElement("td");
      td.textContent = formatValue(row[c]);
      td.title = td.textContent;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  return table;
}

function toggleSort(col) {
  if (!lastQueryResult) return;
  if (sortState.col !== col) {
    sortState = { col, dir: 1 };
  } else {
    // cycle: asc -> desc -> none
    sortState.dir = sortState.dir === 1 ? -1 : sortState.dir === -1 ? 0 : 1;
    if (sortState.dir === 0) sortState.col = null;
  }
  const div = document.getElementById("query-result");
  div.innerHTML = "";
  div.appendChild(renderTable(lastQueryResult));
}

function cmpValues(a, b) {
  if (a === undefined || a === null) return b === undefined || b === null ? 0 : 1;
  if (b === undefined || b === null) return -1;
  const na = typeof a === "number" ? a : Number(a);
  const nb = typeof b === "number" ? b : Number(b);
  if (!Number.isNaN(na) && !Number.isNaN(nb) && String(na) === String(a) && String(nb) === String(b)) {
    return na - nb;
  }
  return String(a).localeCompare(String(b));
}

function formatValue(v) {
  if (v === undefined || v === null) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
