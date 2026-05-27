"use strict";

// === Health panel: poll /api/healthz every 2s ===

const HEALTH_FIELDS = [
  "sstable_count",
  "frozen_memtable_count",
  "active_memtable_size",
  "next_doc_id",
  "last_scan_pruned_sstables",
  "corrupted_sstables_quarantined",
  "fatal",
];

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

refreshHealth();
setInterval(refreshHealth, 2000);

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
    const resp = await fetch("/api/ingest", {
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

// === Query panel ===

document.querySelectorAll(".sample").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelector("#query-form textarea").value = btn.dataset.sql;
  });
});

document.getElementById("query-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const sql = e.currentTarget.querySelector("textarea").value;
  const status = document.getElementById("query-status");
  const result = document.getElementById("query-result");
  status.textContent = "running…";
  result.innerHTML = "";

  const t0 = performance.now();
  try {
    const resp = await fetch("/api/query", {
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
      result.appendChild(pre);
      return;
    }

    status.textContent = `${body.rows.length} row(s) in ${elapsed}ms`;
    result.appendChild(renderTable(body));
    refreshHealth();
  } catch (err) {
    status.textContent = "fetch error";
    const pre = document.createElement("pre");
    pre.className = "result err";
    pre.textContent = String(err);
    result.appendChild(pre);
  }
});

function renderTable(body) {
  const cols = body.columns || [];
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  // doc_id always shown first.
  const allCols = ["doc_id", ...cols];
  for (const c of allCols) {
    const th = document.createElement("th");
    th.textContent = c;
    headerRow.appendChild(th);
  }
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  for (const row of body.rows) {
    const tr = document.createElement("tr");
    for (const c of allCols) {
      const td = document.createElement("td");
      let v;
      if (c === "doc_id") v = row.doc_id;
      else v = row.values ? row.values[c] : undefined;
      td.textContent = formatValue(v);
      td.title = td.textContent; // ellipsis-hover shows full
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  return table;
}

function formatValue(v) {
  if (v === undefined || v === null) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
