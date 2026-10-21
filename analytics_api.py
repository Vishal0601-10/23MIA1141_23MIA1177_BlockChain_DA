"""
TATKAL-X Layer 5: Interactive Analytics API + Web UI

This is a drop-in replacement for analytics_api.py.

It keeps the read-only analytics API and adds an interactive browser view
at the root URL:

    http://127.0.0.1:5001/

The UI uses the same-origin API endpoints, so no CORS setup is required.

Environment:
    TATKAL_X_DB=tatkal_x_stream.db
    ANALYTICS_HOST=127.0.0.1
    ANALYTICS_PORT=5001
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any

from flask import Flask, jsonify, request, Response


DB_PATH = os.getenv("TATKAL_X_DB", "tatkal_x_stream.db")
HOST = os.getenv("ANALYTICS_HOST", "127.0.0.1")
PORT = int(os.getenv("ANALYTICS_PORT", "5001"))

app = Flask(__name__)


HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TATKAL-X Interactive Analytics</title>
<style>
:root {
  --bg:#0b1020; --panel:#121a2b; --panel2:#0f1727; --text:#edf2f7;
  --muted:#9aa8bd; --border:#26334a; --accent:#7dd3fc;
  --good:#86efac; --warn:#fde68a; --bad:#fca5a5;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,Segoe UI,Arial,sans-serif}
main{max-width:1200px;margin:0 auto;padding:28px 20px 44px}
header{display:flex;justify-content:space-between;gap:20px;align-items:flex-end;margin-bottom:22px}
h1{font-size:26px;margin:0 0 5px}
.sub{color:var(--muted);font-size:14px}
.status{border:1px solid var(--border);background:var(--panel);padding:9px 12px;border-radius:10px;font-size:13px}
.controls{display:grid;grid-template-columns:repeat(4,minmax(150px,1fr));gap:12px;margin-bottom:18px}
.control{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:12px}
label{display:block;color:var(--muted);font-size:12px;margin-bottom:6px}
select,button{width:100%;min-height:40px;border-radius:9px;border:1px solid var(--border);background:var(--panel2);color:var(--text);padding:8px 10px;font-size:14px}
button{cursor:pointer}
button:hover{border-color:var(--accent)}
.cards{display:grid;grid-template-columns:repeat(5,minmax(130px,1fr));gap:12px;margin-bottom:18px}
.card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:15px}
.card .k{color:var(--muted);font-size:12px}
.card .v{font-size:24px;font-weight:700;margin-top:7px}
.grid{display:grid;grid-template-columns:1.45fr 1fr;gap:14px}
.panel{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:15px;min-width:0}
.panel h2{font-size:15px;margin:0 0 12px}
.chart-wrap{width:100%;overflow:hidden}
svg{width:100%;height:290px;display:block}
.axis{stroke:#34445f;stroke-width:1}
.gridline{stroke:#223049;stroke-width:1}
.line{fill:none;stroke:var(--accent);stroke-width:2.5}
.point{fill:var(--accent);cursor:pointer}
.legend{display:flex;gap:14px;flex-wrap:wrap;color:var(--muted);font-size:12px;margin-top:8px}
.insight{padding:11px;border-left:3px solid var(--accent);background:var(--panel2);border-radius:7px;color:#dbeafe;font-size:13px;line-height:1.5}
table{width:100%;border-collapse:collapse;font-size:12px}
th,td{text-align:left;padding:8px;border-bottom:1px solid var(--border);white-space:nowrap}
th{color:var(--muted);font-weight:600;position:sticky;top:0;background:var(--panel)}
.table-scroll{overflow:auto;max-height:360px}
.badge{display:inline-block;padding:3px 7px;border-radius:999px;font-size:11px;border:1px solid var(--border)}
.good{color:var(--good)} .warn{color:var(--warn)} .bad{color:var(--bad)}
.small{font-size:12px;color:var(--muted)}
@media(max-width:900px){
  .controls,.cards{grid-template-columns:repeat(2,minmax(140px,1fr))}
  .grid{grid-template-columns:1fr}
}
@media(max-width:520px){
  main{padding:18px 12px}
  header{display:block}
  .status{margin-top:12px}
  .controls,.cards{grid-template-columns:1fr}
}
</style>
</head>
<body>
<main id="app">
<header>
  <div>
    <h1>TATKAL-X Interactive Analytics</h1>
    <div class="sub">Explore the persistent streaming data without changing the research model.</div>
  </div>
  <div id="status" class="status">Connecting…</div>
</header>

<section class="controls" aria-label="Analytics controls">
  <div class="control">
    <label for="simulation">Simulation</label>
    <select id="simulation"><option value="">All simulations</option></select>
  </div>
  <div class="control">
    <label for="controller">Controller</label>
    <select id="controller"><option value="">All controllers</option></select>
  </div>
  <div class="control">
    <label for="metric">Chart metric</label>
    <select id="metric">
      <option value="pressure">Pressure</option>
      <option value="active">Active</option>
      <option value="inventory">Inventory</option>
      <option value="completed">Completed</option>
      <option value="cbf_headroom">CBF headroom</option>
      <option value="safe_concurrency">Safe concurrency</option>
      <option value="backend_workload_s">Backend workload (s)</option>
      <option value="cpu_pct">CPU (%)</option>
      <option value="mpc_objective">MPC objective</option>
    </select>
  </div>
  <div class="control">
    <label>&nbsp;</label>
    <button id="refresh" type="button">Refresh analytics</button>
  </div>
</section>

<section class="cards" id="cards"></section>

<section class="grid">
  <div class="panel">
    <h2 id="chartTitle">Time series</h2>
    <div class="chart-wrap"><svg id="chart" viewBox="0 0 760 290" role="img" aria-label="Selected metric over time"></svg></div>
    <div id="chartInfo" class="legend"></div>
  </div>
  <div class="panel">
    <h2>Interpretation</h2>
    <div id="insight" class="insight">Select a simulation or metric to explore the stored data.</div>
    <div style="height:14px"></div>
    <h2>Controller comparison</h2>
    <div class="table-scroll"><table id="controllerTable"></table></div>
  </div>
</section>

<section class="panel" style="margin-top:14px">
  <h2>Event explorer</h2>
  <div class="small" style="margin-bottom:9px">Showing the latest 100 matching stored events. Click a chart point to inspect its source event.</div>
  <div class="table-scroll"><table id="eventsTable"></table></div>
</section>
</main>

<script>
(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const state = { summary:null, events:[], selectedEvent:null };

  const metricLabels = {
    pressure:"Pressure", active:"Active", inventory:"Inventory",
    completed:"Completed", cbf_headroom:"CBF headroom",
    safe_concurrency:"Safe concurrency", backend_workload_s:"Backend workload (s)",
    cpu_pct:"CPU (%)", mpc_objective:"MPC objective"
  };

  const fmt = (x, digits=2) => {
    if (x === null || x === undefined || !Number.isFinite(Number(x))) return "—";
    return Number(x).toFixed(digits);
  };

  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, c =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

  async function getJSON(url) {
    const r = await fetch(url, {cache:"no-store"});
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
    return data;
  }

  function query() {
    const params = new URLSearchParams();
    const sim = $("simulation").value;
    const ctl = $("controller").value;
    if (sim) params.set("simulation_id", sim);
    if (ctl) params.set("controller_mode", ctl);
    params.set("limit", "100");
    params.set("offset", "0");
    return params.toString();
  }

  async function loadSummary() {
    $("status").textContent = "Loading analytics…";
    const summary = await getJSON("/api/analytics/summary");
    state.summary = summary;

    const sims = summary.final_state_by_simulation || [];
    const currentSim = $("simulation").value;
    $("simulation").innerHTML =
      '<option value="">All simulations</option>' +
      sims.map(x => `<option value="${esc(x.simulation_id)}">${esc(x.simulation_id)}</option>`).join("");
    if ([...$("simulation").options].some(o => o.value === currentSim)) $("simulation").value = currentSim;

    const controllers = (summary.by_controller || []).map(x => x.controller_mode);
    const currentCtl = $("controller").value;
    $("controller").innerHTML =
      '<option value="">All controllers</option>' +
      controllers.map(x => `<option value="${esc(x)}">${esc(x)}</option>`).join("");
    if ([...$("controller").options].some(o => o.value === currentCtl)) $("controller").value = currentCtl;

    renderSummary(summary);
    renderControllerTable(summary.by_controller || []);
    $("status").textContent = `Connected · ${summary.overall.total_events ?? 0} stored events`;
  }

  async function loadEvents() {
    const data = await getJSON("/api/analytics/events?" + query());
    state.events = data.events || [];
    renderChart();
    renderEvents();
  }

  function renderSummary(s) {
    const o = s.overall || {};
    const cards = [
      ["Stored events", o.total_events],
      ["Simulations", o.simulations],
      ["Peak pressure", o.max_pressure],
      ["Peak active", o.max_active],
      ["Safety violations", o.safety_violation_events]
    ];
    $("cards").innerHTML = cards.map(([k,v]) =>
      `<div class="card"><div class="k">${esc(k)}</div><div class="v">${fmt(v, Number.isInteger(Number(v)) ? 0 : 2)}</div></div>`
    ).join("");
  }

  function renderControllerTable(rows) {
    const head = ["Controller","Simulations","Events","Avg pressure","Peak pressure","Avg active","Peak active","CBF headroom","Safety violations"];
    $("controllerTable").innerHTML =
      "<thead><tr>" + head.map(x => `<th>${esc(x)}</th>`).join("") + "</tr></thead>" +
      "<tbody>" + rows.map(r => `<tr>
        <td><span class="badge">${esc(r.controller_mode)}</span></td>
        <td>${fmt(r.simulations,0)}</td><td>${fmt(r.events,0)}</td>
        <td>${fmt(r.avg_pressure)}</td><td>${fmt(r.peak_pressure)}</td>
        <td>${fmt(r.avg_active)}</td><td>${fmt(r.peak_active,0)}</td>
        <td>${fmt(r.avg_cbf_headroom)}</td>
        <td class="${Number(r.safety_violation_events)>0?'bad':'good'}">${fmt(r.safety_violation_events,0)}</td>
      </tr>`).join("") + "</tbody>";
  }

  function renderChart() {
    const svg = $("chart");
    const metric = $("metric").value;
    const data = state.events.filter(e => Number.isFinite(Number(e.t)) && Number.isFinite(Number(e[metric])));
    $("chartTitle").textContent = `${metricLabels[metric] || metric} over time`;

    if (!data.length) {
      svg.innerHTML = '<text x="380" y="145" text-anchor="middle" fill="#9aa8bd">No matching events</text>';
      $("chartInfo").textContent = "";
      return;
    }

    const W=760,H=290,L=55,R=18,T=18,B=42;
    const xs=data.map(e=>Number(e.t)), ys=data.map(e=>Number(e[metric]));
    let xmin=Math.min(...xs), xmax=Math.max(...xs);
    let ymin=Math.min(...ys), ymax=Math.max(...ys);
    if (xmin===xmax) xmax=xmin+1;
    if (ymin===ymax) { ymin-=1; ymax+=1; }
    const pad=(ymax-ymin)*0.08; ymin-=pad; ymax+=pad;
    const X=x => L+(x-xmin)/(xmax-xmin)*(W-L-R);
    const Y=y => H-B-(y-ymin)/(ymax-ymin)*(H-T-B);

    let html="";
    for(let i=0;i<5;i++){
      const y=T+i*(H-T-B)/4;
      const val=ymax-i*(ymax-ymin)/4;
      html += `<line class="gridline" x1="${L}" y1="${y}" x2="${W-R}" y2="${y}"/>`;
      html += `<text x="${L-8}" y="${y+4}" text-anchor="end" fill="#9aa8bd" font-size="11">${fmt(val,2)}</text>`;
    }
    html += `<line class="axis" x1="${L}" y1="${H-B}" x2="${W-R}" y2="${H-B}"/>`;
    html += `<line class="axis" x1="${L}" y1="${T}" x2="${L}" y2="${H-B}"/>`;

    const points=data.map((e,i)=>`${X(xs[i]).toFixed(1)},${Y(ys[i]).toFixed(1)}`).join(" ");
    html += `<polyline class="line" points="${points}"/>`;

    const step=Math.max(1,Math.ceil(data.length/80));
    data.forEach((e,i)=>{
      if(i%step!==0 && i!==data.length-1) return;
      html += `<circle class="point" cx="${X(Number(e.t))}" cy="${Y(Number(e[metric]))}" r="3.5" data-index="${i}">
        <title>t=${fmt(e.t,2)} · ${metricLabels[metric]}=${fmt(e[metric],3)}</title>
      </circle>`;
    });

    html += `<text x="${W/2}" y="${H-7}" text-anchor="middle" fill="#9aa8bd" font-size="11">time (s)</text>`;
    $("chart").innerHTML=html;
    $("chartInfo").innerHTML =
      `<span>min ${fmt(Math.min(...ys),2)}</span><span>max ${fmt(Math.max(...ys),2)}</span><span>points ${data.length}</span>`;

    svg.querySelectorAll(".point").forEach(p => p.addEventListener("click", () => {
      const e=data[Number(p.dataset.index)];
      state.selectedEvent=e;
      showInsight(e, metric);
    }));
    showInsight(data[data.length-1], metric);
  }

  function showInsight(e, metric) {
    if (!e) return;
    const safety = Number(e.safety_violation) > 0;
    const headroom = Number(e.cbf_headroom);
    const pressure = Number(e.pressure);
    let message = `At t=${fmt(e.t,2)}s, ${metricLabels[metric] || metric} is ${fmt(e[metric],3)}. `;
    if (safety) message += `<span class="bad">A safety violation is recorded.</span>`;
    else if (headroom <= 0) message += `<span class="warn">CBF headroom is at or below zero.</span>`;
    else message += `<span class="good">No safety violation is recorded at this event.</span>`;
    message += ` Pressure=${fmt(pressure,2)}, active=${fmt(e.active,0)}, inventory=${fmt(e.inventory,0)}, completed=${fmt(e.completed,0)}.`;
    $("insight").innerHTML=message;
  }

  function renderEvents() {
    const fields=["t","pressure","active","inventory","completed","attempts","retry_attempts","safe_concurrency","cbf_headroom","safety_violation"];
    $("eventsTable").innerHTML =
      "<thead><tr>" + fields.map(x=>`<th>${esc(metricLabels[x]||x)}</th>`).join("") + "</tr></thead>" +
      "<tbody>" + state.events.map(e => `<tr>
        ${fields.map(x=>`<td class="${x==='safety_violation' && Number(e[x]) ? 'bad':''}">${fmt(e[x], x==='t'?2:2)}</td>`).join("")}
      </tr>`).join("") + "</tbody>";
  }

  async function refresh() {
    try {
      await loadSummary();
      await loadEvents();
    } catch (err) {
      $("status").textContent = "Analytics error";
      $("insight").innerHTML = `<span class="bad">${esc(err.message)}</span>`;
    }
  }

  $("refresh").addEventListener("click", refresh);
  $("simulation").addEventListener("change", loadEvents);
  $("controller").addEventListener("change", loadEvents);
  $("metric").addEventListener("change", renderChart);

  refresh();
})();
</script>
</body>
</html>
"""


def get_connection() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(
            f"SQLite database not found: {DB_PATH}. Start stream_storage.py first."
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='tatkal_x_events'
        """
    ).fetchone()
    return row is not None


def row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


@app.get("/")
def home():
    return Response(HTML, mimetype="text/html")


@app.get("/health")
def health():
    try:
        with get_connection() as conn:
            exists = table_exists(conn)
            count = (
                conn.execute("SELECT COUNT(*) FROM tatkal_x_events").fetchone()[0]
                if exists else 0
            )
        return jsonify({
            "status": "ok",
            "database": DB_PATH,
            "table": "tatkal_x_events",
            "table_exists": exists,
            "stored_events": count,
        })
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 503


@app.get("/api/analytics/summary")
def summary():
    try:
        with get_connection() as conn:
            if not table_exists(conn):
                return jsonify({"error": "tatkal_x_events table not found"}), 404

            overview = conn.execute("""
                SELECT
                    COUNT(*) AS total_events,
                    COUNT(DISTINCT simulation_id) AS simulations,
                    MIN(t) AS min_t,
                    MAX(t) AS max_t,
                    AVG(pressure) AS avg_pressure,
                    MAX(pressure) AS max_pressure,
                    AVG(active) AS avg_active,
                    MAX(active) AS max_active,
                    MAX(safety_violation) AS any_safety_violation,
                    SUM(CASE WHEN safety_violation != 0 THEN 1 ELSE 0 END) AS safety_violation_events,
                    SUM(CASE WHEN cbf_filtered != 0 THEN 1 ELSE 0 END) AS cbf_filtered_events,
                    AVG(cbf_headroom) AS avg_cbf_headroom,
                    MIN(cbf_headroom) AS min_cbf_headroom,
                    AVG(backend_workload_s) AS avg_backend_workload_s,
                    MAX(backend_workload_s) AS max_backend_workload_s,
                    AVG(cpu_pct) AS avg_cpu_pct,
                    MAX(cpu_pct) AS max_cpu_pct,
                    AVG(mpc_objective) AS avg_mpc_objective
                FROM tatkal_x_events
            """).fetchone()

            latest = conn.execute("""
                SELECT e.*
                FROM tatkal_x_events e
                INNER JOIN (
                    SELECT simulation_id, MAX(t) AS max_t
                    FROM tatkal_x_events
                    GROUP BY simulation_id
                ) x ON e.simulation_id=x.simulation_id AND e.t=x.max_t
                ORDER BY e.simulation_id
            """).fetchall()

            by_controller = conn.execute("""
                SELECT
                    controller_mode,
                    COUNT(*) AS events,
                    COUNT(DISTINCT simulation_id) AS simulations,
                    AVG(pressure) AS avg_pressure,
                    MAX(pressure) AS max_pressure,
                    AVG(active) AS avg_active,
                    MAX(active) AS max_active,
                    SUM(CASE WHEN safety_violation != 0 THEN 1 ELSE 0 END) AS safety_violation_events,
                    SUM(CASE WHEN cbf_filtered != 0 THEN 1 ELSE 0 END) AS cbf_filtered_events,
                    AVG(cbf_headroom) AS avg_cbf_headroom,
                    AVG(backend_workload_s) AS avg_backend_workload_s
                FROM tatkal_x_events
                GROUP BY controller_mode
                ORDER BY controller_mode
            """).fetchall()

        return jsonify({
            "database": DB_PATH,
            "overall": row_to_dict(overview),
            "final_state_by_simulation": [row_to_dict(r) for r in latest],
            "by_controller": [row_to_dict(r) for r in by_controller],
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/analytics/controllers")
def controllers():
    try:
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT
                    controller_mode,
                    COUNT(*) AS events,
                    COUNT(DISTINCT simulation_id) AS simulations,
                    AVG(pressure) AS avg_pressure,
                    MAX(pressure) AS peak_pressure,
                    AVG(active) AS avg_active,
                    MAX(active) AS peak_active,
                    AVG(admission_rate) AS avg_admission_rate,
                    AVG(service_rate) AS avg_service_rate,
                    AVG(safe_concurrency) AS avg_safe_concurrency,
                    AVG(cbf_headroom) AS avg_cbf_headroom,
                    SUM(CASE WHEN cbf_filtered != 0 THEN 1 ELSE 0 END) AS cbf_filtered_events,
                    SUM(CASE WHEN safety_violation != 0 THEN 1 ELSE 0 END) AS safety_violation_events,
                    AVG(backend_workload_s) AS avg_backend_workload_s,
                    AVG(cpu_pct) AS avg_cpu_pct,
                    AVG(mpc_objective) AS avg_mpc_objective
                FROM tatkal_x_events
                GROUP BY controller_mode
                ORDER BY controller_mode
            """).fetchall()
        return jsonify({"controllers": [row_to_dict(r) for r in rows]})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/analytics/simulation/<simulation_id>")
def simulation_summary(simulation_id: str):
    try:
        with get_connection() as conn:
            first = conn.execute("""
                SELECT * FROM tatkal_x_events
                WHERE simulation_id=? ORDER BY t ASC LIMIT 1
            """, (simulation_id,)).fetchone()

            last = conn.execute("""
                SELECT * FROM tatkal_x_events
                WHERE simulation_id=? ORDER BY t DESC LIMIT 1
            """, (simulation_id,)).fetchone()

            if first is None:
                return jsonify({"error": f"simulation '{simulation_id}' not found"}), 404

            stats = conn.execute("""
                SELECT
                    COUNT(*) AS events,
                    MIN(t) AS start_t,
                    MAX(t) AS end_t,
                    AVG(pressure) AS avg_pressure,
                    MAX(pressure) AS peak_pressure,
                    AVG(active) AS avg_active,
                    MAX(active) AS peak_active,
                    MAX(inventory) AS max_inventory,
                    MIN(inventory) AS final_inventory,
                    MAX(completed) AS final_completed,
                    MAX(attempts) AS final_attempts,
                    MAX(initial_attempts) AS final_initial_attempts,
                    MAX(retry_attempts) AS final_retry_attempts,
                    MAX(activated) AS final_activated,
                    SUM(CASE WHEN safety_violation != 0 THEN 1 ELSE 0 END) AS safety_violation_events,
                    SUM(CASE WHEN cbf_filtered != 0 THEN 1 ELSE 0 END) AS cbf_filtered_events,
                    AVG(cbf_headroom) AS avg_cbf_headroom,
                    MIN(cbf_headroom) AS min_cbf_headroom,
                    AVG(safe_concurrency) AS avg_safe_concurrency,
                    MIN(safe_concurrency) AS min_safe_concurrency,
                    AVG(backend_workload_s) AS avg_backend_workload_s,
                    MAX(backend_workload_s) AS max_backend_workload_s,
                    AVG(cpu_pct) AS avg_cpu_pct,
                    MAX(cpu_pct) AS max_cpu_pct,
                    AVG(mpc_objective) AS avg_mpc_objective
                FROM tatkal_x_events WHERE simulation_id=?
            """, (simulation_id,)).fetchone()

            controllers = conn.execute("""
                SELECT DISTINCT controller_mode FROM tatkal_x_events
                WHERE simulation_id=? ORDER BY controller_mode
            """, (simulation_id,)).fetchall()

        final_state = row_to_dict(last)
        initial_state = row_to_dict(first)
        final_completed = final_state.get("completed")
        final_initial = final_state.get("initial_attempts")
        final_attempts = final_state.get("attempts")

        result = row_to_dict(stats)
        result.update({
            "simulation_id": simulation_id,
            "controller_modes": [r["controller_mode"] for r in controllers],
            "initial_state": initial_state,
            "final_state": final_state,
            "completion_ratio_vs_initial_attempts": (
                final_completed / final_initial
                if final_completed is not None and final_initial not in (None, 0) else None
            ),
            "retry_amplification": (
                final_attempts / final_initial
                if final_attempts is not None and final_initial not in (None, 0) else None
            ),
        })
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/analytics/events")
def events():
    try:
        limit = min(max(int(request.args.get("limit", "100")), 1), 1000)
        offset = max(int(request.args.get("offset", "0")), 0)
        simulation_id = request.args.get("simulation_id")
        controller_mode = request.args.get("controller_mode")

        where = []
        params: list[Any] = []

        if simulation_id:
            where.append("simulation_id=?")
            params.append(simulation_id)
        if controller_mode:
            where.append("controller_mode=?")
            params.append(controller_mode)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        with get_connection() as conn:
            rows = conn.execute(
                f"""SELECT * FROM tatkal_x_events
                    {where_sql}
                    ORDER BY simulation_id,t
                    LIMIT ? OFFSET ?""",
                (*params, limit, offset),
            ).fetchall()

            total = conn.execute(
                f"SELECT COUNT(*) FROM tatkal_x_events {where_sql}",
                tuple(params),
            ).fetchone()[0]

        return jsonify({
            "count": len(rows),
            "total_matching": total,
            "limit": limit,
            "offset": offset,
            "events": [row_to_dict(r) for r in rows],
        })
    except ValueError:
        return jsonify({"error": "limit and offset must be integers"}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.errorhandler(404)
def not_found(_error):
    return jsonify({
        "error": "endpoint not found",
        "available_endpoints": [
            "/",
            "/health",
            "/api/analytics/summary",
            "/api/analytics/controllers",
            "/api/analytics/simulation/<simulation_id>",
            "/api/analytics/events",
        ],
    }), 404


if __name__ == "__main__":
    print("Starting TATKAL-X Interactive Analytics")
    print(f"Database : {DB_PATH}")
    print(f"Address  : http://{HOST}:{PORT}")
    app.run(host=HOST, port=PORT, debug=False)
