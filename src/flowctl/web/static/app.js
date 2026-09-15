/*
 * Lightweight polling-based "live" refresh. No framework, no build
 * step, on purpose (see PROJECT_CONTEXT.md's tech-stack decision to
 * stay in plain HTML/JS). Each page that wants live updates defines a
 * small config via a <script type="application/json" id="flowctl-page-data">
 * block and calls the matching init function below.
 */

const POLL_INTERVAL_MS = 4000;

function fcFmtTime(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  return d.toLocaleString();
}

function fcBadge(status, color, extraClass) {
  const cls = status === "running" ? "badge running" : "badge";
  return `<span class="${cls}${extraClass ? " " + extraClass : ""}" style="background:${color}">${status}</span>`;
}

/* ---------------- Homepage polling ---------------- */

function fcInitHomepage() {
  const table = document.getElementById("pipelines-table-body");
  if (!table) return;

  async function refresh() {
    try {
      const res = await fetch("/api/pipelines");
      if (!res.ok) return;
      const data = await res.json();
      for (const p of data.pipelines) {
        const row = document.querySelector(`tr[data-pipeline="${CSS.escape(p.name)}"]`);
        if (!row) continue;
        const statusCell = row.querySelector(".js-status");
        if (statusCell) {
          statusCell.innerHTML =
            fcBadge(p.last_status, p.color) +
            (p.last_ended ? ` <span class="muted">${fcFmtTime(p.last_ended)}</span>` : "");
        }
        const dotsCell = row.querySelector(".js-dots");
        if (dotsCell) {
          dotsCell.innerHTML = p.recent_dots
            .map((d) => `<span class="dot" style="background:${d.color}" title="${d.status}"></span>`)
            .join("");
        }
      }
    } catch (e) {
      /* silent: a failed poll just means we try again next interval */
    }
  }

  setInterval(refresh, POLL_INTERVAL_MS);
}

/* ---------------- Pipeline detail polling + task-detail panel ---------------- */

function fcFmtDuration(seconds) {
  if (seconds === null || seconds === undefined) return "-";
  return `${seconds.toFixed(3)}s`;
}

function fcInitPipelineDetail(pipelineName, initialTaskDetails) {
  // Kept in memory and refreshed on every poll tick, so the panel shows
  // live data if it's open while a run is in progress -- no page reload,
  // consistent with how the graph's node colors and run history already
  // update live.
  let taskDetails = initialTaskDetails || {};
  let openTaskName = null;

  const panel = document.getElementById("task-panel");
  const closeBtn = document.getElementById("task-panel-close");

  function renderPanel(taskName) {
    const info = taskDetails[taskName];
    document.getElementById("task-panel-title").textContent = taskName;

    const badgeEl = document.getElementById("task-panel-badge");
    const emptyEl = document.getElementById("task-panel-empty");
    const fieldsEl = panel.querySelector(".task-panel-fields");
    const resultWrap = document.getElementById("task-panel-result-wrap");
    const errorWrap = document.getElementById("task-panel-error-wrap");

    if (!info) {
      badgeEl.innerHTML = "";
      fieldsEl.style.display = "none";
      resultWrap.hidden = true;
      errorWrap.hidden = true;
      emptyEl.hidden = false;
      return;
    }

    emptyEl.hidden = true;
    fieldsEl.style.display = "";
    badgeEl.innerHTML = fcBadge(info.status, info.color || "#6e7681");
    document.getElementById("task-panel-attempts").textContent = info.attempts ?? "-";
    document.getElementById("task-panel-duration").textContent = fcFmtDuration(info.duration);
    document.getElementById("task-panel-started").textContent = fcFmtTime(info.started_at);
    document.getElementById("task-panel-ended").textContent = fcFmtTime(info.ended_at);

    if (info.result) {
      resultWrap.hidden = false;
      document.getElementById("task-panel-result").textContent = info.result;
    } else {
      resultWrap.hidden = true;
    }

    if (info.error) {
      errorWrap.hidden = false;
      document.getElementById("task-panel-error").textContent = info.error;
    } else {
      errorWrap.hidden = true;
    }
  }

  function openPanel(taskName) {
    openTaskName = taskName;
    renderPanel(taskName);
    panel.hidden = false;
  }

  function closePanel() {
    openTaskName = null;
    panel.hidden = true;
  }

  document.querySelectorAll(".dag-node").forEach((node) => {
    const taskName = node.getAttribute("data-task");
    node.addEventListener("click", () => openPanel(taskName));
    node.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        openPanel(taskName);
      }
    });
  });

  if (closeBtn) closeBtn.addEventListener("click", closePanel);

  async function refresh() {
    try {
      const res = await fetch(`/api/pipelines/${encodeURIComponent(pipelineName)}`);
      if (!res.ok) return;
      const data = await res.json();

      for (const [taskName, info] of Object.entries(data.task_statuses)) {
        const node = document.querySelector(`[data-task="${CSS.escape(taskName)}"]`);
        if (node) {
          const rect = node.querySelector("rect");
          if (rect) rect.setAttribute("fill", info.color);
        }
      }

      taskDetails = data.task_details || {};
      if (openTaskName) renderPanel(openTaskName);

      const historyBody = document.getElementById("run-history-body");
      if (historyBody && data.runs.length) {
        historyBody.innerHTML = data.runs
          .map(
            (r) => `<tr>
              <td><a href="/runs/${r.id}">#${r.id}</a></td>
              <td>${fcBadge(r.status, r.color)}</td>
              <td class="muted">${fcFmtTime(r.started_at)}</td>
              <td class="muted">${fcFmtTime(r.ended_at)}</td>
            </tr>`
          )
          .join("");
      }
    } catch (e) {
      /* silent */
    }
  }

  setInterval(refresh, POLL_INTERVAL_MS);
}

/* ---------------- All-runs page polling ---------------- */

function fcInitRunsPage(pipelineFilter, statusFilter) {
  const body = document.getElementById("all-runs-body");
  if (!body) return;

  async function refresh() {
    try {
      const params = new URLSearchParams();
      if (pipelineFilter) params.set("pipeline", pipelineFilter);
      if (statusFilter) params.set("status", statusFilter);
      const res = await fetch(`/api/runs?${params.toString()}`);
      if (!res.ok) return;
      const data = await res.json();
      body.innerHTML = data.runs
        .map(
          (r) => `<tr>
            <td><a href="/runs/${r.id}">#${r.id}</a></td>
            <td><a href="/pipelines/${encodeURIComponent(r.pipeline_name)}">${r.pipeline_name}</a></td>
            <td>${fcBadge(r.status, r.color)}</td>
            <td class="muted">${fcFmtTime(r.started_at)}</td>
            <td class="muted">${fcFmtTime(r.ended_at)}</td>
          </tr>`
        )
        .join("");
    } catch (e) {
      /* silent */
    }
  }

  setInterval(refresh, POLL_INTERVAL_MS);
}
