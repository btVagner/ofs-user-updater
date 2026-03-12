(function () {
  const page = document.getElementById("ofsReprocessingPage");
  if (!page) return;

  const routesEl = document.getElementById("reprocessingRoutesData");
  let routes = {};

  try {
    routes = JSON.parse(routesEl.textContent || "{}");
  } catch (e) {
    console.error("Falha ao ler rotas do reprocessamento:", e);
    return;
  }

  const dateFromEl = document.getElementById("dateFrom");
  const dateToEl = document.getElementById("dateTo");
  const statusCompletedEl = document.getElementById("statusCompleted");
  const statusNotdoneEl = document.getElementById("statusNotdone");

  const btnSelectAllTypes = document.getElementById("btnSelectAllTypes");
  const btnClearAllTypes = document.getElementById("btnClearAllTypes");
  const btnPreviewTargets = document.getElementById("btnPreviewTargets");
  const btnStartReprocessing = document.getElementById("btnStartReprocessing");
  const btnCancelReprocessing = document.getElementById("btnCancelReprocessing");

  const previewTotalEl = document.getElementById("previewTotal");
  const previewTableBody = document.getElementById("previewTableBody");

  const jobIdValueEl = document.getElementById("jobIdValue");
  const jobStatusBox = document.getElementById("jobStatus");
  const jobStatusTextEl = document.getElementById("jobStatusText");
  const jobProgressBarEl = document.getElementById("jobProgressBar");
  const jobMessageEl = document.getElementById("jobMessage");

  const logsBody = document.getElementById("reprocessingLogsBody");

  let currentJobId = null;
  let statusTimer = null;
  let logsTimer = null;

  function getSelectedActivityTypes() {
    return Array.from(document.querySelectorAll(".activity-type-checkbox:checked"))
      .map(el => el.value)
      .filter(Boolean);
  }

  function getSelectedStatuses() {
    const statuses = [];

    if (statusCompletedEl.checked) statuses.push("completed");
    if (statusNotdoneEl.checked) statuses.push("notdone");

    return statuses;
  }

  function buildPayload() {
    return {
      dateFrom: (dateFromEl.value || "").trim(),
      dateTo: (dateToEl.value || "").trim(),
      activityTypes: getSelectedActivityTypes(),
      statuses: getSelectedStatuses(),
    };
  }

  function setButtonsDisabled(disabled) {
    btnPreviewTargets.disabled = disabled;
    btnStartReprocessing.disabled = disabled;
    btnSelectAllTypes.disabled = disabled;
    btnClearAllTypes.disabled = disabled;
  }

  function setCancelButtonEnabled(enabled) {
    if (btnCancelReprocessing) {
      btnCancelReprocessing.disabled = !enabled;
    }
  }

  function setJobCard(status, message, progress) {
    jobStatusBox.classList.remove("idle", "running", "done", "error", "canceled");
    jobStatusBox.classList.add(status || "idle");

    jobStatusTextEl.textContent = status || "idle";
    jobMessageEl.textContent = message || "-";
    jobProgressBarEl.style.width = `${Number(progress || 0)}%`;
  }

  function renderPreview(items) {
    if (!items || !items.length) {
      previewTableBody.innerHTML = `
        <tr>
          <td colspan="4" class="empty-row">Nenhum item encontrado.</td>
        </tr>
      `;
      return;
    }

    previewTableBody.innerHTML = items.map(item => `
      <tr>
        <td>${escapeHtml(item.activity_id || "")}</td>
        <td>${escapeHtml(item.activity_type || "")}</td>
        <td>${escapeHtml(item.status || "")}</td>
        <td>${escapeHtml(item.date || "")}</td>
      </tr>
    `).join("");
  }

  function renderLogs(items) {
    if (!items || !items.length) {
      logsBody.innerHTML = `
        <tr>
          <td colspan="5" class="empty-row">Nenhum log disponível.</td>
        </tr>
      `;
      return;
    }

    logsBody.innerHTML = items.map(item => `
      <tr>
        <td>${escapeHtml(item.created_at || "")}</td>
        <td>${escapeHtml(item.activity_id || "")}</td>
        <td>${escapeHtml(item.event_type || "")}</td>
        <td>${escapeHtml(String(item.status_code ?? ""))}</td>
        <td><div class="cell-ellipsis" title="${escapeAttr(item.response_text || "")}">${escapeHtml(item.response_text || "")}</div></td>
      </tr>
    `).join("");
  }

  async function doPreview() {
    const payload = buildPayload();

    setButtonsDisabled(true);

    try {
      const resp = await fetch(routes.previewUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        throw new Error(data.error || "Erro ao carregar prévia.");
      }

      previewTotalEl.textContent = data.total || 0;
      renderPreview(data.sample || []);
    } catch (e) {
      alert(e.message || "Erro ao carregar prévia.");
    } finally {
      setButtonsDisabled(false);
    }
  }

  async function startReprocessing() {
    const payload = buildPayload();

    setButtonsDisabled(true);

    try {
      const resp = await fetch(routes.startUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        throw new Error(data.error || "Erro ao iniciar reprocessamento.");
      }

      currentJobId = data.jobId;
      jobIdValueEl.textContent = currentJobId;
      previewTotalEl.textContent = data.total || 0;

      setJobCard("running", "Job iniciado.", 0);
      renderLogs([]);
      setCancelButtonEnabled(true);

      startPolling();
    } catch (e) {
      alert(e.message || "Erro ao iniciar reprocessamento.");
      setButtonsDisabled(false);
      setCancelButtonEnabled(false);
    }
  }

  async function cancelReprocessing() {
    if (!currentJobId) return;

    try {
      const resp = await fetch(`${routes.cancelBaseUrl}/${currentJobId}`, {
        method: "POST"
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        throw new Error(data.error || "Erro ao solicitar cancelamento.");
      }

      setJobCard("running", "Cancelamento solicitado...", parseInt(jobProgressBarEl.style.width || "0", 10) || 0);
      setCancelButtonEnabled(false);
    } catch (e) {
      alert(e.message || "Erro ao cancelar reprocessamento.");
    }
  }

  async function fetchJobStatus() {
    if (!currentJobId) return;

    const resp = await fetch(`${routes.statusBaseUrl}/${currentJobId}`);
    const data = await resp.json();

    if (!resp.ok || !data.ok) {
      throw new Error(data.error || "Erro ao consultar status.");
    }

    const job = data.job || {};
    const status = job.status || "idle";
    const progress = Number(job.progress || 0);
    const message = job.message || "-";

    setJobCard(status, message, progress);

    if (status === "running") {
      setCancelButtonEnabled(true);
    } else {
      setCancelButtonEnabled(false);
    }

    if (status === "done" || status === "error" || status === "canceled") {
      stopPolling();
      setButtonsDisabled(false);
    }
  }

  async function fetchLogs() {
    if (!currentJobId) return;

    const resp = await fetch(`${routes.logsBaseUrl}/${currentJobId}?limit=200`);
    const data = await resp.json();

    if (!resp.ok || !data.ok) {
      throw new Error(data.error || "Erro ao consultar logs.");
    }

    renderLogs(data.items || []);
  }

  function startPolling() {
    stopPolling();

    statusTimer = setInterval(async () => {
      try {
        await fetchJobStatus();
      } catch (e) {
        console.error(e);
      }
    }, 2000);

    logsTimer = setInterval(async () => {
      try {
        await fetchLogs();
      } catch (e) {
        console.error(e);
      }
    }, 2000);

    fetchJobStatus().catch(console.error);
    fetchLogs().catch(console.error);
  }

  function stopPolling() {
    if (statusTimer) {
      clearInterval(statusTimer);
      statusTimer = null;
    }

    if (logsTimer) {
      clearInterval(logsTimer);
      logsTimer = null;
    }
  }

  function escapeHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function escapeAttr(value) {
    return escapeHtml(value).replaceAll("\n", " ");
  }

  btnSelectAllTypes?.addEventListener("click", function () {
    document.querySelectorAll(".activity-type-checkbox").forEach(el => {
      el.checked = true;
    });
  });

  btnClearAllTypes?.addEventListener("click", function () {
    document.querySelectorAll(".activity-type-checkbox").forEach(el => {
      el.checked = false;
    });
  });

  btnPreviewTargets?.addEventListener("click", doPreview);
  btnStartReprocessing?.addEventListener("click", startReprocessing);
  btnCancelReprocessing?.addEventListener("click", cancelReprocessing);

  setCancelButtonEnabled(false);
})();