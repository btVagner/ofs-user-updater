/* ===============================
   OFS Activities Errors - Import Job + Detail Modal
   Padrão: /importar/start | /importar/status/<id> | /importar/cancel/<id>
================================ */

(function () {
    // -------- Config --------
    const TWO_HOURS_MS = 2 * 60 * 60 * 1000;

    const STORAGE_KEY_NEXT_RUN = "ofs_activities_errors_next_run_v1";
    const STORAGE_KEY_FILTERS = "ofs_activities_errors_filters_v1";
    const STORAGE_KEY_ACTIVE_JOB = "ofs_activities_errors_active_job_v1";

    // Rotas novas
    const URL_START = "/ofs/activities-errors/importar/start";
    const URL_STATUS = (jobId) => `/ofs/activities-errors/importar/status/${jobId}`;
    const URL_CANCEL = (jobId) => `/ofs/activities-errors/importar/cancel/${jobId}`;

    // -------- Elements --------
    const importForm = document.getElementById("importForm");
    const btnImport = document.getElementById("btnImport");
    const btnCancel = document.getElementById("btnCancel");
    const jobStatus = document.getElementById("jobStatus");

    // Hidden inputs (agora com id no HTML)
    const hidDateFrom = document.getElementById("hidDateFrom");
    const hidDateTo = document.getElementById("hidDateTo");
    const hidResources = document.getElementById("hidResources");

    // Filtro (GET)
    const filtroForm = document.querySelector(".filtro-form");

    // -------- Helpers --------
    function pad(n) {
        return String(n).padStart(2, "0");
    }

    function formatRemaining(ms) {
        if (ms < 0) ms = 0;
        const totalSec = Math.floor(ms / 1000);
        const h = Math.floor(totalSec / 3600);
        const m = Math.floor((totalSec % 3600) / 60);
        const s = totalSec % 60;
        return `${pad(h)}:${pad(m)}:${pad(s)}`;
    }

    function setStatusUI(html) {
        if (jobStatus) jobStatus.innerHTML = html;
    }

    function setRunningUI(isRunning) {
        if (btnImport) btnImport.disabled = !!isRunning;
        if (btnCancel) btnCancel.style.display = isRunning ? "inline-block" : "none";
    }

    function saveNextRun(tsMs) {
        localStorage.setItem(STORAGE_KEY_NEXT_RUN, String(tsMs));
    }

    function loadNextRun() {
        const v = localStorage.getItem(STORAGE_KEY_NEXT_RUN);
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
    }

    function saveFilters(f) {
        localStorage.setItem(STORAGE_KEY_FILTERS, JSON.stringify(f));
    }

    function loadFilters() {
        try {
            const raw = localStorage.getItem(STORAGE_KEY_FILTERS);
            if (!raw) return null;
            return JSON.parse(raw);
        } catch {
            return null;
        }
    }

    function setActiveJobId(jobId) {
        localStorage.setItem(STORAGE_KEY_ACTIVE_JOB, String(jobId));
    }

    function getActiveJobId() {
        const v = localStorage.getItem(STORAGE_KEY_ACTIVE_JOB);
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
    }

    function clearActiveJobId() {
        localStorage.removeItem(STORAGE_KEY_ACTIVE_JOB);
    }

    function getFiltersFromPage() {
        // Prioridade: hidden inputs do importForm (reflete os filtros atuais)
        const dateFrom =
            (hidDateFrom?.value || "").trim() ||
            (filtroForm?.querySelector('input[name="dateFrom"]')?.value || "").trim();

        const dateTo =
            (hidDateTo?.value || "").trim() ||
            (filtroForm?.querySelector('input[name="dateTo"]')?.value || "").trim();

        const resources =
            (hidResources?.value || "").trim() ||
            (filtroForm?.querySelector('input[name="resources"]')?.value || "").trim() ||
            "02";

        return { dateFrom, dateTo, resources };
    }

    async function postFormUrlEncoded(url, payload) {
        const body = new URLSearchParams(payload);
        const resp = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body,
        });

        const data = await resp.json().catch(() => ({}));
        if (!resp.ok || !data.ok) {
            throw new Error(data.error || "Falha na operação");
        }
        return data;
    }

    async function startJob(filters) {
        const data = await postFormUrlEncoded(URL_START, filters);
        const jobId = data.jobId;
        if (!jobId) throw new Error("JobId não retornado");
        setActiveJobId(jobId);
        return jobId;
    }

    async function fetchJobStatus(jobId) {
        const resp = await fetch(URL_STATUS(jobId));
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok || !data.ok) {
            throw new Error(data.error || "Falha ao consultar status");
        }
        return data.job;
    }

    async function cancelJob(jobId) {
        await postFormUrlEncoded(URL_CANCEL(jobId), {});
    }

    function renderJob(job, nextRunTs) {
        const status = String(job?.status || "-");
        const progress = Number(job?.progress || 0);
        const message = String(job?.message || "");

        let extra = "";
        if (nextRunTs) {
            const remaining = nextRunTs - Date.now();
            extra = `
        <div class="job-next-run">
          Próxima atualização automática em: <b class="job-countdown">${formatRemaining(remaining)}</b>
        </div>
      `;
        }

        return `
      <div class="job-card ${status}">
        <div class="job-line">
          <span class="job-label">Status:</span> <b>${status}</b>
        </div>
        <div class="job-line">
          <span class="job-label">Progresso:</span> <b>${progress}%</b>
        </div>

        <div class="job-progress">
          <div class="job-progress-bar" style="width:${Math.max(0, Math.min(100, progress))}%"></div>
        </div>

        <div class="job-msg">${message || ""}</div>
        ${extra}
      </div>
    `;
    }

    let pollTimer = null;
    function stopPolling() {
        if (pollTimer) clearInterval(pollTimer);
        pollTimer = null;
    }

    async function poll(jobId, filters) {
        setRunningUI(true);

        // Poll rápido enquanto running
        stopPolling();
        pollTimer = setInterval(async () => {
            try {
                const job = await fetchJobStatus(jobId);

                const nextRun = loadNextRun();
                setStatusUI(renderJob(job, nextRun));

                if (job.status === "running") return;

                // Finalizou
                stopPolling();
                clearActiveJobId();
                setRunningUI(false);

                if (job.status === "done") {
                    // agenda próxima execução em 2h e recarrega com os filtros
                    const newNext = Date.now() + TWO_HOURS_MS;
                    saveNextRun(newNext);
                    saveFilters(filters);

                    const q = new URLSearchParams(filters).toString();
                    window.location.href = `${window.location.pathname}?${q}`;
                    return;
                }

                if (job.status === "canceled") {
                    setStatusUI(renderJob(job, loadNextRun()));
                    return;
                }

                // error
                setStatusUI(renderJob(job, loadNextRun()));
                // retry automático em 2 minutos
                const retry = Date.now() + (2 * 60 * 1000);
                saveNextRun(retry);
            } catch (e) {
                // falha de rede/status: não derruba; mantém tentando
                setStatusUI(`
          <div class="job-card error">
            <div class="job-line"><b>Falha ao consultar status</b></div>
            <div class="job-msg">${String(e.message || e)}</div>
          </div>
        `);
            }
        }, 2000);

        // Primeira chamada imediata
        try {
            const job = await fetchJobStatus(jobId);
            setStatusUI(renderJob(job, loadNextRun()));
        } catch (e) {
            setStatusUI(`
        <div class="job-card error">
          <div class="job-line"><b>Falha ao iniciar polling</b></div>
          <div class="job-msg">${String(e.message || e)}</div>
        </div>
      `);
        }
    }

    // -------- Auto scheduler (2h) --------
    let countdownTimer = null;
    function startCountdown() {
        if (countdownTimer) clearInterval(countdownTimer);

        countdownTimer = setInterval(() => {
            const nextRun = loadNextRun();
            if (!nextRun) return;

            // Atualiza somente o countdown se existir no DOM
            const el = document.querySelector(".job-countdown");
            if (el) el.textContent = formatRemaining(nextRun - Date.now());
        }, 1000);
    }

    async function autoTick() {
        const nextRun = loadNextRun();
        if (!nextRun) return;

        const remaining = nextRun - Date.now();
        if (remaining > 0) return;

        // Se já tem job rodando, não dispara outro
        const active = getActiveJobId();
        if (active) return;

        const filters = loadFilters() || getFiltersFromPage();

        try {
            setStatusUI(`
        <div class="job-card running">
          <div class="job-line"><b>Disparando atualização automática...</b></div>
        </div>
      `);

            const jobId = await startJob(filters);
            await poll(jobId, filters);
        } catch (e) {
            // retry em 2 minutos
            const retry = Date.now() + (2 * 60 * 1000);
            saveNextRun(retry);

            setStatusUI(`
        <div class="job-card error">
          <div class="job-line"><b>Falha na atualização automática</b></div>
          <div class="job-msg">${String(e.message || e)}</div>
          <div class="job-next-run">
            Próxima tentativa em: <b class="job-countdown">00:02:00</b>
          </div>
        </div>
      `);
            startCountdown();
        }
    }

    // Checa autoTick a cada 3s
    let autoTimer = setInterval(autoTick, 3000);

    // -------- Manual import (botão Atualizar agora) --------
    if (importForm) {
        importForm.addEventListener("submit", async (ev) => {
            ev.preventDefault();

            // Se já rodando, ignora
            const active = getActiveJobId();
            if (active) return;

            const filters = getFiltersFromPage();

            // Agenda próximo auto-run em 2h (a partir do clique) — padrão antigo mantido
            saveFilters(filters);
            saveNextRun(Date.now() + TWO_HOURS_MS);

            setStatusUI(`
        <div class="job-card running">
          <div class="job-line"><b>Iniciando atualização...</b></div>
        </div>
      `);

            try {
                const jobId = await startJob(filters);
                await poll(jobId, filters);
            } catch (e) {
                setRunningUI(false);
                clearActiveJobId();
                setStatusUI(`
          <div class="job-card error">
            <div class="job-line"><b>Erro ao iniciar atualização</b></div>
            <div class="job-msg">${String(e.message || e)}</div>
          </div>
        `);

                // retry em 2 minutos
                const retry = Date.now() + (2 * 60 * 1000);
                saveNextRun(retry);
            }
        });
    }

    if (btnCancel) {
        btnCancel.addEventListener("click", async () => {
            const jobId = getActiveJobId();
            if (!jobId) return;

            try {
                setStatusUI(`
          <div class="job-card running">
            <div class="job-line"><b>Solicitando cancelamento...</b></div>
          </div>
        `);
                await cancelJob(jobId);
            } catch (e) {
                setStatusUI(`
          <div class="job-card error">
            <div class="job-line"><b>Falha ao cancelar</b></div>
            <div class="job-msg">${String(e.message || e)}</div>
          </div>
        `);
            }
        });
    }

    // Se tiver job ativo salvo (ex: refresh no meio), retoma polling
    (async function resumeIfNeeded() {
        const activeJob = getActiveJobId();
        if (activeJob) {
            const filters = loadFilters() || getFiltersFromPage();
            await poll(activeJob, filters);
        } else {
            // exibe countdown se já existir nextRun salvo
            const nextRun = loadNextRun();
            if (nextRun) {
                setStatusUI(`
          <div class="job-card idle">
            <div class="job-line">Aguardando próxima atualização automática...</div>
            <div class="job-next-run">
              Próxima em: <b class="job-countdown">${formatRemaining(nextRun - Date.now())}</b>
            </div>
          </div>
        `);
                startCountdown();
            }
        }
    })();

    // -------- Detail Modal --------
    const detailModal = document.getElementById("detailModal");
    const detailBackdrop = document.getElementById("detailBackdrop");
    const detailClose = document.getElementById("detailClose");

    function openDetailModal() {
        if (detailModal) detailModal.style.display = "block";
        if (detailBackdrop) detailBackdrop.style.display = "block";
    }
    function closeDetailModal() {
        if (detailModal) detailModal.style.display = "none";
        if (detailBackdrop) detailBackdrop.style.display = "none";
    }

    if (detailClose) detailClose.onclick = closeDetailModal;
    if (detailBackdrop) detailBackdrop.onclick = closeDetailModal;

    document.addEventListener("click", async (ev) => {
        const btn = ev.target.closest(".js-open-detail");
        if (!btn) return;

        const id = btn.dataset.id;
        if (!id) return;

        btn.disabled = true;
        const originalText = btn.textContent;
        btn.textContent = "Carregando...";

        try {
            const resp = await fetch(`/ofs/activities-errors/${encodeURIComponent(id)}`);
            const data = await resp.json().catch(() => ({}));

            if (!resp.ok || !data.ok) {
                throw new Error(data.error || "Erro ao carregar detalhe");
            }

            document.getElementById("ngDispatch").value =
                data.item.XA_API_NG_DISPATCH || "";

            document.getElementById("ngResponse").value =
                data.item.XA_RES_API_NG_RESPONSE || "";

            document.getElementById("sapLdg").value =
                data.item.XA_SAP_CRT_LDG || "";

            openDetailModal();

        } catch (e) {
            alert(String(e.message || e));
        } finally {
            btn.disabled = false;
            btn.textContent = originalText;
        }
    });
})();