document.addEventListener("DOMContentLoaded", function () {
    const body = document.body;

    const startUrl = body.dataset.ofsOsStartUrl;
    const statusUrlBase = body.dataset.ofsOsStatusUrlBase;
    const downloadUrlBase = body.dataset.ofsOsDownloadUrlBase;
    const discardUrlBase = body.dataset.ofsOsDiscardUrlBase;

    const resourceSyncStartUrl = body.dataset.resourceSyncStartUrl;
    const resourceSyncStatusUrlBase = body.dataset.resourceSyncStatusUrlBase;

    const btnOpen = document.getElementById("btnOpenOfsOsReport");
    const modal = document.getElementById("ofsOsReportModal");
    const backdrop = document.getElementById("ofsOsReportBackdrop");
    const btnClose = document.getElementById("btnCloseOfsOsReport");
    const form = document.getElementById("ofsOsReportForm");
    const btnStart = document.getElementById("btnStartOfsOsReport");
    const statusBox = document.getElementById("reportStatusBox");

    const RESOURCE_SYNC_STORAGE_KEY = "ofs_resource_sync_job_id";

    function todayStr() {
        const d = new Date();
        const year = d.getFullYear();
        const month = String(d.getMonth() + 1).padStart(2, "0");
        const day = String(d.getDate()).padStart(2, "0");
        return `${year}-${month}-${day}`;
    }

    function escapeHtml(value) {
        return String(value ?? "")
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#039;");
    }

    async function fetchJson(url, options = {}) {
        const resp = await fetch(url, options);
        const raw = await resp.text();

        let data = {};
        try {
            data = raw ? JSON.parse(raw) : {};
        } catch (e) {
            data = { _raw: raw };
        }

        return { resp, data, raw };
    }

    function getReadableError(data, fallbackMessage) {
        if (data && data.error) {
            return data.error;
        }

        if (data && data._raw) {
            const clean = String(data._raw)
                .replace(/<[^>]*>/g, " ")
                .replace(/\s+/g, " ")
                .trim();

            if (clean) {
                return clean.slice(0, 220);
            }
        }

        return fallbackMessage || "Erro inesperado.";
    }

    function openModal() {
        const dateFrom = document.getElementById("dateFrom");
        const dateTo = document.getElementById("dateTo");

        if (dateFrom && !dateFrom.value) dateFrom.value = todayStr();
        if (dateTo && !dateTo.value) dateTo.value = todayStr();

        if (backdrop) backdrop.style.display = "block";
        if (modal) modal.style.display = "block";
    }

    function closeModal() {
        if (modal) modal.style.display = "none";
        if (backdrop) backdrop.style.display = "none";
    }

    if (btnOpen) btnOpen.addEventListener("click", openModal);
    if (btnClose) btnClose.addEventListener("click", closeModal);
    if (backdrop) backdrop.addEventListener("click", closeModal);

    function showToast(type, title, message, durationMs = 7000) {
        const container = document.getElementById("toastContainer");
        if (!container) return;

        const toast = document.createElement("div");
        toast.className = `toast-msg ${type}`;

        toast.innerHTML = `
            <div class="toast-body">
                <div class="toast-title">${escapeHtml(title)}</div>
                <div class="toast-text">${escapeHtml(message)}</div>
            </div>
            <div class="toast-progress">
                <div class="toast-progress-bar"></div>
            </div>
        `;

        container.appendChild(toast);

        if (durationMs === null) {
            return;
        }

        setTimeout(() => {
            toast.style.animation = "toastOut 0.28s ease forwards";
            setTimeout(() => {
                toast.remove();
            }, 280);
        }, durationMs);
    }

    function showPersistentExtractionToast(job) {
        const container = document.getElementById("toastContainer");
        if (!container) return;

        const existing = document.getElementById(`toast-job-${job.job_id}`);
        if (existing) existing.remove();

        const downloadUrl = downloadUrlBase.replace("__JOB_ID__", encodeURIComponent(job.job_id));
        const discardUrl = discardUrlBase.replace("__JOB_ID__", encodeURIComponent(job.job_id));

        const toast = document.createElement("div");
        toast.id = `toast-job-${job.job_id}`;
        toast.className = "toast-msg success toast-persistent";

        toast.innerHTML = `
            <div class="toast-body">
                <div class="toast-title">Extração concluída</div>
                <div class="toast-text">
                    Relatório pronto para download.<br>
                    Linhas extraídas: <b>${escapeHtml(job.total_rows || 0)}</b>
                </div>
                <div class="toast-buttons">
                    <button type="button" class="toast-action download">Download XLSX</button>
                    <button type="button" class="toast-action discard">Descartar extração</button>
                </div>
            </div>
            <div class="toast-progress">
                <div class="toast-progress-bar"></div>
            </div>
        `;

        const btnDownload = toast.querySelector(".toast-action.download");
        const btnDiscard = toast.querySelector(".toast-action.discard");

        btnDownload.addEventListener("click", function () {
            window.location.href = downloadUrl;
            toast.style.animation = "toastOut 0.28s ease forwards";
            setTimeout(() => toast.remove(), 280);
        });

        btnDiscard.addEventListener("click", async function () {
            const ok = confirm("Ao descartar esta extração, será necessário executar o relatório novamente. Deseja continuar?");
            if (!ok) return;

            try {
                const { resp, data } = await fetchJson(discardUrl, { method: "POST" });

                if (!resp.ok || !data.ok) {
                    showToast("error", "Erro", getReadableError(data, "Erro ao descartar extração."));
                    return;
                }

                toast.style.animation = "toastOut 0.28s ease forwards";
                setTimeout(() => toast.remove(), 280);
                showToast("success", "Sucesso", "Extração descartada.");
            } catch (err) {
                console.error(err);
                showToast("error", "Erro", "Falha de rede ao descartar extração.");
            }
        });

        container.appendChild(toast);
    }

    function checkedValues(name) {
        return Array.from(document.querySelectorAll(`input[name="${name}"]:checked`))
            .map(el => el.value)
            .filter(Boolean);
    }

    function setChecked(name, checked, scope) {
        const root = scope || document;
        root.querySelectorAll(`input[name="${name}"]`).forEach(el => {
            el.checked = checked;
        });
    }

    function setStatusBox(text, type) {
        if (!statusBox) return;

        statusBox.style.display = "block";
        statusBox.textContent = text || "";

        if (type === "error") {
            statusBox.style.background = "#2a171a";
            statusBox.style.borderColor = "#6c2932";
            statusBox.style.color = "#ffdce1";
        } else {
            statusBox.style.background = "#16202d";
            statusBox.style.borderColor = "#28415f";
            statusBox.style.color = "#dcecff";
        }
    }

    function clearStatusBox() {
        if (!statusBox) return;
        statusBox.style.display = "none";
        statusBox.textContent = "";
    }

    document.getElementById("btnSelectAllStatus")?.addEventListener("click", () => {
        setChecked("statuses", true);
    });

    document.getElementById("btnClearStatus")?.addEventListener("click", () => {
        setChecked("statuses", false);
    });
    document.getElementById("btnSelectAllActivityTypes")?.addEventListener("click", () => {
        setChecked("activityTypes", true);
    });

    document.getElementById("btnClearActivityTypes")?.addEventListener("click", () => {
        setChecked("activityTypes", false);
    });

    document.getElementById("btnSelectAllFields")?.addEventListener("click", () => {
        setChecked("fields", true);
    });

    document.getElementById("btnClearFields")?.addEventListener("click", () => {
        setChecked("fields", false);
    });

    document.querySelectorAll(".resource-type-btn").forEach(btn => {
        btn.addEventListener("click", function () {
            const type = btn.dataset.resourceType;

            document.querySelectorAll(".resource-type-btn").forEach(x => x.classList.remove("active"));
            btn.classList.add("active");

            document.querySelectorAll(".resource-group").forEach(group => {
                group.classList.toggle("hidden", group.dataset.resourceGroup !== type);
            });
        });
    });

    document.getElementById("btnSelectVisibleResources")?.addEventListener("click", () => {
        const visibleGroup = document.querySelector(".resource-group:not(.hidden)");
        if (visibleGroup) setChecked("resources", true, visibleGroup);
    });

    document.getElementById("btnClearVisibleResources")?.addEventListener("click", () => {
        const visibleGroup = document.querySelector(".resource-group:not(.hidden)");
        if (visibleGroup) setChecked("resources", false, visibleGroup);
    });

    async function pollJob(jobId) {
        const statusUrl = statusUrlBase.replace("__JOB_ID__", encodeURIComponent(jobId));

        const interval = setInterval(async function () {
            try {
                const { resp, data } = await fetchJson(statusUrl);

                if (!resp.ok || !data.ok) {
                    clearInterval(interval);
                    setStatusBox(getReadableError(data, "Erro ao consultar status da extração."), "error");
                    btnStart.disabled = false;
                    return;
                }

                const job = data.job || {};
                const rows = job.rows_so_far || 0;
                const phase = job.phase || "Processando";

                setStatusBox(`${phase} | Linhas até agora: ${rows}`, "info");

                if (job.status === "completed") {
                    clearInterval(interval);
                    setStatusBox(`Extração concluída. Linhas extraídas: ${job.total_rows || 0}.`, "info");
                    btnStart.disabled = false;
                    showPersistentExtractionToast(job);
                    return;
                }

                if (job.status === "failed") {
                    clearInterval(interval);
                    setStatusBox(job.error || "Falha na extração.", "error");
                    btnStart.disabled = false;
                    showToast("error", "Erro", job.error || "Falha na extração.");
                }
            } catch (err) {
                console.error(err);
                clearInterval(interval);
                setStatusBox("Falha de rede ao consultar status da extração.", "error");
                btnStart.disabled = false;
            }
        }, 2500);
    }

    if (form) {
        form.addEventListener("submit", async function (e) {
            e.preventDefault();

            const dateFrom = document.getElementById("dateFrom")?.value || "";
            const dateTo = document.getElementById("dateTo")?.value || "";
            const statuses = checkedValues("statuses");
            const activityTypes = checkedValues("activityTypes");
            const resources = checkedValues("resources");
            const fields = checkedValues("fields");

            if (!dateFrom || !dateTo) {
                showToast("warning", "Atenção", "Informe o período do relatório.");
                return;
            }

            if (statuses.length === 0) {
                showToast("warning", "Atenção", "Selecione pelo menos um status.");
                return;
            }
            if (activityTypes.length === 0) {
                showToast("warning", "Atenção", "Selecione pelo menos um tipo de atividade.");
                return;
            }
            if (resources.length === 0) {
                showToast("warning", "Atenção", "Selecione pelo menos um recurso.");
                return;
            }

            if (fields.length === 0) {
                showToast("warning", "Atenção", "Selecione pelo menos um campo.");
                return;
            }

            const payload = {
                dateFrom,
                dateTo,
                statuses,
                activityTypes,
                resources,
                fields,
            };
            btnStart.disabled = true;
            clearStatusBox();
            setStatusBox("Iniciando extração em segundo plano...", "info");

            try {
                const { resp, data } = await fetchJson(startUrl, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify(payload),
                });

                if (!resp.ok || !data.ok) {
                    btnStart.disabled = false;
                    const errorMessage = getReadableError(data, "Erro ao iniciar extração.");
                    setStatusBox(errorMessage, "error");
                    showToast("error", "Erro", errorMessage);
                    return;
                }

                showToast("success", "Extração iniciada", "O relatório está sendo processado em segundo plano.");
                pollJob(data.jobId);
            } catch (err) {
                console.error(err);
                btnStart.disabled = false;
                setStatusBox("Falha de rede ao iniciar extração.", "error");
                showToast("error", "Erro", "Falha de rede ao iniciar extração.");
            }
        });
    }

    // ===============================
    // Atualização de recursos OFS
    // ===============================
    (function () {
        const btnSync = document.getElementById("btnSyncResources");
        const progressWrap = document.getElementById("resourceSyncProgress");
        const progressText = document.getElementById("resourceSyncText");
        const progressPercent = document.getElementById("resourceSyncPercent");
        const progressFill = document.getElementById("resourceSyncBarFill");

        if (!btnSync || !resourceSyncStartUrl || !resourceSyncStatusUrlBase) return;

        let resourceSyncPollingActive = false;

        function setResourceProgress(percent, text, mode = "running") {
            const safePercent = Math.max(0, Math.min(100, parseInt(percent || 0, 10)));

            if (progressWrap) progressWrap.style.display = "block";
            if (progressText) progressText.textContent = text || "Atualizando recursos...";
            if (progressPercent) progressPercent.textContent = `${safePercent}%`;

            if (progressFill) {
                progressFill.style.width = `${safePercent}%`;

                if (mode === "error") {
                    progressFill.style.background = "#dc3545";
                } else if (mode === "success") {
                    progressFill.style.background = "#198754";
                } else {
                    progressFill.style.background = "#0d6efd";
                }
            }
        }

        function finishResourceProgress(text) {
            setResourceProgress(100, text || "Lista de recursos atualizada com sucesso.", "success");
            btnSync.disabled = false;
        }

        function failResourceProgress(text) {
            if (progressWrap) progressWrap.style.display = "block";
            if (progressText) progressText.textContent = text || "Falha ao atualizar recursos.";
            if (progressPercent) progressPercent.textContent = "Erro";

            if (progressFill) {
                progressFill.style.width = "100%";
                progressFill.style.background = "#dc3545";
            }

            btnSync.disabled = false;
        }

        function clearStoredResourceJob() {
            localStorage.removeItem(RESOURCE_SYNC_STORAGE_KEY);
        }

        function storeResourceJob(jobId) {
            localStorage.setItem(RESOURCE_SYNC_STORAGE_KEY, jobId);
        }

        async function pollResourceSync(jobId) {
            if (!jobId || resourceSyncPollingActive) return;

            resourceSyncPollingActive = true;
            btnSync.disabled = true;

            const statusUrl = resourceSyncStatusUrlBase.replace("__JOB_ID__", encodeURIComponent(jobId));
            let consecutiveErrors = 0;

            async function tick() {
                try {
                    const { resp, data } = await fetchJson(statusUrl);

                    if (!resp.ok || !data.ok) {
                        const errorMessage = getReadableError(data, "Erro ao consultar andamento da atualização.");

                        if (resp.status === 404) {
                            resourceSyncPollingActive = false;
                            clearStoredResourceJob();
                            failResourceProgress(errorMessage);
                            showToast("error", "Erro", errorMessage);
                            return;
                        }

                        consecutiveErrors += 1;

                        if (consecutiveErrors >= 5) {
                            resourceSyncPollingActive = false;
                            failResourceProgress(errorMessage);
                            showToast("error", "Erro", errorMessage);
                            return;
                        }

                        setResourceProgress(
                            0,
                            `Tentando reconectar ao status da atualização... tentativa ${consecutiveErrors}/5`,
                            "running"
                        );

                        setTimeout(tick, 2500);
                        return;
                    }

                    consecutiveErrors = 0;

                    const job = data.job || {};
                    const percent = job.percent || 0;
                    const rawTotal = job.raw_total_from_api || 0;
                    const insertedSoFar = job.inserted_so_far || 0;
                    const phase = job.phase || "Atualizando recursos";

                    setResourceProgress(
                        percent,
                        `${phase} | API: ${rawTotal} | Carregados: ${insertedSoFar}`,
                        "running"
                    );

                    if (job.status === "completed") {
                        resourceSyncPollingActive = false;
                        clearStoredResourceJob();

                        const insertedTotal = job.inserted_total || insertedSoFar || 0;

                        finishResourceProgress(`Concluído. Recursos ativos carregados: ${insertedTotal}.`);

                        showToast(
                            "success",
                            "Lista de recursos atualizada",
                            `Recursos ativos carregados: ${insertedTotal}. Reabra o relatório para ver a lista atualizada.`
                        );

                        return;
                    }

                    if (job.status === "failed") {
                        resourceSyncPollingActive = false;
                        clearStoredResourceJob();

                        const errorMessage = job.error || "Falha ao atualizar lista de recursos.";

                        failResourceProgress(errorMessage);
                        showToast("error", "Erro", errorMessage);
                        return;
                    }

                    setTimeout(tick, 2500);
                } catch (err) {
                    console.error(err);

                    consecutiveErrors += 1;

                    if (consecutiveErrors >= 5) {
                        resourceSyncPollingActive = false;
                        failResourceProgress("Falha de rede ao consultar andamento da atualização.");
                        showToast("error", "Erro", "Falha de rede ao consultar andamento da atualização.");
                        return;
                    }

                    setResourceProgress(
                        0,
                        `Falha temporária ao consultar status. Tentando novamente ${consecutiveErrors}/5...`,
                        "running"
                    );

                    setTimeout(tick, 2500);
                }
            }

            tick();
        }

        btnSync.addEventListener("click", async function () {
            btnSync.disabled = true;
            setResourceProgress(0, "Iniciando atualização da lista de recursos...", "running");

            try {
                const { resp, data } = await fetchJson(resourceSyncStartUrl, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                });

                if (resp.status === 409 || data.alreadyRunning) {
                    btnSync.disabled = false;

                    const activeUser = data.activeJob?.actor_username || "outro usuário";

                    failResourceProgress("Já existe uma atualização em andamento.");

                    showToast(
                        "warning",
                        "Atualização em andamento",
                        `Já existe uma atualização da lista de recursos sendo executada por ${activeUser}. Aguarde finalizar.`
                    );

                    return;
                }

                if (!resp.ok || !data.ok) {
                    btnSync.disabled = false;

                    const errorMessage = getReadableError(data, "Erro ao iniciar atualização da lista de recursos.");

                    failResourceProgress(errorMessage);
                    showToast("error", "Erro", errorMessage);

                    return;
                }

                storeResourceJob(data.jobId);

                showToast(
                    "warning",
                    "Atualização iniciada",
                    "A lista de recursos continuará sendo atualizada em segundo plano mesmo se você fechar esta tela.",
                    7000
                );

                setResourceProgress(1, "Atualização iniciada em segundo plano...", "running");
                pollResourceSync(data.jobId);
            } catch (err) {
                console.error(err);

                btnSync.disabled = false;
                failResourceProgress("Falha de rede ao iniciar atualização.");

                showToast(
                    "error",
                    "Erro",
                    "Falha de rede ao iniciar atualização da lista de recursos."
                );
            }
        });

        const storedJobId = localStorage.getItem(RESOURCE_SYNC_STORAGE_KEY);
        if (storedJobId) {
            setResourceProgress(1, "Retomando acompanhamento da atualização de recursos...", "running");
            pollResourceSync(storedJobId);
        }
    })();
});