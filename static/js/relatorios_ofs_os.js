document.addEventListener("DOMContentLoaded", function () {
    const body = document.body;

    const startUrl = body.dataset.ofsOsStartUrl;
    const statusUrlBase = body.dataset.ofsOsStatusUrlBase;
    const downloadUrlBase = body.dataset.ofsOsDownloadUrlBase;
    const discardUrlBase = body.dataset.ofsOsDiscardUrlBase;

    const resourceSyncStartUrl = body.dataset.resourceSyncStartUrl;
    const resourceSyncStatusUrlBase = body.dataset.resourceSyncStatusUrlBase;

    const taskTypeSyncUrl = body.dataset.taskTypeSyncUrl;

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

    function setDefaultDates() {
        const dateFrom = document.getElementById("dateFrom");
        const dateTo = document.getElementById("dateTo");

        if (dateFrom && !dateFrom.value) dateFrom.value = todayStr();
        if (dateTo && !dateTo.value) dateTo.value = todayStr();
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
        const resp = await fetch(url, {
            credentials: "same-origin",
            ...options,
        });

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

        if (data && data.detail) {
            return data.detail;
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
        if (!container || !job || !job.job_id) return;

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

    function checkedValuesInScope(name, scope) {
        if (!scope) return [];
        return Array.from(scope.querySelectorAll(`input[name="${name}"]:checked`))
            .map(el => el.value)
            .filter(Boolean);
    }
    function selectedCountLabel(count) {
        const value = Number(count) || 0;
        return value === 1 ? "1 selecionado" : `${value} selecionados`;
    }

    function getActiveResourceTypeLabel() {
        const activeButton = getActiveResourceTypeButton();

        if (!activeButton) {
            return "-";
        }

        const clone = activeButton.cloneNode(true);
        const span = clone.querySelector("span");

        if (span) {
            span.remove();
        }

        return clone.textContent.trim() || "-";
    }

    function updateReportSummary() {
        const dateFrom = document.getElementById("dateFrom")?.value || "";
        const dateTo = document.getElementById("dateTo")?.value || "";

        const statuses = checkedValues("statuses");
        const activityTypes = checkedValues("activityTypes");
        const fields = checkedValues("fields");

        const visibleResourceGroup = getVisibleResourceGroup();
        const resources = checkedValuesInScope("resources", visibleResourceGroup);

        const summaryPeriod = document.getElementById("summaryPeriod");
        const summaryStatuses = document.getElementById("summaryStatuses");
        const summaryActivityTypes = document.getElementById("summaryActivityTypes");
        const summaryResourceGroup = document.getElementById("summaryResourceGroup");
        const summaryResources = document.getElementById("summaryResources");
        const summaryFields = document.getElementById("summaryFields");

        if (summaryPeriod) {
            summaryPeriod.textContent = dateFrom && dateTo ? `${dateFrom} até ${dateTo}` : "-";
        }

        if (summaryStatuses) {
            summaryStatuses.textContent = selectedCountLabel(statuses.length);
        }

        if (summaryActivityTypes) {
            summaryActivityTypes.textContent = selectedCountLabel(activityTypes.length);
        }

        if (summaryResourceGroup) {
            summaryResourceGroup.textContent = getActiveResourceTypeLabel();
        }

        if (summaryResources) {
            summaryResources.textContent = selectedCountLabel(resources.length);
        }

        if (summaryFields) {
            summaryFields.textContent = selectedCountLabel(fields.length);
        }
    }

    function normalizeResourceSearchText(value) {
        return String(value || "")
            .normalize("NFD")
            .replace(/[\u0300-\u036f]/g, "")
            .toLowerCase()
            .trim();
    }

    function getResourceSearchValue() {
        return normalizeResourceSearchText(
            document.getElementById("resourceSearchInput")?.value || ""
        );
    }
    function getResourceStatusFilterValue() {
        const activeBtn = document.querySelector(".resource-status-filter-btn.active");
        return activeBtn?.dataset.resourceStatusFilter || "active";
    }

    function resourceMatchesStatusFilter(row) {
        const filterValue = getResourceStatusFilterValue();
        const rowStatus = String(row.dataset.resourceStatus || "unknown").toLowerCase();

        if (filterValue === "all") {
            return true;
        }

        if (filterValue === "active") {
            return rowStatus === "active";
        }

        if (filterValue === "inactive") {
            return rowStatus !== "active";
        }

        return true;
    }
    function getVisibleResourceRowsInActiveGroup() {
        const activeGroup = getVisibleResourceGroup();

        if (!activeGroup) {
            return [];
        }

        return Array.from(activeGroup.querySelectorAll(".resource-check"))
            .filter(row => row.style.display !== "none");
    }

    function updateResourceSearchCount() {
        const activeGroup = getVisibleResourceGroup();
        const countEl = document.getElementById("resourceSearchCount");

        if (!activeGroup || !countEl) {
            return;
        }

        const rows = Array.from(activeGroup.querySelectorAll(".resource-check"));
        const total = rows.length;
        const visible = getVisibleResourceRowsInActiveGroup().length;

        const activeTotal = rows.filter(row => {
            return String(row.dataset.resourceStatus || "").toLowerCase() === "active";
        }).length;

        const inactiveTotal = rows.filter(row => {
            return String(row.dataset.resourceStatus || "").toLowerCase() !== "active";
        }).length;

        const searchValue = getResourceSearchValue();
        const statusFilter = getResourceStatusFilterValue();

        if (statusFilter === "active" && !searchValue) {
            countEl.textContent = `${activeTotal} recursos ativos nesta aba`;
            return;
        }

        if (statusFilter === "inactive" && !searchValue) {
            countEl.textContent = `${inactiveTotal} recursos inativos nesta aba`;
            return;
        }

        if (statusFilter === "all" && !searchValue) {
            countEl.textContent = `${total} recursos nesta aba`;
            return;
        }

        countEl.textContent = `${visible} de ${total} recursos encontrados`;
    }

    function applyResourceSearch() {
        const activeGroup = getVisibleResourceGroup();

        if (!activeGroup) {
            updateResourceSearchCount();
            return;
        }

        const searchValue = getResourceSearchValue();

        activeGroup.querySelectorAll(".resource-check").forEach(row => {
            const name = normalizeResourceSearchText(
                row.querySelector(".resource-name")?.textContent || ""
            );

            const id = normalizeResourceSearchText(
                row.querySelector(".resource-id")?.textContent || ""
            );

            const matchesSearch = !searchValue || name.includes(searchValue) || id.includes(searchValue);
            const matchesStatus = resourceMatchesStatusFilter(row);

            row.style.display = matchesSearch && matchesStatus ? "" : "none";
        });

        updateResourceSearchCount();
        updateReportSummary();
    }

    function setChecked(name, checked, scope) {
        const root = scope || document;
        root.querySelectorAll(`input[name="${name}"]`).forEach(el => {
            el.checked = checked;
        });
    }

    function getActiveResourceTypeButton() {
        return document.querySelector(".resource-type-btn.active");
    }

    function getVisibleResourceGroup() {
        return document.querySelector(".resource-group:not(.hidden)");
    }

    function clearResourceSelectionsOutsideGroup(activeGroup) {
        document.querySelectorAll(".resource-group").forEach(group => {
            if (group !== activeGroup) {
                setChecked("resources", false, group);
            }
        });
    }

    function activateResourceGroup(resourceType) {
        if (!resourceType) return;

        document.querySelectorAll(".resource-type-btn").forEach(btn => {
            btn.classList.toggle("active", btn.dataset.resourceType === resourceType);
        });

        let activeGroup = null;

        document.querySelectorAll(".resource-group").forEach(group => {
            const isActive = group.dataset.resourceGroup === resourceType;
            group.classList.toggle("hidden", !isActive);

            if (isActive) {
                activeGroup = group;
            }
        });

        if (activeGroup) {
            clearResourceSelectionsOutsideGroup(activeGroup);
        }
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
        updateReportSummary();
    });

    document.getElementById("btnClearStatus")?.addEventListener("click", () => {
        setChecked("statuses", false);
        updateReportSummary();
    });

    document.getElementById("btnSelectAllActivityTypes")?.addEventListener("click", () => {
        document.querySelectorAll('input[name="activityTypes"]').forEach(el => {
            el.checked = el.dataset.category !== "internal";
        });
        updateReportSummary();
    });

    document.getElementById("btnClearActivityTypes")?.addEventListener("click", () => {
        setChecked("activityTypes", false);
        updateReportSummary();
    });
    document.getElementById("btnSelectAllFields")?.addEventListener("click", () => {
        setChecked("fields", true);
        updateReportSummary();
    });

    document.getElementById("btnClearFields")?.addEventListener("click", () => {
        setChecked("fields", false);
        updateReportSummary();
    });

    document.querySelectorAll(".resource-type-btn").forEach(btn => {
        btn.addEventListener("click", function () {
            const type = btn.dataset.resourceType;
            activateResourceGroup(type);
            applyResourceSearch();
            updateReportSummary();
        });
    });

    const initiallyActiveButton = getActiveResourceTypeButton();
    if (initiallyActiveButton?.dataset.resourceType) {
        activateResourceGroup(initiallyActiveButton.dataset.resourceType);
    }

    document.getElementById("btnSelectVisibleResources")?.addEventListener("click", () => {
        const rows = getVisibleResourceRowsInActiveGroup();

        rows.forEach(row => {
            const checkbox = row.querySelector('input[name="resources"]');
            if (checkbox) {
                checkbox.checked = true;
            }
        });

        updateReportSummary();
    });

    document.getElementById("btnClearVisibleResources")?.addEventListener("click", () => {
        const rows = getVisibleResourceRowsInActiveGroup();

        rows.forEach(row => {
            const checkbox = row.querySelector('input[name="resources"]');
            if (checkbox) {
                checkbox.checked = false;
            }
        });

        updateReportSummary();
    });
    async function pollJob(jobId) {
        const statusUrl = statusUrlBase.replace("__JOB_ID__", encodeURIComponent(jobId));

        const interval = setInterval(async function () {
            try {
                const { resp, data } = await fetchJson(statusUrl);

                if (!resp.ok || !data.ok) {
                    clearInterval(interval);
                    setStatusBox(getReadableError(data, "Erro ao consultar status da extração."), "error");
                    if (btnStart) btnStart.disabled = false;
                    return;
                }

                const job = data.job || {};
                const rows = job.rows_so_far || 0;
                const phase = job.phase || "Processando";

                setStatusBox(`${phase} | Linhas até agora: ${rows}`, "info");

                if (job.status === "completed") {
                    clearInterval(interval);
                    setStatusBox(`Extração concluída. Linhas extraídas: ${job.total_rows || 0}.`, "info");
                    if (btnStart) btnStart.disabled = false;
                    showPersistentExtractionToast(job);
                    return;
                }

                if (job.status === "failed") {
                    clearInterval(interval);
                    setStatusBox(job.error || "Falha na extração.", "error");
                    if (btnStart) btnStart.disabled = false;
                    showToast("error", "Erro", job.error || "Falha na extração.");
                }
            } catch (err) {
                console.error(err);
                clearInterval(interval);
                setStatusBox("Falha de rede ao consultar status da extração.", "error");
                if (btnStart) btnStart.disabled = false;
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
            const activeResourceGroup = getVisibleResourceGroup();
            const resources = checkedValuesInScope("resources", activeResourceGroup);
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
                const activeTypeBtn = getActiveResourceTypeButton();
                const activeTypeLabel = activeTypeBtn?.textContent?.trim() || "aba ativa";
                showToast("warning", "Atenção", `Selecione pelo menos um recurso na aba ${activeTypeLabel}.`);
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

            if (btnStart) btnStart.disabled = true;
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
                    if (btnStart) btnStart.disabled = false;
                    const errorMessage = getReadableError(data, "Erro ao iniciar extração.");
                    setStatusBox(errorMessage, "error");
                    showToast("error", "Erro", errorMessage);
                    return;
                }

                showToast("success", "Extração iniciada", "O relatório está sendo processado em segundo plano.");
                pollJob(data.jobId);
            } catch (err) {
                console.error(err);
                if (btnStart) btnStart.disabled = false;
                setStatusBox("Falha de rede ao iniciar extração.", "error");
                showToast("error", "Erro", "Falha de rede ao iniciar extração.");
            }
        });
    }

    function initResourceSync() {
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

                        const result = job.result || {};
                        const activeTotal = result.active_total || 0;
                        const inactiveTotal = result.inactive_total || 0;

                        finishResourceProgress(
                            `Concluído. Recursos carregados: ${insertedTotal}. Ativos: ${activeTotal}. Inativos: ${inactiveTotal}.`
                        );

                        showToast(
                            "success",
                            "Lista de recursos atualizada",
                            `Recursos carregados: ${insertedTotal}. Ativos: ${activeTotal}. Inativos: ${inactiveTotal}. Recarregue esta página para ver a lista atualizada.`
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
                    "A lista de recursos continuará sendo atualizada em segundo plano mesmo se você sair desta tela.",
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
    }

    function initTaskTypeSync() {
        const btnSyncTaskTypes = document.getElementById("btnSyncTaskTypes");
        const taskTypeSyncProgress = document.getElementById("taskTypeSyncProgress");
        const taskTypeSyncText = document.getElementById("taskTypeSyncText");
        const taskTypeSyncPercent = document.getElementById("taskTypeSyncPercent");
        const taskTypeSyncBarFill = document.getElementById("taskTypeSyncBarFill");

        if (!btnSyncTaskTypes || !taskTypeSyncUrl) {
            return;
        }

        function setTaskTypeSyncProgress(percent, text, mode = "running") {
            const safePercent = Math.max(0, Math.min(100, Number(percent) || 0));

            if (taskTypeSyncProgress) {
                taskTypeSyncProgress.style.display = "block";
            }

            if (taskTypeSyncText) {
                taskTypeSyncText.textContent = text || "";
            }

            if (taskTypeSyncPercent) {
                taskTypeSyncPercent.textContent = `${safePercent}%`;
            }

            if (taskTypeSyncBarFill) {
                taskTypeSyncBarFill.style.width = `${safePercent}%`;

                if (mode === "error") {
                    taskTypeSyncBarFill.style.background = "#dc3545";
                } else if (mode === "success") {
                    taskTypeSyncBarFill.style.background = "#198754";
                } else {
                    taskTypeSyncBarFill.style.background = "#0d6efd";
                }
            }
        }

        btnSyncTaskTypes.addEventListener("click", async function () {
            btnSyncTaskTypes.disabled = true;
            setTaskTypeSyncProgress(10, "Iniciando atualização dos tipos de tarefa...");

            try {
                setTaskTypeSyncProgress(35, "Consultando propriedade XA_TSK_TYP no OFS...");

                const { resp, data } = await fetchJson(taskTypeSyncUrl, {
                    method: "POST",
                    headers: {
                        "Accept": "application/json",
                    },
                });

                if (!resp.ok || !data.ok) {
                    throw new Error(getReadableError(data, "Falha ao atualizar tipos de tarefa."));
                }

                setTaskTypeSyncProgress(
                    100,
                    `Tipos de tarefa atualizados com sucesso. Itens processados: ${data.total_items || 0}.`,
                    "success"
                );

                showToast(
                    "success",
                    "Tipos de tarefa atualizados",
                    `Itens processados: ${data.total_items || 0}.`
                );

            } catch (error) {
                setTaskTypeSyncProgress(
                    100,
                    error.message || "Erro ao atualizar tipos de tarefa.",
                    "error"
                );

                showToast(
                    "error",
                    "Erro",
                    error.message || "Erro ao atualizar tipos de tarefa."
                );

            } finally {
                btnSyncTaskTypes.disabled = false;
            }
        });
    }
    if (form) {
        form.addEventListener("change", updateReportSummary);
        form.addEventListener("input", updateReportSummary);
    }
    document.querySelectorAll(".resource-status-filter-btn").forEach(btn => {
        btn.addEventListener("click", function () {
            document.querySelectorAll(".resource-status-filter-btn").forEach(item => {
                item.classList.toggle("active", item === btn);
            });

            applyResourceSearch();
            updateReportSummary();
        });
    });
    document.getElementById("resourceSearchInput")?.addEventListener("input", function () {
        applyResourceSearch();
    });

    document.getElementById("btnClearResourceSearch")?.addEventListener("click", function () {
        const input = document.getElementById("resourceSearchInput");

        if (input) {
            input.value = "";
            input.focus();
        }

        applyResourceSearch();
    });
    setDefaultDates();
    applyResourceSearch();
    updateReportSummary();
    initResourceSync();
    initTaskTypeSync();
});

