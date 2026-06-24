document.addEventListener("DOMContentLoaded", function () {
    const body = document.body;

    const startUrl = body.dataset.thermometerStartUrl;
    const statusUrlBase = body.dataset.thermometerStatusUrlBase;
    const downloadUrlBase = body.dataset.thermometerDownloadUrlBase;
    const discardUrlBase = body.dataset.thermometerDiscardUrlBase;

    const form = document.getElementById("thermometerReportForm");
    const btnStart = document.getElementById("btnStartThermometerReport");
    const statusBox = document.getElementById("reportStatusBox");

    function todayStr() {
        const d = new Date();
        return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
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

        return { resp, data };
    }

    function getReadableError(data, fallbackMessage) {
        if (data && data.error) return data.error;
        if (data && data.detail) return data.detail;
        return fallbackMessage || "Erro inesperado.";
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

    function setChecked(name, checked, scope) {
        const root = scope || document;
        root.querySelectorAll(`input[name="${name}"]`).forEach(el => {
            el.checked = checked;
        });
    }

    function selectedCountLabel(count) {
        const value = Number(count) || 0;
        return value === 1 ? "1 selecionado" : `${value} selecionados`;
    }

    function getActiveResourceTypeButton() {
        return document.querySelector(".resource-type-btn.active");
    }

    function getVisibleResourceGroup() {
        return document.querySelector(".resource-group:not(.hidden)");
    }

    function getActiveResourceTypeLabel() {
        const activeButton = getActiveResourceTypeButton();
        if (!activeButton) return "-";

        const clone = activeButton.cloneNode(true);
        const span = clone.querySelector("span");
        if (span) span.remove();

        return clone.textContent.trim() || "-";
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

            if (isActive) activeGroup = group;
        });

        if (activeGroup) {
            clearResourceSelectionsOutsideGroup(activeGroup);
        }
    }

    function normalizeSearch(value) {
        return String(value || "")
            .normalize("NFD")
            .replace(/[\u0300-\u036f]/g, "")
            .toLowerCase()
            .trim();
    }

    function getResourceStatusFilterValue() {
        const activeBtn = document.querySelector(".resource-status-filter-btn.active");
        return activeBtn?.dataset.resourceStatusFilter || "active";
    }

    function resourceMatchesStatusFilter(row) {
        const filterValue = getResourceStatusFilterValue();
        const rowStatus = String(row.dataset.resourceStatus || "unknown").toLowerCase();

        if (filterValue === "all") return true;
        if (filterValue === "active") return rowStatus === "active";
        if (filterValue === "inactive") return rowStatus !== "active";

        return true;
    }

    function getVisibleResourceRowsInActiveGroup() {
        const activeGroup = getVisibleResourceGroup();
        if (!activeGroup) return [];

        return Array.from(activeGroup.querySelectorAll(".resource-check"))
            .filter(row => row.style.display !== "none");
    }

    function updateResourceSearchCount() {
        const activeGroup = getVisibleResourceGroup();
        const countEl = document.getElementById("resourceSearchCount");

        if (!activeGroup || !countEl) return;

        const rows = Array.from(activeGroup.querySelectorAll(".resource-check"));
        const visible = getVisibleResourceRowsInActiveGroup().length;

        countEl.textContent = `${visible} de ${rows.length} recursos visíveis`;
    }

    function applyResourceSearch() {
        const activeGroup = getVisibleResourceGroup();
        const searchValue = normalizeSearch(document.getElementById("resourceSearchInput")?.value || "");

        if (!activeGroup) {
            updateResourceSearchCount();
            return;
        }

        activeGroup.querySelectorAll(".resource-check").forEach(row => {
            const name = normalizeSearch(row.querySelector(".resource-name")?.textContent || "");
            const id = normalizeSearch(row.querySelector(".resource-id")?.textContent || "");

            const matchesSearch = !searchValue || name.includes(searchValue) || id.includes(searchValue);
            const matchesStatus = resourceMatchesStatusFilter(row);

            row.style.display = matchesSearch && matchesStatus ? "" : "none";
        });

        updateResourceSearchCount();
        updateReportSummary();
    }

    function updateReportSummary() {
        const dateFrom = document.getElementById("dateFrom")?.value || "";
        const dateTo = document.getElementById("dateTo")?.value || "";
        const activeResourceGroup = getVisibleResourceGroup();

        const resources = checkedValuesInScope("resources", activeResourceGroup);
        const activityTypes = checkedValues("activityTypes");

        const summaryPeriod = document.getElementById("summaryPeriod");
        const summaryResourceGroup = document.getElementById("summaryResourceGroup");
        const summaryResources = document.getElementById("summaryResources");
        const summaryActivityTypes = document.getElementById("summaryActivityTypes");

        if (summaryPeriod) {
            summaryPeriod.textContent = dateFrom && dateTo ? `${dateFrom} até ${dateTo}` : "-";
        }

        if (summaryResourceGroup) {
            summaryResourceGroup.textContent = getActiveResourceTypeLabel();
        }

        if (summaryResources) {
            summaryResources.textContent = selectedCountLabel(resources.length);
        }

        if (summaryActivityTypes) {
            summaryActivityTypes.textContent = selectedCountLabel(activityTypes.length);
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

        if (durationMs === null) return;

        setTimeout(() => {
            toast.style.animation = "toastOut 0.28s ease forwards";
            setTimeout(() => toast.remove(), 280);
        }, durationMs);
    }

    function showPersistentExtractionToast(job) {
        const container = document.getElementById("toastContainer");
        if (!container || !job || !job.job_id) return;

        const downloadUrl = downloadUrlBase.replace("__JOB_ID__", encodeURIComponent(job.job_id));
        const discardUrl = discardUrlBase.replace("__JOB_ID__", encodeURIComponent(job.job_id));

        const toast = document.createElement("div");
        toast.className = "toast-msg success toast-persistent";

        toast.innerHTML = `
            <div class="toast-body">
                <div class="toast-title">Extração concluída</div>
                <div class="toast-text">
                    Relatório pronto para download.<br>
                    Linhas exportadas: <b>${escapeHtml(job.total_rows || 0)}</b>
                </div>
                <div class="toast-buttons">
                    <button type="button" class="toast-action download">Download XLSX</button>
                    <button type="button" class="toast-action discard">Descartar extração</button>
                </div>
            </div>
        `;

        toast.querySelector(".toast-action.download")?.addEventListener("click", function () {
            window.location.href = downloadUrl;
        });

        toast.querySelector(".toast-action.discard")?.addEventListener("click", async function () {
            const ok = confirm("Ao descartar esta extração, será necessário executar o relatório novamente. Deseja continuar?");
            if (!ok) return;

            const { resp, data } = await fetchJson(discardUrl, { method: "POST" });

            if (!resp.ok || !data.ok) {
                showToast("error", "Erro", getReadableError(data, "Erro ao descartar extração."));
                return;
            }

            toast.remove();
            showToast("success", "Sucesso", "Extração descartada.");
        });

        container.appendChild(toast);
    }

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
                setStatusBox(`${job.phase || "Processando"} | Linhas até agora: ${job.rows_so_far || 0}`, "info");

                if (job.status === "completed") {
                    clearInterval(interval);
                    setStatusBox(`Extração concluída. Linhas exportadas: ${job.total_rows || 0}.`, "info");
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
                clearInterval(interval);
                setStatusBox("Falha de rede ao consultar status da extração.", "error");
                if (btnStart) btnStart.disabled = false;
            }
        }, 2500);
    }

    document.querySelectorAll(".resource-type-btn").forEach(btn => {
        btn.addEventListener("click", function () {
            activateResourceGroup(btn.dataset.resourceType);
            applyResourceSearch();
            updateReportSummary();
        });
    });

    document.querySelectorAll(".resource-status-filter-btn").forEach(btn => {
        btn.addEventListener("click", function () {
            document.querySelectorAll(".resource-status-filter-btn").forEach(item => {
                item.classList.toggle("active", item === btn);
            });

            applyResourceSearch();
        });
    });

    document.getElementById("resourceSearchInput")?.addEventListener("input", applyResourceSearch);

    document.getElementById("btnClearResourceSearch")?.addEventListener("click", function () {
        const input = document.getElementById("resourceSearchInput");
        if (input) input.value = "";
        applyResourceSearch();
    });

    document.getElementById("btnSelectVisibleResources")?.addEventListener("click", function () {
        getVisibleResourceRowsInActiveGroup().forEach(row => {
            const checkbox = row.querySelector('input[name="resources"]');
            if (checkbox) checkbox.checked = true;
        });
        updateReportSummary();
    });

    document.getElementById("btnClearVisibleResources")?.addEventListener("click", function () {
        getVisibleResourceRowsInActiveGroup().forEach(row => {
            const checkbox = row.querySelector('input[name="resources"]');
            if (checkbox) checkbox.checked = false;
        });
        updateReportSummary();
    });

    document.getElementById("btnSelectAllActivityTypes")?.addEventListener("click", function () {
        setChecked("activityTypes", true);
        updateReportSummary();
    });

    document.getElementById("btnClearActivityTypes")?.addEventListener("click", function () {
        setChecked("activityTypes", false);
        updateReportSummary();
    });

    if (form) {
        form.addEventListener("change", updateReportSummary);
        form.addEventListener("input", updateReportSummary);

        form.addEventListener("submit", async function (event) {
            event.preventDefault();

            const dateFrom = document.getElementById("dateFrom")?.value || "";
            const dateTo = document.getElementById("dateTo")?.value || "";
            const activeResourceGroup = getVisibleResourceGroup();
            const resources = checkedValuesInScope("resources", activeResourceGroup);
            const activityTypes = checkedValues("activityTypes");

            if (!dateFrom || !dateTo) {
                showToast("warning", "Atenção", "Informe o período do relatório.");
                return;
            }

            if (!activityTypes.length) {
                showToast("warning", "Atenção", "Selecione pelo menos um tipo de atividade.");
                return;
            }

            if (!resources.length) {
                showToast("warning", "Atenção", "Selecione pelo menos um recurso.");
                return;
            }

            if (btnStart) btnStart.disabled = true;
            clearStatusBox();
            setStatusBox("Iniciando relatório do termômetro em segundo plano...", "info");

            const payload = {
                dateFrom,
                dateTo,
                resources,
                activityTypes,
            };

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
                    const errorMessage = getReadableError(data, "Erro ao iniciar relatório.");
                    setStatusBox(errorMessage, "error");
                    showToast("error", "Erro", errorMessage);
                    return;
                }

                showToast("success", "Extração iniciada", "O relatório está sendo processado em segundo plano.");
                pollJob(data.jobId);
            } catch (err) {
                if (btnStart) btnStart.disabled = false;
                setStatusBox("Falha de rede ao iniciar relatório.", "error");
                showToast("error", "Erro", "Falha de rede ao iniciar relatório.");
            }
        });
    }

    const firstResourceButton = getActiveResourceTypeButton();
    if (firstResourceButton?.dataset.resourceType) {
        activateResourceGroup(firstResourceButton.dataset.resourceType);
    }

    const dateFrom = document.getElementById("dateFrom");
    const dateTo = document.getElementById("dateTo");
    if (dateFrom && !dateFrom.value) dateFrom.value = todayStr();
    if (dateTo && !dateTo.value) dateTo.value = todayStr();

    applyResourceSearch();
    updateReportSummary();
});