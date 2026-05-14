document.addEventListener("DOMContentLoaded", function () {
    const body = document.body;

    const collectUrl = body.dataset.biCollectUrl;
    const jobsUrl = body.dataset.biJobsUrl;
    const summaryUrl = body.dataset.biSummaryUrl;
    const closeReasonsSyncUrl = body.dataset.biCloseReasonsSyncUrl;
    const form = document.getElementById("biCollectForm");
    const btnRunCollect = document.getElementById("btnRunCollect");
    const btnReloadJobs = document.getElementById("btnReloadJobs");
    const btnSyncCloseReasons = document.getElementById("btnSyncCloseReasons");
    const btnSelectAllActivityTypes = document.getElementById("btnSelectAllActivityTypes");
    const btnClearActivityTypes = document.getElementById("btnClearActivityTypes");
    const resultBox = document.getElementById("biResultBox");
    const jobsTbody = document.getElementById("biJobsTbody");
    const summaryCards = document.getElementById("biSummaryCards");
    function showResult(type, message) {
        resultBox.classList.remove("hidden", "success", "error");
        resultBox.classList.add(type);
        resultBox.textContent = message;
    }

    function setLoading(isLoading) {
        btnRunCollect.disabled = isLoading;
        btnReloadJobs.disabled = isLoading;

        if (btnSyncCloseReasons) {
            btnSyncCloseReasons.disabled = isLoading;
        }

        btnRunCollect.textContent = isLoading ? "Executando..." : "Executar coleta";
    }

    function getCheckedStatuses() {
        return Array.from(document.querySelectorAll('input[name="statuses"]:checked'))
            .map((input) => input.value);
    }

    function getCheckedActivityTypes() {
        return Array.from(document.querySelectorAll('input[name="activityTypes"]:checked'))
            .map((input) => input.value);
    }

    function setAllActivityTypes(checked) {
        document.querySelectorAll('input[name="activityTypes"]').forEach((input) => {
            input.checked = checked;
        });
    }

    function formatValue(value) {
        if (value === null || value === undefined || value === "") {
            return "-";
        }
        return String(value);
    }

    function statusBadge(status) {
        const safeStatus = formatValue(status);
        const normalized = safeStatus.toLowerCase();

        let cls = "";
        if (normalized === "success") cls = "success";
        else if (normalized === "error") cls = "error";
        else if (normalized === "running") cls = "running";

        return `<span class="bi-badge ${cls}">${safeStatus}</span>`;
    }

    function shortError(text) {
        if (!text) return "-";
        const str = String(text);
        if (str.length <= 280) return str;
        return str.slice(0, 280) + "...";
    }
    function renderSummaryCards(payload) {
        if (!summaryCards) return;

        if (!payload || !payload.has_data) {
            summaryCards.innerHTML = `
                <div class="bi-summary-card">
                    <span>Último job</span>
                    <strong>-</strong>
                    <small>Nenhum job encontrado</small>
                </div>
            `;
            return;
        }

        const job = payload.job || {};
        const summary = payload.summary || {};

        const closedWithoutReason = Number(summary.closed_without_close_reason || 0);
        const alertClass = closedWithoutReason > 0 ? "danger" : "ok";

        summaryCards.innerHTML = `
            <div class="bi-summary-card">
                <span>Último job</span>
                <strong>#${formatValue(job.id)}</strong>
                <small>${formatValue(job.date_from)} até ${formatValue(job.date_to)}</small>
            </div>

            <div class="bi-summary-card">
                <span>Status</span>
                <strong>${formatValue(job.status)}</strong>
                <small>${formatValue(job.finished_at)}</small>
            </div>

            <div class="bi-summary-card">
                <span>Total inserido</span>
                <strong>${formatValue(job.total_inserted)}</strong>
                <small>Buscados: ${formatValue(job.total_fetched)}</small>
            </div>

            <div class="bi-summary-card">
                <span>Completed</span>
                <strong>${formatValue(summary.total_completed)}</strong>
                <small>Atividades concluídas</small>
            </div>

            <div class="bi-summary-card">
                <span>Notdone</span>
                <strong>${formatValue(summary.total_notdone)}</strong>
                <small>Atividades não realizadas</small>
            </div>

            <div class="bi-summary-card">
                <span>Pending</span>
                <strong>${formatValue(summary.total_pending)}</strong>
                <small>Sem fechamento esperado</small>
            </div>

            <div class="bi-summary-card">
                <span>Started</span>
                <strong>${formatValue(summary.total_started)}</strong>
                <small>Sem fechamento esperado</small>
            </div>

            <div class="bi-summary-card ${alertClass}">
                <span>Fechadas sem motivo</span>
                <strong>${formatValue(summary.closed_without_close_reason)}</strong>
                <small>Completed/Notdone sem fechamento</small>
            </div>

            <div class="bi-summary-card ${Number(summary.resource_not_found || 0) > 0 ? "warning" : ""}">
                <span>Técnico não encontrado</span>
                <strong>${formatValue(summary.resource_not_found)}</strong>
                <small>Validação da base de recursos</small>
            </div>

            <div class="bi-summary-card ${Number(summary.empty_city || 0) > 0 ? "warning" : ""}">
                <span>Cidade vazia</span>
                <strong>${formatValue(summary.empty_city)}</strong>
                <small>Validação do campo city</small>
            </div>
        `;
    }

    async function loadSummary() {
        if (!summaryUrl) return;

        try {
            const response = await fetch(summaryUrl, {
                method: "GET",
                headers: {
                    "Accept": "application/json"
                }
            });

            const data = await response.json().catch(() => ({}));

            if (!response.ok || !data.ok) {
                throw new Error(data.error || data.detail || "Erro ao carregar resumo.");
            }

            renderSummaryCards(data.summary);

        } catch (error) {
            if (!summaryCards) return;

            summaryCards.innerHTML = `
                <div class="bi-summary-card danger">
                    <span>Resumo</span>
                    <strong>Erro</strong>
                    <small>${error.message}</small>
                </div>
            `;
        }
    }
    async function loadJobs() {
        jobsTbody.innerHTML = `
            <tr>
                <td colspan="8" class="bi-empty">Carregando jobs...</td>
            </tr>
        `;

        try {
            const response = await fetch(`${jobsUrl}?limit=5`, {
                method: "GET",
                headers: {
                    "Accept": "application/json"
                }
            });

            const data = await response.json().catch(() => ({}));

            if (!response.ok || !data.ok) {
                throw new Error(data.error || data.detail || "Erro ao carregar jobs.");
            }

            const jobs = data.jobs || [];

            if (!jobs.length) {
                jobsTbody.innerHTML = `
                    <tr>
                        <td colspan="8" class="bi-empty">Nenhum job encontrado.</td>
                    </tr>
                `;
                return;
            }

            jobsTbody.innerHTML = jobs.map((job) => {
                const periodo = `${formatValue(job.date_from)} até ${formatValue(job.date_to)}`;

                return `
                    <tr>
                        <td>${formatValue(job.id)}</td>
                        <td>${statusBadge(job.status)}</td>
                        <td>${periodo}</td>
                        <td>${formatValue(job.started_at)}</td>
                        <td>${formatValue(job.finished_at)}</td>
                        <td>${formatValue(job.total_fetched)}</td>
                        <td>${formatValue(job.total_inserted)}</td>
                        <td class="bi-error-preview">${shortError(job.error_text)}</td>
                    </tr>
                `;
            }).join("");

        } catch (error) {
            jobsTbody.innerHTML = `
                <tr>
                    <td colspan="8" class="bi-empty">Erro ao carregar jobs: ${error.message}</td>
                </tr>
            `;
        }
    }

    async function runCollection(event) {
        event.preventDefault();

        const dateFrom = document.getElementById("dateFrom").value;
        const dateTo = document.getElementById("dateTo").value;
        const resources = document.getElementById("resources").value.trim() || "02";
        const statuses = getCheckedStatuses();
        const activityTypes = getCheckedActivityTypes();

        if (!dateFrom || !dateTo) {
            showResult("error", "Informe data inicial e data final.");
            return;
        }

        if (!statuses.length) {
            showResult("error", "Selecione pelo menos um status.");
            return;
        }

        if (!activityTypes.length) {
            showResult("error", "Selecione pelo menos um tipo de atividade.");
            return;
        }

        setLoading(true);
        showResult("success", "Coleta iniciada. Aguarde a resposta da API OFS...");

        try {
            const response = await fetch(collectUrl, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                body: JSON.stringify({
                    dateFrom,
                    dateTo,
                    resources,
                    statuses,
                    activityTypes
                })
            });

            const data = await response.json().catch(() => ({}));

            if (!response.ok || !data.ok) {
                throw new Error(data.error || data.detail || "Erro ao executar coleta.");
            }

            showResult(
                "success",
                [
                    "Coleta finalizada com sucesso.",
                    `Job: ${data.job_id}`,
                    `Período: ${data.date_from} até ${data.date_to}`,
                    `Coletado em: ${data.collected_at}`,
                    `Total buscado: ${data.total_fetched}`,
                    `Total inserido: ${data.total_inserted}`,
                    `Tipos usados: ${(data.activity_types || []).join(", ")}`
                ].join("\n")
            );

            await loadJobs();
            await loadSummary();

        } catch (error) {
            showResult("error", error.message);
            await loadJobs();
            await loadSummary();

        } finally {
            setLoading(false);
        }
    }

    if (form) {
        form.addEventListener("submit", runCollection);
    }

    if (btnReloadJobs) {
        btnReloadJobs.addEventListener("click", loadJobs);
    }
    if (btnSyncCloseReasons) {
        btnSyncCloseReasons.addEventListener("click", syncCloseReasons);
    }
    if (btnSelectAllActivityTypes) {
        btnSelectAllActivityTypes.addEventListener("click", function () {
            setAllActivityTypes(true);
        });
    }

    if (btnClearActivityTypes) {
        btnClearActivityTypes.addEventListener("click", function () {
            setAllActivityTypes(false);
        });
    }
    async function syncCloseReasons() {
        if (!closeReasonsSyncUrl) {
            showResult("error", "URL de atualização dos motivos de fechamento não encontrada.");
            return;
        }

        setLoading(true);

        if (btnSyncCloseReasons) {
            btnSyncCloseReasons.textContent = "Atualizando motivos...";
        }

        showResult("success", "Atualizando motivos de fechamento pelo OFS Metadata. Aguarde...");

        try {
            const response = await fetch(closeReasonsSyncUrl, {
                method: "POST",
                headers: {
                    "Accept": "application/json"
                }
            });

            const data = await response.json().catch(() => ({}));

            if (!response.ok || !data.ok) {
                throw new Error(data.error || data.detail || "Erro ao atualizar motivos de fechamento.");
            }

            const details = (data.details || [])
                .map((item) => `${item.property_code}: ${item.items} itens`)
                .join("\n");

            showResult(
                "success",
                [
                    "Motivos de fechamento atualizados com sucesso.",
                    `Início: ${data.started_at}`,
                    `Fim: ${data.finished_at}`,
                    `Propriedades consultadas: ${data.total_properties}`,
                    `Itens processados: ${data.total_items}`,
                    "",
                    details
                ].join("\n")
            );

        } catch (error) {
            showResult("error", error.message);

        } finally {
            if (btnSyncCloseReasons) {
                btnSyncCloseReasons.textContent = "Atualizar motivos de fechamento";
            }

            setLoading(false);
        }
    }
    loadJobs();
    loadSummary();
});