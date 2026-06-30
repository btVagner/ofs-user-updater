document.addEventListener("DOMContentLoaded", function () {
    const root = document.querySelector(".cleanup-page");
    if (!root) return;

    const simulateUrl = root.dataset.simulateUrl;
    const unlockUrl = root.dataset.unlockUrl;
    const lockUrl = root.dataset.lockUrl;

    const pageSessionId = createPageSessionId();

    let applyUnlocked = false;
    let unlockToken = null;

    const form = document.querySelector("[data-cleanup-form]");
    const simulateButton = document.querySelector("[data-cleanup-simulate-btn]");

    const statusText = document.querySelector("[data-cleanup-status-text]");
    const progressBox = document.querySelector("[data-cleanup-progress]");
    const phaseEl = document.querySelector("[data-cleanup-phase]");
    const percentEl = document.querySelector("[data-cleanup-percent]");
    const progressBar = document.querySelector("[data-cleanup-progress-bar]");

    const kpisBox = document.querySelector("[data-cleanup-kpis]");
    const totalScannedEl = document.querySelector("[data-cleanup-total-scanned]");
    const candidatesEl = document.querySelector("[data-cleanup-candidates]");
    const ignoredLoginEl = document.querySelector("[data-cleanup-ignored-login]");
    const ignoredInactiveEl = document.querySelector("[data-cleanup-ignored-inactive]");

    const actionsBox = document.querySelector("[data-cleanup-actions]");
    const downloadLink = document.querySelector("[data-cleanup-download]");
    const openApplyButton = document.querySelector("[data-cleanup-open-apply]");

    const applyPanel = document.querySelector("[data-cleanup-apply-panel]");
    const applyForm = document.querySelector("[data-cleanup-apply-form]");
    const cancelApplyButton = document.querySelector("[data-cleanup-cancel-apply]");

    const previewPanel = document.querySelector("[data-cleanup-preview-panel]");
    const previewTitle = document.querySelector("[data-cleanup-preview-title]");
    const previewSubtitle = document.querySelector("[data-cleanup-preview-subtitle]");
    const tableHead = document.querySelector("[data-cleanup-table-head]");
    const tableBody = document.querySelector("[data-cleanup-table-body]");

    const openUnlockButton = document.querySelector("[data-cleanup-open-unlock]");
    const unlockModal = document.querySelector("[data-cleanup-unlock-modal]");
    const closeUnlockButtons = document.querySelectorAll("[data-cleanup-close-unlock]");
    const unlockForm = document.querySelector("[data-cleanup-unlock-form]");
    const lockButton = document.querySelector("[data-cleanup-lock-btn]");
    const lockStatus = document.querySelector("[data-cleanup-lock-status]");
    const lockMessage = document.querySelector("[data-cleanup-lock-message]");
    const applyNote = document.querySelector("[data-cleanup-apply-note]");
    const unlockTokenInputs = document.querySelectorAll("[data-cleanup-unlock-token]");
    const pageSessionInputs = document.querySelectorAll("[data-cleanup-page-session]");

    const confirmModal = document.querySelector("[data-cleanup-confirm-modal]");
    const confirmApplyButton = document.querySelector("[data-cleanup-confirm-apply]");
    const cancelConfirmButton = document.querySelector("[data-cleanup-cancel-confirm]");

    let currentSimulationJobId = null;
    let currentSimulationStatusUrl = null;
    let currentSimulationDownloadUrl = null;
    let currentApplyUrl = null;
    let currentApplicationDownloadUrl = null;
    let pollingTimer = null;

    pageSessionInputs.forEach(function (input) {
        input.value = pageSessionId;
    });

    lockScreen();

    if (form && simulateUrl) {
        form.addEventListener("submit", async function (event) {
            event.preventDefault();

            clearPolling();
            resetApplyPanel();
            resetPreview();

            currentSimulationJobId = null;
            currentSimulationStatusUrl = null;
            currentSimulationDownloadUrl = null;
            currentApplyUrl = null;
            currentApplicationDownloadUrl = null;

            if (actionsBox) {
                actionsBox.classList.add("hidden");
            }

            if (openApplyButton) {
                openApplyButton.disabled = true;
            }

            const formData = new FormData(form);

            setButtonLoading(simulateButton, true, "Simulando...");
            setStatus("Iniciando simulação...", 0, "Simulação na fila");
            showProgress();

            try {
                const response = await fetch(simulateUrl, {
                    method: "POST",
                    body: formData,
                    headers: {
                        "Accept": "application/json"
                    }
                });

                const data = await response.json().catch(function () {
                    return {};
                });

                if (!response.ok || data.ok === false) {
                    throw new Error(
                        data.error ||
                        data.message ||
                        `Falha ao iniciar simulação. Status HTTP: ${response.status}`
                    );
                }

                currentSimulationJobId = data.job_id;
                currentSimulationStatusUrl = data.status_url;
                currentSimulationDownloadUrl = data.download_url;
                currentApplyUrl = buildApplyUrl(data.job_id);

                pollStatus(currentSimulationStatusUrl, "simulation");

                pollingTimer = window.setInterval(function () {
                    pollStatus(currentSimulationStatusUrl, "simulation");
                }, 2500);

            } catch (error) {
                setButtonLoading(simulateButton, false);
                setStatus(error.message || "Falha ao iniciar simulação.", 0, "Erro");
                markProgressError();
            }
        });
    }

    if (openUnlockButton) {
        openUnlockButton.addEventListener("click", function () {
            openUnlockModal();
        });
    }

    closeUnlockButtons.forEach(function (button) {
        button.addEventListener("click", function () {
            closeUnlockModal();
        });
    });

    if (unlockModal) {
        unlockModal.addEventListener("click", function (event) {
            if (event.target === unlockModal) {
                closeUnlockModal();
            }
        });
    }

    if (unlockForm && unlockUrl) {
        unlockForm.addEventListener("submit", async function (event) {
            event.preventDefault();

            const formData = new FormData(unlockForm);
            formData.append("page_session_id", pageSessionId);

            const button = unlockForm.querySelector("button[type='submit']");

            setButtonLoading(button, true, "Validando...");

            try {
                const response = await fetch(unlockUrl, {
                    method: "POST",
                    body: formData,
                    headers: {
                        "Accept": "application/json"
                    }
                });

                const data = await response.json().catch(function () {
                    return {};
                });

                if (!response.ok || data.ok === false) {
                    console.error("Erro ao desbloquear modo seguro:", {
                        status: response.status,
                        data: data
                    });

                    throw new Error(
                        data.error ||
                        data.message ||
                        `Falha ao desbloquear. Status HTTP: ${response.status}`
                    );
                }

                applyUnlocked = true;
                unlockToken = data.unlock_token || null;

                unlockTokenInputs.forEach(function (input) {
                    input.value = unlockToken || "";
                });

                updateLockState();
                unlockForm.reset();
                closeUnlockModal();

            } catch (error) {
                alert(error.message || "Senha inválida.");
            } finally {
                setButtonLoading(button, false);
            }
        });
    }

    if (lockButton) {
        lockButton.addEventListener("click", function () {
            revokeUnlockToken(false);
            lockScreen();
        });
    }

    if (openApplyButton) {
        openApplyButton.addEventListener("click", function () {
            if (!currentSimulationJobId) {
                alert("Execute uma simulação antes de aplicar.");
                return;
            }

            if (!applyUnlocked || !unlockToken) {
                alert("Desbloqueie o modo seguro com sua senha antes de aplicar.");
                return;
            }

            if (applyPanel) {
                applyPanel.classList.remove("hidden");
                applyPanel.scrollIntoView({ behavior: "smooth", block: "start" });
            }
        });
    }

    if (cancelApplyButton) {
        cancelApplyButton.addEventListener("click", resetApplyPanel);
    }

    if (applyForm) {
        applyForm.addEventListener("submit", function (event) {
            event.preventDefault();

            if (!currentApplyUrl) {
                alert("Execute uma simulação antes de aplicar.");
                return;
            }

            if (!applyUnlocked || !unlockToken) {
                alert("Desbloqueie o modo seguro com sua senha antes de aplicar.");
                return;
            }

            openConfirmModal();
        });
    }

    if (confirmApplyButton) {
        confirmApplyButton.addEventListener("click", function () {
            closeConfirmModal();
            executeApply();
        });
    }

    if (cancelConfirmButton) {
        cancelConfirmButton.addEventListener("click", function () {
            closeConfirmModal();
        });
    }

    if (confirmModal) {
        confirmModal.addEventListener("click", function (event) {
            if (event.target === confirmModal) {
                closeConfirmModal();
            }
        });
    }

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape") {
            closeUnlockModal();
            closeConfirmModal();
        }
    });


    async function pollStatus(statusUrl, expectedType) {
        if (!statusUrl) return;

        try {
            const urlWithNoCache = `${statusUrl}${statusUrl.includes("?") ? "&" : "?"}_=${Date.now()}`;

            const response = await fetch(urlWithNoCache, {
                method: "GET",
                cache: "no-store",
                headers: {
                    "Accept": "application/json",
                    "Cache-Control": "no-cache"
                }
            });

            const data = await response.json().catch(function () {
                return {};
            });

            if (!response.ok || data.ok === false) {
                throw new Error(
                    data.error ||
                    data.message ||
                    `Falha ao consultar status. Status HTTP: ${response.status}`
                );
            }

            updateStatus(data);

            if (data.status === "success") {
                clearPolling();
                setButtonLoading(simulateButton, false);

                if (expectedType === "application" || data.job_type === "application") {
                    renderResults(data);

                    if (downloadLink && currentApplicationDownloadUrl) {
                        downloadLink.href = currentApplicationDownloadUrl;
                    }

                    if (actionsBox) {
                        actionsBox.classList.remove("hidden");
                    }

                    if (openApplyButton) {
                        openApplyButton.disabled = true;
                    }

                    return;
                }

                renderCandidates(data);
                showSimulationActions();
                return;
            }

            if (data.status === "error") {
                clearPolling();
                setButtonLoading(simulateButton, false);
                markProgressError();
                setStatus(
                    data.error || "Erro no processamento.",
                    data.progress_percent || 0,
                    data.phase || "Erro"
                );
            }

        } catch (error) {
            clearPolling();
            setButtonLoading(simulateButton, false);
            markProgressError();
            setStatus(error.message || "Falha ao consultar status.", 0, "Erro");
        }
    }

    async function executeApply() {
        if (!currentApplyUrl) {
            alert("Execute uma simulação antes de aplicar.");
            return;
        }

        if (!applyUnlocked || !unlockToken) {
            alert("Desbloqueie o modo seguro com sua senha antes de aplicar.");
            return;
        }

        if (!unlockToken) {
            setStatus("Desbloqueio ausente. Informe sua senha novamente.", 0, "Erro");
            markProgressError();
            lockScreen();
            return;
        }

        clearPolling();
        const formData = new FormData(applyForm);
        formData.set("unlock_token", unlockToken);
        formData.set("page_session_id", pageSessionId);

        setStatus("Iniciando aplicação...", 0, "Aplicação na fila");
        showProgress();

        if (actionsBox) {
            actionsBox.classList.add("hidden");
        }

        resetPreview();

        if (confirmApplyButton) {
            setButtonLoading(confirmApplyButton, true, "Aplicando...");
        }

        try {
            const response = await fetch(currentApplyUrl, {
                method: "POST",
                body: formData,
                headers: {
                    "Accept": "application/json"
                }
            });

            const data = await response.json().catch(function () {
                return {};
            });

            if (!response.ok || data.ok === false) {
                throw new Error(
                    data.error ||
                    data.message ||
                    `Falha ao iniciar aplicação. Status HTTP: ${response.status}`
                );
            }

            resetApplyPanel();

            currentApplicationDownloadUrl = data.download_url || null;

            pollStatus(data.status_url, "application");

            pollingTimer = window.setInterval(function () {
                pollStatus(data.status_url, "application");
            }, 2500);

            if (downloadLink && currentApplicationDownloadUrl) {
                downloadLink.href = currentApplicationDownloadUrl;
            }

        } catch (error) {
            setStatus(error.message || "Falha ao iniciar aplicação.", 0, "Erro");
            markProgressError();
        } finally {
            if (confirmApplyButton) {
                setButtonLoading(confirmApplyButton, false);
            }
        }
    }

    function updateStatus(data) {
        const percent = normalizePercent(data.progress_percent);

        setStatus(
            data.phase || "Processando...",
            percent,
            data.phase || "Processando..."
        );

        if (totalScannedEl) {
            totalScannedEl.textContent = data.total_scanned || data.total || 0;
        }

        if (candidatesEl) {
            candidatesEl.textContent = data.candidates || data.results_total || data.processed || 0;
        }

        if (ignoredLoginEl) {
            ignoredLoginEl.textContent = data.ignored_without_login || 0;
        }

        if (ignoredInactiveEl) {
            ignoredInactiveEl.textContent = data.ignored_inactive || 0;
        }

        if (kpisBox) {
            kpisBox.classList.remove("hidden");
        }
    }

    function renderCandidates(data) {
        const rows = Array.isArray(data.candidates_preview) ? data.candidates_preview : [];
        const total = Number(data.candidates_total || data.candidates || rows.length || 0);

        if (previewTitle) {
            previewTitle.textContent = "Prévia — usuários candidatos";
        }

        if (previewSubtitle) {
            previewSubtitle.textContent = `Exibindo ${rows.length} de ${total} usuário(s). Use o CSV para validar a lista completa.`;
        }

        if (tableHead) {
            tableHead.innerHTML = `
                <tr>
                    <th>Login</th>
                    <th>Nome</th>
                    <th>Status</th>
                    <th>Último login</th>
                    <th>UserType</th>
                    <th>Recurso</th>
                </tr>
            `;
        }

        if (!rows.length) {
            renderEmptyTable(6, "Nenhum usuário atende ao critério.");
            return;
        }

        tableBody.innerHTML = rows.map(function (row) {
            return `
                <tr>
                    <td>${escapeHtml(row.login || "-")}</td>
                    <td>${escapeHtml(row.name || "-")}</td>
                    <td>${escapeHtml(row.status || "-")}</td>
                    <td>${escapeHtml(row.lastLoginTime || "-")}</td>
                    <td>${escapeHtml(row.userType || "-")}</td>
                    <td>${escapeHtml(row.mainResourceId || "-")}</td>
                </tr>
            `;
        }).join("");

        if (previewPanel) {
            previewPanel.classList.remove("hidden");
            previewPanel.scrollIntoView({ behavior: "smooth", block: "start" });
        }
    }

    function renderResults(data) {
        const rows = Array.isArray(data.results_preview) ? data.results_preview : [];
        const total = Number(data.results_total || data.total || rows.length || 0);

        if (previewTitle) {
            previewTitle.textContent = "Resultado da aplicação";
        }

        if (previewSubtitle) {
            previewSubtitle.textContent = `Exibindo ${rows.length} de ${total} resultado(s). Use o CSV para baixar a lista completa.`;
        }

        if (tableHead) {
            tableHead.innerHTML = `
                <tr>
                    <th>Login</th>
                    <th>Nome</th>
                    <th>Último login</th>
                    <th>UserType</th>
                    <th>Recurso</th>
                    <th>Delete usuário</th>
                    <th>Inativar recurso</th>
                </tr>
            `;
        }

        if (!rows.length) {
            renderEmptyTable(7, "Nenhum resultado retornado.");
            return;
        }

        tableBody.innerHTML = rows.map(function (row) {
            return `
                <tr>
                    <td>${escapeHtml(row.login || "-")}</td>
                    <td>${escapeHtml(row.name || "-")}</td>
                    <td>${escapeHtml(row.lastLoginTime || "-")}</td>
                    <td>${escapeHtml(row.userType || "-")}</td>
                    <td>${escapeHtml(row.mainResourceId || "-")}</td>
                    <td>${escapeHtml(row.delete_user || "-")}</td>
                    <td>${escapeHtml(row.inactivate_resource || "-")}</td>
                </tr>
            `;
        }).join("");

        if (previewPanel) {
            previewPanel.classList.remove("hidden");
            previewPanel.scrollIntoView({ behavior: "smooth", block: "start" });
        }
    }

    function showSimulationActions() {
        if (!actionsBox) return;

        actionsBox.classList.remove("hidden");

        if (downloadLink && currentSimulationDownloadUrl) {
            downloadLink.href = currentSimulationDownloadUrl;
        }

        if (openApplyButton) {
            openApplyButton.disabled = !applyUnlocked;
        }

        updateLockState();
    }

    function updateLockState() {
        if (applyUnlocked) {
            if (lockStatus) {
                lockStatus.textContent = "Modo desbloqueado";
                lockStatus.classList.remove("running");
            }

            if (lockMessage) {
                lockMessage.textContent = "Aplicação liberada temporariamente para esta tela.";
            }

            if (openUnlockButton) {
                openUnlockButton.classList.add("hidden");
            }

            if (lockButton) {
                lockButton.classList.remove("hidden");
            }

            if (openApplyButton) {
                openApplyButton.disabled = !currentSimulationJobId;
            }

            if (applyNote) {
                applyNote.textContent = currentSimulationJobId
                    ? "Modo seguro desbloqueado. Você já pode aplicar a simulação concluída."
                    : "Modo seguro desbloqueado. Execute uma simulação para liberar a aplicação.";
            }

            return;
        }

        lockScreen();
    }

    function lockScreen() {
        applyUnlocked = false;
        unlockToken = null;

        unlockTokenInputs.forEach(function (input) {
            input.value = "";
        });

        if (lockStatus) {
            lockStatus.textContent = "Modo seguro";
            lockStatus.classList.add("running");
        }

        if (lockMessage) {
            lockMessage.textContent = "Aplicação bloqueada.";
        }

        if (openUnlockButton) {
            openUnlockButton.classList.remove("hidden");
        }

        if (lockButton) {
            lockButton.classList.add("hidden");
        }

        if (openApplyButton) {
            openApplyButton.disabled = true;
        }

        if (applyNote) {
            applyNote.textContent = "Para aplicar, primeiro simule a lista e desbloqueie o modo seguro com sua senha.";
        }

        resetApplyPanel();
    }

    function resetPreview() {
        if (previewPanel) {
            previewPanel.classList.add("hidden");
        }

        if (tableHead) {
            tableHead.innerHTML = `
                <tr>
                    <th>Login</th>
                    <th>Nome</th>
                    <th>Status</th>
                    <th>Último login</th>
                    <th>UserType</th>
                    <th>Recurso</th>
                </tr>
            `;
        }

        if (tableBody) {
            tableBody.innerHTML = `<tr><td colspan="6">Nenhum dado carregado.</td></tr>`;
        }
    }

    function renderEmptyTable(colspan, message) {
        if (!tableBody) return;

        tableBody.innerHTML = `<tr><td colspan="${colspan}">${escapeHtml(message)}</td></tr>`;

        if (previewPanel) {
            previewPanel.classList.remove("hidden");
        }
    }

    function resetApplyPanel() {
        if (applyPanel) {
            applyPanel.classList.add("hidden");
        }

        if (applyForm) {
            applyForm.reset();

            pageSessionInputs.forEach(function (input) {
                input.value = pageSessionId;
            });

            unlockTokenInputs.forEach(function (input) {
                input.value = unlockToken || "";
            });
        }
    }

    function openUnlockModal() {
        if (!unlockModal) return;

        unlockModal.classList.add("is-open");
        unlockModal.setAttribute("aria-hidden", "false");

        const passwordInput = unlockModal.querySelector("input[type='password']");

        if (passwordInput) {
            window.setTimeout(function () {
                passwordInput.focus();
            }, 80);
        }
    }

    function closeUnlockModal() {
        if (!unlockModal) return;

        unlockModal.classList.remove("is-open");
        unlockModal.setAttribute("aria-hidden", "true");
    }

    function openConfirmModal() {
        if (!confirmModal) {
            executeApply();
            return;
        }

        confirmModal.classList.add("is-open");
        confirmModal.setAttribute("aria-hidden", "false");

        if (confirmApplyButton) {
            window.setTimeout(function () {
                confirmApplyButton.focus();
            }, 80);
        }
    }

    function closeConfirmModal() {
        if (!confirmModal) return;

        confirmModal.classList.remove("is-open");
        confirmModal.setAttribute("aria-hidden", "true");
    }

    function revokeUnlockToken(useBeacon) {
        if (!unlockToken || !lockUrl) return;

        const tokenToRevoke = unlockToken;

        const formData = new FormData();
        formData.append("unlock_token", tokenToRevoke);

        if (useBeacon && navigator.sendBeacon) {
            navigator.sendBeacon(lockUrl, formData);
            return;
        }

        fetch(lockUrl, {
            method: "POST",
            body: formData,
            headers: {
                "Accept": "application/json"
            }
        }).catch(function () { });
    }

    function buildApplyUrl(jobId) {
        const base = simulateUrl.replace(/\/simular$/, "");
        return `${base}/aplicar/${jobId}`;
    }

    function setStatus(text, percent, phase) {
        const normalized = normalizePercent(percent);

        if (statusText) {
            statusText.textContent = text || "";
        }

        if (phaseEl) {
            phaseEl.textContent = phase || text || "";
        }

        if (percentEl) {
            percentEl.textContent = `${normalized}%`;
        }

        if (progressBar) {
            progressBar.style.width = `${normalized}%`;
        }
    }

    function showProgress() {
        if (progressBox) {
            progressBox.classList.remove("hidden", "error");
        }
    }

    function markProgressError() {
        if (progressBox) {
            progressBox.classList.add("error");
        }
    }

    function clearPolling() {
        if (pollingTimer) {
            window.clearInterval(pollingTimer);
            pollingTimer = null;
        }
    }

    function normalizePercent(value) {
        const number = Number(value || 0);
        return Math.max(0, Math.min(100, Math.round(number)));
    }

    function setButtonLoading(button, loading, text) {
        if (!button) return;

        if (loading) {
            if (!button.dataset.originalText) {
                button.dataset.originalText = button.textContent;
            }

            button.textContent = text || "Processando...";
            button.disabled = true;
            return;
        }

        button.disabled = false;
        button.textContent = button.dataset.originalText || button.textContent;
    }

    function createPageSessionId() {
        if (window.crypto && typeof window.crypto.randomUUID === "function") {
            return window.crypto.randomUUID();
        }

        return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }
});