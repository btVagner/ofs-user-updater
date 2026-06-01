// static/js/consultar_usuarios_export.js
"use strict";

document.addEventListener("DOMContentLoaded", function () {
    const body = document.body;

    const startUrl = body.dataset.usersExportStartUrl;

    const btnStart = document.getElementById("btnStartUsersExport");
    const progressBox = document.getElementById("usersExportProgressBox");
    const phaseEl = document.getElementById("usersExportPhase");
    const percentEl = document.getElementById("usersExportPercent");
    const barEl = document.getElementById("usersExportBar");
    const detailsEl = document.getElementById("usersExportDetails");
    const downloadBox = document.getElementById("usersExportDownloadBox");
    const downloadLink = document.getElementById("usersExportDownloadLink");
    const errorBox = document.getElementById("usersExportErrorBox");

    let pollingTimer = null;

    function setProgress(percent) {
        const safePercent = Math.max(0, Math.min(100, percent || 0));
        barEl.style.width = safePercent + "%";
        percentEl.textContent = safePercent + "%";
    }

    function showError(message) {
        errorBox.style.display = "block";
        errorBox.textContent = message || "Erro ao gerar exportação.";
        btnStart.disabled = false;
        btnStart.textContent = "Gerar exportação CSV";
    }

    function stopPolling() {
        if (pollingTimer) {
            clearInterval(pollingTimer);
            pollingTimer = null;
        }
    }

    async function fetchStatus(statusUrl, downloadUrl) {
        try {
            const response = await fetch(statusUrl, {
                method: "GET",
                headers: {
                    "Accept": "application/json"
                }
            });

            const data = await response.json();

            if (!response.ok || !data.ok) {
                throw new Error(data.error || "Falha ao consultar status.");
            }

            const status = data.status;
            const totalUsers = Number(data.total_users || 0);
            const processedUsers = Number(data.processed_users || 0);

            phaseEl.textContent = data.phase || "Processando exportação...";

            const progressPercent = Number(data.progress_percent || 0);

            if (progressPercent > 0) {
                setProgress(progressPercent);
            } else if (totalUsers > 0) {
                const percent = Math.round((processedUsers / totalUsers) * 100);
                setProgress(percent);
            } else {
                setProgress(status === "success" ? 100 : 5);
            }

            if (Number(data.processed_resources || 0) > 0 && processedUsers === 0) {
                detailsEl.textContent = `${data.processed_resources} recursos carregados.`;
            } else if (totalUsers > 0) {
                detailsEl.textContent = `${processedUsers} de ${totalUsers} usuários processados.`;
            } else {
                detailsEl.textContent = "Buscando usuários no OFS...";
            }

            if (status === "success") {
                stopPolling();
                setProgress(100);

                phaseEl.textContent = "Exportação concluída";
                detailsEl.textContent = "Arquivo CSV gerado com sucesso.";

                downloadLink.href = downloadUrl;
                downloadBox.style.display = "block";

                btnStart.disabled = false;
                btnStart.textContent = "Gerar nova exportação CSV";
            }

            if (status === "error") {
                stopPolling();
                showError(data.error || "Erro ao gerar exportação.");
            }

        } catch (error) {
            stopPolling();
            showError(error.message || "Erro inesperado ao consultar status.");
        }
    }

    async function startExport() {
        stopPolling();

        btnStart.disabled = true;
        btnStart.textContent = "Gerando...";

        progressBox.style.display = "block";
        downloadBox.style.display = "none";
        errorBox.style.display = "none";
        errorBox.textContent = "";

        phaseEl.textContent = "Iniciando exportação...";
        detailsEl.textContent = "Aguarde enquanto o processo é iniciado.";
        setProgress(0);

        try {
            const response = await fetch(startUrl, {
                method: "POST",
                headers: {
                    "Accept": "application/json"
                }
            });

            const data = await response.json();

            if (!response.ok || !data.ok) {
                throw new Error(data.error || "Falha ao iniciar exportação.");
            }

            phaseEl.textContent = "Exportação iniciada";
            detailsEl.textContent = "Consultando status do processamento...";
            setProgress(2);

            pollingTimer = setInterval(function () {
                fetchStatus(data.status_url, data.download_url);
            }, 1500);

            fetchStatus(data.status_url, data.download_url);

        } catch (error) {
            showError(error.message || "Erro inesperado ao iniciar exportação.");
        }
    }

    if (btnStart) {
        btnStart.addEventListener("click", startExport);
    }
});