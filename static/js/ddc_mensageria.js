(function () {
  const pageEl = document.getElementById("ddcMensageriaPage");

  const singleSendUrl = pageEl?.dataset.urlSingleSend || "";
  const massiveStartUrl = pageEl?.dataset.urlMassiveStart || "";
  const massiveStatusBaseUrl = pageEl?.dataset.urlMassiveStatusBase || "";

  const toggleButtons = document.querySelectorAll(".ddc-toggle-btn");

  const singleSendForm = document.getElementById("singleSendForm");
  const osIdInput = document.getElementById("osId");
  const singleEventInput = document.getElementById("singleEvent");
  const singleSendResult = document.getElementById("singleSendResult");
  const sendAnimation = document.getElementById("ddcSendAnimation");
  const singleSendButton = document.getElementById("singleSendButton");

  const massiveSendForm = document.getElementById("massiveSendForm");
  const massiveFileInput = document.getElementById("massiveFile");
  const massiveEventInput = document.getElementById("massiveEvent");
  const massiveValidateButton = document.getElementById("massiveValidateButton");
  const massiveStartButton = document.getElementById("massiveStartButton");

  const massiveTotalEl = document.getElementById("massiveTotal");
  const massiveProcessedEl = document.getElementById("massiveProcessed");
  const massiveSuccessEl = document.getElementById("massiveSuccess");
  const massiveErrorEl = document.getElementById("massiveError");
  const massivePercentLabel = document.getElementById("massivePercentLabel");
  const massiveProgressFill = document.getElementById("massiveProgressFill");
  const massiveProgressMeta = document.getElementById("massiveProgressMeta");
  const massiveLogList = document.getElementById("massiveLogList");
  const massiveLogCounter = document.getElementById("massiveLogCounter");

  let massiveIds = [];
  let massiveJobRunning = false;
  let massivePollTimer = null;
  let currentJobId = null;

  function buildMassiveStatusUrl(jobId) {
    return massiveStatusBaseUrl.replace("__JOB_ID__", encodeURIComponent(jobId));
  }

  function resetResultBox() {
    if (!singleSendResult) return;

    singleSendResult.classList.remove(
      "ddc-result-open",
      "ddc-result-success",
      "ddc-result-error"
    );

    setTimeout(() => {
      if (!singleSendResult.classList.contains("ddc-result-open")) {
        singleSendResult.className = "ddc-result-box ddc-hidden";
        singleSendResult.textContent = "";
      }
    }, 300);
  }

  function showResult(type, message) {
    if (!singleSendResult) return;

    singleSendResult.className = "ddc-result-box";
    singleSendResult.textContent = message;

    if (type === "success") {
      singleSendResult.classList.add("ddc-result-success");
    } else {
      singleSendResult.classList.add("ddc-result-error");
    }

    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        singleSendResult.classList.add("ddc-result-open");
      });
    });
  }

  function hideSendAnimation() {
    if (!sendAnimation) return;
    sendAnimation.className = "ddc-send-animation ddc-hidden";
  }

  function showSendAnimation() {
    if (!sendAnimation) return;

    sendAnimation.className = "ddc-send-animation";

    requestAnimationFrame(() => {
      sendAnimation.classList.add("ddc-send-visible");
    });

    setTimeout(() => {
      sendAnimation.classList.add("is-sending");
    }, 120);
  }

  function closeBox(box) {
    if (!box) return;

    box.classList.remove("ddc-open");

    setTimeout(() => {
      if (!box.classList.contains("ddc-open")) {
        box.classList.add("ddc-hidden");
      }
    }, 350);
  }

  function openBox(box) {
    if (!box) return;

    box.classList.remove("ddc-hidden");

    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        box.classList.add("ddc-open");
      });
    });
  }

  function closeAllBoxes(exceptBox = null) {
    document.querySelectorAll(".ddc-expand-box").forEach((box) => {
      if (box !== exceptBox) {
        closeBox(box);
      }
    });
  }

  function setMassiveButtonsState(isRunning) {
    massiveJobRunning = isRunning;

    if (massiveValidateButton) massiveValidateButton.disabled = isRunning;
    if (massiveFileInput) massiveFileInput.disabled = isRunning;
    if (massiveEventInput) massiveEventInput.disabled = isRunning;

    if (massiveStartButton) {
      massiveStartButton.disabled = isRunning || massiveIds.length === 0;
      massiveStartButton.textContent = isRunning ? "Enviando..." : "Iniciar envio";
    }
  }

  function parseIdsFromText(content) {
    return String(content || "")
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line.length > 0);
  }

  function resetMassiveLogs() {
    if (!massiveLogList) return;

    massiveLogList.innerHTML = `
      <div class="massive-log-empty" id="massiveLogEmpty">
        Nenhum envio iniciado ainda.
      </div>
    `;

    if (massiveLogCounter) {
      massiveLogCounter.textContent = "0 registro(s)";
    }
  }

  function renderMassiveLogs(logs) {
    if (!massiveLogList) return;

    if (!logs || logs.length === 0) {
      resetMassiveLogs();
      return;
    }

    massiveLogList.innerHTML = "";

    logs.forEach((log) => {
      const item = document.createElement("div");
      item.className = "massive-log-item";

      const time = document.createElement("div");
      time.className = "massive-log-time";
      time.textContent = log.timestamp || "--:--:--";

      const text = document.createElement("div");
      text.className = "massive-log-message";
      text.textContent = log.message || "";

      const statusWrap = document.createElement("div");
      statusWrap.className = "massive-log-status";

      const badge = document.createElement("span");
      badge.className = "badge";

      if (log.status === "success") {
        badge.classList.add("badge-ok");
        badge.textContent = "Sucesso";
      } else {
        badge.classList.add("badge-error");
        badge.textContent = "Erro";
      }

      statusWrap.appendChild(badge);

      item.appendChild(time);
      item.appendChild(text);
      item.appendChild(statusWrap);

      massiveLogList.appendChild(item);
    });

    if (massiveLogCounter) {
      massiveLogCounter.textContent = `${logs.length} registro(s)`;
    }
  }

  function updateMassiveSummaryFromApi(data) {
    const total = Number(data.total || 0);
    const processed = Number(data.processed || 0);
    const success = Number(data.success_count || 0);
    const error = Number(data.error_count || 0);
    const percent = Number(data.percent || 0);

    if (massiveTotalEl) massiveTotalEl.textContent = String(total);
    if (massiveProcessedEl) massiveProcessedEl.textContent = String(processed);
    if (massiveSuccessEl) massiveSuccessEl.textContent = String(success);
    if (massiveErrorEl) massiveErrorEl.textContent = String(error);
    if (massivePercentLabel) massivePercentLabel.textContent = `${percent}%`;
    if (massiveProgressFill) massiveProgressFill.style.width = `${percent}%`;

    if (massiveProgressMeta) {
      if (data.status === "running" || data.status === "pending") {
        massiveProgressMeta.textContent = `${processed} de ${total} envios processados.`;
      } else if (data.status === "finished") {
        massiveProgressMeta.textContent = `Processamento finalizado. ${success} sucesso(s) e ${error} erro(s).`;
      } else if (data.status === "error") {
        massiveProgressMeta.textContent = `Job finalizado com erro. ${success} sucesso(s) e ${error} erro(s).`;
      } else {
        massiveProgressMeta.textContent = "Aguardando processamento...";
      }
    }

    renderMassiveLogs(data.logs || []);
  }

  async function loadMassiveFile() {
    const file = massiveFileInput?.files?.[0];

    if (!file) {
      massiveIds = [];

      if (massiveProgressMeta) {
        massiveProgressMeta.textContent = "Selecione um arquivo para continuar.";
      }

      if (massiveStartButton) {
        massiveStartButton.disabled = true;
      }

      return;
    }

    const content = await file.text();
    massiveIds = parseIdsFromText(content);

    if (massiveTotalEl) massiveTotalEl.textContent = String(massiveIds.length);
    if (massiveProcessedEl) massiveProcessedEl.textContent = "0";
    if (massiveSuccessEl) massiveSuccessEl.textContent = "0";
    if (massiveErrorEl) massiveErrorEl.textContent = "0";
    if (massivePercentLabel) massivePercentLabel.textContent = "0%";
    if (massiveProgressFill) massiveProgressFill.style.width = "0%";

    resetMassiveLogs();

    if (massiveProgressMeta) {
      massiveProgressMeta.textContent =
        massiveIds.length > 0
          ? `${massiveIds.length} ID(s) carregado(s) e prontos para envio.`
          : "Nenhum ID válido encontrado no arquivo.";
    }

    if (massiveStartButton) {
      massiveStartButton.disabled = massiveIds.length === 0;
    }
  }

  async function startMassiveJob() {
    if (!massiveStartUrl) {
      throw new Error("URL de início do envio massivo não configurada.");
    }

    const eventValue = (massiveEventInput?.value || "").trim();

    const response = await fetch(massiveStartUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        ids: massiveIds,
        event: eventValue,
      }),
    });

    const data = await response.json();

    if (!response.ok || !data.success) {
      throw new Error(data.message || "Falha ao iniciar envio massivo.");
    }

    return data;
  }

  async function fetchMassiveStatus(jobId) {
    const statusUrl = buildMassiveStatusUrl(jobId);

    if (!statusUrl) {
      throw new Error("URL de status do envio massivo não configurada.");
    }

    const response = await fetch(statusUrl);
    const data = await response.json();

    if (!response.ok || !data.success) {
      throw new Error(data.message || "Falha ao consultar status do job.");
    }

    return data;
  }

  function stopMassivePolling() {
    if (massivePollTimer) {
      clearTimeout(massivePollTimer);
      massivePollTimer = null;
    }
  }

  async function pollMassiveStatus() {
    if (!currentJobId) return;

    try {
      const data = await fetchMassiveStatus(currentJobId);
      updateMassiveSummaryFromApi(data);

      if (data.status === "finished" || data.status === "error") {
        setMassiveButtonsState(false);
        stopMassivePolling();
        return;
      }

      massivePollTimer = setTimeout(pollMassiveStatus, 1000);
    } catch (error) {
      if (massiveProgressMeta) {
        massiveProgressMeta.textContent = error.message;
      }

      setMassiveButtonsState(false);
      stopMassivePolling();
    }
  }

  toggleButtons.forEach((button) => {
    button.addEventListener("click", function () {
      const targetId = this.dataset.target;
      const targetEl = document.getElementById(targetId);
      if (!targetEl) return;

      const isOpen = targetEl.classList.contains("ddc-open");

      resetResultBox();
      hideSendAnimation();

      if (isOpen) {
        closeBox(targetEl);
        return;
      }

      closeAllBoxes(targetEl);
      openBox(targetEl);
    });
  });

  if (singleSendForm) {
    singleSendForm.addEventListener("submit", async function (event) {
      event.preventDefault();

      const activityId = (osIdInput?.value || "").trim();
      const eventValue = (singleEventInput?.value || "").trim();

      resetResultBox();
      hideSendAnimation();

      if (!activityId) {
        showResult("error", "Informe o ID da OS.");
        return;
      }

      if (!singleSendUrl) {
        showResult("error", "URL do envio único não configurada.");
        return;
      }

      try {
        if (singleSendButton) {
          singleSendButton.disabled = true;
          singleSendButton.textContent = "Enviando...";
        }

        showSendAnimation();

        const response = await fetch(singleSendUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            activity_id: activityId,
            event: eventValue,
          }),
        });

        const data = await response.json();

        hideSendAnimation();

        if (!response.ok || !data.success) {
          showResult("error", data.message || "Falha ao enviar OS.");
          return;
        }

        showResult("success", data.message || "OS enviada com sucesso.");
      } catch (error) {
        hideSendAnimation();
        showResult("error", error.message || "Erro ao enviar OS.");
      } finally {
        if (singleSendButton) {
          singleSendButton.disabled = false;
          singleSendButton.textContent = "Enviar";
        }
      }
    });
  }

  if (massiveValidateButton) {
    massiveValidateButton.addEventListener("click", async function () {
      if (massiveJobRunning) return;

      try {
        await loadMassiveFile();
      } catch (error) {
        if (massiveProgressMeta) {
          massiveProgressMeta.textContent = "Erro ao ler o arquivo selecionado.";
        }
      }
    });
  }

  if (massiveFileInput) {
    massiveFileInput.addEventListener("change", function () {
      if (massiveJobRunning) return;

      massiveIds = [];
      currentJobId = null;
      stopMassivePolling();
      resetMassiveLogs();

      if (massiveProgressMeta) {
        massiveProgressMeta.textContent = "Arquivo selecionado. Clique em “Carregar arquivo”.";
      }

      if (massiveStartButton) {
        massiveStartButton.disabled = true;
      }
    });
  }

  if (massiveSendForm) {
    massiveSendForm.addEventListener("submit", async function (event) {
      event.preventDefault();

      if (massiveJobRunning || massiveIds.length === 0) {
        return;
      }

      try {
        resetMassiveLogs();
        stopMassivePolling();
        setMassiveButtonsState(true);

        const startData = await startMassiveJob();
        currentJobId = startData.job_id;

        if (massiveProgressMeta) {
          massiveProgressMeta.textContent = "Job iniciado. Aguardando primeiros retornos...";
        }

        pollMassiveStatus();
      } catch (error) {
        setMassiveButtonsState(false);

        if (massiveProgressMeta) {
          massiveProgressMeta.textContent = error.message || "Erro ao iniciar envio massivo.";
        }
      }
    });
  }
})();