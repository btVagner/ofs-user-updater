document.addEventListener("DOMContentLoaded", function () {
  const bodyEl = document.body;
  const viewMode = (bodyEl?.dataset?.viewMode || "pendentes").trim();
  const detailUrlBase = bodyEl?.dataset?.detailUrlBase || "";
  const tratarUrl = bodyEl?.dataset?.tratarUrl || "/atividades-notdone/tratar";
  const revogarUrl = bodyEl?.dataset?.revogarUrl || "/atividades-notdone/revogar";

  const activitiesEl = document.getElementById("activities-json");
  const activities = activitiesEl ? JSON.parse(activitiesEl.textContent || "[]") : [];

  const modal = document.getElementById("modalTratar");
  const backdrop = document.getElementById("modalBackdrop");
  const btnFechar = document.getElementById("btnFecharModal");
  const btnAbrirTratativa = document.getElementById("btnAbrirTratativa");
  const btnRevogarTratativa = document.getElementById("btnRevogarTratativa");

  const tModal = document.getElementById("modalTratativa");
  const tBackdrop = document.getElementById("modalTratativaBackdrop");
  const btnFecharTratativa = document.getElementById("btnFecharTratativa");
  const btnSalvarTratativa = document.getElementById("btnSalvarTratativa");

  const rModal = document.getElementById("modalRevogar");
  const rBackdrop = document.getElementById("modalRevogarBackdrop");
  const btnFecharRevogar = document.getElementById("btnFecharRevogar");
  const btnConfirmarRevogar = document.getElementById("btnConfirmarRevogar");

  let currentActivity = null;
  function showToast(type, title, message) {
    const container = document.getElementById("toastContainer");
    if (!container) return;

    const toast = document.createElement("div");
    toast.className = `toast-msg ${type}`;

    toast.innerHTML = `
    <div class="toast-body">
      <div class="toast-title">${title}</div>
      <div class="toast-text">${message}</div>
    </div>
    <div class="toast-progress">
      <div class="toast-progress-bar"></div>
    </div>
  `;

    container.appendChild(toast);

    setTimeout(() => {
      toast.style.animation = "toastOut 0.28s ease forwards";
      setTimeout(() => {
        toast.remove();
      }, 280);
    }, 1700);
  }
  function setVal(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value ?? "";
  }

  function setTratadoUI(isTratado) {
    if (btnAbrirTratativa) btnAbrirTratativa.style.display = isTratado ? "none" : "inline-block";
    if (btnRevogarTratativa) btnRevogarTratativa.style.display = isTratado ? "inline-block" : "none";
  }

  function openModal() {
    if (backdrop) backdrop.style.display = "block";
    if (modal) modal.style.display = "block";
  }

  function closeModal() {
    if (modal) modal.style.display = "none";
    if (backdrop) backdrop.style.display = "none";
    currentActivity = null;
    setTratadoUI(false);
  }

  if (btnFechar) btnFechar.addEventListener("click", closeModal);
  if (backdrop) backdrop.addEventListener("click", closeModal);

  function openTratativaModal() {
    const tStatus = document.getElementById("t_status");
    const tObs = document.getElementById("t_obs");
    if (tStatus) tStatus.value = "";
    if (tObs) tObs.value = "";
    if (tBackdrop) tBackdrop.style.display = "block";
    if (tModal) tModal.style.display = "block";
  }

  function closeTratativaModal() {
    if (tModal) tModal.style.display = "none";
    if (tBackdrop) tBackdrop.style.display = "none";
  }

  if (btnFecharTratativa) btnFecharTratativa.addEventListener("click", closeTratativaModal);
  if (tBackdrop) tBackdrop.addEventListener("click", closeTratativaModal);

  function openRevogarModal() {
    const rObs = document.getElementById("r_obs");
    if (rObs) rObs.value = "";
    if (rBackdrop) rBackdrop.style.display = "block";
    if (rModal) rModal.style.display = "block";
  }

  function closeRevogarModal() {
    if (rModal) rModal.style.display = "none";
    if (rBackdrop) rBackdrop.style.display = "none";
  }

  if (btnFecharRevogar) btnFecharRevogar.addEventListener("click", closeRevogarModal);
  if (rBackdrop) rBackdrop.addEventListener("click", closeRevogarModal);

  function updateLocalActivity(activityId, patch) {
    const idx = activities.findIndex(a => String(a.activityId) === String(activityId));
    if (idx >= 0) {
      activities[idx] = { ...activities[idx], ...patch };
    }
    if (currentActivity && String(currentActivity.activityId) === String(activityId)) {
      currentActivity = { ...currentActivity, ...patch };
    }
  }

  function updateKpi(id, delta) {
    const el = document.getElementById(id);
    if (!el) return;
    const current = parseInt(el.textContent || "0", 10) || 0;
    el.textContent = String(Math.max(0, current + delta));
  }

  function ensureEmptyRow() {
    const tbody = document.querySelector(".tabela-atividades tbody");
    const table = document.querySelector(".tabela-atividades");
    if (!tbody || !table) return;

    const dataRows = Array.from(tbody.querySelectorAll("tr")).filter(tr => !tr.classList.contains("js-empty-row"));
    if (dataRows.length > 0) return;

    const colspan = table.dataset.emptyColspan || "5";
    const tr = document.createElement("tr");
    tr.className = "js-empty-row";
    tr.innerHTML = `<td colspan="${colspan}" class="empty-row">Nenhuma atividade encontrada.</td>`;
    tbody.appendChild(tr);
  }

  function removeEmptyRowIfNeeded() {
    const emptyRow = document.querySelector(".js-empty-row");
    if (emptyRow) emptyRow.remove();
  }

  function removeRow(activityId) {
    const row = document.getElementById("row-" + activityId);
    if (row) row.remove();
    ensureEmptyRow();
  }

  function updateRowToTratado(activityId, tratadoPor, status) {
    const row = document.getElementById("row-" + activityId);
    if (!row) return;

    const tdTratadoPor = row.querySelector(".td-tratado-por");
    if (tdTratadoPor) tdTratadoPor.innerText = tratadoPor || "-";

    const tdStatus = row.querySelector(".td-tratativa-status");
    if (tdStatus) tdStatus.innerText = status || "-";

    const tdAcoes = row.querySelector("td:last-child");
    if (tdAcoes) {
      tdAcoes.innerHTML = `<button type="button" class="btn-treated btn-ver-tratado" data-activityid="${activityId}">Tratado</button>`;
    }
  }

  function updateRowToPendente(activityId) {
    const row = document.getElementById("row-" + activityId);
    if (!row) return;

    const tdTratadoPor = row.querySelector(".td-tratado-por");
    if (tdTratadoPor) tdTratadoPor.innerText = "-";

    const tdStatus = row.querySelector(".td-tratativa-status");
    if (tdStatus) tdStatus.innerText = "-";

    const tdAcoes = row.querySelector("td:last-child");
    if (tdAcoes) {
      tdAcoes.innerHTML = `<button type="button" class="btn-tratar btn-tratar-byid" data-activityid="${activityId}">Tratar</button>`;
    }
  }

  function fillModalFromItem(a) {
    currentActivity = a;

    setVal("m_activityId", a.activityId);
    setVal("m_resourceId", a.resourceId);
    setVal("m_apptNumber", a.apptNumber);
    setVal("m_customerNumber", a.customerNumber);
    setVal("m_customerPhone", a.customerPhone);
    setVal("m_customerName", a.customerName);
    setVal("m_city", a.city);
    setVal("m_bucket", a.XA_ORIGIN_BUCKET);
    setVal("m_date", a.date);
    setVal("m_tskNot", a.XA_TSK_NOT);

    setVal("m_tratativa_status", a.tratativa_status);
    setVal("m_tratado_por", a.tratado_por_username);
    setVal("m_tratativa_obs", a.tratativa_obs);

    const isTratado = a.tratado_em !== null && a.tratado_em !== undefined && String(a.tratado_em).trim() !== "";
    setTratadoUI(isTratado);
  }

  async function fetchByActivityId(activityId) {
    const url = detailUrlBase.replace("__ACTIVITY_ID__", encodeURIComponent(activityId));
    const resp = await fetch(url);
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok || !data.ok) {
      throw new Error(data.error || "Falha ao buscar detalhes");
    }
    return data.item;
  }

  document.addEventListener("click", function (e) {
    const btn = e.target.closest(".btn-tratar");
    if (!btn) return;

    const activityId = btn.getAttribute("data-activityid");
    if (!activityId) return;

    fetchByActivityId(activityId)
      .then((item) => {
        fillModalFromItem(item);
        openModal();
      })
      .catch((err) => showToast("error", "Erro", err.message || "Falha ao buscar detalhes."));
  });

  document.addEventListener("click", function (e) {
    const btn = e.target.closest(".btn-ver-tratado");
    if (!btn) return;

    const activityId = btn.getAttribute("data-activityid");
    if (!activityId) return;

    fetchByActivityId(activityId)
      .then((item) => {
        fillModalFromItem(item);
        openModal();
      })
      .catch((err) => alert(err.message));
  });

  if (btnAbrirTratativa) {
    btnAbrirTratativa.addEventListener("click", function () {
      if (!currentActivity) return;
      openTratativaModal();
    });
  }

  if (btnSalvarTratativa) {
    btnSalvarTratativa.addEventListener("click", async function () {
      if (!currentActivity) return;

      const status = (document.getElementById("t_status")?.value || "").trim();
      const obs = (document.getElementById("t_obs")?.value || "").trim();

      if (!status) {
        showToast("warning", "Atenção", "Selecione o resultado da tratativa.");
        return;
      }

      const payload = { activityId: currentActivity.activityId, status, observacoes: obs };

      try {
        const resp = await fetch(tratarUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });

        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) {
          showToast("error", "Erro", data.error || "Erro ao salvar tratativa.");
          return;
        }

        updateLocalActivity(currentActivity.activityId, {
          tratado_em: data.tratadoEm || new Date().toISOString(),
          tratativa_status: data.status,
          tratado_por_username: data.tratadoPor,
          tratativa_obs: data.observacoes || obs,
        });

        setVal("m_tratativa_status", data.status);
        setVal("m_tratado_por", data.tratadoPor);
        setVal("m_tratativa_obs", data.observacoes || obs);

        if (viewMode === "pendentes") {
          removeRow(currentActivity.activityId);
          updateKpi("kpiPendentes", -1);
          updateKpi("kpiTratados", 1);
          updateKpi("kpiTotal", -1);
        } else {
          updateRowToTratado(currentActivity.activityId, data.tratadoPor, data.status);
        }

        setTratadoUI(true);
        showToast("success", "Sucesso", "Tratativa realizada com sucesso.");
        closeTratativaModal();
        closeModal();
      } catch (err) {
        showToast("error", "Erro", "Falha de rede ao salvar tratativa.");
        console.error(err);
      }
    });
  }

  if (btnRevogarTratativa) {
    btnRevogarTratativa.addEventListener("click", function () {
      if (!currentActivity) return;
      openRevogarModal();
    });
  }

  if (btnConfirmarRevogar) {
    btnConfirmarRevogar.addEventListener("click", async function () {
      if (!currentActivity) return;

      const obs = (document.getElementById("r_obs")?.value || "").trim();
      if (!obs) {
        showToast("warning", "Atenção", "Observação é obrigatória para revogar.");
        return;
      }

      try {
        const resp = await fetch(revogarUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ activityId: currentActivity.activityId, observacoes: obs }),
        });

        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) {
          showToast("error", "Erro", data.error || "Erro ao revogar tratativa.");
          return;
        }

        updateLocalActivity(currentActivity.activityId, {
          tratado_em: null,
          tratativa_status: null,
          tratado_por_username: null,
          tratativa_obs: null,
        });

        if (viewMode === "tratadas") {
          removeRow(currentActivity.activityId);
          updateKpi("kpiTratados", -1);
          updateKpi("kpiPendentes", 1);
          updateKpi("kpiTotal", -1);
        } else {
          updateRowToPendente(currentActivity.activityId);
        }
        showToast("success", "Sucesso", "Revogação realizada com sucesso.");
        closeRevogarModal();
        closeModal();
      } catch (err) {
        showToast("error", "Erro", "Falha de rede ao revogar.");
        console.error(err);
      }
    });
  }

  removeEmptyRowIfNeeded();
});

// ===============================
// Filtro estilo Excel (thead)
// ===============================
(function () {
  const menu = document.getElementById("thFilterMenu");
  const input = document.getElementById("thFilterInput");
  const btnApply = document.getElementById("thFilterApply");
  const btnClear = document.getElementById("thFilterClear");
  const dateRangeBox = document.getElementById("thFilterDateRange");
  const dateFromInput = document.getElementById("thFilterDateFrom");
  const dateToInput = document.getElementById("thFilterDateTo");

  if (!menu || !input || !btnApply || !btnClear) return;

  let activeCol = null;

  const filters = {};
  const dateFilters = {};

  function getTableBody() {
    return document.querySelector(".tabela-atividades tbody");
  }

  function getRows() {
    const tbody = getTableBody();
    if (!tbody) return [];
    return Array.from(tbody.querySelectorAll("tr")).filter(
      tr => tr.querySelector("td") && !tr.classList.contains("js-empty-row")
    );
  }

  function cellTextByCol(row, col) {
    const td = row.querySelector(`td[data-col="${col}"]`);
    return (td ? td.textContent : "").trim();
  }

  function normalizeDateText(value) {
    return String(value || "").trim().slice(0, 10);
  }

  function matchDateRange(cellValue, fromValue, toValue) {
    const dateValue = normalizeDateText(cellValue);
    if (!dateValue) return false;
    if (fromValue && dateValue < fromValue) return false;
    if (toValue && dateValue > toValue) return false;
    return true;
  }

  function ensureEmptyStateForFilters() {
    const tbody = getTableBody();
    const table = document.querySelector(".tabela-atividades");
    if (!tbody || !table) return;

    const allRows = Array.from(tbody.querySelectorAll("tr")).filter(tr => !tr.classList.contains("js-empty-row"));
    const visibleRows = allRows.filter(tr => tr.style.display !== "none");
    const emptyRow = tbody.querySelector(".js-empty-row");

    if (visibleRows.length === 0 && allRows.length > 0 && !emptyRow) {
      const colspan = table.dataset.emptyColspan || "5";
      const tr = document.createElement("tr");
      tr.className = "js-empty-row";
      tr.innerHTML = `<td colspan="${colspan}" class="empty-row">Nenhuma atividade encontrada com os filtros aplicados.</td>`;
      tbody.appendChild(tr);
      return;
    }

    if (visibleRows.length > 0 && emptyRow) {
      emptyRow.remove();
    }
  }

  function applyAllFilters() {
    const rows = getRows();

    rows.forEach(row => {
      let visible = true;

      for (const col in filters) {
        const q = String(filters[col] || "").trim().toLowerCase();
        if (!q) continue;

        const cell = cellTextByCol(row, col).toLowerCase();
        if (!cell.includes(q)) {
          visible = false;
          break;
        }
      }

      if (visible) {
        for (const col in dateFilters) {
          const cfg = dateFilters[col] || {};
          const fromValue = (cfg.from || "").trim();
          const toValue = (cfg.to || "").trim();

          if (!fromValue && !toValue) continue;

          const cell = cellTextByCol(row, col);
          if (!matchDateRange(cell, fromValue, toValue)) {
            visible = false;
            break;
          }
        }
      }

      row.style.display = visible ? "" : "none";
    });

    ensureEmptyStateForFilters();
  }

  function sortRows(col, dir) {
    const tbody = getTableBody();
    if (!tbody) return;

    const rows = getRows();

    rows.sort((a, b) => {
      const A = cellTextByCol(a, col).toLowerCase();
      const B = cellTextByCol(b, col).toLowerCase();

      if (A < B) return dir === "asc" ? -1 : 1;
      if (A > B) return dir === "asc" ? 1 : -1;
      return 0;
    });

    rows.forEach(r => tbody.appendChild(r));

    const emptyRow = tbody.querySelector(".js-empty-row");
    if (emptyRow) tbody.appendChild(emptyRow);
  }

  function syncMenuState(col) {
    input.value = filters[col] || "";

    if (col === "date" && dateRangeBox) {
      dateRangeBox.style.display = "block";
      const cfg = dateFilters[col] || {};
      if (dateFromInput) dateFromInput.value = cfg.from || "";
      if (dateToInput) dateToInput.value = cfg.to || "";
    } else if (dateRangeBox) {
      dateRangeBox.style.display = "none";
      if (dateFromInput) dateFromInput.value = "";
      if (dateToInput) dateToInput.value = "";
    }
  }

  function openMenu(btn, col) {
    activeCol = col;
    syncMenuState(col);

    const rect = btn.getBoundingClientRect();
    menu.style.display = "block";
    menu.style.top = `${rect.bottom + 6}px`;
    menu.style.left = `${Math.min(rect.left, window.innerWidth - 320)}px`;

    if (col === "date" && dateFromInput) {
      dateFromInput.focus();
    } else {
      input.focus();
    }
  }

  function closeMenu() {
    menu.style.display = "none";
    activeCol = null;
  }

  document.addEventListener("click", function (e) {
    const btn = e.target.closest(".th-filter-btn");
    if (!btn) {
      if (menu.style.display === "block" && !e.target.closest("#thFilterMenu")) {
        closeMenu();
      }
      return;
    }

    const col = btn.getAttribute("data-col");
    if (!col) return;

    if (menu.style.display === "block" && activeCol === col) {
      closeMenu();
      return;
    }

    openMenu(btn, col);
  });

  menu.addEventListener("click", function (e) {
    const act = e.target.closest(".th-filter-action");
    if (!act || !activeCol) return;

    const action = act.getAttribute("data-action");
    if (action === "asc" || action === "desc") {
      sortRows(activeCol, action);
      applyAllFilters();
      closeMenu();
    }
  });

  btnApply.addEventListener("click", function () {
    if (!activeCol) return;

    filters[activeCol] = (input.value || "").trim();

    if (activeCol === "date") {
      dateFilters[activeCol] = {
        from: (dateFromInput?.value || "").trim(),
        to: (dateToInput?.value || "").trim(),
      };
    }

    applyAllFilters();
    closeMenu();
  });

  btnClear.addEventListener("click", function () {
    if (!activeCol) return;

    filters[activeCol] = "";

    if (input) input.value = "";

    if (activeCol === "date") {
      dateFilters[activeCol] = { from: "", to: "" };
      if (dateFromInput) dateFromInput.value = "";
      if (dateToInput) dateToInput.value = "";
    }

    applyAllFilters();
    closeMenu();
  });

  input.addEventListener("keydown", function (e) {
    if (e.key === "Enter") {
      e.preventDefault();
      btnApply.click();
    }
    if (e.key === "Escape") {
      e.preventDefault();
      closeMenu();
    }
  });

  if (dateFromInput) {
    dateFromInput.addEventListener("keydown", function (e) {
      if (e.key === "Enter") {
        e.preventDefault();
        btnApply.click();
      }
      if (e.key === "Escape") {
        e.preventDefault();
        closeMenu();
      }
    });
  }

  if (dateToInput) {
    dateToInput.addEventListener("keydown", function (e) {
      if (e.key === "Enter") {
        e.preventDefault();
        btnApply.click();
      }
      if (e.key === "Escape") {
        e.preventDefault();
        closeMenu();
      }
    });
  }
})();

// ===== Exportação (modal) =====
(function () {
  const btn = document.getElementById("btnExportacao");
  const modal = document.getElementById("modalExport");
  const backdrop = document.getElementById("modalExportBackdrop");
  const closeBtn = document.getElementById("btnFecharExport");

  if (!btn || !modal || !backdrop || !closeBtn) return;

  function openExport() {
    modal.style.display = "block";
    backdrop.style.display = "block";
  }

  function closeExport() {
    modal.style.display = "none";
    backdrop.style.display = "none";
  }

  btn.addEventListener("click", openExport);
  closeBtn.addEventListener("click", closeExport);
  backdrop.addEventListener("click", closeExport);

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeExport();
  });
})();