document.addEventListener("DOMContentLoaded", function () {
  const activitiesEl = document.getElementById("activities-json");
  const activities = activitiesEl ? JSON.parse(activitiesEl.textContent || "[]") : [];

  // Modal grande
  const modal = document.getElementById("modalTratar");
  const backdrop = document.getElementById("modalBackdrop");
  const btnFechar = document.getElementById("btnFecharModal");
  const btnAbrirTratativa = document.getElementById("btnAbrirTratativa");
  const btnRevogarTratativa = document.getElementById("btnRevogarTratativa");

  // Modal pequeno tratativa
  const tModal = document.getElementById("modalTratativa");
  const tBackdrop = document.getElementById("modalTratativaBackdrop");
  const btnFecharTratativa = document.getElementById("btnFecharTratativa");
  const btnSalvarTratativa = document.getElementById("btnSalvarTratativa");

  // Modal pequeno revogar
  const rModal = document.getElementById("modalRevogar");
  const rBackdrop = document.getElementById("modalRevogarBackdrop");
  const btnFecharRevogar = document.getElementById("btnFecharRevogar");
  const btnConfirmarRevogar = document.getElementById("btnConfirmarRevogar");

  let currentActivity = null;

  function setVal(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value ?? "";
  }
  function setTratadoUI(isTratado) {
    if (btnAbrirTratativa) btnAbrirTratativa.style.display = isTratado ? "none" : "inline-block";
    if (btnRevogarTratativa) btnRevogarTratativa.style.display = isTratado ? "inline-block" : "none";
  }
  // ===== Modal grande =====
  function openModal() {
    backdrop.style.display = "block";
    modal.style.display = "block";
  }
  function closeModal() {
    modal.style.display = "none";
    backdrop.style.display = "none";
    currentActivity = null;

  // reset visual
    setTratadoUI(false);
  }
  if (btnFechar) btnFechar.addEventListener("click", closeModal);
  if (backdrop) backdrop.addEventListener("click", closeModal);

  // ===== Modal tratativa =====
  function openTratativaModal() {
    document.getElementById("t_status").value = "";
    document.getElementById("t_obs").value = "";
    tBackdrop.style.display = "block";
    tModal.style.display = "block";
  }
  function closeTratativaModal() {
    tModal.style.display = "none";
    tBackdrop.style.display = "none";
  }
  if (btnFecharTratativa) btnFecharTratativa.addEventListener("click", closeTratativaModal);
  if (tBackdrop) tBackdrop.addEventListener("click", closeTratativaModal);

  // ===== Modal revogar =====
  function openRevogarModal() {
    document.getElementById("r_obs").value = "";
    rBackdrop.style.display = "block";
    rModal.style.display = "block";
  }
  function closeRevogarModal() {
    rModal.style.display = "none";
    rBackdrop.style.display = "none";
  }
  if (btnFecharRevogar) btnFecharRevogar.addEventListener("click", closeRevogarModal);
  if (rBackdrop) rBackdrop.addEventListener("click", closeRevogarModal);

  // ===== Helpers de UI =====
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
      // NOTE: não temos mais idx aqui; mantemos pelo activityId e buscamos no JSON local
      // então vamos recriar o botão com data-activityid e abrir pelo GET no clique.
      tdAcoes.innerHTML = `<button type="button" class="btn-tratar btn-tratar-byid" data-activityid="${activityId}">Tratar</button>`;
    }
  }

  // ===== Abrir modal com dados =====
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

    // tratativa atual (pode ser null)
    setVal("m_tratativa_status", a.tratativa_status);
    setVal("m_tratado_por", a.tratado_por_username);
    setVal("m_tratativa_obs", a.tratativa_obs);

    // Se tratado -> mostra botão revogar
    const isTratado = (a.tratado_em !== null && a.tratado_em !== undefined && String(a.tratado_em).trim() !== "");
    setTratadoUI(isTratado);
  }

  async function fetchByActivityId(activityId) {
    const resp = await fetch(`/atividades-notdone/${encodeURIComponent(activityId)}`);
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok || !data.ok) {
      throw new Error(data.error || "Falha ao buscar detalhes");
    }
    return data.item;
  }

  // Clique em "Tratar" (pendente) usando idx
  document.addEventListener("click", function (e) {
    const btn = e.target.closest(".btn-tratar");
    if (!btn) return;

    // Se for botão por idx
    const idxAttr = btn.getAttribute("data-idx");
    if (idxAttr !== null) {
      const idx = parseInt(idxAttr, 10);
      const a = activities[idx];
      if (!a) return;
      fillModalFromItem(a);
      openModal();
      return;
    }

    // Se for botão por activityId (caso revogou e recriou botão)
    const byId = btn.classList.contains("btn-tratar-byid");
    if (byId) {
      const activityId = btn.getAttribute("data-activityid");
      if (!activityId) return;

      fetchByActivityId(activityId)
        .then((item) => {
          fillModalFromItem(item);
          openModal();
        })
        .catch((err) => alert(err.message));
    }
  });

  // Clique em "Tratado" (clicável)
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

  // Abrir modal pequeno de finalizar tratativa
  if (btnAbrirTratativa) {
    btnAbrirTratativa.addEventListener("click", function () {
      if (!currentActivity) return;
      openTratativaModal();
    });
  }

  // Salvar tratativa
  if (btnSalvarTratativa) {
    btnSalvarTratativa.addEventListener("click", async function () {
      if (!currentActivity) return;

      const status = (document.getElementById("t_status").value || "").trim();
      const obs = (document.getElementById("t_obs").value || "").trim();

      if (!status) {
        alert("Selecione o resultado da tratativa.");
        return;
      }

      const payload = { activityId: currentActivity.activityId, status, observacoes: obs };

      try {
        const resp = await fetch("/atividades-notdone/tratar", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });

        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) {
          alert(data.error || "Erro ao salvar tratativa.");
          return;
        }

        // Atualiza a linha da tabela
        updateRowToTratado(currentActivity.activityId, data.tratadoPor, data.status);
        // força UI do modal para tratado (sem depender do objeto)
        setTratadoUI(true);

        // atualiza campos da tratativa no modal
        setVal("m_tratativa_status", data.status);
        setVal("m_tratado_por", data.tratadoPor);
        setVal("m_tratativa_obs", document.getElementById("t_obs").value);

        // garante estado local também (opcional, mas bom)
        currentActivity.tratado_em = new Date().toISOString();
        currentActivity.tratativa_status = data.status;
        currentActivity.tratado_por_username = data.tratadoPor;
        currentActivity.tratativa_obs = document.getElementById("t_obs").value;

        closeTratativaModal();
        // Atualiza o estado do objeto atual
        currentActivity.tratado_em = new Date().toISOString();
        currentActivity.tratativa_status = data.status;
        currentActivity.tratado_por_username = data.tratadoPor;
        currentActivity.tratativa_obs = document.getElementById("t_obs").value;

        // 🔥 Atualiza os botões do modal grande
        if (btnAbrirTratativa) btnAbrirTratativa.style.display = "none";
        if (btnRevogarTratativa) btnRevogarTratativa.style.display = "inline-block";

        // Atualiza os campos visíveis no modal
        setVal("m_tratativa_status", data.status);
        setVal("m_tratado_por", data.tratadoPor);
        setVal("m_tratativa_obs", document.getElementById("t_obs").value);

        closeTratativaModal();
      } catch (err) {
        alert("Falha de rede ao salvar tratativa.");
        console.error(err);
      }
    });
  }

  // Revogar tratativa (abre modal)
  if (btnRevogarTratativa) {
    btnRevogarTratativa.addEventListener("click", function () {
      if (!currentActivity) return;
      openRevogarModal();
    });
  }

  // Confirmar revogação
  if (btnConfirmarRevogar) {
    btnConfirmarRevogar.addEventListener("click", async function () {
      if (!currentActivity) return;

      const obs = (document.getElementById("r_obs").value || "").trim();
      if (!obs) {
        alert("Observação é obrigatória para revogar.");
        return;
      }

      try {
        const resp = await fetch("/atividades-notdone/revogar", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ activityId: currentActivity.activityId, observacoes: obs }),
        });

        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) {
          alert(data.error || "Erro ao revogar tratativa.");
          return;
        }

        updateRowToPendente(currentActivity.activityId);

        closeRevogarModal();
        closeModal();

      } catch (err) {
        alert("Falha de rede ao revogar.");
        console.error(err);
      }
    });
  }
});

// ===============================
// Filtro estilo Excel (thead)
// ===============================
(function () {
  const menu = document.getElementById("thFilterMenu");
  const input = document.getElementById("thFilterInput");
  const btnApply = document.getElementById("thFilterApply");
  const btnClear = document.getElementById("thFilterClear");

  if (!menu || !input || !btnApply || !btnClear) return;

  let activeCol = null;
  let anchorBtn = null;

  // filtros por coluna (mantém estado)
  const filters = {}; // { col: "texto" }

  function getTableBody() {
    return document.querySelector(".tabela-atividades tbody");
  }

  function getRows() {
    const tbody = getTableBody();
    if (!tbody) return [];
    return Array.from(tbody.querySelectorAll("tr")).filter(tr => tr.querySelector("td"));
  }

  function cellTextByCol(row, col) {
    const td = row.querySelector(`td[data-col="${col}"]`);
    return (td ? td.textContent : "").trim().toLowerCase();
  }

  function applyAllFilters() {
    const rows = getRows();
    rows.forEach(row => {
      let visible = true;
      for (const col in filters) {
        const q = (filters[col] || "").toLowerCase();
        if (!q) continue;
        if (!cellTextByCol(row, col).includes(q)) {
          visible = false;
          break;
        }
      }
      row.style.display = visible ? "" : "none";
    });
  }

  function sortRows(col, dir) {
    const tbody = getTableBody();
    if (!tbody) return;

    const rows = getRows();

    rows.sort((a, b) => {
      const A = cellTextByCol(a, col);
      const B = cellTextByCol(b, col);
      if (A < B) return dir === "asc" ? -1 : 1;
      if (A > B) return dir === "asc" ? 1 : -1;
      return 0;
    });

    rows.forEach(r => tbody.appendChild(r));
  }

  function openMenu(btn, col) {
    activeCol = col;
    anchorBtn = btn;

    // carrega filtro atual da coluna
    input.value = filters[col] || "";

    const rect = btn.getBoundingClientRect();
    menu.style.display = "block";
    menu.style.top = `${rect.bottom + 6}px`;
    menu.style.left = `${Math.min(rect.left, window.innerWidth - 300)}px`;

    input.focus();
  }

  function closeMenu() {
    menu.style.display = "none";
    activeCol = null;
    anchorBtn = null;
  }

  // clique no botãozinho do header
  document.addEventListener("click", function (e) {
    const btn = e.target.closest(".th-filter-btn");
    if (!btn) {
      // clique fora fecha
      if (menu.style.display === "block" && !e.target.closest("#thFilterMenu")) closeMenu();
      return;
    }

    const col = btn.getAttribute("data-col");
    if (!col) return;

    // toggle
    if (menu.style.display === "block" && activeCol === col) {
      closeMenu();
      return;
    }
    openMenu(btn, col);
  });

  // ordenar A→Z / Z→A
  menu.addEventListener("click", function (e) {
    const act = e.target.closest(".th-filter-action");
    if (!act || !activeCol) return;

    const action = act.getAttribute("data-action");
    if (action === "asc" || action === "desc") {
      sortRows(activeCol, action);
      applyAllFilters(); // mantém filtros ativos após ordenar
      closeMenu();
    }
  });

  // aplicar filtro
  btnApply.addEventListener("click", function () {
    if (!activeCol) return;
    filters[activeCol] = (input.value || "").trim();
    applyAllFilters();
    closeMenu();
  });

  // limpar filtro
  btnClear.addEventListener("click", function () {
    if (!activeCol) return;
    filters[activeCol] = "";
    input.value = "";
    applyAllFilters();
    closeMenu();
  });

  // enter aplica
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