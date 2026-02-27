// ===============================
// Filtro estilo Excel (thead) - Base
// ===============================
(function () {
  const menu = document.getElementById("thFilterMenu");
  const input = document.getElementById("thFilterInput");
  const btnApply = document.getElementById("thFilterApply");
  const btnClear = document.getElementById("thFilterClear");

  if (!menu || !input || !btnApply || !btnClear) return;

  let activeCol = null;
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

  function clamp(n, min, max) {
    return Math.max(min, Math.min(max, n));
  }

  function openMenu(btn, col) {
    activeCol = col;
    input.value = filters[col] || "";

    const rect = btn.getBoundingClientRect();
    menu.style.display = "block";

    const menuW = menu.offsetWidth || 320;
    const menuH = menu.offsetHeight || 180;

    let left = rect.left;
    let top = rect.bottom + 6;

    left = clamp(left, 8, window.innerWidth - menuW - 8);
    top = clamp(top, 8, window.innerHeight - menuH - 8);

    menu.style.left = `${left}px`;
    menu.style.top = `${top}px`;

    input.focus();
  }

  function closeMenu() {
    menu.style.display = "none";
    activeCol = null;
  }

  document.addEventListener("click", function (e) {
    const btn = e.target.closest(".th-filter-btn");
    if (!btn) {
      if (menu.style.display === "block" && !e.target.closest("#thFilterMenu")) closeMenu();
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

  window.addEventListener("scroll", () => { if (menu.style.display === "block") closeMenu(); }, true);
  window.addEventListener("resize", () => { if (menu.style.display === "block") closeMenu(); });

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
    applyAllFilters();
    closeMenu();
  });

  btnClear.addEventListener("click", function () {
    if (!activeCol) return;
    filters[activeCol] = "";
    input.value = "";
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
})();

// ===============================
// Exportação (modal)
// ===============================
(function () {
  const btn = document.getElementById("btnExportXlsx");
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

  // ESC global (somente para o modal)
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeExport();
  });
})();

// ===============================
// Multi-select dropdown (checkbox) - refinado
//  - sem múltiplos listeners globais duplicados
// ===============================

// Registry global de dropdowns
const MS_REGISTRY = (function () {
  const instances = new Set();

  function closeAll(exceptInstance) {
    instances.forEach((inst) => {
      if (exceptInstance && inst === exceptInstance) return;
      inst.close();
    });
  }

  // Clique fora fecha tudo
  document.addEventListener("click", (e) => {
    // Se clicou dentro de algum wrap, não fecha aquele
    let clickedInside = null;
    instances.forEach((inst) => {
      if (inst.wrap && inst.wrap.contains(e.target)) clickedInside = inst;
    });

    if (clickedInside) {
      // fecha os outros, mantém o atual
      closeAll(clickedInside);
    } else {
      closeAll(null);
    }
  });

  // ESC fecha tudo
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeAll(null);
  });

  return {
    add(inst) { instances.add(inst); },
    closeAll,
  };
})();

function initMultiSelect(cfg) {
  const wrap = document.getElementById(cfg.wrapId);
  if (!wrap) return null;

  const btn = document.getElementById(cfg.btnId);
  const panel = document.getElementById(cfg.panelId);
  const label = document.getElementById(cfg.labelId);
  const search = document.getElementById(cfg.searchId);
  const list = document.getElementById(cfg.listId);
  const hidden = document.getElementById(cfg.hiddenId);
  const btnClear = document.getElementById(cfg.clearId);
  const btnAll = document.getElementById(cfg.allId);

  if (!btn || !panel || !label || !list || !hidden || !btnClear || !btnAll) return null;

  function isOpen() {
    return panel.classList.contains("open");
  }
  function open() {
    panel.classList.add("open");
    if (search) search.focus();
  }
  function close() {
    panel.classList.remove("open");
  }
  function toggle() {
    isOpen() ? close() : open();
  }

  function getChecks() {
    return Array.from(wrap.querySelectorAll(".ms-check"));
  }
  function selectedValues() {
    return getChecks().filter(c => c.checked).map(c => c.value);
  }

  function refresh() {
    const vals = selectedValues();

    if (vals.length === 0) label.textContent = "(Todos)";
    else if (vals.length === 1) label.textContent = vals[0];
    else label.textContent = `${vals.length} selecionados`;

    hidden.innerHTML = "";
    vals.forEach(v => {
      const inp = document.createElement("input");
      inp.type = "hidden";
      inp.name = cfg.inputName; // activityType/status/buckets
      inp.value = v;
      hidden.appendChild(inp);
    });
  }

  // Toggle abre/fecha
  btn.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation(); // evita o click global fechar na mesma hora
    // fecha os outros antes de abrir
    MS_REGISTRY.closeAll(null);
    toggle();
  });

  // Seleção atualiza
  wrap.addEventListener("change", (e) => {
    if (e.target.classList.contains("ms-check")) refresh();
  });

  // Buscar
  if (search) {
    search.addEventListener("input", () => {
      const q = (search.value || "").trim().toLowerCase();
      const items = Array.from(list.querySelectorAll(".ms-item"));
      items.forEach(it => {
        const text = (it.textContent || "").toLowerCase();
        it.style.display = text.includes(q) ? "" : "none";
      });
    });
  }

  // Limpar
  btnClear.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    getChecks().forEach(c => (c.checked = false));
    refresh();
  });

  // Selecionar tudo
  btnAll.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    getChecks().forEach(c => (c.checked = true));
    refresh();
  });

  // Estado inicial
  refresh();

  const instance = { wrap, open, close, toggle, refresh, isOpen };
  MS_REGISTRY.add(instance);
  return instance;
}

// ===============================
// init dos multi-selects (EXPORT + GET)
// ===============================
(function () {
  // EXPORT - Recursos
  initMultiSelect({
    wrapId: "msBuckets",
    btnId: "msBtn",
    panelId: "msPanel",
    labelId: "msLabel",
    searchId: "msSearch",
    listId: "msList",
    hiddenId: "msHidden",
    clearId: "msClear",
    allId: "msAll",
    inputName: "buckets",
  });

  // EXPORT - Activity Types
  initMultiSelect({
    wrapId: "msTypes",
    btnId: "msTypesBtn",
    panelId: "msTypesPanel",
    labelId: "msTypesLabel",
    searchId: "msTypesSearch",
    listId: "msTypesList",
    hiddenId: "msTypesHidden",
    clearId: "msTypesClear",
    allId: "msTypesAll",
    inputName: "activityType",
  });

  // EXPORT - Status
  initMultiSelect({
    wrapId: "msStatus",
    btnId: "msStatusBtn",
    panelId: "msStatusPanel",
    labelId: "msStatusLabel",
    searchId: "msStatusSearch",
    listId: "msStatusList",
    hiddenId: "msStatusHidden",
    clearId: "msStatusClear",
    allId: "msStatusAll",
    inputName: "status",
  });

  // GET (TOPO) - Activity Types
  initMultiSelect({
    wrapId: "fltTypes",
    btnId: "fltTypesBtn",
    panelId: "fltTypesPanel",
    labelId: "fltTypesLabel",
    searchId: "fltTypesSearch",
    listId: "fltTypesList",
    hiddenId: "fltTypesHidden",
    clearId: "fltTypesClear",
    allId: "fltTypesAll",
    inputName: "activityType",
  });

  // GET (TOPO) - Status
  initMultiSelect({
    wrapId: "fltStatus",
    btnId: "fltStatusBtn",
    panelId: "fltStatusPanel",
    labelId: "fltStatusLabel",
    searchId: "fltStatusSearch",
    listId: "fltStatusList",
    hiddenId: "fltStatusHidden",
    clearId: "fltStatusClear",
    allId: "fltStatusAll",
    inputName: "status",
  });
})();