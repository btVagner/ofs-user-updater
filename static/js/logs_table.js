(function () {
  const table = document.getElementById("logs-table");
  if (!table) return;

  const thead = table.querySelector("thead");
  const tbody = table.querySelector("tbody");
  const headers = Array.from(thead.querySelectorAll("th"));

  // Modal elements
  const modal = document.getElementById("apiRespModal");
  const modalContent = document.getElementById("apiRespContent");
  const modalCloseBtn = document.getElementById("apiRespClose");

  /* ===========================
   * MODAL – Resposta da API
   * =========================== */

  function openApiModal(text) {
    if (!modal || !modalContent) return;

    let pretty = text || "";
    try {
      const obj = JSON.parse(pretty);
      pretty = JSON.stringify(obj, null, 2);
    } catch (e) {
      // mantém texto como está (não era JSON puro)
    }

    modalContent.textContent = pretty;
    modal.classList.add("is-open");
  }

  function closeApiModal() {
    if (!modal || !modalContent) return;
    modal.classList.remove("is-open");
    modalContent.textContent = "";
  }

  // Clique no botão "Ver"
  document.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-api-id]");
    if (!btn) return;

    const id = btn.getAttribute("data-api-id");
    const script = document.getElementById("apiresp-" + id);

    if (!script) {
      openApiModal("Resposta da API não encontrada para este log.");
      return;
    }

    /*
      O conteúdo vem assim (por causa do |tojson no template):
      "\"{\\\"motivo\\\":null, ... }\""

      1º JSON.parse -> vira string real
      2º JSON.parse (se aplicável) -> objeto
    */
    let txt = "";
    try {
      txt = JSON.parse(script.textContent);
    } catch (err) {
      txt = script.textContent || "";
    }

    openApiModal(txt);
  });

  // Fechar no botão
  if (modalCloseBtn) {
    modalCloseBtn.addEventListener("click", closeApiModal);
  }

  // Fechar clicando fora
  if (modal) {
    modal.addEventListener("click", (e) => {
      if (e.target === modal) closeApiModal();
    });
  }

  // Fechar com ESC
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeApiModal();
  });

  /* ===========================
   * ORDENAÇÃO DA TABELA
   * =========================== */

  function getRows() {
    return Array.from(tbody.querySelectorAll("tr"))
      .filter(tr => tr.querySelectorAll("td").length > 1); // ignora "Nenhum registro"
  }

  function parseDatetimeBR(s) {
    // "dd/mm/yyyy HH:MM:SS"
    const str = (s || "").trim();
    const m = str.match(/^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$/);
    if (!m) return 0;

    const dd = Number(m[1]);
    const mm = Number(m[2]) - 1;
    const yyyy = Number(m[3]);
    const HH = Number(m[4]);
    const MM = Number(m[5]);
    const SS = Number(m[6]);

    return new Date(yyyy, mm, dd, HH, MM, SS).getTime();
  }

  function getCellValue(row, index, kind) {
    if (kind === "none") return "";

    const td = row.children[index];
    const raw = (td ? td.textContent : "").trim();

    if (kind === "datetime") return parseDatetimeBR(raw);
    return raw.toLowerCase();
  }

  function clearSortState() {
    headers.forEach(h => {
      h.classList.remove("sort-asc");
      h.classList.remove("sort-desc");
    });
  }

  function sortByColumn(colIndex, kind, direction) {
    if (kind === "none") return;

    const rows = getRows();

    rows.sort((a, b) => {
      const av = getCellValue(a, colIndex, kind);
      const bv = getCellValue(b, colIndex, kind);

      if (av < bv) return direction === "asc" ? -1 : 1;
      if (av > bv) return direction === "asc" ? 1 : -1;
      return 0;
    });

    const frag = document.createDocumentFragment();
    rows.forEach(r => frag.appendChild(r));
    tbody.appendChild(frag);
  }

  headers.forEach((th, idx) => {
    th.addEventListener("click", () => {
      const kind = th.getAttribute("data-sort") || "text";
      if (kind === "none") return;

      const isAsc = th.classList.contains("sort-asc");
      const isDesc = th.classList.contains("sort-desc");

      clearSortState();

      let direction = "asc";
      if (!isAsc && !isDesc) direction = "asc";
      else if (isAsc) direction = "desc";
      else direction = "asc";

      th.classList.add(direction === "asc" ? "sort-asc" : "sort-desc");
      sortByColumn(idx, kind, direction);
    });
  });
})();
