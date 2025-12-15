(function () {
  const table = document.getElementById("logs-table");
  if (!table) return;

  const thead = table.querySelector("thead");
  const tbody = table.querySelector("tbody");
  const headers = Array.from(thead.querySelectorAll("th"));

  // Mantém cópia das linhas (para reordenar sem perder)
  function getRows() {
    return Array.from(tbody.querySelectorAll("tr"))
      .filter(tr => tr.querySelectorAll("td").length > 1); // ignora linha "Nenhum registro"
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
    const td = row.children[index];
    const raw = (td ? td.textContent : "").trim();

    if (kind === "datetime") return parseDatetimeBR(raw);
    // text
    return raw.toLowerCase();
  }

  function clearSortState() {
    headers.forEach(h => {
      h.classList.remove("sort-asc");
      h.classList.remove("sort-desc");
    });
  }

  function sortByColumn(colIndex, kind, direction) {
    const rows = getRows();

    rows.sort((a, b) => {
      const av = getCellValue(a, colIndex, kind);
      const bv = getCellValue(b, colIndex, kind);

      if (av < bv) return direction === "asc" ? -1 : 1;
      if (av > bv) return direction === "asc" ? 1 : -1;
      return 0;
    });

    // re-render
    const frag = document.createDocumentFragment();
    rows.forEach(r => frag.appendChild(r));
    tbody.appendChild(frag);
  }

  headers.forEach((th, idx) => {
    th.addEventListener("click", () => {
      const kind = th.getAttribute("data-sort") || "text";
      const isAsc = th.classList.contains("sort-asc");
      const isDesc = th.classList.contains("sort-desc");

      clearSortState();

      // alterna direção
      let direction = "asc";
      if (!isAsc && !isDesc) direction = "asc";
      else if (isAsc) direction = "desc";
      else direction = "asc";

      th.classList.add(direction === "asc" ? "sort-asc" : "sort-desc");
      sortByColumn(idx, kind, direction);
    });
  });


})();
