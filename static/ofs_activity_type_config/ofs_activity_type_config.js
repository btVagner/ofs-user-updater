(function () {
  const searchInput = document.getElementById("searchType");
  const statusFilter = document.getElementById("statusFilter");
  const sortFilter = document.getElementById("sortFilter");
  const tbody = document.getElementById("activityTypesTableBody");
  const counter = document.getElementById("tableCounter");
  const emptyStateRow = document.getElementById("emptyStateRow");

  if (!tbody) return;

  function getRows() {
    return Array.from(tbody.querySelectorAll(".table-row"));
  }

  function normalize(value) {
    return String(value || "").toLowerCase().trim();
  }

  function matchesFilters(row) {
    const term = normalize(searchInput?.value);
    const status = statusFilter?.value || "all";

    
    const code = normalize(row.dataset.code);
    const description = normalize(row.dataset.description);
    const rowStatus = row.dataset.status || "";
    
    const matchesSearch =
      !term || code.includes(term) || description.includes(term);

    const matchesStatus =
      status === "all" || rowStatus === status;

    return matchesSearch && matchesStatus;
  }

  function sortRows(rows) {
    const mode = sortFilter?.value || "volume_desc";

    rows.sort((a, b) => {
      const qtdA = Number(a.dataset.qtd || 0);
      const qtdB = Number(b.dataset.qtd || 0);
      const codeA = String(a.dataset.code || "");
      const codeB = String(b.dataset.code || "");
      const statusA = a.dataset.status || "";
      const statusB = b.dataset.status || "";

      if (mode === "code_asc") {
        return codeA.localeCompare(codeB);
      }

      if (mode === "status_first") {
        if (statusA !== statusB) {
          return statusA === "pending" ? -1 : 1;
        }
        return qtdB - qtdA;
      }

      return qtdB - qtdA;
    });

    return rows;
  }

  function applyFilters() {
    const allRows = getRows();
    const filtered = allRows.filter(matchesFilters);
    const sorted = sortRows(filtered);

    allRows.forEach(row => {
      row.style.display = "none";
    });

    sorted.forEach(row => {
      row.style.display = "";
      tbody.appendChild(row);
    });

    if (counter) {
      counter.textContent = `${sorted.length} registros`;
    }

    if (emptyStateRow) {
      emptyStateRow.style.display = sorted.length ? "none" : "";
      if (!sorted.length) tbody.appendChild(emptyStateRow);
    }
  }

  [searchInput, statusFilter, sortFilter].forEach(el => {
    if (!el) return;
    el.addEventListener("input", applyFilters);
    el.addEventListener("change", applyFilters);
  });

  applyFilters();
})();