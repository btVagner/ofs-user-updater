(function () {
    const table = document.querySelector(".error-owners-table");
    if (!table) return;

    const tbody = table.querySelector("tbody");
    const headers = table.querySelectorAll("th.sortable");

    if (!tbody || !headers.length) return;

    let currentSort = "";
    let currentDirection = "desc";

    function getRows() {
        return Array.from(tbody.querySelectorAll("tr"))
            .filter(row => !row.querySelector(".empty-row"));
    }

    function compareValues(a, b, key, direction) {
        let aVal = a.querySelector(`[data-${key}]`)?.dataset[key];
        let bVal = b.querySelector(`[data-${key}]`)?.dataset[key];

        if (key === "status" || key === "qtd") {
            aVal = Number(aVal || 0);
            bVal = Number(bVal || 0);
        } else {
            aVal = String(aVal || "").toLowerCase();
            bVal = String(bVal || "").toLowerCase();
        }

        if (aVal < bVal) return direction === "asc" ? -1 : 1;
        if (aVal > bVal) return direction === "asc" ? 1 : -1;
        return 0;
    }

    function updateHeaderState(activeKey, direction) {
        headers.forEach(th => {
            th.classList.remove("sort-asc", "sort-desc");

            if (th.dataset.sort === activeKey) {
                th.classList.add(direction === "asc" ? "sort-asc" : "sort-desc");
            }
        });
    }

    function sortTable(key) {
        const rows = getRows();
        if (!rows.length) return;

        let direction = "desc";

        if (currentSort === key) {
            direction = currentDirection === "asc" ? "desc" : "asc";
        }

        rows.sort((a, b) => compareValues(a, b, key, direction));

        tbody.innerHTML = "";
        rows.forEach(row => tbody.appendChild(row));

        currentSort = key;
        currentDirection = direction;
        updateHeaderState(key, direction);
    }

    headers.forEach(th => {
        th.addEventListener("click", function () {
            const key = this.dataset.sort;
            if (!key) return;
            sortTable(key);
        });
    });
})();