let dashboardDataCache = null;
let selectedTypeLabels = new Set();
let chartByOwnerInstance = null;
let chartTopSapMessagesInstance = null;
let chartSapByCategoryInstance = null;
(function () {
    const pageEl = document.getElementById("dashboardPage");
    if (!pageEl) return;

    const ownerData = JSON.parse(pageEl.dataset.owner || "[]");

    const dateFrom = pageEl.dataset.dateFrom || "";
    const dateTo = pageEl.dataset.dateTo || "";
    const resources = pageEl.dataset.resources || "";

    const dashboardDataUrl = pageEl.dataset.urlDashboardData || "";
    const exportTopMessagesUrl = pageEl.dataset.urlExportTopMessages || "";

    let chartByDayInstance = null;
    let chartByTypeInstance = null;
    let chartTopMessagesInstance = null;

    function shorten(text, max = 42) {
        const value = String(text || "");
        if (value.length <= max) return value;
        return value.slice(0, max - 3) + "...";
    }

    function defaultNoDataPlugin() {
        return {
            id: "noDataPlugin",
            afterDraw(chart) {
                const hasData = chart.data?.datasets?.some(ds =>
                    Array.isArray(ds.data) && ds.data.some(v => Number(v) > 0)
                );

                if (hasData) return;

                const { ctx, chartArea } = chart;
                if (!chartArea) return;

                ctx.save();
                ctx.textAlign = "center";
                ctx.textBaseline = "middle";
                ctx.font = "14px sans-serif";
                ctx.fillStyle = "#9aa0a6";
                ctx.fillText(
                    "Sem dados no período",
                    (chartArea.left + chartArea.right) / 2,
                    (chartArea.top + chartArea.bottom) / 2
                );
                ctx.restore();
            }
        };
    }

    Chart.register(defaultNoDataPlugin());
    const valueLabelPlugin = {
        id: "valueLabelPlugin",
        afterDatasetsDraw(chart) {

            const { ctx } = chart;

            chart.data.datasets.forEach((dataset, i) => {

                const meta = chart.getDatasetMeta(i);

                const total = dataset.data.reduce((a, b) => a + b, 0);

                meta.data.forEach((element, index) => {

                    const value = dataset.data[index];

                    if (!value) return;

                    const percent = ((value / total) * 100).toFixed(1);

                    const label = `${value} (${percent}%)`;

                    const pos = element.tooltipPosition();

                    ctx.save();
                    ctx.fillStyle = "#ffffff";
                    ctx.font = "11px sans-serif";
                    ctx.textAlign = "center";
                    ctx.textBaseline = "middle";

                    const width = ctx.measureText(label).width;

                    // se barra pequena demais não mostra
                    if (width > element.width) {
                        ctx.restore();
                        return;
                    }

                    ctx.fillText(label, pos.x, pos.y);
                    ctx.restore();

                });

            });
        }
    };

    Chart.register(valueLabelPlugin);
    const commonOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                labels: {
                    color: "#d8d8d8",
                    font: { size: 12 }
                }
            },
            tooltip: {
                backgroundColor: "rgba(20,20,20,0.95)",
                titleColor: "#ffffff",
                bodyColor: "#d8d8d8",
                borderColor: "#3a3a3a",
                borderWidth: 1
            }
        },
        scales: {
            x: {
                ticks: { color: "#cfcfcf" },
                grid: { color: "rgba(255,255,255,0.06)" }
            },
            y: {
                beginAtZero: true,
                ticks: { color: "#cfcfcf", precision: 0 },
                grid: { color: "rgba(255,255,255,0.06)" }
            }
        }
    };

    function updateKpis(data) {
        const total = Number(data.total || 0);
        const totalNg = Number(data.total_ng || 0);
        const typesCount = Array.isArray(data.by_type) ? data.by_type.length : 0;
        const percent = total > 0 ? ((totalNg / total) * 100).toFixed(1) + "%" : "0%";

        const kpiTotal = document.getElementById("kpiTotal");
        const kpiTotalNg = document.getElementById("kpiTotalNg");
        const metricTotal = document.getElementById("metricTotal");
        const metricTotalNg = document.getElementById("metricTotalNg");
        const metricPercentNg = document.getElementById("metricPercentNg");
        const metricTypesCount = document.getElementById("metricTypesCount");

        if (kpiTotal) kpiTotal.textContent = total;
        if (kpiTotalNg) kpiTotalNg.textContent = totalNg;
        if (metricTotal) metricTotal.textContent = total;
        if (metricTotalNg) metricTotalNg.textContent = totalNg;
        if (metricPercentNg) metricPercentNg.textContent = percent;
        if (metricTypesCount) metricTypesCount.textContent = typesCount;
    }

    function buildCharts(data) {
        const byDay = Array.isArray(data.by_day) ? data.by_day : [];
        const byType = Array.isArray(data.by_type) ? data.by_type : [];
        const topMessages = Array.isArray(data.top_messages) ? data.top_messages : [];
        const topSapMessages = Array.isArray(data.top_sap_messages) ? data.top_sap_messages : [];
        const sapByCategory = Array.isArray(data.sap_by_category) ? data.sap_by_category : [];


        const byDayLabels = byDay.map(item => item.date);
        const byDayValues = byDay.map(item => Number(item.qtd || 0));

        const byTypeLabels = byType.map(item => item.activityTypeLabel || item.activityType || "-");
        const byTypeValues = byType.map(item => Number(item.qtd || 0));

        const topMessagesLabels = topMessages.slice(0, 10).map(item => shorten(item.msg, 55));
        const topMessagesValues = topMessages.slice(0, 10).map(item => Number(item.qtd || 0));

        const topSapMessagesLabels = topSapMessages.slice(0, 10).map(item => shorten(item.msg, 55));
        const topSapMessagesValues = topSapMessages.slice(0, 10).map(item => Number(item.qtd || 0));

        const sapCategoryLabels = sapByCategory.map(item => item.category || "-");
        const sapCategoryValues = sapByCategory.map(item => Number(item.qtd || 0));
        const ownerLabels = ownerData.map(i => i.responsavel);
        const ownerValues = ownerData.map(i => Number(i.qtd || 0));

        if (chartByOwnerInstance) chartByOwnerInstance.destroy();

        const ctxOwner = document.getElementById("chartByOwner");

        if (ctxOwner) {
            chartByOwnerInstance = new Chart(ctxOwner, {
                type: "doughnut",
                data: {
                    labels: ownerLabels,
                    datasets: [{
                        label: "Erros por responsável",
                        data: ownerValues
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: "right",
                            labels: {
                                color: "#d8d8d8",
                                boxWidth: 14,
                                padding: 14,
                                font: { size: 11 }
                            }
                        },
                        tooltip: {
                            callbacks: {
                                label(context) {
                                    const total = ownerValues.reduce((a, b) => a + b, 0);
                                    const value = context.raw;
                                    const percent = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                                    return `${context.label}: ${value} (${percent}%)`;
                                }
                            }
                        }
                    }
                }
            });
        }
        if (chartByDayInstance) chartByDayInstance.destroy();
        if (chartByTypeInstance) chartByTypeInstance.destroy();
        if (chartTopMessagesInstance) chartTopMessagesInstance.destroy();
        if (chartTopSapMessagesInstance) chartTopSapMessagesInstance.destroy();
        if (chartSapByCategoryInstance) chartSapByCategoryInstance.destroy();
        const ctxByDay = document.getElementById("chartByDay");
        if (ctxByDay) {
            chartByDayInstance = new Chart(ctxByDay, {
                type: "line",
                data: {
                    labels: byDayLabels,
                    datasets: [{
                        label: "Erros por dia",
                        data: byDayValues,
                        tension: 0.35,
                        fill: true
                    }]
                },
                options: {
                    ...commonOptions,
                    plugins: {
                        ...commonOptions.plugins,
                        legend: { display: true }
                    }
                }
            });
        }

        const ctxByType = document.getElementById("chartByType");
        if (ctxByType) {
            chartByTypeInstance = new Chart(ctxByType, {
                type: "doughnut",
                data: {
                    labels: byTypeLabels,
                    datasets: [{
                        label: "Erros por tipo",
                        data: byTypeValues
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: "right",
                            labels: {
                                color: "#d8d8d8",
                                boxWidth: 14,
                                padding: 14,
                                font: { size: 11 }
                            }
                        },
                        tooltip: {
                            backgroundColor: "rgba(20,20,20,0.95)",
                            titleColor: "#ffffff",
                            bodyColor: "#d8d8d8",
                            borderColor: "#3a3a3a",
                            borderWidth: 1
                        }
                    }
                }
            });
        }

        const ctxTopMessages = document.getElementById("chartTopMessages");
        if (ctxTopMessages) {
            chartTopMessagesInstance = new Chart(ctxTopMessages, {
                type: "bar",
                data: {
                    labels: topMessagesLabels,
                    datasets: [{
                        label: "Qtd",
                        data: topMessagesValues
                    }]
                },
                options: {
                    ...commonOptions,
                    indexAxis: "y",
                    plugins: {
                        ...commonOptions.plugins,
                        legend: { display: false }
                    },
                    scales: {
                        x: {
                            beginAtZero: true,
                            ticks: { color: "#cfcfcf", precision: 0 },
                            grid: { color: "rgba(255,255,255,0.06)" }
                        },
                        y: {
                            ticks: { color: "#cfcfcf" },
                            grid: { display: false }
                        }
                    }
                }
            });
        }
        const ctxTopSapMessages = document.getElementById("chartTopSapMessages");
        if (ctxTopSapMessages) {
            chartTopSapMessagesInstance = new Chart(ctxTopSapMessages, {
                type: "bar",
                data: {
                    labels: topSapMessagesLabels,
                    datasets: [{
                        label: "Qtd",
                        data: topSapMessagesValues
                    }]
                },
                options: {
                    ...commonOptions,
                    indexAxis: "y",
                    plugins: {
                        ...commonOptions.plugins,
                        legend: { display: false }
                    },
                    scales: {
                        x: {
                            beginAtZero: true,
                            ticks: { color: "#cfcfcf", precision: 0 },
                            grid: { color: "rgba(255,255,255,0.06)" }
                        },
                        y: {
                            ticks: { color: "#cfcfcf" },
                            grid: { display: false }
                        }
                    }
                }
            });
        }
        const ctxSapByCategory = document.getElementById("chartSapByCategory");
        if (ctxSapByCategory) {
            chartSapByCategoryInstance = new Chart(ctxSapByCategory, {
                type: "doughnut",
                data: {
                    labels: sapCategoryLabels,
                    datasets: [{
                        label: "Erros SAP por categoria",
                        data: sapCategoryValues
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: "right",
                            labels: {
                                color: "#d8d8d8",
                                boxWidth: 14,
                                padding: 14,
                                font: { size: 11 }
                            }
                        },
                        tooltip: {
                            callbacks: {
                                label(context) {
                                    const total = sapCategoryValues.reduce((a, b) => a + b, 0);
                                    const value = context.raw;
                                    const percent = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                                    return `${context.label}: ${value} (${percent}%)`;
                                }
                            }
                        }

                    }
                }
            });
        }
    }

    function openTypeFilterModal() {
        const modal = document.getElementById("typeFilterModal");
        const backdrop = document.getElementById("typeFilterBackdrop");
        if (modal) modal.style.display = "block";
        if (backdrop) backdrop.style.display = "block";
    }

    function closeTypeFilterModal() {
        const modal = document.getElementById("typeFilterModal");
        const backdrop = document.getElementById("typeFilterBackdrop");
        if (modal) modal.style.display = "none";
        if (backdrop) backdrop.style.display = "none";
    }

    function renderTypeFilterList() {
        const list = document.getElementById("typeFilterList");
        const search = document.getElementById("typeFilterSearch");
        if (!list || !dashboardDataCache) return;

        const term = String(search?.value || "").toLowerCase().trim();
        const byType = Array.isArray(dashboardDataCache.by_type) ? dashboardDataCache.by_type : [];

        const filtered = byType.filter(item => {
            const label = String(item.activityTypeLabel || item.activityType || "-").toLowerCase();
            const code = String(item.activityType || "-").toLowerCase();
            return !term || label.includes(term) || code.includes(term);
        });

        list.innerHTML = filtered.map(item => {
            const code = item.activityType || "-";
            const label = item.activityTypeLabel || code;
            const qtd = Number(item.qtd || 0);
            const checked = selectedTypeLabels.has(label) ? "checked" : "";

            return `
                <label class="type-filter-item">
                    <div class="type-filter-left">
                        <input type="checkbox" class="js-type-filter-check" value="${label}" ${checked}>
                        <div class="type-filter-labels">
                            <span class="type-filter-name">${label}</span>
                            <span class="type-filter-code">${code}</span>
                        </div>
                    </div>
                    <div class="type-filter-qtd">${qtd}</div>
                </label>
            `;
        }).join("");
    }
    function buildByDayFiltered() {
        if (!dashboardDataCache) return [];

        const rows = Array.isArray(dashboardDataCache.by_day_type)
            ? dashboardDataCache.by_day_type
            : [];

        const map = new Map();

        rows.forEach(item => {
            const label = item.activityTypeLabel || item.activityType || "-";

            if (!selectedTypeLabels.has(label)) return;

            const date = item.date;
            const qtd = Number(item.qtd || 0);

            map.set(date, (map.get(date) || 0) + qtd);
        });

        return Array.from(map.entries()).map(([date, qtd]) => ({
            date,
            qtd
        }));
    }
    function applyTypeFilterToChart() {
        if (!dashboardDataCache) return;

        const filteredByType = (dashboardDataCache.by_type || []).filter(item => {
            const label = item.activityTypeLabel || item.activityType || "-";
            return selectedTypeLabels.has(label);
        });

        const filteredByDay = buildByDayFiltered();

        buildCharts({
            ...dashboardDataCache,
            by_type: filteredByType,
            by_day: filteredByDay
        });
    }

    async function loadDashboardData() {
        const params = new URLSearchParams({
            dateFrom,
            dateTo,
            resources
        });

        console.log("dashboardDataUrl:", dashboardDataUrl);
        console.log("params:", params.toString());

        const resp = await fetch(`${dashboardDataUrl}?${params.toString()}`);
        const data = await resp.json().catch(() => ({}));

        if (!resp.ok || !data.ok) {
            throw new Error(data.error || "Erro ao carregar dados do dashboard");
        }
        
        return data;
    }

    async function init() {
        try {
            const data = await loadDashboardData();

            dashboardDataCache = data;

            const byType = Array.isArray(data.by_type) ? data.by_type : [];
            selectedTypeLabels = new Set(
                byType.map(item => item.activityTypeLabel || item.activityType || "-")
            );

            updateKpis(data);
            buildCharts(data);
            renderTypeFilterList();
        } catch (e) {
            console.error("Erro ao carregar dashboard:", e);
        }
    }

    document.getElementById("btnOpenTypeFilter")?.addEventListener("click", () => {
        renderTypeFilterList();
        openTypeFilterModal();
    });

    document.getElementById("btnCloseTypeFilter")?.addEventListener("click", closeTypeFilterModal);
    document.getElementById("typeFilterBackdrop")?.addEventListener("click", closeTypeFilterModal);

    document.getElementById("typeFilterSearch")?.addEventListener("input", () => {
        renderTypeFilterList();
    });

    document.getElementById("btnMarkAllTypes")?.addEventListener("click", () => {
        if (!dashboardDataCache) return;

        selectedTypeLabels = new Set(
            (dashboardDataCache.by_type || []).map(item => item.activityTypeLabel || item.activityType || "-")
        );

        renderTypeFilterList();
    });

    document.getElementById("btnUnmarkAllTypes")?.addEventListener("click", () => {
        selectedTypeLabels = new Set();
        renderTypeFilterList();
    });

    document.getElementById("typeFilterList")?.addEventListener("change", (ev) => {
        const input = ev.target.closest(".js-type-filter-check");
        if (!input) return;

        const value = input.value;
        if (input.checked) {
            selectedTypeLabels.add(value);
        } else {
            selectedTypeLabels.delete(value);
        }
    });

    document.getElementById("btnApplyTypeFilter")?.addEventListener("click", () => {
        applyTypeFilterToChart();
        closeTypeFilterModal();
    });
    function toggleConfigMenu() {
        const menu = document.getElementById("configMenu");
        if (!menu) return;
        menu.classList.toggle("show");
    }

    function closeConfigMenu() {
        const menu = document.getElementById("configMenu");
        if (!menu) return;
        menu.classList.remove("show");
    }
    document.getElementById("btnConfigMenu")?.addEventListener("click", (ev) => {
        ev.stopPropagation();
        toggleConfigMenu();
    });

    document.addEventListener("click", (ev) => {
        const dropdown = ev.target.closest(".dashboard-dropdown");
        if (!dropdown) {
            closeConfigMenu();
        }
    });
    function exportTopMessages() {
        const pageEl = document.getElementById("dashboardPage");
        if (!pageEl) return;

        const dateFrom = pageEl.dataset.dateFrom || "";
        const dateTo = pageEl.dataset.dateTo || "";

        const params = new URLSearchParams({
            dateFrom,
            dateTo
        });

        const url = `${exportTopMessagesUrl}?${params.toString()}`;

        window.location.href = url;
    }

    document.getElementById("btnExportTopMessages")?.addEventListener("click", exportTopMessages);
    init();
})();