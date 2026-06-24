document.addEventListener("DOMContentLoaded", function () {
    const root = document.getElementById("dashboard-root");
    if (!root) return;

    const statusUrl = root.dataset.statusUrl;
    const currentUpdatedAt = root.dataset.currentUpdatedAt || "";
    const refreshCard = document.querySelector("[data-dashboard-refresh-card]");
    const newDataBox = document.querySelector("[data-dashboard-new-data]");
    const reloadButton = document.querySelector("[data-dashboard-reload]");
    const payloadScript = document.getElementById("dashboard-payload");
    const typeFilter = document.querySelector("[data-dashboard-type-filter]");
    const statusChart = document.querySelector("[data-dashboard-status-chart]");
    const lineChart = document.querySelector("[data-dashboard-line-chart]");
    const hourlyLineChart = document.querySelector("[data-dashboard-hourly-line-chart]");
    const b2cTypesChart = document.querySelector("[data-dashboard-b2c-types-chart]");
    const redesTypesChart = document.querySelector("[data-dashboard-redes-types-chart]");
    const b2cCitiesChart = document.querySelector("[data-dashboard-b2c-cities-chart]");
    const redesCitiesChart = document.querySelector("[data-dashboard-redes-cities-chart]");
    const btnSelectAll = document.querySelector("[data-dashboard-select-all]");
    const btnClearAll = document.querySelector("[data-dashboard-clear-all]");
    const payload = parsePayload(payloadScript);
    const filterCount = document.querySelector("[data-dashboard-filter-count]");
    const filterSearch = document.querySelector("[data-dashboard-filter-search]");
    const btnSelectB2c = document.querySelector("[data-dashboard-select-b2c]");
    const btnSelectRedes = document.querySelector("[data-dashboard-select-redes]");
    const unlockUrl = root.dataset.unlockUrl;
    const canUnlock = root.dataset.canUnlock === "1";
    const ratingChart = document.querySelector("[data-dashboard-rating-chart]");
    const ratingCategoriesChart = document.querySelector("[data-dashboard-rating-categories]");
    const ratingSubcategoriesChart = document.querySelector("[data-dashboard-rating-subcategories]");
    const criticalRatingsTable = document.querySelector("[data-dashboard-critical-ratings]");
    const selectedLineComparisonPointsByChart = {
        full: [],
        hourly: []
    };
    if (!statusUrl) return;

    function setRefreshMessage(type, title, subtitle, progressPercent, progressMessage, showUnlock) {
        if (!refreshCard) return;

        const hasProgress = progressPercent !== null && progressPercent !== undefined && progressPercent !== "";
        const safeProgress = Math.max(0, Math.min(Number(progressPercent || 0), 100));
        const progressHtml = hasProgress ? `
        <div class="dashboard-progress">
            <div class="dashboard-progress-head">
                <span>${escapeHtml(progressMessage || "Atualizando dados")}</span>
                <strong>${safeProgress}%</strong>
            </div>
            <div class="dashboard-progress-track">
                <span style="width: ${safeProgress}%"></span>
            </div>
        </div>
    ` : "";

        const unlockHtml = showUnlock && canUnlock && unlockUrl ? `
        <button type="button" class="dashboard-unlock-btn" data-dashboard-unlock>
            Destravar atualização
        </button>
    ` : "";

        refreshCard.innerHTML = `
        <span class="dashboard-refresh-status ${type || ""}">${title}</span>
        <small>${subtitle || ""}</small>
        ${progressHtml}
        ${unlockHtml}
    `;

        const unlockButton = refreshCard.querySelector("[data-dashboard-unlock]");
        if (unlockButton) {
            unlockButton.addEventListener("click", unlockDashboardUpdate);
        }
    }
    async function unlockDashboardUpdate() {
        if (!unlockUrl) return;

        const confirmed = window.confirm("Deseja destravar a atualização do dashboard?");
        if (!confirmed) return;

        try {
            const response = await fetch(unlockUrl, {
                method: "POST",
                headers: {
                    "Accept": "application/json"
                }
            });

            const data = await response.json().catch(function () {
                return {};
            });

            if (!response.ok || data.ok === false) {
                throw new Error(data.error || "Falha ao destravar atualização.");
            }

            window.location.reload();

        } catch (error) {
            alert(error.message || "Falha ao destravar atualização.");
        }
    }
    function showNewDataMessage() {
        if (!newDataBox) return;
        newDataBox.classList.remove("hidden");
    }

    if (reloadButton) {
        reloadButton.addEventListener("click", function () {
            window.location.reload();
        });
    }

    if (typeFilter) {
        typeFilter.addEventListener("change", renderFilteredBlocks);
    }
    if (filterSearch) {
        filterSearch.addEventListener("input", function () {
            const term = filterSearch.value.trim().toLowerCase();

            typeFilter.querySelectorAll(".dashboard-type-chip").forEach((chip) => {
                const text = chip.textContent.toLowerCase();
                chip.classList.toggle("hidden", term && !text.includes(term));
            });
        });
    }
    if (btnSelectAll) {
        btnSelectAll.addEventListener("click", function () {
            setAllTypes(true);
        });
        if (btnSelectB2c) {
            btnSelectB2c.addEventListener("click", function () {
                setTypeGroup("b2c");
            });
        }

        if (btnSelectRedes) {
            btnSelectRedes.addEventListener("click", function () {
                setTypeGroup("redes");
            });
        }
    }

    if (btnClearAll) {
        btnClearAll.addEventListener("click", function () {
            setAllTypes(false);
        });
    }

    async function checkStatus() {
        try {
            const response = await fetch(statusUrl, {
                method: "GET",
                headers: {
                    "Accept": "application/json"
                }
            });

            const data = await response.json().catch(function () {
                return {};
            });

            if (!response.ok) {
                throw new Error(data.error || "Falha ao consultar status.");
            }

            if (data.status === "running") {
                if (data.has_payload) {
                    setRefreshMessage(
                        "running",
                        "Atualizando em segundo plano",
                        "Exibindo última versão disponível.",
                        data.progress_percent,
                        data.progress_message,
                        true
                    );
                } else {
                    setRefreshMessage(
                        "running",
                        "Preparando dashboard",
                        "A primeira carga está em andamento.",
                        data.progress_percent,
                        data.progress_message,
                        true
                    );
                }
                return;
            }

            if (data.status === "failed") {
                if (data.has_payload) {
                    setRefreshMessage("failed", "Não foi possível atualizar agora", "Exibindo última versão disponível.");
                } else {
                    setRefreshMessage("failed", "Falha ao preparar dashboard", "Verifique a conexão com o OFS.");
                }
                return;
            }

            if (data.status === "completed") {
                setRefreshMessage("", "Dados atualizados", data.updated_at || "-");

                if (data.updated_at && data.has_payload && data.updated_at !== currentUpdatedAt) {
                    showNewDataMessage();
                }
            }
        } catch (error) {
            setRefreshMessage("failed", "Status indisponível", "Não foi possível consultar o status do dashboard.");
        }
    }

    window.setTimeout(checkStatus, 2000);
    window.setInterval(checkStatus, 15000);

    renderFilteredBlocks();
    renderCustomerThermometer();
    function parsePayload(script) {
        if (!script) return {};

        try {
            return JSON.parse(script.textContent || "{}");
        } catch (error) {
            return {};
        }
    }

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }

    function getSelectedTypes() {
        if (!typeFilter) return new Set();

        return new Set(
            Array.from(typeFilter.querySelectorAll("input[type='checkbox']:checked"))
                .map((input) => input.value)
        );
    }
    function updateFilterCount() {
        if (!filterCount || !typeFilter) return;

        const total = typeFilter.querySelectorAll("input[type='checkbox']").length;
        const selected = typeFilter.querySelectorAll("input[type='checkbox']:checked").length;

        if (selected === total) {
            filterCount.textContent = "Todos selecionados";
            return;
        }

        if (selected === 0) {
            filterCount.textContent = "Nenhum selecionado";
            return;
        }

        filterCount.textContent = `${selected} de ${total} selecionados`;
    }
    function setAllTypes(checked) {
        if (!typeFilter) return;

        typeFilter.querySelectorAll("input[type='checkbox']").forEach((input) => {
            input.checked = checked;
        });

        renderFilteredBlocks();
    }
    function setTypeGroup(group) {
        if (!typeFilter) return;

        typeFilter.querySelectorAll(".dashboard-type-chip").forEach((chip) => {
            const input = chip.querySelector("input[type='checkbox']");
            if (!input) return;

            input.checked = chip.dataset.group === group;
        });

        renderFilteredBlocks();
    }
    function getFilteredRows() {
        const rows = Array.isArray(payload.dashboard_rows) ? payload.dashboard_rows : [];
        const selectedTypes = getSelectedTypes();

        if (!selectedTypes.size) return [];

        return rows.filter((row) => {
            const filterCode = row.activityTypeFilterCode || row.activityType;
            return selectedTypes.has(filterCode);
        });
    }

    function completionRate(total, completed) {
        if (!total) return 0;
        return Math.round((completed / total) * 10000) / 100;
    }

    function renderFilteredBlocks() {
        const rows = getFilteredRows();
        const allRows = Array.isArray(payload.dashboard_rows) ? payload.dashboard_rows : [];
        const today = payload.periods ? payload.periods.today : "";

        updateFilterCount();

        renderTypeChart(allRows, today, "b2c", b2cTypesChart, "Nenhum tipo B2C encontrado hoje.");
        renderTypeChart(allRows, today, "redes", redesTypesChart, "Nenhum tipo de Redes/B2B encontrado hoje.");

        renderStatusChart(rows, today);

        renderLineChart(rows, lineChart, {
            chartKey: "full",
            title: "Evolução últimos 7 dias",
            emptyMessage: "Nenhuma atividade encontrada para o filtro selecionado."
        });

        renderLineChart(rows, hourlyLineChart, {
            chartKey: "hourly",
            title: "Evolução até o horário atual",
            emptyMessage: "Nenhuma atividade encontrada até o horário atual.",
            untilTime: payload.periods ? payload.periods.comparison_until_time : ""
        });

        renderCityChart(rows, today, "b2c", b2cCitiesChart, "Nenhuma cidade B2C encontrada para o filtro.");
        renderCityChart(rows, today, "redes", redesCitiesChart, "Nenhuma cidade de Redes/B2B encontrada para o filtro.");
    }

    function renderStatusChart(rows, today) {
        if (!statusChart) return;

        const todayRows = rows.filter((row) => row.date === today);
        const counts = {};

        todayRows.forEach((row) => {
            const status = row.status || "nao_informado";
            counts[status] = (counts[status] || 0) + 1;
        });

        const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
        const total = entries.reduce((sum, item) => sum + item[1], 0);

        if (!total) {
            statusChart.innerHTML = `<p class="dashboard-muted">Nenhuma atividade encontrada para o filtro selecionado.</p>`;
            return;
        }

        const max = Math.max(...entries.map((item) => item[1]), 1);

        statusChart.innerHTML = `
            <div class="dashboard-status-total">
                <strong>${total}</strong>
                <span>atividades hoje</span>
            </div>
            <div class="dashboard-status-bars">
                ${entries.map(([status, value]) => {
            const width = Math.max(5, Math.round((value / max) * 100));
            const percent = Math.round((value / total) * 100);

            return `
                        <div class="dashboard-status-item">
                            <div class="dashboard-status-item-head">
                                <span>${escapeHtml(status)}</span>
                                <strong>${value} <small>${percent}%</small></strong>
                            </div>
                            <div class="dashboard-status-track">
                                <span style="width: ${width}%"></span>
                            </div>
                        </div>
                    `;
        }).join("")}
            </div>
        `;
    }
    function renderBarChart(target, config) {
        if (!target) return;

        const entries = config.entries || [];
        const total = Number(config.total || 0);

        if (!total || !entries.length) {
            target.innerHTML = `<p class="dashboard-muted">${escapeHtml(config.emptyMessage || "Nenhum dado encontrado.")}</p>`;
            return;
        }

        const max = Math.max(...entries.map((item) => item.value), 1);

        target.innerHTML = `
        <div class="dashboard-status-total">
            <strong>${total}</strong>
            <span>${escapeHtml(config.totalLabel || "registros")}</span>
        </div>
        <div class="dashboard-status-bars">
            ${entries.map((item) => {
            const value = Number(item.value || 0);
            const width = Math.max(5, Math.round((value / max) * 100));
            const percent = Math.round((value / total) * 100);

            return `
                    <div class="dashboard-status-item">
                        <div class="dashboard-status-item-head">
                            <span>${escapeHtml(item.label || "Não informado")}</span>
                            <strong>${value} <small>${percent}%</small></strong>
                        </div>
                        <div class="dashboard-status-track">
                            <span style="width: ${width}%"></span>
                        </div>
                    </div>
                `;
        }).join("")}
        </div>
    `;
    }

    function renderTypeChart(rows, today, group, target, emptyMessage) {
        const map = {};

        rows.filter((row) => row.date === today && row.group === group).forEach((row) => {
            const label = row.activityTypeLabel || row.activityType || "Não informado";
            map[label] = (map[label] || 0) + 1;
        });

        const entries = Object.entries(map)
            .map(([label, value]) => ({ label, value }))
            .sort((a, b) => b.value - a.value);

        renderBarChart(target, {
            total: entries.reduce((sum, item) => sum + item.value, 0),
            totalLabel: "atividades hoje",
            entries,
            emptyMessage
        });
    }

    function renderLineChart(rows, target, options) {
        if (!target) return;

        options = options || {};
        const chartKey = options.chartKey || "full";
        const selectedLineComparisonPoints = selectedLineComparisonPointsByChart[chartKey] || [];
        const periods = payload.periods || {};
        const dates = buildDateRange(periods.last_7_days_from, periods.last_7_days_to);
        const series = buildEvolutionSeries(rows, dates, options.untilTime);

        const completed = series.completed;
        const notdone = series.notdone;
        const maxValue = Math.max(...completed, ...notdone, 1);

        const width = 960;
        const height = 320;
        const padX = 58;
        const padY = 36;
        const plotWidth = width - padX * 2;
        const plotHeight = height - padY * 2;

        if (!dates.length || (!completed.some(Boolean) && !notdone.some(Boolean))) {
            target.innerHTML = `<p class="dashboard-muted">${escapeHtml(options.emptyMessage || "Nenhum dado encontrado.")}</p>`;
            return;
        }

        const completedPoints = pointsFor(completed, maxValue, width, height, padX, padY, plotWidth, plotHeight)
            .map((point, index) => ({
                ...point,
                index,
                date: dates[index],
                series: "completed",
                label: "Completed"
            }));

        const notdonePoints = pointsFor(notdone, maxValue, width, height, padX, padY, plotWidth, plotHeight)
            .map((point, index) => ({
                ...point,
                index,
                date: dates[index],
                series: "notdone",
                label: "Notdone"
            }));

        const trendPoints = pointsFor(trendValues(completed), maxValue, width, height, padX, padY, plotWidth, plotHeight);

        const selectedPoints = selectedLineComparisonPoints
            .map((selected) => {
                const source = selected.series === "notdone" ? notdonePoints : completedPoints;
                return source[selected.index];
            })
            .filter(Boolean);

        const comparisonLine = selectedPoints.length === 2
            ? buildComparisonLineMarkup(selectedPoints[0], selectedPoints[1])
            : "";

        const comparisonSummary = selectedPoints.length === 2
            ? buildLineComparisonSummary(selectedPoints[0], selectedPoints[1])
            : `<span>Clique em dois pontos do gráfico para comparar Completed e Notdone.</span>`;

        target.innerHTML = `
        <svg class="dashboard-svg-chart dashboard-svg-chart-clean" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(options.title || "Evolução últimos 7 dias")}">
            ${[0, 1, 2, 3].map((index) => {
            const y = padY + (plotHeight / 3) * index;
            return `<line class="dashboard-grid-line-clean" x1="${padX}" y1="${y}" x2="${width - padX}" y2="${y}"></line>`;
        }).join("")}

            <path class="dashboard-line-clean completed" d="${smoothPath(completedPoints)}"></path>
            <path class="dashboard-line-clean notdone" d="${smoothPath(notdonePoints)}"></path>
            <path class="dashboard-line-clean trend" d="${smoothPath(trendPoints)}"></path>

            ${comparisonLine}

            ${completedPoints.map((point) => {
            const selected = selectedLineComparisonPoints.some((item) => {
                return item.series === "completed" && item.index === point.index;
            });

            return `
                    <circle
                        class="dashboard-line-point completed ${selected ? "selected" : ""}"
                        cx="${point.x}"
                        cy="${point.y}"
                        r="${selected ? 6 : 5}"
                        data-point-series="completed"
                        data-point-index="${point.index}"
                    ></circle>
                    <text class="dashboard-point-label-clean" x="${point.x}" y="${Math.max(16, point.y - 12)}" text-anchor="middle">
                        ${point.value}
                    </text>
                `;
        }).join("")}

            ${notdonePoints.map((point) => {
            const selected = selectedLineComparisonPoints.some((item) => {
                return item.series === "notdone" && item.index === point.index;
            });

            return `
                    <circle
                        class="dashboard-line-point notdone ${selected ? "selected" : ""}"
                        cx="${point.x}"
                        cy="${point.y}"
                        r="${selected ? 6 : 4.5}"
                        data-point-series="notdone"
                        data-point-index="${point.index}"
                    ></circle>
                    <text class="dashboard-point-label-clean notdone" x="${point.x}" y="${Math.min(height - 26, point.y + 20)}" text-anchor="middle">
                        ${point.value}
                    </text>
                `;
        }).join("")}

            ${dates.map((date, index) => {
            const x = completedPoints[index] ? completedPoints[index].x : padX;
            return `<text class="dashboard-axis-label-clean" x="${x}" y="${height - 8}" text-anchor="middle">${date.slice(5)}</text>`;
        }).join("")}
        </svg>

        <div class="dashboard-chart-legend dashboard-chart-legend-clean">
            <span><i class="completed"></i>Completed</span>
            <span><i class="notdone"></i>Notdone</span>
            <span><i class="trend"></i>Tendência</span>
        </div>

        <div class="dashboard-line-comparison">
            ${comparisonSummary}
        </div>
    `;

        target.querySelectorAll("[data-point-index][data-point-series]").forEach((point) => {
            point.addEventListener("click", function () {
                const index = Number(point.dataset.pointIndex);
                const series = point.dataset.pointSeries;

                const currentSelection = selectedLineComparisonPointsByChart[chartKey] || [];
                const exists = currentSelection.some((item) => {
                    return item.index === index && item.series === series;
                });

                if (exists) {
                    selectedLineComparisonPointsByChart[chartKey] = currentSelection.filter((item) => {
                        return !(item.index === index && item.series === series);
                    });
                } else {
                    if (currentSelection.length >= 2) {
                        currentSelection.shift();
                    }

                    currentSelection.push({ index, series });
                    selectedLineComparisonPointsByChart[chartKey] = currentSelection;
                }

                renderLineChart(rows, target, options);
            });
        });
    }

    function buildEvolutionSeries(rows, dates, untilTime) {
        const completedByDate = {};
        const notdoneByDate = {};

        dates.forEach((date) => {
            completedByDate[date] = 0;
            notdoneByDate[date] = 0;
        });

        rows.forEach((row) => {
            if (!(row.date in completedByDate)) return;
            if (untilTime && !rowFinishedUntilTime(row, untilTime)) return;

            if (row.status === "completed") {
                completedByDate[row.date] += 1;
            }

            if (row.status === "notdone") {
                notdoneByDate[row.date] += 1;
            }
        });

        return {
            completed: dates.map((date) => completedByDate[date] || 0),
            notdone: dates.map((date) => notdoneByDate[date] || 0)
        };
    }

    function rowFinishedUntilTime(row, untilTime) {
        const endMinutes = parseTimeToMinutes(row.endTime);
        const untilMinutes = parseTimeToMinutes(untilTime);

        if (endMinutes === null || untilMinutes === null) {
            return false;
        }

        return endMinutes <= untilMinutes;
    }

    function parseTimeToMinutes(value) {
        const text = String(value || "").trim();
        const match = text.match(/(?:^|\s|T)(\d{2}):(\d{2})(?::\d{2})?/);

        if (!match) return null;

        const hours = Number(match[1]);
        const minutes = Number(match[2]);

        if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) {
            return null;
        }

        return hours * 60 + minutes;
    }

    function buildLineComparisonSummary(firstPoint, secondPoint) {
        const diff = secondPoint.value - firstPoint.value;
        const percent = firstPoint.value
            ? Math.round((diff / firstPoint.value) * 1000) / 10
            : 0;

        const diffText = diff > 0 ? `+${diff}` : String(diff);
        const percentText = percent > 0 ? `+${percent}%` : `${percent}%`;

        return `
        <span>
            Comparando <strong>${firstPoint.label} ${firstPoint.date.slice(5)}</strong>
            com <strong>${secondPoint.label} ${secondPoint.date.slice(5)}</strong>:
            <strong>${diffText}</strong> atividades (${percentText})
        </span>
    `;
    }

    function buildComparisonLineMarkup(firstPoint, secondPoint) {
        const diff = secondPoint.value - firstPoint.value;
        const percent = firstPoint.value
            ? Math.round((diff / firstPoint.value) * 1000) / 10
            : 0;

        const diffText = diff > 0 ? `+${diff}` : String(diff);
        const percentText = percent > 0 ? `+${percent}%` : `${percent}%`;

        const label = `${firstPoint.label} x ${secondPoint.label}: ${diffText} (${percentText})`;
        const midX = (firstPoint.x + secondPoint.x) / 2;
        const midY = (firstPoint.y + secondPoint.y) / 2;
        const labelY = Math.max(24, midY - 14);
        const labelWidth = Math.max(120, label.length * 7 + 24);

        return `
        <line
            class="dashboard-comparison-line"
            x1="${firstPoint.x}"
            y1="${firstPoint.y}"
            x2="${secondPoint.x}"
            y2="${secondPoint.y}"
        ></line>

        <g class="dashboard-comparison-badge">
            <rect
                x="${midX - labelWidth / 2}"
                y="${labelY - 17}"
                width="${labelWidth}"
                height="26"
                rx="13"
            ></rect>
            <text x="${midX}" y="${labelY}" text-anchor="middle">
                ${escapeHtml(label)}
            </text>
        </g>
    `;
    }

    function buildDateRange(dateFrom, dateTo) {
        if (!dateFrom || !dateTo) return [];

        const dates = [];
        const current = new Date(dateFrom + "T00:00:00");
        const end = new Date(dateTo + "T00:00:00");

        while (current <= end && dates.length < 15) {
            dates.push(current.toISOString().slice(0, 10));
            current.setDate(current.getDate() + 1);
        }

        return dates;
    }

    function pointsFor(values, maxValue, width, height, padX, padY, plotWidth, plotHeight) {
        const step = values.length > 1 ? plotWidth / (values.length - 1) : 0;

        return values.map((value, index) => ({
            x: padX + step * index,
            y: height - padY - ((value / maxValue) * plotHeight),
            value
        }));
    }

    function smoothPath(points) {
        if (!points.length) return "";
        if (points.length === 1) return `M ${points[0].x} ${points[0].y}`;

        let path = `M ${points[0].x} ${points[0].y}`;

        for (let index = 1; index < points.length; index += 1) {
            const previous = points[index - 1];
            const current = points[index];
            const controlX = (previous.x + current.x) / 2;
            path += ` C ${controlX} ${previous.y}, ${controlX} ${current.y}, ${current.x} ${current.y}`;
        }

        return path;
    }

    function trendValues(values) {
        if (values.length <= 1) return values;

        const n = values.length;
        const sumX = values.reduce((sum, _value, index) => sum + index, 0);
        const sumY = values.reduce((sum, value) => sum + value, 0);
        const sumXY = values.reduce((sum, value, index) => sum + index * value, 0);
        const sumXX = values.reduce((sum, _value, index) => sum + index * index, 0);
        const denominator = n * sumXX - sumX * sumX;
        const slope = denominator ? (n * sumXY - sumX * sumY) / denominator : 0;
        const intercept = (sumY - slope * sumX) / n;

        return values.map((_value, index) => Math.max(0, intercept + slope * index));
    }

    function renderCityChart(rows, today, group, target, emptyMessage) {
        const cityMap = {};

        rows.filter((row) => row.date === today && row.group === group).forEach((row) => {
            const city = row.city || "Não informado";

            if (!cityMap[city]) {
                cityMap[city] = { city, total: 0, completed: 0, notdone: 0 };
            }

            cityMap[city].total += 1;
            if (row.status === "completed") cityMap[city].completed += 1;
            if (row.status === "notdone") cityMap[city].notdone += 1;
        });

        const entries = Object.values(cityMap)
            .sort((a, b) => b.total - a.total)
            .slice(0, 10)
            .map((row) => ({
                label: `${row.city} (${completionRate(row.total, row.completed)}%)`,
                value: row.total
            }));

        renderBarChart(target, {
            total: entries.reduce((sum, item) => sum + item.value, 0),
            totalLabel: "atividades hoje",
            entries,
            emptyMessage
        });
    }
    function renderCustomerThermometer() {
        const thermometer = payload.customer_thermometer || {};

        renderRatingDistribution(thermometer.rating_distribution || []);
        renderThermometerBarList(
            ratingCategoriesChart,
            thermometer.categories || [],
            "category",
            "Nenhuma categoria encontrada hoje."
        );
        renderThermometerBarList(
            ratingSubcategoriesChart,
            thermometer.subcategories || [],
            "subcategory",
            "Nenhuma subcategoria encontrada hoje."
        );
        renderCriticalRatingsTable(thermometer.critical_rows || []);
    }

    function renderRatingDistribution(rows) {
        if (!ratingChart) return;

        const entries = rows.map((row) => ({
            label: `${row.rating} estrela${Number(row.rating) === 1 ? "" : "s"}`,
            value: Number(row.total || 0)
        }));

        renderBarChart(ratingChart, {
            total: entries.reduce((sum, item) => sum + item.value, 0),
            totalLabel: "avaliações hoje",
            entries,
            emptyMessage: "Nenhuma avaliação encontrada hoje."
        });
    }

    function renderThermometerBarList(target, rows, labelKey, emptyMessage) {
        if (!target) return;

        const entries = rows.map((row) => {
            const critical = Number(row.critical || 0);
            const total = Number(row.total || 0);
            const suffix = critical > 0 ? ` - ${critical} crítica${critical === 1 ? "" : "s"}` : "";

            return {
                label: `${row[labelKey] || "Não informado"}${suffix}`,
                value: total
            };
        });

        renderBarChart(target, {
            total: entries.reduce((sum, item) => sum + item.value, 0),
            totalLabel: "citações hoje",
            entries,
            emptyMessage
        });
    }

    function renderCriticalRatingsTable(rows) {
        if (!criticalRatingsTable) return;

        if (!rows.length) {
            criticalRatingsTable.innerHTML = `<p class="dashboard-muted">Nenhuma avaliação crítica encontrada hoje.</p>`;
            return;
        }

        criticalRatingsTable.innerHTML = `
                <table class="dashboard-table dashboard-rating-table">
                    <thead>
                        <tr>
                            <th>OS</th>
                            <th>Nota</th>
                            <th>Categoria</th>
                            <th>Subcategoria</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rows.map((row) => `
                            <tr>
                                <td>${escapeHtml(row.apptNumber || "-")}</td>
                                <td>
                                    <span class="dashboard-rating-badge rating-${escapeHtml(row.rating || "")}">
                                        ${escapeHtml(row.rating || "-")}
                                    </span>
                                </td>
                                <td>${escapeHtml(row.category || "-")}</td>
                                <td>${escapeHtml(row.subcategory || "-")}</td>
                            </tr>
                        `).join("")}
                    </tbody>
                </table>
            `;
    }
    document.querySelectorAll("[data-kpi-filter]").forEach((filter) => {
        filter.addEventListener("click", function (event) {
            const button = event.target.closest("[data-kpi-group]");
            if (!button) return;

            const scope = filter.dataset.kpiFilter;
            const group = button.dataset.kpiGroup;

            filter.querySelectorAll("[data-kpi-group]").forEach((item) => {
                item.classList.toggle("active", item === button);
            });

            document.querySelectorAll(`[data-kpi-card="${scope}"]`).forEach((card) => {
                const shouldShow = group === "all" || card.dataset.group === group;
                card.classList.toggle("hidden", !shouldShow);
            });
        });
    });
});
