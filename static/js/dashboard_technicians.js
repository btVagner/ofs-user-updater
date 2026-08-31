document.addEventListener("DOMContentLoaded", function () {
    const root = document.getElementById("dashboard-root");
    if (!root) return;

    const summaryUrl = root.dataset.techniciansSummaryUrl;
    const treeUrl = root.dataset.techniciansTreeUrl;
    const routeHistoryUrlTemplate = root.dataset.techniciansRouteHistoryUrlTemplate;
    const tabs = Array.from(document.querySelectorAll("[data-dashboard-view]"));
    const osPanel = document.querySelector('[data-dashboard-view-panel="os"]');
    const techniciansPanel = document.querySelector('[data-dashboard-view-panel="technicians"]');
    if (!summaryUrl || !treeUrl || !routeHistoryUrlTemplate || !tabs.length || !osPanel || !techniciansPanel) return;

    const summaryContainer = techniciansPanel.querySelector("[data-technicians-summary]");
    const healthContainer = techniciansPanel.querySelector("[data-technicians-health]");
    const structureContainer = techniciansPanel.querySelector("[data-technicians-structure-status]");
    const clusterContainer = techniciansPanel.querySelector("[data-technicians-clusters]");
    const errorContainer = techniciansPanel.querySelector("[data-technicians-error]");
    const tree = techniciansPanel.querySelector("[data-technicians-tree]");
    const treeState = techniciansPanel.querySelector("[data-technicians-tree-state]");
    const filters = techniciansPanel.querySelector("[data-technicians-filters]");
    const searchInput = techniciansPanel.querySelector("[data-technicians-search]");
    const refreshButton = techniciansPanel.querySelector("[data-technicians-refresh]");
    const osRefreshCard = document.querySelector("[data-dashboard-refresh-card]");

    const ROOT_CACHE_KEY = "__ROOT__";
    const POLL_INTERVAL_MS = 60000;
    const REQUEST_TIMEOUT_MS = 15000;
    const MAX_REFRESH_CONCURRENCY = 3;
    const DISPLAY_TIMEZONE = "America/Sao_Paulo";

    const ALERT_LABELS = {
        ROUTE_NOT_STARTED: "Rota não iniciada no horário",
        ROUTE_ACTIVE_AFTER_SHIFT: "Rota ativa após o fim da jornada",
        ACTIVITY_OPEN_AFTER_SHIFT: "OS em atendimento após o fim da jornada",
    };

    const KPI_FILTERS = {
        alert: "alert",
        attention: "attention",
        waiting_route: "waiting_route",
        active_route: "active_route",
        started: "started",
        suspended: "suspended",
        open: "open",
    };

    const state = {
        active: false,
        initialized: false,
        loadingInitial: false,
        refreshPromise: null,
        selectedFilter: "all",
        serverFilter: null,
        onlyProblems: false,
        summary: null,
        clusters: [],
        rootResourceId: null,
        childCache: new Map(),
        expanded: new Set(),
        technicianExpanded: new Set(),
        routeHistoryCache: new Map(),
        routeHistoryLoading: new Set(),
        routeHistoryErrors: new Map(),
        loadingParents: new Set(),
        parentErrors: new Map(),
        pollTimer: null,
        initialLoadCompleted: false,
        hierarchyVersionObserved: false,
        hierarchyVersion: null,
        structureNoticeTimer: null,
    };

    const metrics = {
        technicianRequestsBeforeFirstClick: 0,
        firstLoadRequestCount: 0,
        firstLoadBytes: 0,
        firstRootDomElements: null,
        currentTreeDomElements: 0,
        lastTreeRenderMs: 0,
        hierarchyCacheInvalidations: 0,
        requestLog: [],
    };
    window.__ofsTechnicianMonitorMetrics = metrics;

    function cacheKey(parentId) {
        return parentId === null ? ROOT_CACHE_KEY : String(parentId);
    }

    function buildUrl(baseUrl, params) {
        const url = new URL(baseUrl, window.location.href);
        Object.entries(params || {}).forEach(([key, value]) => {
            if (value === null || value === undefined || value === "") return;
            url.searchParams.set(key, String(value));
        });
        return url.toString();
    }

    function errorMessage(body, fallback) {
        const error = body && body.error;
        if (typeof error === "string" && error.trim()) return error;
        if (error && typeof error.message === "string" && error.message.trim()) return error.message;
        return fallback;
    }

    async function fetchJson(url) {
        const controller = new AbortController();
        const timeoutId = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
        try {
            const response = await fetch(url, {
                method: "GET",
                headers: { Accept: "application/json" },
                signal: controller.signal,
            });
            const text = await response.text();
            let body = {};
            try { body = text ? JSON.parse(text) : {}; } catch (_error) { body = {}; }
            const bytes = new TextEncoder().encode(text).length;
            const parsedUrl = new URL(url, window.location.href);
            metrics.requestLog.push({
                path: parsedUrl.pathname,
                query: parsedUrl.search,
                bytes,
                status: response.status,
            });
            if (!response.ok || body.ok === false) {
                throw new Error(errorMessage(body, "Não foi possível consultar o monitor operacional local."));
            }
            return { data: body.data || {}, bytes };
        } catch (error) {
            if (error && error.name === "AbortError") throw new Error("A consulta ao monitor local excedeu o tempo limite.");
            throw error;
        } finally {
            window.clearTimeout(timeoutId);
        }
    }

    function showError(message) {
        if (!errorContainer) return;
        errorContainer.textContent = message || "Não foi possível atualizar o monitor local.";
        errorContainer.classList.remove("hidden");
    }

    function clearError() {
        if (!errorContainer) return;
        errorContainer.textContent = "";
        errorContainer.classList.add("hidden");
    }

    function setTreeState(message, type) {
        if (!treeState) return;
        treeState.textContent = message || "";
        treeState.classList.toggle("hidden", !message);
        treeState.classList.toggle("error", type === "error");
    }

    function setRefreshBusy(busy) {
        if (!refreshButton) return;
        refreshButton.disabled = Boolean(busy);
        refreshButton.textContent = busy ? "Atualizando leitura..." : "Atualizar leitura";
    }

    function formatNumber(value) {
        return new Intl.NumberFormat("pt-BR").format(Number(value || 0));
    }

    function formatDateTime(value) {
        if (!value) return "-";
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) return String(value);
        return new Intl.DateTimeFormat("pt-BR", {
            timeZone: DISPLAY_TIMEZONE,
            day: "2-digit",
            month: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
        }).format(parsed);
    }

    function formatAge(seconds) {
        const value = Number(seconds);
        if (!Number.isFinite(value) || value < 0) return "idade indisponível";
        if (value < 60) return `${Math.round(value)} s`;
        if (value < 3600) return `${Math.round(value / 60)} min`;
        return `${Math.round(value / 360) / 10} h`;
    }

    function hierarchyVersion(payload) {
        const sync = payload && payload.hierarchy_sync;
        if (!sync || !sync.version) return null;
        return String(sync.version);
    }

    function clearHierarchyCache() {
        state.childCache.clear();
        state.expanded.clear();
        state.technicianExpanded.clear();
        state.routeHistoryCache.clear();
        state.routeHistoryLoading.clear();
        state.routeHistoryErrors.clear();
        state.parentErrors.clear();
        state.loadingParents.clear();
        state.clusters = [];
        state.rootResourceId = null;
        metrics.hierarchyCacheInvalidations += 1;
        renderClusters();
        renderTree();
    }

    function observeHierarchyVersion(payload) {
        const nextVersion = hierarchyVersion(payload);
        if (!state.hierarchyVersionObserved) {
            state.hierarchyVersionObserved = true;
            state.hierarchyVersion = nextVersion;
            return false;
        }
        if (nextVersion === state.hierarchyVersion) return false;
        state.hierarchyVersion = nextVersion;
        clearHierarchyCache();
        return true;
    }

    function showStructureRefreshNotice() {
        if (!structureContainer) return;
        if (state.structureNoticeTimer !== null) window.clearTimeout(state.structureNoticeTimer);
        structureContainer.className = "technicians-structure-status updated";
        structureContainer.textContent = "Estrutura de técnicos atualizada. A árvore foi recarregada a partir da raiz.";
        state.structureNoticeTimer = window.setTimeout(() => {
            state.structureNoticeTimer = null;
            renderStructureStatus(state.summary);
        }, 5000);
    }

    function renderStructureStatus(summary) {
        if (!structureContainer) return;
        const sync = summary && summary.hierarchy_sync ? summary.hierarchy_sync : null;
        if (!sync) {
            structureContainer.className = "technicians-structure-status unknown";
            structureContainer.textContent = "Status da atualização estrutural indisponível.";
            return;
        }
        const stateName = String(sync.state || "unknown").toLowerCase();
        const lastSuccess = sync.last_success_at ? formatDateTime(sync.last_success_at) : "sem atualização registrada";
        if (stateName === "ok") {
            structureContainer.className = "technicians-structure-status hidden";
            structureContainer.textContent = "";
            return;
        }
        if (stateName === "running") {
            structureContainer.className = "technicians-structure-status running";
            structureContainer.textContent = "Atualizando estrutura de técnicos… O painel continua disponível.";
            return;
        }
        if (stateName === "error") {
            structureContainer.className = "technicians-structure-status error";
            structureContainer.textContent = `A estrutura de técnicos não foi atualizada recentemente. Última atualização local: ${lastSuccess}.`;
            return;
        }
        structureContainer.className = `technicians-structure-status ${stateName === "stale" ? "stale" : "unknown"}`;
        structureContainer.textContent = `A estrutura de técnicos não foi atualizada recentemente. Última atualização local: ${lastSuccess}.`;
    }

    function healthClass(value) {
        const key = String(value || "").toUpperCase();
        if (key === "OK") return "ok";
        if (key === "DADOS_DESATUALIZADOS" || key === "DADOS_DESATUALIZADOS".toLowerCase()) return "stale";
        return "unknown";
    }

    function scheduleLabel(value) {
        const labels = { WORKING: "Trabalhando", NON_WORKING: "Fora de jornada", ON_CALL: "Sobreaviso", DESCONHECIDA: "Jornada desconhecida" };
        return labels[String(value || "").toUpperCase()] || "Jornada desconhecida";
    }

    function routeLabel(value) {
        const labels = { SEM_ESCALA: "Sem escala", AGUARDANDO_ATIVACAO: "Aguardando ativação", ATIVA: "Rota ativa", ENCERRADA: "Rota encerrada", DESCONHECIDA: "Rota desconhecida" };
        return labels[String(value || "").toUpperCase()] || "Rota desconhecida";
    }

    function alertLabel(code) {
        return ALERT_LABELS[String(code || "").toUpperCase()] || "Alerta operacional";
    }

    function renderHealth(summary) {
        if (!healthContainer) return;
        healthContainer.replaceChildren();
        const health = summary && summary.health ? summary.health : {};
        const sources = health.sources && typeof health.sources === "object" ? health.sources : {};
        const runtimeKeys = ["events", "activities", "calendars"];
        const staleEntries = runtimeKeys
            .map((key) => [key, sources[key]])
            .filter(([, source]) => source && String(source.state || "").toUpperCase() === "DADOS_DESATUALIZADOS")
            .sort((a, b) => Number(b[1].age_seconds || -1) - Number(a[1].age_seconds || -1));
        const unknownEntries = runtimeKeys.filter((key) => !sources[key] || !sources[key].last_success_at);
        const names = { events: "Events", activities: "Activities", calendars: "Calendars", routes: "Routes" };

        const headline = document.createElement("div");
        headline.className = `technicians-health-head ${staleEntries.length ? "stale" : unknownEntries.length ? "unknown" : "ok"}`;
        const strong = document.createElement("strong");
        if (staleEntries.length) {
            const [key, source] = staleEntries[0];
            strong.textContent = `Sincronização automática desatualizada — última atualização de ${names[key]} há ${formatAge(source.age_seconds)}.`;
        } else if (unknownEntries.length) {
            strong.textContent = "Sincronização automática com status incompleto — há fontes sem última atualização conhecida.";
        } else {
            strong.textContent = "Sincronização automática atualizada.";
        }
        const generated = document.createElement("span");
        generated.textContent = `Leitura gerada em ${formatDateTime(summary && summary.generated_at)} (${DISPLAY_TIMEZONE})`;
        headline.append(strong, generated);
        healthContainer.appendChild(headline);

        const list = document.createElement("div");
        list.className = "technicians-health-sources";
        Object.entries(names).forEach(([key, label]) => {
            const source = sources[key];
            if (!source) return;
            const item = document.createElement("span");
            item.className = `technicians-health-source ${healthClass(source.state)}`;
            const lastSuccess = source.last_success_at ? formatDateTime(source.last_success_at) : "sem sucesso registrado";
            const age = source.age_seconds !== null && source.age_seconds !== undefined ? ` · ${formatAge(source.age_seconds)}` : "";
            item.textContent = `${label}: ${lastSuccess}${age}`;
            list.appendChild(item);
        });
        const caughtUp = health.events_caught_up;
        if (caughtUp === true || caughtUp === false) {
            const item = document.createElement("span");
            item.className = `technicians-health-source ${caughtUp ? "ok" : "attention"}`;
            item.textContent = caughtUp ? "Events: sincronizados" : "Events: sincronização pendente";
            list.appendChild(item);
        }
        healthContainer.appendChild(list);
    }

    function summaryCard(label, value, semantic, filterName) {
        const article = document.createElement(filterName ? "button" : "article");
        if (filterName) article.type = "button";
        article.className = `technicians-stat ${semantic || ""} ${filterName ? "clickable" : ""}`.trim();
        if (filterName) {
            article.dataset.techniciansKpiFilter = filterName;
            article.setAttribute("aria-pressed", state.serverFilter === filterName ? "true" : "false");
            article.classList.toggle("active", state.serverFilter === filterName);
        }
        const span = document.createElement("span");
        span.textContent = label;
        const strong = document.createElement("strong");
        strong.textContent = formatNumber(value);
        article.append(span, strong);
        return article;
    }

    function renderSummary() {
        if (!summaryContainer) return;
        summaryContainer.replaceChildren();
        const summary = state.summary;
        if (!summary) {
            const message = document.createElement("p");
            message.className = "technicians-empty";
            message.textContent = "Resumo operacional indisponível.";
            summaryContainer.appendChild(message);
            return;
        }
        renderHealth(summary);
        renderStructureStatus(summary);
        if (summary.data_available === false) {
            const empty = document.createElement("div");
            empty.className = "technicians-data-warning";
            empty.textContent = "Sem dados operacionais materializados para o dia. A hierarquia existe, mas o estado corrente dos técnicos está indisponível.";
            summaryContainer.appendChild(empty);
        }
        const schedule = summary.schedule || {};
        const routes = summary.routes || {};
        const operational = summary.operational || {};
        const grid = document.createElement("div");
        grid.className = "technicians-stat-grid";
        [
            ["Técnicos", summary.total_technicians],
            ["Em jornada hoje", schedule.working],
            ["Alertas", operational.alerta, Number(operational.alerta || 0) > 0 ? "kpi-alert" : "", "alert"],
            ["Atenção", operational.atencao, Number(operational.atencao || 0) > 0 ? "attention" : "", "attention"],
            ["Aguardando ativação", routes.aguardando_ativacao, Number(routes.aguardando_ativacao || 0) > 0 ? "attention" : "", "waiting_route"],
            ["Rota ativa", routes.ativa, "ok", "active_route"],
            ["OS iniciadas", summary.started_count, "", "started"],
            ["OS abertas", summary.open_activity_count, "", "open"],
        ].forEach(([label, value, semantic, filterName]) => grid.appendChild(summaryCard(label, value, semantic, filterName)));
        summaryContainer.appendChild(grid);
    }

    function clusterMetricButton(label, value, filterName, clusterId) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "technicians-cluster-metric";
        button.dataset.clusterMetricFilter = filterName;
        button.dataset.clusterId = clusterId;
        button.setAttribute("aria-label", `${label}: ${formatNumber(value)}. Filtrar este cluster.`);
        button.textContent = formatNumber(value);
        return button;
    }

    function renderClusters() {
        if (!clusterContainer) return;
        clusterContainer.replaceChildren();
        if (!state.clusters.length) {
            const empty = document.createElement("p");
            empty.className = "technicians-empty";
            empty.textContent = state.serverFilter ? "Nenhum cluster corresponde ao filtro atual." : "Nenhum cluster disponível na raiz da hierarquia.";
            clusterContainer.appendChild(empty);
            return;
        }
        const table = document.createElement("table");
        table.className = "technicians-cluster-table";
        table.innerHTML = "<thead><tr><th>Cluster</th><th>Em jornada hoje</th><th>Rota ativa</th><th>% de ativação</th><th>Aguardando ativação</th><th>Alertas</th></tr></thead>";
        const tbody = document.createElement("tbody");
        state.clusters.forEach((cluster) => {
            const tr = document.createElement("tr");
            const nameCell = document.createElement("td");
            const clusterButton = document.createElement("button");
            clusterButton.type = "button";
            clusterButton.className = "technicians-cluster-link";
            clusterButton.dataset.clusterId = cluster.resource_id;
            clusterButton.textContent = cluster.resource_name || cluster.resource_id;
            nameCell.appendChild(clusterButton);
            const working = document.createElement("td");
            working.textContent = formatNumber(cluster.working_count);
            const active = document.createElement("td");
            active.appendChild(clusterMetricButton("Rota ativa", cluster.active_route_count, "active_route", cluster.resource_id));
            const percent = document.createElement("td");
            percent.textContent = cluster.activation_percent === null || cluster.activation_percent === undefined ? "—" : `${Number(cluster.activation_percent).toLocaleString("pt-BR", { maximumFractionDigits: 1 })}%`;
            const waiting = document.createElement("td");
            waiting.appendChild(clusterMetricButton("Aguardando ativação", cluster.waiting_route_count, "waiting_route", cluster.resource_id));
            const alerts = document.createElement("td");
            alerts.appendChild(clusterMetricButton("Alertas", cluster.alert_count, "alert", cluster.resource_id));
            tr.append(nameCell, working, active, percent, waiting, alerts);
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        clusterContainer.appendChild(table);
    }

    async function loadSummary() {
        const result = await fetchJson(summaryUrl);
        const hierarchyChanged = observeHierarchyVersion(result.data);
        state.summary = result.data;
        renderSummary();
        if (hierarchyChanged) showStructureRefreshNotice();
        return { ...result, hierarchyChanged };
    }

    async function loadChildren(parentId, options) {
        const key = cacheKey(parentId);
        const force = options && options.force;
        if (!force && state.childCache.has(key)) return { data: { nodes: state.childCache.get(key) }, bytes: 0, cached: true };
        state.loadingParents.add(key);
        state.parentErrors.delete(key);
        renderTree();
        try {
            const result = await fetchJson(buildUrl(treeUrl, {
                mode: "children",
                parent_id: parentId,
                filter: state.serverFilter,
                only_problems: state.onlyProblems ? 1 : null,
            }));
            const nodes = Array.isArray(result.data.nodes) ? result.data.nodes : [];
            state.childCache.set(key, nodes);
            if (parentId === null) {
                state.clusters = Array.isArray(result.data.clusters) ? result.data.clusters : [];
                state.rootResourceId = result.data.root_resource_id || (nodes[0] && nodes[0].resource_id) || null;
                renderClusters();
            }
            return result;
        } catch (error) {
            state.parentErrors.set(key, error.message || "Falha ao carregar este ramo.");
            throw error;
        } finally {
            state.loadingParents.delete(key);
            renderTree();
        }
    }

    function routeHistoryUrl(resourceId) {
        return routeHistoryUrlTemplate.replace("__RESOURCE_ID__", encodeURIComponent(String(resourceId)));
    }

    async function loadRouteHistory(resourceId, options) {
        const id = String(resourceId);
        const force = options && options.force;
        if (!force && state.routeHistoryCache.has(id)) {
            return { data: state.routeHistoryCache.get(id), bytes: 0, cached: true };
        }
        state.routeHistoryLoading.add(id);
        state.routeHistoryErrors.delete(id);
        renderTree();
        try {
            const result = await fetchJson(routeHistoryUrl(id));
            state.routeHistoryCache.set(id, result.data || {});
            return result;
        } catch (error) {
            state.routeHistoryErrors.set(id, error.message || "Falha ao carregar o histórico de rota.");
            throw error;
        } finally {
            state.routeHistoryLoading.delete(id);
            renderTree();
        }
    }

    function routeHistoryValue(value) {
        if (value) {
            const text = document.createElement("span");
            text.textContent = value;
            return text;
        }
        const missing = document.createElement("span");
        missing.className = "technicians-route-history-missing";
        missing.textContent = "—";
        missing.title = "Ainda não há informação";
        missing.setAttribute("aria-label", "Ainda não há informação");
        return missing;
    }

    function appendRouteHistory(container, node) {
        const id = String(node.resource_id || "");
        const history = state.routeHistoryCache.get(id);
        const section = document.createElement("section");
        section.className = "technicians-route-history";
        section.setAttribute("aria-label", `Histórico de rota de ${node.resource_name || id}`);

        const title = document.createElement("strong");
        title.className = "technicians-route-history-title";
        title.textContent = "Histórico de rota";
        section.appendChild(title);

        if (state.routeHistoryLoading.has(id)) {
            const loading = document.createElement("p");
            loading.className = "technicians-route-history-state";
            loading.textContent = "Carregando histórico local...";
            section.appendChild(loading);
            container.appendChild(section);
            return;
        }

        if (state.routeHistoryErrors.has(id)) {
            const error = document.createElement("p");
            error.className = "technicians-route-history-state error";
            error.textContent = state.routeHistoryErrors.get(id);
            section.appendChild(error);
            container.appendChild(section);
            return;
        }

        const days = history && Array.isArray(history.days) ? history.days : [];
        if (!days.length) {
            const empty = document.createElement("p");
            empty.className = "technicians-route-history-state";
            empty.textContent = "Não há dias disponíveis no read model para este técnico.";
            section.appendChild(empty);
            container.appendChild(section);
            return;
        }

        const table = document.createElement("div");
        table.className = "technicians-route-history-grid";
        table.setAttribute("role", "table");
        const header = document.createElement("div");
        header.className = "technicians-route-history-row header";
        header.setAttribute("role", "row");
        ["Data", "Ativação", "Inativação"].forEach((label) => {
            const cell = document.createElement("span");
            cell.setAttribute("role", "columnheader");
            cell.textContent = label;
            header.appendChild(cell);
        });
        table.appendChild(header);

        days.forEach((day) => {
            const row = document.createElement("div");
            row.className = "technicians-route-history-row";
            row.setAttribute("role", "row");
            const dateCell = document.createElement("span");
            dateCell.setAttribute("role", "cell");
            dateCell.textContent = day.date_label || day.work_date || "—";
            const activationCell = document.createElement("span");
            activationCell.setAttribute("role", "cell");
            activationCell.appendChild(routeHistoryValue(day.activation_time));
            const endCell = document.createElement("span");
            endCell.setAttribute("role", "cell");
            endCell.appendChild(routeHistoryValue(day.end_time));
            row.append(dateCell, activationCell, endCell);
            table.appendChild(row);
        });
        section.appendChild(table);
        container.appendChild(section);
    }

    function isTechnicianNode(node) {
        return Object.prototype.hasOwnProperty.call(node || {}, "schedule_state");
    }

    function textMatch(node) {
        const term = (searchInput && searchInput.value || "").trim().toLowerCase();
        if (!term) return true;
        return `${node.resource_name || ""} ${node.resource_id || ""}`.toLowerCase().includes(term);
    }

    function branchMatches(node, visited) {
        const path = visited || new Set();
        const id = String(node.resource_id || "");
        if (path.has(id)) return false;
        const nextPath = new Set(path);
        nextPath.add(id);
        if (textMatch(node)) return true;
        const children = state.childCache.get(cacheKey(id)) || [];
        return children.some((child) => branchMatches(child, nextPath));
    }

    function fact(label, value, className) {
        const span = document.createElement("span");
        span.className = `technicians-fact ${className || ""}`.trim();
        const strong = document.createElement("strong");
        strong.textContent = label;
        const text = document.createElement("span");
        text.textContent = value;
        span.append(strong, text);
        return span;
    }

    function appendTechnicianDetails(container, node) {
        const details = document.createElement("div");
        details.className = "technicians-node-details technicians-node-grid";
        details.appendChild(fact("Jornada", scheduleLabel(node.schedule_state)));
        details.appendChild(fact("Rota", routeLabel(node.route_state)));
        details.appendChild(fact("OS abertas", formatNumber(node.open_activity_count)));
        details.appendChild(fact("Em atendimento", formatNumber(node.started_count)));

        const alertCell = document.createElement("span");
        alertCell.className = "technicians-fact technicians-alert-fact";
        const alertTitle = document.createElement("strong");
        alertTitle.textContent = "Alerta";
        alertCell.appendChild(alertTitle);
        const codes = Array.isArray(node.alert_codes) ? node.alert_codes : [];
        if (codes.length) {
            codes.forEach((code) => {
                const message = document.createElement("span");
                message.className = "technicians-alert-message";
                message.textContent = alertLabel(code);
                alertCell.appendChild(message);
            });
        } else if (node.has_operational_state === false) {
            const missing = document.createElement("span");
            missing.className = "technicians-state-missing";
            missing.textContent = "Sem estado operacional do dia";
            alertCell.appendChild(missing);
        } else {
            const normal = document.createElement("span");
            normal.textContent = "—";
            alertCell.appendChild(normal);
        }
        details.appendChild(alertCell);
        container.appendChild(details);
    }

    function appendAggregateDetails(container, node) {
        const a = node.aggregates || {};
        const details = document.createElement("div");
        details.className = "technicians-node-details technicians-node-grid aggregate";
        details.appendChild(fact("Técnicos", formatNumber(a.technician_count)));
        details.appendChild(fact("Em jornada", formatNumber(a.working_count)));
        details.appendChild(fact("Rota ativa", formatNumber(a.active_route_count), "positive"));
        details.appendChild(fact("OS abertas", formatNumber(a.open_activity_count)));
        const exception = document.createElement("span");
        exception.className = "technicians-fact technicians-alert-fact";
        const title = document.createElement("strong");
        title.textContent = "Exceções";
        const parts = [];
        if (Number(a.alert_count || 0) > 0) parts.push(`${formatNumber(a.alert_count)} alertas`);
        if (Number(a.attention_count || 0) > 0) parts.push(`${formatNumber(a.attention_count)} atenção`);
        if (Number(a.missing_state_count || 0) > 0) parts.push(`${formatNumber(a.missing_state_count)} sem estado`);
        const value = document.createElement("span");
        value.textContent = parts.length ? parts.join(" · ") : "—";
        exception.append(title, value);
        details.appendChild(exception);
        container.appendChild(details);
    }

    function createTreeNode(node, level, path) {
        const id = String(node.resource_id || "");
        const pathSet = new Set(path || []);
        if (pathSet.has(id)) return null;
        pathSet.add(id);
        const item = document.createElement("li");
        item.className = `technicians-tree-item ${isTechnicianNode(node) ? "technician" : "group"}`;
        item.setAttribute("role", "treeitem");
        item.setAttribute("aria-level", String(level));
        item.dataset.resourceId = id;
        const technician = isTechnicianNode(node);
        const expanded = node.has_children ? state.expanded.has(id) : technician && state.technicianExpanded.has(id);
        if (node.has_children || technician) item.setAttribute("aria-expanded", expanded ? "true" : "false");

        const row = document.createElement("div");
        row.className = "technicians-node-row";
        if (node.has_children || technician) {
            const toggle = document.createElement("button");
            toggle.type = "button";
            toggle.className = "technicians-tree-toggle";
            if (node.has_children) toggle.dataset.treeToggle = id;
            else toggle.dataset.technicianToggle = id;
            toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
            toggle.setAttribute("aria-label", `${expanded ? "Recolher" : "Expandir"} ${node.resource_name || id}`);
            toggle.textContent = expanded ? "−" : "+";
            row.appendChild(toggle);
        } else {
            const spacer = document.createElement("span");
            spacer.className = "technicians-tree-toggle-spacer";
            spacer.setAttribute("aria-hidden", "true");
            row.appendChild(spacer);
        }

        const copy = document.createElement("div");
        copy.className = "technicians-node-copy";
        const titleLine = document.createElement("div");
        titleLine.className = "technicians-node-title";
        const name = document.createElement("strong");
        name.textContent = node.resource_name || id;
        const meta = document.createElement("span");
        meta.textContent = `${node.resource_type || "tipo não informado"} · ${id}`;
        titleLine.append(name, meta);
        copy.appendChild(titleLine);
        if (isTechnicianNode(node)) appendTechnicianDetails(copy, node); else appendAggregateDetails(copy, node);
        row.appendChild(copy);
        item.appendChild(row);

        if (technician && expanded) appendRouteHistory(item, node);

        if (node.has_children && expanded) {
            const key = cacheKey(id);
            const group = document.createElement("ul");
            group.className = "technicians-tree-children";
            group.setAttribute("role", "group");
            if (state.loadingParents.has(key)) {
                const loading = document.createElement("li");
                loading.className = "technicians-tree-message";
                loading.textContent = "Carregando filhos...";
                group.appendChild(loading);
            } else if (state.parentErrors.has(key)) {
                const error = document.createElement("li");
                error.className = "technicians-tree-message error";
                error.textContent = state.parentErrors.get(key);
                group.appendChild(error);
            } else if (state.childCache.has(key)) {
                const children = state.childCache.get(key) || [];
                const visible = children.filter((child) => branchMatches(child, pathSet));
                visible.forEach((child) => {
                    const childElement = createTreeNode(child, level + 1, pathSet);
                    if (childElement) group.appendChild(childElement);
                });
                if (!visible.length) {
                    const empty = document.createElement("li");
                    empty.className = "technicians-tree-message";
                    empty.textContent = "Nó sem filhos.";
                    group.appendChild(empty);
                }
            }
            item.appendChild(group);
        }
        return item;
    }

    function renderTree() {
        if (!tree) return;
        const started = performance.now();
        tree.replaceChildren();
        const rootKey = cacheKey(null);
        const roots = state.childCache.get(rootKey);
        if (!roots) {
            tree.classList.add("hidden");
            if (state.loadingParents.has(rootKey)) setTreeState("Carregando raiz da hierarquia...");
            else if (state.parentErrors.has(rootKey)) setTreeState(state.parentErrors.get(rootKey), "error");
            return;
        }
        const visibleRoots = roots.filter((node) => branchMatches(node));
        if (!visibleRoots.length) {
            tree.classList.add("hidden");
            const hasSearch = Boolean((searchInput && searchInput.value || "").trim());
            setTreeState(hasSearch ? "Nenhum nó já carregado corresponde à busca atual." : "Nenhum ramo corresponde ao filtro atual.");
            return;
        }
        visibleRoots.forEach((node) => {
            const element = createTreeNode(node, 1, new Set());
            if (element) tree.appendChild(element);
        });
        tree.classList.remove("hidden");
        setTreeState("");
        metrics.lastTreeRenderMs = Math.round((performance.now() - started) * 100) / 100;
        metrics.currentTreeDomElements = tree.querySelectorAll("*").length;
        if (metrics.firstRootDomElements === null && state.expanded.size === 0 && state.initialLoadCompleted) metrics.firstRootDomElements = metrics.currentTreeDomElements;
    }

    async function applyFilter(filterName) {
        const normalized = filterName === "all" ? null : filterName;
        const nextOnlyProblems = normalized === "problems";
        const nextServerFilter = normalized === "problems" ? null : normalized;
        if (state.selectedFilter === filterName && state.initialized) return;
        state.selectedFilter = filterName;
        state.onlyProblems = nextOnlyProblems;
        state.serverFilter = nextServerFilter;
        updateFilterControls();
        renderSummary();
        if (!state.initialized) return;
        state.childCache.clear();
        state.expanded.clear();
        state.technicianExpanded.clear();
        state.routeHistoryCache.clear();
        state.routeHistoryLoading.clear();
        state.routeHistoryErrors.clear();
        state.parentErrors.clear();
        state.clusters = [];
        renderClusters();
        clearError();
        setTreeState("Recarregando a raiz com o filtro selecionado...");
        try { await loadChildren(null, { force: true }); }
        catch (error) { showError(error.message || "Não foi possível aplicar o filtro no monitor local."); }
        renderTree();
    }

    function updateFilterControls() {
        if (filters) {
            filters.querySelectorAll("[data-technicians-filter]").forEach((button) => {
                const active = (button.dataset.techniciansFilter || "all") === state.selectedFilter;
                button.classList.toggle("active", active);
                button.setAttribute("aria-pressed", active ? "true" : "false");
            });
        }
    }

    async function locateCluster(clusterId) {
        if (!clusterId || !state.rootResourceId) return;
        const rootId = String(state.rootResourceId);
        state.expanded.add(rootId);
        if (!state.childCache.has(cacheKey(rootId))) await loadChildren(rootId, { force: false });
        state.expanded.add(String(clusterId));
        if (!state.childCache.has(cacheKey(clusterId))) await loadChildren(clusterId, { force: false });
        renderTree();
        window.requestAnimationFrame(() => {
            const target = tree && tree.querySelector(`[data-resource-id="${CSS.escape(String(clusterId))}"]`);
            if (target) target.scrollIntoView({ behavior: "smooth", block: "center" });
        });
    }

    async function loadInitial() {
        if (state.loadingInitial || state.initialized) return;
        state.initialized = true;
        state.loadingInitial = true;
        clearError();
        setRefreshBusy(true);
        setTreeState("Carregando resumo e raiz da hierarquia...");
        const requestLogStart = metrics.requestLog.length;
        metrics.technicianRequestsBeforeFirstClick = requestLogStart;
        const results = await Promise.allSettled([loadSummary(), loadChildren(null, { force: true })]);
        state.loadingInitial = false;
        state.initialLoadCompleted = true;
        setRefreshBusy(false);
        renderTree();
        const firstRequests = metrics.requestLog.slice(requestLogStart);
        metrics.firstLoadRequestCount = firstRequests.length;
        metrics.firstLoadBytes = firstRequests.reduce((total, item) => total + Number(item.bytes || 0), 0);
        if (metrics.firstRootDomElements === null) metrics.firstRootDomElements = tree ? tree.querySelectorAll("*").length : 0;
        const failures = results.filter((result) => result.status === "rejected");
        if (!state.summary) renderSummary();
        if (failures.length) showError(failures[0].reason && failures[0].reason.message ? failures[0].reason.message : "Parte do monitor local não pôde ser carregada.");
        startPolling();
    }

    async function runWithConcurrency(items, worker, limit) {
        const queue = items.slice();
        const runners = Array.from({ length: Math.min(limit, queue.length) }, async function () {
            while (queue.length) await worker(queue.shift());
        });
        await Promise.all(runners);
    }

    async function refreshControlled() {
        if (!state.initialized || !state.active || document.hidden) return;
        if (state.refreshPromise) return state.refreshPromise;
        clearError();
        setRefreshBusy(true);
        state.refreshPromise = (async function () {
            const branchFailures = [];
            let summaryResult;
            try {
                summaryResult = await loadSummary();
            } catch (error) {
                showError(error.message || "Não foi possível atualizar o resumo do monitor local.");
                return;
            }
            const parents = summaryResult.hierarchyChanged ? [null] : [null, ...Array.from(state.expanded)];
            await runWithConcurrency(parents, async function (parentId) {
                try { await loadChildren(parentId, { force: true }); }
                catch (error) { branchFailures.push(error); }
            }, MAX_REFRESH_CONCURRENCY);
            if (branchFailures.length) showError(branchFailures[0].message || "Não foi possível concluir a atualização do monitor local.");
            renderTree();
        })().finally(function () {
            state.refreshPromise = null;
            setRefreshBusy(false);
        });
        return state.refreshPromise;
    }

    function stopPolling() {
        if (state.pollTimer !== null) {
            window.clearInterval(state.pollTimer);
            state.pollTimer = null;
        }
    }

    function startPolling() {
        stopPolling();
        if (!state.active || document.hidden || !state.initialized) return;
        state.pollTimer = window.setInterval(() => {
            if (!state.active || document.hidden) return;
            refreshControlled();
        }, POLL_INTERVAL_MS);
    }

    function setActivePanel(view) {
        const techniciansActive = view === "technicians";
        state.active = techniciansActive;
        osPanel.classList.toggle("hidden", techniciansActive);
        techniciansPanel.classList.toggle("hidden", !techniciansActive);
        techniciansPanel.setAttribute("aria-hidden", techniciansActive ? "false" : "true");
        osPanel.setAttribute("aria-hidden", techniciansActive ? "true" : "false");
        if (osRefreshCard) osRefreshCard.classList.toggle("hidden", techniciansActive);
        tabs.forEach((button) => {
            const active = button.dataset.dashboardView === view;
            button.classList.toggle("active", active);
            button.setAttribute("aria-selected", active ? "true" : "false");
            button.tabIndex = active ? 0 : -1;
        });
        if (techniciansActive) {
            if (!state.initialized) loadInitial(); else { refreshControlled(); startPolling(); }
        } else stopPolling();
    }

    tabs.forEach((button) => {
        button.addEventListener("click", () => setActivePanel(button.dataset.dashboardView));
        button.addEventListener("keydown", function (event) {
            if (!["ArrowLeft", "ArrowRight"].includes(event.key)) return;
            event.preventDefault();
            const index = tabs.indexOf(button);
            const nextIndex = event.key === "ArrowRight" ? (index + 1) % tabs.length : (index - 1 + tabs.length) % tabs.length;
            tabs[nextIndex].focus();
            setActivePanel(tabs[nextIndex].dataset.dashboardView);
        });
    });

    if (tree) {
        tree.addEventListener("click", async function (event) {
            const technicianButton = event.target.closest("[data-technician-toggle]");
            if (technicianButton && tree.contains(technicianButton)) {
                const resourceId = technicianButton.dataset.technicianToggle;
                if (!resourceId) return;
                if (state.technicianExpanded.has(resourceId)) {
                    state.technicianExpanded.delete(resourceId);
                    state.routeHistoryCache.delete(resourceId);
                    state.routeHistoryErrors.delete(resourceId);
                    renderTree();
                    return;
                }
                state.technicianExpanded.add(resourceId);
                renderTree();
                try { await loadRouteHistory(resourceId, { force: false }); }
                catch (error) { showError(error.message || "Não foi possível carregar o histórico local de rota."); }
                renderTree();
                return;
            }

            const button = event.target.closest("[data-tree-toggle]");
            if (!button || !tree.contains(button)) return;
            const resourceId = button.dataset.treeToggle;
            if (!resourceId) return;
            if (state.expanded.has(resourceId)) { state.expanded.delete(resourceId); renderTree(); return; }
            state.expanded.add(resourceId);
            renderTree();
            const key = cacheKey(resourceId);
            if (!state.childCache.has(key)) {
                try { await loadChildren(resourceId, { force: false }); }
                catch (error) { showError(error.message || "Não foi possível carregar os filhos deste nó."); }
            }
            renderTree();
        });
    }

    if (filters) {
        filters.addEventListener("click", function (event) {
            const button = event.target.closest("[data-technicians-filter]");
            if (!button || !filters.contains(button)) return;
            applyFilter(button.dataset.techniciansFilter || "all");
        });
    }

    if (summaryContainer) {
        summaryContainer.addEventListener("click", function (event) {
            const button = event.target.closest("[data-technicians-kpi-filter]");
            if (!button || !summaryContainer.contains(button)) return;
            const next = button.dataset.techniciansKpiFilter;
            applyFilter(state.serverFilter === next ? "all" : next);
        });
    }

    if (clusterContainer) {
        clusterContainer.addEventListener("click", async function (event) {
            const metric = event.target.closest("[data-cluster-metric-filter]");
            if (metric && clusterContainer.contains(metric)) {
                await applyFilter(metric.dataset.clusterMetricFilter || "all");
                try { await locateCluster(metric.dataset.clusterId); } catch (error) { showError(error.message); }
                return;
            }
            const button = event.target.closest("[data-cluster-id]");
            if (!button || !clusterContainer.contains(button)) return;
            try { await locateCluster(button.dataset.clusterId); } catch (error) { showError(error.message || "Não foi possível localizar o cluster na árvore."); }
        });
    }

    if (searchInput) searchInput.addEventListener("input", renderTree);
    if (refreshButton) refreshButton.addEventListener("click", () => refreshControlled());
    document.addEventListener("visibilitychange", function () {
        if (!state.active) return;
        if (document.hidden) stopPolling(); else { refreshControlled(); startPolling(); }
    });
});
