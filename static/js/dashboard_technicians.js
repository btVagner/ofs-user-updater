document.addEventListener("DOMContentLoaded", function () {
    const root = document.getElementById("dashboard-root");
    if (!root) return;

    const summaryUrl = root.dataset.techniciansSummaryUrl;
    const treeUrl = root.dataset.techniciansTreeUrl;
    const tabs = Array.from(document.querySelectorAll("[data-dashboard-view]"));
    const osPanel = document.querySelector('[data-dashboard-view-panel="os"]');
    const techniciansPanel = document.querySelector('[data-dashboard-view-panel="technicians"]');

    if (!summaryUrl || !treeUrl || !tabs.length || !osPanel || !techniciansPanel) return;

    const summaryContainer = techniciansPanel.querySelector("[data-technicians-summary]");
    const healthContainer = techniciansPanel.querySelector("[data-technicians-health]");
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

    const state = {
        active: false,
        initialized: false,
        loadingInitial: false,
        refreshPromise: null,
        selectedFilter: "all",
        onlyProblems: false,
        summary: null,
        childCache: new Map(),
        expanded: new Set(),
        loadingParents: new Set(),
        parentErrors: new Map(),
        pollTimer: null,
        initialLoadCompleted: false,
    };

    const metrics = {
        technicianRequestsBeforeFirstClick: 0,
        firstLoadRequestCount: 0,
        firstLoadBytes: 0,
        firstRootDomElements: null,
        currentTreeDomElements: 0,
        lastTreeRenderMs: 0,
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
        const timeoutId = window.setTimeout(function () {
            controller.abort();
        }, REQUEST_TIMEOUT_MS);

        try {
            const response = await fetch(url, {
                method: "GET",
                headers: { "Accept": "application/json" },
                signal: controller.signal,
            });
            const text = await response.text();
            let body = {};
            try {
                body = text ? JSON.parse(text) : {};
            } catch (_error) {
                body = {};
            }

            const bytes = new TextEncoder().encode(text).length;
            metrics.requestLog.push({
                path: new URL(url, window.location.href).pathname,
                bytes: bytes,
                status: response.status,
            });

            if (!response.ok || body.ok === false) {
                throw new Error(errorMessage(body, "Não foi possível consultar o monitor operacional local."));
            }
            return { data: body.data || {}, bytes: bytes };
        } catch (error) {
            if (error && error.name === "AbortError") {
                throw new Error("A consulta ao monitor local excedeu o tempo limite.");
            }
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
        refreshButton.textContent = busy ? "Atualizando..." : "Atualizar";
    }

    function formatNumber(value) {
        return new Intl.NumberFormat("pt-BR").format(Number(value || 0));
    }

    function formatDateTime(value) {
        if (!value) return "-";
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) return String(value);
        return new Intl.DateTimeFormat("pt-BR", {
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

    function mappedIntegrity(value) {
        const key = String(value || "").toUpperCase();
        if (key === "OK") return { label: "[OK] Dados íntegros", className: "ok" };
        if (key === "DADOS_DESATUALIZADOS") return { label: "[?] Dados desatualizados", className: "stale" };
        return { label: "[?] Integridade desconhecida", className: "unknown" };
    }

    function severityInfo(value) {
        switch (String(value || "").toUpperCase()) {
            case "NORMAL": return { label: "[OK] Normal", className: "ok" };
            case "ATENCAO": return { label: "[!] Atenção", className: "attention" };
            case "ALERTA": return { label: "[ALERTA] Alerta", className: "alert" };
            default: return { label: "[?] Situação operacional incerta", className: "unknown" };
        }
    }

    function scheduleLabel(value) {
        const labels = {
            WORKING: "Trabalhando",
            NON_WORKING: "Fora de jornada",
            ON_CALL: "Sobreaviso",
            DESCONHECIDA: "Jornada desconhecida",
        };
        return labels[String(value || "").toUpperCase()] || "Jornada desconhecida";
    }

    function routeLabel(value) {
        const labels = {
            SEM_ESCALA: "Sem escala",
            AGUARDANDO_ATIVACAO: "Aguardando ativação",
            ATIVA: "Rota ativa",
            ENCERRADA: "Rota encerrada",
            DESCONHECIDA: "Rota desconhecida",
        };
        return labels[String(value || "").toUpperCase()] || "Rota desconhecida";
    }

    function renderHealth(summary) {
        if (!healthContainer) return;
        healthContainer.replaceChildren();

        const health = summary && summary.health ? summary.health : {};
        const overall = mappedIntegrity(health.overall_integrity);
        const headline = document.createElement("div");
        headline.className = `technicians-health-head ${overall.className}`;

        const strong = document.createElement("strong");
        strong.textContent = overall.label;
        headline.appendChild(strong);

        const generated = document.createElement("span");
        generated.textContent = `Leitura gerada em ${formatDateTime(summary && summary.generated_at)}`;
        headline.appendChild(generated);
        healthContainer.appendChild(headline);

        const sources = health.sources && typeof health.sources === "object" ? health.sources : {};
        const sourceNames = {
            events: "Events",
            activities: "Activities",
            calendars: "Calendars",
            routes: "Routes",
        };
        const list = document.createElement("div");
        list.className = "technicians-health-sources";

        Object.entries(sourceNames).forEach(([key, label]) => {
            const source = sources[key];
            if (!source) return;
            const badge = document.createElement("span");
            const sourceIntegrity = mappedIntegrity(source.state);
            badge.className = `technicians-health-source ${sourceIntegrity.className}`;
            const lastSuccess = source.last_success_at ? formatDateTime(source.last_success_at) : "sem sucesso registrado";
            const age = source.age_seconds !== null && source.age_seconds !== undefined
                ? ` · ${formatAge(source.age_seconds)}`
                : "";
            badge.textContent = `${label}: ${lastSuccess}${age}`;
            list.appendChild(badge);
        });

        const caughtUp = health.events_caught_up;
        if (caughtUp === true || caughtUp === false) {
            const badge = document.createElement("span");
            badge.className = `technicians-health-source ${caughtUp ? "ok" : "attention"}`;
            badge.textContent = caughtUp ? "Events: sincronizados" : "Events: sincronização pendente";
            list.appendChild(badge);
        }

        healthContainer.appendChild(list);
    }

    function summaryCard(label, value, semantic) {
        const article = document.createElement("article");
        article.className = `technicians-stat ${semantic || ""}`.trim();
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

        if (summary.data_available === false) {
            const empty = document.createElement("div");
            empty.className = "technicians-data-warning";
            empty.textContent = "Sem dados operacionais materializados para o dia. A hierarquia pode existir, mas o estado corrente dos técnicos está indisponível.";
            summaryContainer.appendChild(empty);
        }

        const schedule = summary.schedule || {};
        const routes = summary.routes || {};
        const operational = summary.operational || {};
        const integrity = summary.integrity || {};
        const grid = document.createElement("div");
        grid.className = "technicians-stat-grid";

        [
            ["Técnicos", summary.total_technicians],
            ["Com estado", summary.technicians_with_operational_state],
            ["Sem estado", summary.technicians_without_operational_state, Number(summary.technicians_without_operational_state || 0) > 0 ? "unknown" : ""],
            ["Trabalhando", schedule.working],
            ["Fora de jornada", schedule.non_working],
            ["Sobreaviso", schedule.on_call],
            ["Aguardando ativação", routes.aguardando_ativacao, Number(routes.aguardando_ativacao || 0) > 0 ? "attention" : ""],
            ["Rota ativa", routes.ativa, "ok"],
            ["Rota encerrada", routes.encerrada],
            ["Rota desconhecida", routes.desconhecida, Number(routes.desconhecida || 0) > 0 ? "unknown" : ""],
            ["Atenção", operational.atencao, Number(operational.atencao || 0) > 0 ? "attention" : ""],
            ["Alertas", operational.alerta, Number(operational.alerta || 0) > 0 ? "alert" : ""],
            ["Integridade degradada", Number(integrity.dados_desatualizados || 0) + Number(integrity.desconhecida || 0), Number(integrity.dados_desatualizados || 0) + Number(integrity.desconhecida || 0) > 0 ? "stale" : ""],
            ["OS iniciadas", summary.started_count],
            ["OS suspensas", summary.suspended_count],
            ["OS abertas", summary.open_activity_count],
        ].forEach(([label, value, semantic]) => grid.appendChild(summaryCard(label, value, semantic)));

        summaryContainer.appendChild(grid);
    }

    async function loadSummary() {
        const result = await fetchJson(summaryUrl);
        state.summary = result.data;
        renderSummary();
        return result;
    }

    async function loadChildren(parentId, options) {
        const key = cacheKey(parentId);
        const force = options && options.force;
        if (!force && state.childCache.has(key)) {
            return { data: { nodes: state.childCache.get(key) }, bytes: 0, cached: true };
        }

        state.loadingParents.add(key);
        state.parentErrors.delete(key);
        renderTree();

        try {
            const result = await fetchJson(buildUrl(treeUrl, {
                mode: "children",
                parent_id: parentId,
                only_problems: state.onlyProblems ? 1 : null,
            }));
            const nodes = Array.isArray(result.data.nodes) ? result.data.nodes : [];
            state.childCache.set(key, nodes);
            return result;
        } catch (error) {
            state.parentErrors.set(key, error.message || "Falha ao carregar este ramo.");
            throw error;
        } finally {
            state.loadingParents.delete(key);
            renderTree();
        }
    }

    function isTechnicianNode(node) {
        return Object.prototype.hasOwnProperty.call(node || {}, "schedule_state");
    }

    function nodeOperationalMatch(node) {
        const filter = state.selectedFilter;
        if (filter === "all" || filter === "problems") return true;

        if (isTechnicianNode(node)) {
            if (filter === "waiting") return String(node.route_state || "").toUpperCase() === "AGUARDANDO_ATIVACAO";
            if (filter === "active") return String(node.route_state || "").toUpperCase() === "ATIVA";
            if (filter === "open") return Number(node.open_activity_count || 0) > 0 || Number(node.started_count || 0) > 0;
            return true;
        }

        const aggregates = node.aggregates || {};
        if (filter === "waiting") return Number(aggregates.waiting_route_count || 0) > 0;
        if (filter === "active") return Number(aggregates.active_route_count || 0) > 0;
        if (filter === "open") return Number(aggregates.open_activity_count || 0) > 0 || Number(aggregates.started_count || 0) > 0;
        return true;
    }

    function textMatch(node) {
        const term = (searchInput && searchInput.value || "").trim().toLowerCase();
        if (!term) return true;
        const haystack = `${node.resource_name || ""} ${node.resource_id || ""}`.toLowerCase();
        return haystack.includes(term);
    }

    function branchMatches(node, visited) {
        const path = visited || new Set();
        const id = String(node.resource_id || "");
        if (path.has(id)) return false;
        const nextPath = new Set(path);
        nextPath.add(id);

        if (nodeOperationalMatch(node) && textMatch(node)) return true;
        const children = state.childCache.get(cacheKey(id)) || [];
        return children.some((child) => branchMatches(child, nextPath));
    }

    function createBadge(text, className) {
        const span = document.createElement("span");
        span.className = `technicians-badge ${className || ""}`.trim();
        span.textContent = text;
        return span;
    }

    function appendTechnicianDetails(container, node) {
        const details = document.createElement("div");
        details.className = "technicians-node-details";
        const severity = severityInfo(node.operational_severity);
        const integrity = mappedIntegrity(node.integrity_state);
        details.appendChild(createBadge(severity.label, severity.className));
        details.appendChild(createBadge(integrity.label, integrity.className));
        details.appendChild(createBadge(scheduleLabel(node.schedule_state), "neutral"));
        details.appendChild(createBadge(routeLabel(node.route_state), "neutral"));

        if (Number(node.open_activity_count || 0) > 0) {
            details.appendChild(createBadge(`${formatNumber(node.open_activity_count)} OS abertas`, "neutral"));
        }
        if (Number(node.started_count || 0) > 0) {
            details.appendChild(createBadge(`${formatNumber(node.started_count)} iniciadas`, "neutral"));
        }
        if (Number(node.suspended_count || 0) > 0) {
            details.appendChild(createBadge(`${formatNumber(node.suspended_count)} suspensas`, "attention"));
        }
        (node.alert_codes || []).forEach((code) => details.appendChild(createBadge(code, "alert-code")));
        container.appendChild(details);
    }

    function appendAggregateDetails(container, node) {
        const aggregates = node.aggregates || {};
        const details = document.createElement("div");
        details.className = "technicians-node-details";
        details.appendChild(createBadge(`${formatNumber(aggregates.technician_count)} técnicos`, "neutral"));
        if (Number(aggregates.active_route_count || 0) > 0) {
            details.appendChild(createBadge(`${formatNumber(aggregates.active_route_count)} rotas ativas`, "ok"));
        }
        if (Number(aggregates.waiting_route_count || 0) > 0) {
            details.appendChild(createBadge(`${formatNumber(aggregates.waiting_route_count)} aguardando`, "attention"));
        }
        if (Number(aggregates.alert_count || 0) > 0) {
            details.appendChild(createBadge(`${formatNumber(aggregates.alert_count)} alertas`, "alert"));
        }
        if (Number(aggregates.attention_count || 0) > 0) {
            details.appendChild(createBadge(`${formatNumber(aggregates.attention_count)} atenção`, "attention"));
        }
        if (Number(aggregates.integrity_degraded_count || 0) > 0) {
            details.appendChild(createBadge(`${formatNumber(aggregates.integrity_degraded_count)} dados degradados`, "stale"));
        }
        if (Number(aggregates.open_activity_count || 0) > 0) {
            details.appendChild(createBadge(`${formatNumber(aggregates.open_activity_count)} OS abertas`, "neutral"));
        }
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

        const expanded = state.expanded.has(id);
        if (node.has_children) item.setAttribute("aria-expanded", expanded ? "true" : "false");

        const row = document.createElement("div");
        row.className = "technicians-node-row";

        if (node.has_children) {
            const toggle = document.createElement("button");
            toggle.type = "button";
            toggle.className = "technicians-tree-toggle";
            toggle.dataset.treeToggle = id;
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

        if (isTechnicianNode(node)) appendTechnicianDetails(copy, node);
        else appendAggregateDetails(copy, node);

        row.appendChild(copy);
        item.appendChild(row);

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
            } else {
                if (state.parentErrors.has(key)) {
                    const error = document.createElement("li");
                    error.className = "technicians-tree-message error";
                    error.textContent = `${state.parentErrors.get(key)} Exibindo o último conteúdo carregado, quando disponível.`;
                    group.appendChild(error);
                }
                const children = state.childCache.get(key) || [];
                const visibleChildren = children.filter((child) => branchMatches(child, pathSet));
                if (!visibleChildren.length) {
                    const empty = document.createElement("li");
                    empty.className = "technicians-tree-message";
                    empty.textContent = children.length ? "Nenhum item neste ramo corresponde ao filtro atual." : "Nó sem filhos.";
                    group.appendChild(empty);
                } else {
                    visibleChildren.forEach((child) => {
                        const childElement = createTreeNode(child, level + 1, pathSet);
                        if (childElement) group.appendChild(childElement);
                    });
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
            if (state.loadingParents.has(rootKey)) {
                setTreeState("Carregando raiz da hierarquia...");
            } else if (state.parentErrors.has(rootKey)) {
                setTreeState(state.parentErrors.get(rootKey), "error");
            }
            return;
        }

        if (state.parentErrors.has(rootKey) && !roots.length) {
            tree.classList.add("hidden");
            setTreeState(state.parentErrors.get(rootKey), "error");
            return;
        }

        const visibleRoots = roots.filter((node) => branchMatches(node));
        if (!visibleRoots.length) {
            tree.classList.add("hidden");
            const hasSearch = Boolean((searchInput && searchInput.value || "").trim());
            setTreeState(hasSearch
                ? "Nenhum nó já carregado corresponde à busca e ao filtro atuais."
                : "Nenhum ramo corresponde ao filtro atual.");
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
        if (metrics.firstRootDomElements === null && state.expanded.size === 0 && state.initialLoadCompleted) {
            metrics.firstRootDomElements = metrics.currentTreeDomElements;
        }
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

        const results = await Promise.allSettled([
            loadSummary(),
            loadChildren(null, { force: true }),
        ]);

        state.loadingInitial = false;
        state.initialLoadCompleted = true;
        setRefreshBusy(false);
        renderTree();

        const firstRequests = metrics.requestLog.slice(requestLogStart);
        metrics.firstLoadRequestCount = firstRequests.length;
        metrics.firstLoadBytes = firstRequests.reduce((total, item) => total + Number(item.bytes || 0), 0);
        if (metrics.firstRootDomElements === null) {
            metrics.firstRootDomElements = tree ? tree.querySelectorAll("*").length : 0;
        }

        const failures = results.filter((result) => result.status === "rejected");
        if (!state.summary) renderSummary();
        if (failures.length) {
            showError(failures[0].reason && failures[0].reason.message
                ? failures[0].reason.message
                : "Parte do monitor local não pôde ser carregada.");
        }
        startPolling();
    }

    async function runWithConcurrency(items, worker, limit) {
        const queue = items.slice();
        const runners = Array.from({ length: Math.min(limit, queue.length) }, async function () {
            while (queue.length) {
                const item = queue.shift();
                await worker(item);
            }
        });
        await Promise.all(runners);
    }

    async function refreshControlled() {
        if (!state.initialized || !state.active || document.hidden) return;
        if (state.refreshPromise) return state.refreshPromise;

        clearError();
        setRefreshBusy(true);
        state.refreshPromise = (async function () {
            const parents = [null, ...Array.from(state.expanded)];
            const branchFailures = [];
            const summaryTask = loadSummary();
            const treeTask = runWithConcurrency(parents, async function (parentId) {
                try {
                    await loadChildren(parentId, { force: true });
                } catch (error) {
                    branchFailures.push(error);
                    // O ramo antigo permanece no cache quando já existia; o erro fica sinalizado localmente.
                }
            }, MAX_REFRESH_CONCURRENCY);

            const results = await Promise.allSettled([summaryTask, treeTask]);
            const failure = results.find((result) => result.status === "rejected");
            const effectiveFailure = failure && failure.reason ? failure.reason : branchFailures[0];
            if (effectiveFailure) {
                showError(effectiveFailure.message || "Não foi possível concluir a atualização do monitor local.");
            }
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
        state.pollTimer = window.setInterval(function () {
            if (!state.active || document.hidden) return;
            refreshControlled();
        }, POLL_INTERVAL_MS);
    }

    function setActivePanel(view) {
        const techniciansActive = view === "technicians";
        if (state.active === techniciansActive && ((techniciansActive && !techniciansPanel.classList.contains("hidden")) || (!techniciansActive && !osPanel.classList.contains("hidden")))) {
            return;
        }

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
            if (!state.initialized) {
                loadInitial();
            } else {
                refreshControlled();
                startPolling();
            }
        } else {
            stopPolling();
        }
    }

    tabs.forEach((button) => {
        button.addEventListener("click", function () {
            setActivePanel(button.dataset.dashboardView);
        });
        button.addEventListener("keydown", function (event) {
            if (!['ArrowLeft', 'ArrowRight'].includes(event.key)) return;
            event.preventDefault();
            const index = tabs.indexOf(button);
            const nextIndex = event.key === 'ArrowRight'
                ? (index + 1) % tabs.length
                : (index - 1 + tabs.length) % tabs.length;
            tabs[nextIndex].focus();
            setActivePanel(tabs[nextIndex].dataset.dashboardView);
        });
    });

    if (tree) {
        tree.addEventListener("click", async function (event) {
            const button = event.target.closest("[data-tree-toggle]");
            if (!button || !tree.contains(button)) return;
            const resourceId = button.dataset.treeToggle;
            if (!resourceId) return;

            if (state.expanded.has(resourceId)) {
                state.expanded.delete(resourceId);
                renderTree();
                return;
            }

            state.expanded.add(resourceId);
            renderTree();
            const key = cacheKey(resourceId);
            if (!state.childCache.has(key)) {
                try {
                    await loadChildren(resourceId, { force: false });
                } catch (error) {
                    showError(error.message || "Não foi possível carregar os filhos deste nó.");
                }
            }
            renderTree();
        });
    }

    if (filters) {
        filters.addEventListener("click", async function (event) {
            const button = event.target.closest("[data-technicians-filter]");
            if (!button || !filters.contains(button)) return;
            const nextFilter = button.dataset.techniciansFilter || "all";
            if (nextFilter === state.selectedFilter) return;

            const nextOnlyProblems = nextFilter === "problems";
            const backendScopeChanged = nextOnlyProblems !== state.onlyProblems;
            state.selectedFilter = nextFilter;
            state.onlyProblems = nextOnlyProblems;

            filters.querySelectorAll("[data-technicians-filter]").forEach((item) => {
                item.classList.toggle("active", item === button);
                item.setAttribute("aria-pressed", item === button ? "true" : "false");
            });

            if (backendScopeChanged && state.initialized) {
                state.childCache.clear();
                state.expanded.clear();
                state.parentErrors.clear();
                clearError();
                setTreeState("Recarregando a raiz com o filtro selecionado...");
                try {
                    await loadChildren(null, { force: true });
                } catch (error) {
                    showError(error.message || "Não foi possível aplicar o filtro no monitor local.");
                }
            }
            renderTree();
        });
    }

    if (searchInput) {
        searchInput.addEventListener("input", renderTree);
    }

    if (refreshButton) {
        refreshButton.addEventListener("click", function () {
            refreshControlled();
        });
    }

    document.addEventListener("visibilitychange", function () {
        if (!state.active) return;
        if (document.hidden) {
            stopPolling();
        } else {
            refreshControlled();
            startPolling();
        }
    });
});
