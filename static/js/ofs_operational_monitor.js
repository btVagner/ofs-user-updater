(function () {
  "use strict";

  const root = document.querySelector("#operational-monitor");
  if (!root) return;

  const VIEW_KEYS = ["late", "idle", "notStarted", "slot", "black"];
  const ACTIVITY_TYPE_VIEWS = new Set(["late", "slot", "black"]);
  const TREATABLE_VIEWS = new Set(["late", "idle", "notStarted", "slot"]);
  const TREATMENT_LABELS = { open: "Em aberto", analysis: "Em análise", waiting: "Aguardando", resolved: "Resolvido" };
  const TITLES = {
    late: "OS em alerta",
    idle: "Técnicos sem OS",
    notStarted: "Rotas não iniciadas",
    slot: "Fora do turno",
    black: "Clientes Black",
  };
  const HEADERS = {
    late: ["OS / Atividade", "Técnico", "Área", "Tipo", "Início", "Duração prevista", "Tempo decorrido", "Limite", "Excesso", "Status"],
    idle: ["Técnico", "Resource ID", "Área", "Rota iniciada", "Turno", "OS elegíveis", "Situação"],
    notStarted: ["Técnico", "Resource ID", "Área", "Turno previsto", "Início previsto", "Atraso", "Situação"],
    slot: ["OS / Atividade", "Técnico", "Área", "Tipo", "Turno", "Início da OS", "Limite do turno", "Atraso", "Status"],
    black: ["OS / Atividade", "Técnico", "Área", "Tipo", "Início previsto / real", "Status", "XA_CLI_ATRI"],
  };

  const q = (selector) => root.querySelector(selector);
  const qa = (selector) => Array.from(root.querySelectorAll(selector));
  const activityTypeFilter = q("[data-activity-type-filter]");
  const legacyActivityTypeSelect = q("select[data-activity-type]");
  const statusFilter = q("[data-status-filter]");
  const legacyStatusSelect = q("select[data-status]");
  // Durante um restart gradual, HTML antigo pode receber este asset novo.
  // O monitor básico deve continuar carregando mesmo sem a UI de tratativas.
  const treatmentUiReady = Boolean(
    root.dataset.treatmentsUrl && root.dataset.claimUrl && root.dataset.changeUrl && root.dataset.csrf &&
    q("[data-treatment-filter]") && q("[data-refresh-treatments]") && q("[data-treatment-dialog]")
  );
  const treatableView = (view) => treatmentUiReady && TREATABLE_VIEWS.has(view);
  const state = {
    mode: "all", view: "late", snapshot: null, payload: null, rows: {},
    selectedBuckets: new Set(), selectedStates: new Set(), knownStates: new Set(),
    excludedStatusesByView: new Map(), statusOptionsByView: new Map(),
    excludedActivityTypesByView: new Map(), activityTypeOptionsByView: new Map(),
    busy: false, timer: null, treatments: new Map(), treatmentsLoaded: false,
    treatmentRequestSerial: 0, activeTreatment: null, treatmentBusy: false, treatmentError: false,
  };

  function normalized(value) {
    return String(value == null ? "" : value).trim().toLocaleLowerCase("pt-BR");
  }

  function integerInput(element, minimum, maximum, fallback) {
    const value = Number(element.value);
    return Number.isInteger(value) && value >= minimum && value <= maximum ? value : fallback;
  }

  function minutes(value) {
    return Number.isFinite(value) ? `${Math.round(value).toLocaleString("pt-BR")} min` : "—";
  }

  function activityLabel(row) {
    return row.appt ? `${row.appt} · #${row.id}` : `#${row.id}`;
  }

  function itemKey(row, view) {
    return String(["idle", "notStarted"].includes(view) ? row.resource_id : row.id);
  }

  function treatmentKey(row, view) {
    return `${view}:${itemKey(row, view)}`;
  }

  function treatmentFor(row, view) {
    return state.treatments.get(treatmentKey(row, view)) || { status: "open" };
  }

  function treatmentLabel(item) {
    if (item.status === "analysis" && item.actor_username) return `Em análise · ${item.actor_username}`;
    if (item.status === "resolved" && item.resolved_by_username) return `Resolvido · ${item.resolved_by_username}`;
    return TREATMENT_LABELS[item.status] || "Em aberto";
  }

  function computeRows() {
    const payload = state.payload || {};
    const now = Date.now();
    const tolerance = integerInput(q("[data-tolerance]"), 0, 500, 20);
    const delay = integerInput(q("[data-delay]"), 0, 1440, 15);
    const excludeWithdrawals = q("[data-exclude-withdrawals]").checked;

    const late = (payload.late_candidates || []).map((row) => {
      const elapsed = (now - Number(row.started_epoch_ms)) / 60000;
      const threshold = Number(row.duration_minutes) * (1 + tolerance / 100);
      return Object.assign({}, row, { elapsed, threshold, excess: elapsed - threshold });
    }).filter((row) => Number.isFinite(row.excess) && row.excess > 0)
      .sort((a, b) => b.excess - a.excess);

    const notStarted = (payload.not_started_candidates || []).map((row) => {
      const lateMinutes = (now - Number(row.shift_start_epoch_ms)) / 60000;
      return Object.assign({}, row, { late_minutes: lateMinutes });
    }).filter((row) => row.late_minutes > delay && now < Number(row.shift_end_epoch_ms))
      .sort((a, b) => b.late_minutes - a.late_minutes);

    state.rows = {
      late,
      idle: (payload.idle || []).slice(),
      notStarted,
      slot: (payload.slot || []).filter((row) => !excludeWithdrawals || !row.is_withdrawal),
      black: (payload.black || []).slice(),
    };
  }

  function cells(row, view) {
    if (view === "late") return [activityLabel(row), row.tech, row.area, row.type, row.start, minutes(row.duration_minutes), minutes(row.elapsed), minutes(row.threshold), `+${minutes(row.excess)}`, "Iniciada"];
    if (view === "idle") return [row.tech, row.resource_id, row.area, row.route_start, row.shift, String(row.os_count), row.situation];
    if (view === "notStarted") return [row.tech, row.resource_id, row.area, row.shift, row.expected, minutes(row.late_minutes), row.situation];
    if (view === "slot") return [activityLabel(row), row.tech, row.area, row.type, `${row.slot_label} (${row.slot_key.replace("-", "–")})`, row.start, row.slot_end, minutes(row.slot_late_minutes), row.status];
    if (view === "black") return [activityLabel(row), row.tech, row.area, row.type, row.start, row.status, String(row.black_value)];
    return [];
  }

  function currentRows() {
    const search = normalized(q("[data-search]").value);
    const status = legacyStatusSelect && !statusFilter ? legacyStatusSelect.value : "";
    const excludedStatuses = state.excludedStatusesByView.get(state.view) || new Set();
    const hasStatusOptions = (state.statusOptionsByView.get(state.view) || []).length > 0;
    const activityType = legacyActivityTypeSelect && !activityTypeFilter && ACTIVITY_TYPE_VIEWS.has(state.view)
      ? legacyActivityTypeSelect.value : "";
    const excludedActivityTypes = state.excludedActivityTypesByView.get(state.view) || new Set();
    const hasActivityTypeOptions = (state.activityTypeOptionsByView.get(state.view) || []).length > 0;
    const treatmentFilter = treatmentUiReady ? q("[data-treatment-filter]").value : "";
    const rows = (state.rows[state.view] || []).filter((row) => {
      if (!matchesSelectedStates(row)) return false;
      if (state.selectedBuckets.size && !state.selectedBuckets.has(row.area)) return false;
      if (activityType && String(row.type || "").trim() !== activityType) return false;
      if (activityTypeFilter && hasActivityTypeOptions && excludedActivityTypes.has(normalized(row.type))) return false;
      if (status && normalized(row.status) !== status) return false;
      if (statusFilter && hasStatusOptions && excludedStatuses.has(normalized(row.status))) return false;
      if (treatableView(state.view) && treatmentFilter && treatmentFor(row, state.view).status !== treatmentFilter) return false;
      if (!search) return true;
      return [row.id, row.appt, row.tech, row.resource_id, row.area, row.type, row.status, row.time_slot, row.customer, ...rowStates(row)]
        .some((value) => normalized(value).includes(search));
    });
    if (treatableView(state.view)) {
      const order = { open: 0, waiting: 1, analysis: 2, resolved: 3 };
      rows.sort((a, b) => (order[treatmentFor(a, state.view).status] ?? 0) - (order[treatmentFor(b, state.view).status] ?? 0));
    }
    return rows;
  }

  function rowStates(row) {
    const values = Array.isArray(row.states) ? row.states : [row.state];
    const normalizedStates = values.map((value) => String(value || "").trim()).filter(Boolean);
    return normalizedStates.length ? normalizedStates : ["Sem UF"];
  }

  function matchesSelectedStates(row) {
    return state.selectedStates.size === 0 || rowStates(row).some((value) => state.selectedStates.has(value));
  }

  function renderKpis() {
    const payload = state.payload;
    const groups = payload && payload.technician_uf_groups;
    const allStates = state.selectedStates.size === 0 || state.selectedStates.size === state.knownStates.size;
    const techCount = !payload ? null : allStates ? Number(payload.technicians_count || 0)
      : Array.isArray(groups) ? groups.reduce((total, group) =>
        total + (group.states.some((value) => state.selectedStates.has(value)) ? Number(group.count || 0) : 0), 0) : null;
    const techCard = q('[data-kpi="tech"]');
    techCard.textContent = techCount === null ? "—" : techCount.toLocaleString("pt-BR");
    techCard.title = payload && techCount === null ? "Atualize o snapshot local para consultar o total de técnicos por UF." : "";

    VIEW_KEYS.forEach((key) => {
      const count = (state.rows[key] || []).filter(matchesSelectedStates).length;
      const label = payload ? count.toLocaleString("pt-BR") : "—";
      q(`[data-kpi="${key}"]`).textContent = label;
      q(`[data-count="${key}"]`).textContent = label;
    });
  }

  function renderStateOptions() {
    const options = Array.from(new Set(
      VIEW_KEYS.flatMap((view) => (state.rows[view] || []).flatMap(rowStates)).concat(
        (state.payload && state.payload.technician_uf_groups || []).flatMap((group) => group.states || [])
      )
    )).sort((a, b) => a.localeCompare(b, "pt-BR"));
    const available = new Set(options);
    const hadAllSelected = state.knownStates.size > 0 &&
      Array.from(state.knownStates).every((value) => state.selectedStates.has(value));

    state.selectedStates.forEach((value) => {
      if (!available.has(value)) state.selectedStates.delete(value);
    });
    state.knownStates.forEach((value) => {
      if (!available.has(value)) state.knownStates.delete(value);
    });
    options.forEach((value) => {
      if (hadAllSelected && !state.knownStates.has(value)) state.selectedStates.add(value);
      state.knownStates.add(value);
    });

    const container = q("[data-state-options]");
    // Durante uma atualização sem reinício imediato do Gunicorn, o navegador
    // pode receber o asset novo enquanto o processo ainda renderiza o template
    // antigo. Mantém a tabela funcional até as versões voltarem a coincidir.
    if (!container) return;

    const fragment = document.createDocumentFragment();
    options.forEach((value) => {
      const button = document.createElement("button");
      const selected = state.selectedStates.has(value);
      button.type = "button";
      button.textContent = value;
      button.className = selected ? "is-active" : "";
      button.setAttribute("aria-pressed", String(selected));
      button.addEventListener("click", () => {
        if (state.selectedStates.has(value)) state.selectedStates.delete(value);
        else state.selectedStates.add(value);
        renderStateOptions();
        renderTable();
      });
      fragment.appendChild(button);
    });
    container.replaceChildren(fragment);
    container.closest(".om-state-filter").hidden = options.length === 0;
    const clearButton = q("[data-clear-states]");
    if (clearButton) clearButton.disabled = state.selectedStates.size === 0;
  }

  function clearStateFilter() {
    state.selectedStates.clear();
    renderStateOptions();
    renderTable();
  }

  function replaceStatusOptions(select, options) {
    const selected = select.value;
    select.replaceChildren(new Option("Todos os status", ""), ...options.map((value) => new Option(value, normalized(value))));
    if (options.some((value) => normalized(value) === selected)) select.value = selected;
    select.disabled = options.length === 0;
  }

  function prepareMultiOptions(rows, view, field, emptyLabel, optionsByView, excludedByView) {
    const values = new Map();
    rows.forEach((row) => {
      const label = String(row[field] || "").trim();
      if (label && !values.has(normalized(label))) values.set(normalized(label), label);
    });
    const options = Array.from(values, ([key, label]) => ({ key, label }))
      .sort((a, b) => a.label.localeCompare(b.label, "pt-BR"));
    if (options.length && rows.some((row) => !normalized(row[field]))) {
      options.push({ key: "", label: emptyLabel });
    }
    const previousOptions = optionsByView.get(view) || [];
    const excluded = excludedByView.get(view) || new Set();
    if (previousOptions.some((item) => excluded.has(item.key))) {
      const previousKeys = new Set(previousOptions.map((item) => item.key));
      options.forEach((item) => { if (!previousKeys.has(item.key)) excluded.add(item.key); });
    }
    optionsByView.set(view, options);
    excludedByView.set(view, excluded);
    return { options, excluded };
  }

  function renderMultiCheckboxes(filter, container, summary, options, excluded, allLabel, noneLabel, countLabel) {
    const wasOpen = filter.open;
    const fragment = document.createDocumentFragment();
    const selectedCount = options.filter((item) => !excluded.has(item.key)).length;

    const createOption = (labelText, checked, allOption, key) => {
      const label = document.createElement("label");
      label.className = allOption ? "om-bucket-all" : "";
      const input = document.createElement("input");
      input.type = "checkbox";
      input.checked = checked;
      if (allOption) input.indeterminate = selectedCount > 0 && selectedCount < options.length;
      input.addEventListener("change", () => {
        if (allOption) {
          options.forEach((item) => input.checked ? excluded.delete(item.key) : excluded.add(item.key));
        } else if (input.checked) excluded.delete(key);
        else excluded.add(key);
        renderTable();
      });
      const span = document.createElement("span");
      span.textContent = labelText;
      label.append(input, span);
      return label;
    };

    fragment.appendChild(createOption(allLabel, selectedCount === options.length, true));
    options.forEach((item) => fragment.appendChild(createOption(item.label, !excluded.has(item.key), false, item.key)));
    container.replaceChildren(fragment);
    filter.hidden = options.length === 0;
    filter.open = wasOpen && options.length > 0;
    summary.textContent = selectedCount === options.length ? allLabel
      : selectedCount === 0 ? noneLabel
        : selectedCount === 1 ? options.find((item) => !excluded.has(item.key)).label
          : `${selectedCount} ${countLabel}`;
  }

  function renderStatusOptions(rows, view) {
    const { options, excluded } = prepareMultiOptions(
      rows, view, "status", "Sem status", state.statusOptionsByView, state.excludedStatusesByView
    );
    // Durante um deploy, o asset novo pode chegar antes do template atualizado.
    if (!statusFilter) {
      if (legacyStatusSelect) replaceStatusOptions(legacyStatusSelect, options.filter((item) => item.key).map((item) => item.label));
      return;
    }
    renderMultiCheckboxes(statusFilter, q("[data-status-options]"), q("[data-status-summary]"),
      options, excluded, "Todos os status", "Nenhum status", "status selecionados");
  }

  function renderActivityTypeOptions(rows, view) {
    const eligible = ACTIVITY_TYPE_VIEWS.has(view);
    if (activityTypeFilter) activityTypeFilter.hidden = !eligible;
    if (legacyActivityTypeSelect) legacyActivityTypeSelect.hidden = !eligible;
    if (!eligible) {
      if (legacyActivityTypeSelect) legacyActivityTypeSelect.value = "";
      return;
    }
    const { options, excluded } = prepareMultiOptions(
      rows, view, "type", "Sem tipo", state.activityTypeOptionsByView, state.excludedActivityTypesByView
    );
    if (!activityTypeFilter) {
      if (legacyActivityTypeSelect) {
        const selected = legacyActivityTypeSelect.value;
        const types = options.filter((item) => item.key).map((item) => item.label);
        legacyActivityTypeSelect.replaceChildren(new Option("Todos os tipos", ""), ...types.map((type) => new Option(type, type)));
        if (types.includes(selected)) legacyActivityTypeSelect.value = selected;
        legacyActivityTypeSelect.disabled = types.length === 0;
      }
      return;
    }
    renderMultiCheckboxes(activityTypeFilter, q("[data-activity-type-options]"), q("[data-activity-type-summary]"),
      options, excluded, "Todos os tipos", "Nenhum tipo", "tipos selecionados");
  }

  function renderBucketOptions(options) {
    const available = new Set(options);
    state.selectedBuckets.forEach((value) => {
      if (!available.has(value)) state.selectedBuckets.delete(value);
    });

    const container = q("[data-bucket-options]");
    const details = q("[data-bucket-filter]");
    const wasOpen = details.open;
    const fragment = document.createDocumentFragment();

    const createOption = (labelText, value, checked, allOption) => {
      const label = document.createElement("label");
      label.className = allOption ? "om-bucket-all" : "";
      const input = document.createElement("input");
      input.type = "checkbox";
      input.checked = checked;
      input.dataset.bucket = value;
      input.addEventListener("change", () => {
        if (allOption) state.selectedBuckets.clear();
        else if (input.checked) state.selectedBuckets.add(value);
        else state.selectedBuckets.delete(value);
        renderTable();
      });
      const span = document.createElement("span");
      span.textContent = labelText;
      label.append(input, span);
      return label;
    };

    fragment.appendChild(createOption("Todos os buckets", "", state.selectedBuckets.size === 0, true));
    options.forEach((value) => fragment.appendChild(createOption(value, value, state.selectedBuckets.has(value), false)));
    container.replaceChildren(fragment);
    details.open = wasOpen;

    const selected = Array.from(state.selectedBuckets);
    q("[data-bucket-summary]").textContent = selected.length === 0
      ? "Todos os buckets"
      : selected.length === 1 ? selected[0] : `${selected.length} buckets selecionados`;
  }

  function renderTable() {
    renderKpis();
    const view = state.view;
    q("[data-table-title]").textContent = TITLES[view];
    qa("[data-view]").forEach((button) => button.classList.toggle("is-active", button.dataset.view === view));

    const sourceRows = state.rows[view] || [];
    const buckets = Array.from(new Set(sourceRows.map((row) => row.area).filter(Boolean))).sort((a, b) => a.localeCompare(b, "pt-BR"));
    renderBucketOptions(buckets);
    renderActivityTypeOptions(sourceRows, view);
    renderStatusOptions(sourceRows, view);
    if (treatmentUiReady) q("[data-treatment-filter]").hidden = !treatableView(view);
    const rows = currentRows();

    const headRow = document.createElement("tr");
    const headers = treatableView(view) ? ["Tratativa", "Ação", ...HEADERS[view]] : HEADERS[view];
    headers.forEach((title) => {
      const th = document.createElement("th");
      th.textContent = title;
      headRow.appendChild(th);
    });
    q("[data-thead]").replaceChildren(headRow);

    const body = q("[data-tbody]");
    body.replaceChildren();
    if (!state.payload || !rows.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = headers.length;
      td.className = "om-empty";
      td.textContent = state.payload ? "Nenhum registro para os filtros e parâmetros atuais." : "Não há snapshot disponível para esta macro.";
      tr.appendChild(td);
      body.appendChild(tr);
    } else {
      const fragment = document.createDocumentFragment();
      rows.slice(0, 150).forEach((item) => {
        const tr = document.createElement("tr");
        const treatment = treatmentFor(item, view);
        if (treatableView(view)) tr.classList.add(`om-treatment-${treatment.status}`);
        if (treatableView(view)) {
          const statusCell = document.createElement("td");
          const badge = document.createElement("span");
          badge.className = `om-treatment-badge om-treatment-badge-${treatment.status}`;
          badge.textContent = treatmentLabel(treatment);
          statusCell.appendChild(badge);
          tr.appendChild(statusCell);
          const actionCell = document.createElement("td");
          if (root.dataset.canTreat === "true" && ["open", "waiting"].includes(treatment.status)) {
            const button = document.createElement("button");
            button.type = "button";
            button.className = "om-treat-button";
            button.textContent = "Tratar";
            button.disabled = !state.treatmentsLoaded || state.treatmentBusy;
            button.addEventListener("click", () => claimTreatment(item, view));
            actionCell.appendChild(button);
          } else {
            actionCell.textContent = "—";
          }
          tr.appendChild(actionCell);
        }
        cells(item, view).forEach((value, index) => {
          const td = document.createElement("td");
          td.textContent = String(value == null ? "—" : value);
          if (treatment.status !== "resolved" && ((view === "late" && index === 8) || (view === "slot" && index === 7) || (view === "notStarted" && index === 5))) td.classList.add("om-danger-text");
          tr.appendChild(td);
        });
        fragment.appendChild(tr);
      });
      body.appendChild(fragment);
    }
    q("[data-shown]").textContent = `${rows.length.toLocaleString("pt-BR")} registro(s) filtrados · ${Math.min(rows.length, 150)} exibidos`;
    q("[data-data-status]").textContent = state.payload ?
      (!treatmentUiReady ? "Snapshot compartilhado do MySQL" : state.treatmentError ? "Tratativas indisponíveis" : state.treatmentsLoaded ? "Snapshot e tratativas do MySQL" : "Carregando tratativas...") : "Sem snapshot";
    q("[data-export]").disabled = !state.payload || state.busy;
  }

  function render() {
    computeRows();
    renderStateOptions();
    q("[data-tabs]").hidden = state.mode !== "all";
    qa("[data-param]").forEach((field) => { field.hidden = state.mode !== "all" && field.dataset.param !== state.mode; });
    q("[data-withdrawal-param]").hidden = !["all", "slot"].includes(state.mode);
    renderTable();
  }

  function message(text, type) {
    const element = q("[data-message]");
    element.textContent = text;
    element.className = `om-message${type ? ` ${type}` : ""}`;
  }

  function formatDateTime(value) {
    if (!value) return "—";
    const parsed = new Date(`${value}Z`);
    return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString("pt-BR");
  }

  function updateStatus() {
    const snapshot = state.snapshot || {};
    const dot = q("[data-status-dot]");
    dot.className = "om-status-dot";
    if (snapshot.status === "ready") dot.classList.add("ready");
    else if (snapshot.status === "failed") dot.classList.add("failed");
    else if (snapshot.status === "refreshing") dot.classList.add("warning");

    const title = snapshot.status === "ready" ? "Snapshot disponível" : snapshot.status === "failed" ? "Última atualização falhou" : snapshot.status === "refreshing" ? "Atualização em andamento" : "Snapshot ainda não criado";
    q("[data-status-title]").textContent = title;
    q("[data-status-detail]").textContent = snapshot.has_payload ? `Atualizado em ${formatDateTime(snapshot.refreshed_at)}` : "Nenhuma chamada ao OFS é feita por esta tela.";
    q("[data-last-update]").textContent = snapshot.has_payload ? `Atualizado ${formatDateTime(snapshot.refreshed_at)}` : "Sem snapshot";

    const remaining = Number(snapshot.remaining_seconds || 0);
    q("[data-refresh]").disabled = state.busy || remaining > 0 || snapshot.refresh_in_progress === true;
    q("[data-refresh]").textContent = state.busy ? "Atualizando..." : remaining > 0 ? `Disponível em ${Math.ceil(remaining / 60)} min` : "Atualizar snapshot local";
  }

  function diagnostics() {
    const snapshot = state.snapshot || {};
    const payload = state.payload || {};
    q("[data-diagnostics]").textContent = JSON.stringify({
      macro: snapshot.scope || null,
      status: snapshot.status || "missing",
      atualizado_em: snapshot.refreshed_at || null,
      expira_em: snapshot.expires_at || null,
      atualizado_por: snapshot.requested_by_username || null,
      erro: snapshot.error_text || null,
      fontes: payload.source_health || {},
      diagnostico: payload.diagnostics || {},
    }, null, 2);
  }

  function acceptSnapshot(snapshot) {
    state.snapshot = snapshot || {};
    state.payload = snapshot && snapshot.has_payload ? snapshot.payload : null;
    state.treatmentsLoaded = false;
    state.treatmentError = false;
    state.treatments.clear();
    updateStatus();
    diagnostics();
    render();
    if (snapshot.status === "failed") message(snapshot.has_payload ? "A última atualização falhou; o snapshot anterior foi preservado." : (snapshot.error_text || "A atualização falhou."), "error");
    else if (!snapshot.has_payload) message("Ainda não há snapshot para esta macro. Quando a fonte local estiver sincronizada, use Atualizar snapshot local.", "warning");
    else if (snapshot.payload && snapshot.payload.diagnostics && snapshot.payload.diagnostics.data_complete === false) message("Snapshot carregado, mas uma ou mais fontes locais estão desatualizadas. Regras baseadas em ausência foram suprimidas para evitar falsos alertas.", "warning");
    else message("Dados carregados do snapshot compartilhado. Filtros, abas e exportação não geram chamadas ao OFS.");
    if (state.payload && treatmentUiReady) loadTreatments();
  }

  async function readJson(response) {
    const body = await response.json().catch(() => ({}));
    if (!response.ok && !body.error) throw new Error(`Falha HTTP ${response.status}.`);
    return body;
  }

  async function loadTreatments() {
    if (!treatmentUiReady || !state.payload) return;
    const serial = ++state.treatmentRequestSerial;
    try {
      const scope = root.dataset.scope || "casa-cliente";
      const response = await fetch(`${root.dataset.treatmentsUrl}?scope=${encodeURIComponent(scope)}`, {
        headers: { Accept: "application/json" }, cache: "no-store",
      });
      const body = await readJson(response);
      if (!response.ok || !body.ok) throw new Error((body.error && body.error.message) || "Falha ao atualizar tratativas.");
      if (serial !== state.treatmentRequestSerial) return;
      if (body.data.work_date !== state.payload.work_date ||
          body.data.snapshot_refreshed_at !== state.snapshot.refreshed_at) {
        // Uma tentativa de refresh pode falhar e preservar o payload do dia anterior.
        // Recarregar o mesmo snapshot aqui criaria um ciclo sem fim de requests.
        state.treatmentsLoaded = false;
        state.treatmentError = true;
        message("O snapshot exibido é anterior à fonte operacional atual. As tratativas ficam indisponíveis até um novo snapshot válido.", "warning");
        renderTable();
        return;
      }
      state.treatments = new Map((body.data.items || []).map((item) => [`${item.indicator}:${item.item_key}`, item]));
      state.treatmentsLoaded = true;
      const recovered = state.treatmentError;
      state.treatmentError = false;
      renderTable();
      if (recovered) message("Tratativas locais sincronizadas novamente.");
    } catch (error) {
      if (serial !== state.treatmentRequestSerial) return;
      state.treatmentsLoaded = false;
      state.treatmentError = true;
      message(error.message, "error");
      renderTable();
    }
  }

  async function postTreatment(url, payload) {
    const response = await fetch(url, {
      method: "POST",
      headers: { Accept: "application/json", "Content-Type": "application/json", "X-Monitor-CSRF": root.dataset.csrf },
      body: JSON.stringify(Object.assign({ scope: root.dataset.scope || "casa-cliente" }, payload)),
    });
    const body = await readJson(response);
    if (!response.ok || !body.ok) {
      const error = new Error((body.error && body.error.message) || "Não foi possível salvar a tratativa.");
      error.current = body.current;
      throw error;
    }
    return body.data;
  }

  function treatmentIdentity(row, view) {
    return { work_date: state.payload.work_date, indicator: view, item_key: itemKey(row, view) };
  }

  function updateLocalTreatment(item) {
    if (!item) return;
    // Uma resposta de polling anterior ao clique não pode restaurar estado velho.
    state.treatmentRequestSerial += 1;
    state.treatments.set(`${item.indicator}:${item.item_key}`, item);
    state.treatmentsLoaded = true;
    renderTable();
  }

  function renderTreatmentDetails(row, view) {
    const details = q("[data-treatment-details]");
    const entries = [
      ["Visão", TITLES[view]],
      [view === "idle" || view === "notStarted" ? "Técnico" : "OS / Atividade", view === "idle" || view === "notStarted" ? row.tech : activityLabel(row)],
      ["Técnico / ID", `${row.tech || "—"} · ${row.resource_id || "—"}`],
      ["Área", row.area],
      ["UF", rowStates(row).join(", ")],
    ];
    if (view === "late") entries.push(["Início", row.start], ["Excesso", minutes(row.excess)]);
    if (view === "idle") entries.push(["Turno", row.shift], ["Rota iniciada", row.route_start]);
    if (view === "notStarted") entries.push(["Turno", row.shift], ["Atraso", minutes(row.late_minutes)]);
    if (view === "slot") entries.push(["Turno", row.slot_label], ["Início da OS", row.start]);
    entries.push(["Data operacional", state.payload.work_date]);
    details.replaceChildren();
    entries.forEach(([label, value]) => {
      const wrap = document.createElement("div");
      const dt = document.createElement("dt");
      const dd = document.createElement("dd");
      dt.textContent = label;
      dd.textContent = value == null ? "—" : String(value);
      wrap.append(dt, dd);
      details.appendChild(wrap);
    });
  }

  async function claimTreatment(row, view) {
    if (!state.treatmentsLoaded || state.treatmentBusy || state.activeTreatment) return;
    state.treatmentBusy = true;
    renderTable();
    const identity = treatmentIdentity(row, view);
    try {
      const data = await postTreatment(root.dataset.claimUrl, identity);
      updateLocalTreatment(data.item);
      state.activeTreatment = { ...identity, token: data.token };
      renderTreatmentDetails(row, view);
      q("[data-treatment-note]").value = data.item.note || "";
      q("[data-treatment-feedback]").textContent = "Este caso está reservado para você enquanto o modal estiver aberto.";
      q("[data-treatment-dialog]").showModal();
    } catch (error) {
      updateLocalTreatment(error.current);
      message(error.message, "warning");
      loadTreatments();
    } finally {
      state.treatmentBusy = false;
      renderTable();
    }
  }

  async function finishTreatment(action) {
    if (!state.activeTreatment || state.treatmentBusy) return;
    state.treatmentBusy = true;
    qa("[data-treatment-action], [data-treatment-close]").forEach((button) => { button.disabled = true; });
    const active = state.activeTreatment;
    try {
      const data = await postTreatment(root.dataset.changeUrl, {
        ...active, action, note: q("[data-treatment-note]").value,
      });
      state.activeTreatment = null;
      q("[data-treatment-dialog]").close();
      updateLocalTreatment(data.item);
      message(`Caso marcado como ${TREATMENT_LABELS[action].toLowerCase()}.`);
      await loadTreatments();
    } catch (error) {
      updateLocalTreatment(error.current);
      q("[data-treatment-feedback]").textContent = error.message;
      if (error.current) {
        state.activeTreatment = null;
        q("[data-treatment-dialog]").close();
        await loadTreatments();
      }
    } finally {
      state.treatmentBusy = false;
      qa("[data-treatment-action], [data-treatment-close]").forEach((button) => { button.disabled = false; });
      renderTable();
    }
  }

  async function releaseTreatment() {
    const active = state.activeTreatment;
    if (!active || state.treatmentBusy) return;
    state.activeTreatment = null;
    try {
      const data = await postTreatment(root.dataset.changeUrl, { ...active, action: "release" });
      updateLocalTreatment(data.item);
    } catch (error) {
      message(error.message, "warning");
    } finally {
      await loadTreatments();
    }
  }

  async function renewTreatment() {
    if (!state.activeTreatment || state.treatmentBusy || document.hidden) return;
    try {
      const data = await postTreatment(root.dataset.changeUrl, { ...state.activeTreatment, action: "renew" });
      updateLocalTreatment(data.item);
    } catch (error) {
      state.activeTreatment = null;
      q("[data-treatment-dialog]").close();
      message(error.message, "warning");
      loadTreatments();
    }
  }

  async function loadSnapshot() {
    state.busy = true;
    updateStatus();
    try {
      const scope = root.dataset.scope || "casa-cliente";
      const response = await fetch(`${root.dataset.dataUrl}?scope=${encodeURIComponent(scope)}`, { headers: { Accept: "application/json" }, cache: "no-store" });
      const body = await readJson(response);
      if (!response.ok || !body.ok) throw new Error((body.error && body.error.message) || "Não foi possível carregar o snapshot.");
      acceptSnapshot(body.data);
    } catch (error) {
      state.snapshot = null; state.payload = null;
      state.treatmentsLoaded = false; state.treatments.clear();
      render();
      message(error.message, "error");
    } finally {
      state.busy = false;
      updateStatus();
    }
  }

  async function refreshSnapshot() {
    if (state.busy) return;
    state.busy = true;
    updateStatus();
    message("Gerando um novo snapshot a partir do read model MySQL. Nenhuma consulta direta ao OFS será feita.");
    try {
      const response = await fetch(root.dataset.refreshUrl, {
        method: "POST",
        headers: { Accept: "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({ scope: root.dataset.scope || "casa-cliente" }),
      });
      const body = await readJson(response);
      if (body.data) acceptSnapshot(body.data);
      if (!response.ok || !body.ok) throw new Error((body.error && body.error.message) || "Não foi possível atualizar o snapshot.");
      message("Snapshot local atualizado. A próxima atualização ficará bloqueada por 10 minutos.");
    } catch (error) {
      message(error.message, error.message.includes("10 minutos") ? "warning" : "error");
    } finally {
      state.busy = false;
      updateStatus();
    }
  }

  function selectMode(mode) {
    if (state.busy || (mode !== "all" && !VIEW_KEYS.includes(mode))) return;
    state.mode = mode;
    if (mode !== "all") state.view = mode;
    qa("[data-mode]").forEach((button) => {
      const active = button.dataset.mode === mode;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    q("[data-search]").value = "";
    state.selectedBuckets.clear();
    state.excludedStatusesByView.clear();
    state.excludedActivityTypesByView.clear();
    if (legacyStatusSelect) legacyStatusSelect.value = "";
    if (legacyActivityTypeSelect) legacyActivityTypeSelect.value = "";
    if (treatmentUiReady) q("[data-treatment-filter]").value = "";
    render();
  }

  function escapeCsv(value) {
    let text = String(value == null ? "" : value);
    if (/^[\s]*[=+@\-\t\r]/.test(text)) text = `'${text}`;
    return `"${text.replace(/"/g, '""')}"`;
  }

  function exportCsv() {
    if (!state.payload) return;
    const rows = currentRows();
    const treatable = treatableView(state.view);
    const headers = treatable ? ["Tratativa", "Responsável", ...HEADERS[state.view]] : HEADERS[state.view];
    const content = [headers.map(escapeCsv).join(";"), ...rows.map((row) => {
      const treatment = treatmentFor(row, state.view);
      const values = treatable ? [TREATMENT_LABELS[treatment.status], treatment.resolved_by_username || treatment.actor_username || "", ...cells(row, state.view)] : cells(row, state.view);
      return values.map(escapeCsv).join(";");
    })].join("\r\n");
    const url = URL.createObjectURL(new Blob(["\ufeff" + content], { type: "text/csv;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = `ofs_monitor_${state.view}_${state.payload.work_date || "snapshot"}.csv`;
    document.body.appendChild(link); link.click(); link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  qa("[data-mode]").forEach((button) => button.addEventListener("click", () => selectMode(button.dataset.mode)));
  qa("[data-view]").forEach((button) => button.addEventListener("click", () => { state.view = button.dataset.view; renderTable(); }));
  const clearStatesButton = q("[data-clear-states]");
  if (clearStatesButton) clearStatesButton.addEventListener("click", clearStateFilter);
  q("[data-tolerance]").addEventListener("input", render);
  q("[data-delay]").addEventListener("input", render);
  q("[data-exclude-withdrawals]").addEventListener("change", render);
  q("[data-search]").addEventListener("input", renderTable);
  if (legacyStatusSelect) legacyStatusSelect.addEventListener("change", renderTable);
  if (legacyActivityTypeSelect) legacyActivityTypeSelect.addEventListener("change", renderTable);
  if (treatmentUiReady) q("[data-treatment-filter]").addEventListener("change", renderTable);
  q("[data-refresh]").addEventListener("click", refreshSnapshot);
  if (treatmentUiReady) q("[data-refresh-treatments]").addEventListener("click", loadTreatments);
  q("[data-export]").addEventListener("click", exportCsv);
  if (treatmentUiReady) {
    qa("[data-treatment-action]").forEach((button) => button.addEventListener("click", () => finishTreatment(button.dataset.treatmentAction)));
    q("[data-treatment-close]").addEventListener("click", () => q("[data-treatment-dialog]").close());
    q("[data-treatment-dialog]").addEventListener("cancel", (event) => { if (state.treatmentBusy) event.preventDefault(); });
    q("[data-treatment-dialog]").addEventListener("close", releaseTreatment);
    window.setInterval(() => { if (!document.hidden && state.payload && !state.activeTreatment) loadTreatments(); }, 45000);
    window.setInterval(renewTreatment, 60000);
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) return;
      if (state.activeTreatment) renewTreatment();
      else if (state.payload) loadTreatments();
    });
  }

  state.timer = window.setInterval(() => {
    if (!state.snapshot || !state.snapshot.expires_at) return;
    const expires = new Date(`${state.snapshot.expires_at}Z`).getTime();
    state.snapshot.remaining_seconds = Math.max(Math.ceil((expires - Date.now()) / 1000), 0);
    state.snapshot.refresh_allowed = state.snapshot.remaining_seconds === 0;
    updateStatus();
  }, 1000);

  selectMode("all");
  loadSnapshot();
})();
