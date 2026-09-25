(function () {
  "use strict";

  const root = document.querySelector("#operational-monitor");
  if (!root) return;

  const VIEW_KEYS = ["late", "idle", "notStarted", "slot", "black"];
  const TITLES = {
    late: "OS em alerta",
    idle: "Técnicos sem OS",
    notStarted: "Rotas não iniciadas",
    slot: "Fora do turno",
    black: "Clientes Black",
  };
  const NOTES = {
    late: "Status iniciado; tempo decorrido acima da duração acrescida da tolerância.",
    idle: "Rota ativa, sem atividades reais pendentes, iniciadas ou em deslocamento e dentro da jornada conhecida.",
    notStarted: "Técnicos ativos com jornada no dia, rota ainda não ativada e atraso acima do parâmetro.",
    slot: "OS com início igual ou posterior ao fim do respectivo turno operacional.",
    black: "OS do dia com XA_CLI_ATRI igual a 1, inclusive atividades finalizadas.",
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
  const state = {
    mode: "all", view: "late", snapshot: null, payload: null, rows: {},
    selectedBuckets: new Set(), selectedStates: new Set(), knownStates: new Set(),
    busy: false, timer: null,
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
    const status = q("[data-status]").value;
    return (state.rows[state.view] || []).filter((row) => {
      if (!rowStates(row).some((value) => state.selectedStates.has(value))) return false;
      if (state.selectedBuckets.size && !state.selectedBuckets.has(row.area)) return false;
      if (status && normalized(row.status) !== status) return false;
      if (!search) return true;
      return [row.id, row.appt, row.tech, row.resource_id, row.area, row.type, row.status, row.time_slot, row.customer, ...rowStates(row)]
        .some((value) => normalized(value).includes(search));
    });
  }

  function rowStates(row) {
    const values = Array.isArray(row.states) ? row.states : [row.state];
    const normalizedStates = values.map((value) => String(value || "").trim()).filter(Boolean);
    return normalizedStates.length ? normalizedStates : ["Sem UF"];
  }

  function renderStateOptions() {
    const options = Array.from(new Set(
      VIEW_KEYS.flatMap((view) => (state.rows[view] || []).flatMap(rowStates))
    )).sort((a, b) => a.localeCompare(b, "pt-BR"));
    const available = new Set(options);

    state.selectedStates.forEach((value) => {
      if (!available.has(value)) state.selectedStates.delete(value);
    });
    state.knownStates.forEach((value) => {
      if (!available.has(value)) state.knownStates.delete(value);
    });
    options.forEach((value) => {
      if (!state.knownStates.has(value)) state.selectedStates.add(value);
      state.knownStates.add(value);
    });

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
    const container = q("[data-state-options]");
    container.replaceChildren(fragment);
    container.closest(".om-state-filter").hidden = options.length === 0;
  }

  function replaceStatusOptions(select, options) {
    const selected = select.value;
    select.replaceChildren(new Option("Todos os status", ""), ...options.map((value) => new Option(value, normalized(value))));
    if (options.some((value) => normalized(value) === selected)) select.value = selected;
    select.disabled = options.length === 0;
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
    const view = state.view;
    q("[data-table-title]").textContent = TITLES[view];
    q("[data-table-note]").textContent = NOTES[view] + (view === "slot" && q("[data-exclude-withdrawals]").checked ? " Retiradas excluídas." : "");
    qa("[data-view]").forEach((button) => button.classList.toggle("is-active", button.dataset.view === view));

    const sourceRows = state.rows[view] || [];
    const buckets = Array.from(new Set(sourceRows.map((row) => row.area).filter(Boolean))).sort((a, b) => a.localeCompare(b, "pt-BR"));
    const statuses = Array.from(new Set(sourceRows.map((row) => row.status).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b), "pt-BR"));
    renderBucketOptions(buckets);
    replaceStatusOptions(q("[data-status]"), statuses);
    const rows = currentRows();

    const headRow = document.createElement("tr");
    HEADERS[view].forEach((title) => {
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
      td.colSpan = HEADERS[view].length;
      td.className = "om-empty";
      td.textContent = state.payload ? "Nenhum registro para os filtros e parâmetros atuais." : "Não há snapshot disponível para esta macro.";
      tr.appendChild(td);
      body.appendChild(tr);
    } else {
      const fragment = document.createDocumentFragment();
      rows.slice(0, 150).forEach((item) => {
        const tr = document.createElement("tr");
        cells(item, view).forEach((value, index) => {
          const td = document.createElement("td");
          td.textContent = String(value == null ? "—" : value);
          if ((view === "late" && index === 8) || (view === "slot" && index === 7) || (view === "notStarted" && index === 5)) td.classList.add("om-danger-text");
          tr.appendChild(td);
        });
        fragment.appendChild(tr);
      });
      body.appendChild(fragment);
    }
    q("[data-shown]").textContent = `${rows.length.toLocaleString("pt-BR")} registro(s) filtrados · ${Math.min(rows.length, 150)} exibidos`;
    q("[data-data-status]").textContent = state.payload ? "Snapshot compartilhado do MySQL" : "Sem snapshot";
    q("[data-export]").disabled = !state.payload || state.busy;
  }

  function render() {
    computeRows();
    renderStateOptions();
    const counts = {
      late: state.rows.late.length,
      idle: state.rows.idle.length,
      notStarted: state.rows.notStarted.length,
      slot: state.rows.slot.length,
      black: state.rows.black.length,
    };
    q('[data-kpi="tech"]').textContent = state.payload ? Number(state.payload.technicians_count || 0).toLocaleString("pt-BR") : "—";
    VIEW_KEYS.forEach((key) => {
      q(`[data-kpi="${key}"]`).textContent = state.payload ? counts[key].toLocaleString("pt-BR") : "—";
      q(`[data-count="${key}"]`).textContent = state.payload ? counts[key].toLocaleString("pt-BR") : "—";
      q(`[data-kpi-card="${key}"]`).hidden = state.mode !== "all" && state.mode !== key;
    });
    q('[data-kpi-card="all"]').hidden = state.mode !== "all";
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
    updateStatus();
    diagnostics();
    render();
    if (snapshot.status === "failed") message(snapshot.has_payload ? "A última atualização falhou; o snapshot anterior foi preservado." : (snapshot.error_text || "A atualização falhou."), "error");
    else if (!snapshot.has_payload) message("Ainda não há snapshot para esta macro. Quando a fonte local estiver sincronizada, use Atualizar snapshot local.", "warning");
    else if (snapshot.payload && snapshot.payload.diagnostics && snapshot.payload.diagnostics.data_complete === false) message("Snapshot carregado, mas uma ou mais fontes locais estão desatualizadas. Regras baseadas em ausência foram suprimidas para evitar falsos alertas.", "warning");
    else message("Dados carregados do snapshot compartilhado. Filtros, abas e exportação não geram chamadas ao OFS.");
  }

  async function readJson(response) {
    const body = await response.json().catch(() => ({}));
    if (!response.ok && !body.error) throw new Error(`Falha HTTP ${response.status}.`);
    return body;
  }

  async function loadSnapshot() {
    state.busy = true;
    updateStatus();
    try {
      const scope = root.dataset.scope || "casa-cliente";
      const response = await fetch(`${root.dataset.dataUrl}?scope=${encodeURIComponent(scope)}`, { headers: { Accept: "application/json" } });
      const body = await readJson(response);
      if (!response.ok || !body.ok) throw new Error((body.error && body.error.message) || "Não foi possível carregar o snapshot.");
      acceptSnapshot(body.data);
    } catch (error) {
      state.snapshot = null; state.payload = null;
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
    q("[data-status]").value = "";
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
    const content = [HEADERS[state.view].map(escapeCsv).join(";"), ...rows.map((row) => cells(row, state.view).map(escapeCsv).join(";"))].join("\r\n");
    const url = URL.createObjectURL(new Blob(["\ufeff" + content], { type: "text/csv;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = `ofs_monitor_${state.view}_${state.payload.work_date || "snapshot"}.csv`;
    document.body.appendChild(link); link.click(); link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  qa("[data-mode]").forEach((button) => button.addEventListener("click", () => selectMode(button.dataset.mode)));
  qa("[data-view]").forEach((button) => button.addEventListener("click", () => { state.view = button.dataset.view; renderTable(); }));
  q("[data-tolerance]").addEventListener("input", render);
  q("[data-delay]").addEventListener("input", render);
  q("[data-exclude-withdrawals]").addEventListener("change", render);
  q("[data-search]").addEventListener("input", renderTable);
  q("[data-status]").addEventListener("change", renderTable);
  q("[data-refresh]").addEventListener("click", refreshSnapshot);
  q("[data-export]").addEventListener("click", exportCsv);

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
