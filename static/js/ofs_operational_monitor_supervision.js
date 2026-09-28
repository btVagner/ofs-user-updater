(function () {
  "use strict";
  const root = document.querySelector("#monitor-supervision");
  if (!root) return;
  const q = (selector) => root.querySelector(selector);
  const qa = (selector) => Array.from(root.querySelectorAll(selector));
  const labels = { late: "OS em alerta", idle: "Técnico sem OS", notStarted: "Rota não iniciada", slot: "Fora do turno" };
  const actions = { resolved: "Resolveu", waiting: "Colocou em espera", open: "Manteve em aberto" };
  let requestSerial = 0;

  function dateTime(value) {
    if (!value) return "—";
    const parsed = new Date(`${value}Z`);
    return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString("pt-BR");
  }

  function dateLabel(value) {
    if (!value) return "Data operacional atual";
    const [year, month, day] = value.split("-");
    return `Data operacional: ${day}/${month}/${year}`;
  }

  function empty(container, text) {
    const element = document.createElement(container.tagName === "OL" ? "li" : "p");
    element.className = "om-supervision-empty";
    element.textContent = text;
    container.appendChild(element);
  }

  function updateLeaseTimers() {
    qa("[data-lease-expires]").forEach((element) => {
      const remaining = new Date(`${element.dataset.leaseExpires}Z`).getTime() - Date.now();
      element.textContent = remaining > 0 ? `Reserva: até ${Math.max(1, Math.ceil(remaining / 60000))} min` : "Reserva expirada · atualize";
    });
  }

  function renderAgents(agents) {
    const select = q("[data-agent]");
    const selected = select.value;
    const fragment = document.createDocumentFragment();
    const all = document.createElement("option");
    all.value = "";
    all.textContent = "Todos os agentes";
    fragment.appendChild(all);
    (agents || []).forEach((username) => {
      const option = document.createElement("option");
      option.value = username;
      option.textContent = username;
      fragment.appendChild(option);
    });
    select.replaceChildren(fragment);
    select.value = selected;
  }

  function render(data) {
    q("[data-total]").textContent = Number(data.total_resolved || 0).toLocaleString("pt-BR");
    q("[data-active-total]").textContent = Number(data.active_total || 0).toLocaleString("pt-BR");
    q("[data-waiting-total]").textContent = Number(data.waiting_total || 0).toLocaleString("pt-BR");
    q("[data-work-date]").textContent = dateLabel(data.current_work_date);
    q("[data-period-label]").textContent = q("[data-preset]").selectedOptions[0].textContent;
    q("[data-ranking-label]").textContent = `${q("[data-top]").selectedOptions[0].textContent} · período selecionado`;
    renderAgents(data.agents);

    const active = q("[data-active]");
    active.replaceChildren();
    (data.active || []).forEach((item) => {
      const card = document.createElement("article");
      const title = document.createElement("strong");
      const caseLine = document.createElement("small");
      const areaLine = document.createElement("small");
      const lease = document.createElement("span");
      title.textContent = `${item.actor_username || "Agente não informado"} · ${labels[item.indicator] || item.indicator}`;
      caseLine.textContent = `${item.item_label || item.item_key} · ${item.technician || "—"}`;
      areaLine.textContent = `${item.area || "Sem área"} · ${item.state_uf || "Sem UF"}`;
      lease.className = "om-lease";
      lease.dataset.leaseExpires = item.lease_expires_at || "";
      card.append(title, caseLine, areaLine, lease);
      active.appendChild(card);
    });
    if (!active.children.length) empty(active, "Nenhum caso em análise no momento.");
    updateLeaseTimers();

    const ranking = q("[data-ranking]");
    ranking.replaceChildren();
    const topCount = Math.max(1, ...(data.ranking || []).map((item) => Number(item.total || 0)));
    (data.ranking || []).forEach((item) => {
      const row = document.createElement("li");
      const label = document.createElement("div");
      const name = document.createElement("span");
      const count = document.createElement("strong");
      const track = document.createElement("div");
      const bar = document.createElement("span");
      label.className = "om-ranking-label";
      name.textContent = item.username || "Usuário não informado";
      count.textContent = Number(item.total || 0).toLocaleString("pt-BR");
      track.className = "om-ranking-track";
      bar.style.width = `${Math.min(100, Math.max(0, Number(item.total || 0) / topCount * 100))}%`;
      label.append(name, count);
      track.appendChild(bar);
      row.append(label, track);
      ranking.appendChild(row);
    });
    if (!ranking.children.length) empty(ranking, "Nenhuma resolução neste período.");

    const recent = q("[data-recent]");
    recent.replaceChildren();
    (data.recent || []).forEach((item) => {
      const details = document.createElement("details");
      const summary = document.createElement("summary");
      const title = document.createElement("strong");
      const when = document.createElement("time");
      const caseLine = document.createElement("small");
      const body = document.createElement("div");
      const area = document.createElement("p");
      title.textContent = `${item.actor_username || "Agente"} · ${actions[item.action] || item.action}`;
      when.textContent = dateTime(item.occurred_at);
      when.dateTime = item.occurred_at ? `${item.occurred_at}Z` : "";
      caseLine.textContent = `${labels[item.indicator] || item.indicator} · ${item.item_label || item.item_key}`;
      summary.append(title, when, caseLine);
      body.className = "om-supervision-event-details";
      area.textContent = `${item.technician || "Técnico não informado"} · ${item.area || "Sem área"} · ${item.state_uf || "Sem UF"}`;
      body.appendChild(area);
      if (item.note) {
        const note = document.createElement("p");
        note.textContent = `Observação: ${item.note}`;
        body.appendChild(note);
      }
      details.append(summary, body);
      recent.appendChild(details);
    });
    if (!recent.children.length) empty(recent, "Nenhuma movimentação para estes filtros.");
  }

  async function load() {
    const serial = ++requestSerial;
    const preset = q("[data-preset]").value;
    if (preset === "custom" && (!q("[data-start]").value || !q("[data-end]").value)) {
      q("[data-refresh]").disabled = false;
      q("[data-message]").textContent = "Informe as duas datas para consultar o período personalizado.";
      return;
    }
    const params = new URLSearchParams({ scope: "casa-cliente", preset, top: q("[data-top]").value });
    if (preset === "custom") {
      params.set("start", q("[data-start]").value);
      params.set("end", q("[data-end]").value);
    }
    if (q("[data-action]").value) params.set("action", q("[data-action]").value);
    if (q("[data-agent]").value) params.set("agent", q("[data-agent]").value);
    q("[data-refresh]").disabled = true;
    q("[data-message]").textContent = "Atualizando dados locais...";
    try {
      const response = await fetch(`${root.dataset.url}?${params}`, { headers: { Accept: "application/json" }, cache: "no-store" });
      const body = await response.json();
      if (!response.ok || !body.ok) throw new Error((body.error && body.error.message) || "Falha ao carregar a supervisão.");
      if (serial !== requestSerial) return;
      render(body.data);
      q("[data-message]").textContent = `Dados locais atualizados às ${new Date().toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" })}.`;
    } catch (error) {
      if (serial === requestSerial) q("[data-message]").textContent = error.message;
    } finally {
      if (serial === requestSerial) q("[data-refresh]").disabled = false;
    }
  }

  q("[data-preset]").addEventListener("change", () => {
    qa("[data-custom]").forEach((field) => { field.hidden = q("[data-preset]").value !== "custom"; });
    q("[data-agent]").value = "";
    load();
  });
  qa("[data-start], [data-end]").forEach((field) => field.addEventListener("change", () => {
    q("[data-agent]").value = "";
    load();
  }));
  qa("[data-top], [data-action], [data-agent]").forEach((field) => field.addEventListener("change", load));
  q("[data-refresh]").addEventListener("click", load);
  window.setInterval(updateLeaseTimers, 30000);
  load();
})();
