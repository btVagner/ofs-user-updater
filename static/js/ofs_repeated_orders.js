(() => {
  'use strict';
  const root = document.getElementById('repeated-orders');
  if (!root) return;
  const get = id => document.getElementById(`ro-${id}`);
  const labels = {purple: '3 ou mais OS distintas', red: '2 OS do mesmo tipo', yellow: '2 OS de tipos diferentes'};
  const symbols = {purple: '3+', red: '', yellow: '≠'};
  const diagLabels = {missing_contract: 'Sem contrato', missing_appt: 'Sem número de OS', missing_type: 'Sem tipo', invalid_date: 'Data inválida', outside_window: 'Fora da janela', non_os: 'Não são OS', withdrawal: 'Retirada excluída', outside_hierarchy: 'Fora da hierarquia 02', missing_resource: 'Sem recurso', excluded_status: 'Status desconsiderado', excluded_status_from_previous_mesh: 'Status desconsiderado na malha anterior', duplicate_activity: 'Atividades duplicadas removidas', duplicate_os: 'Registros da mesma OS consolidados', reconciled_activity: 'Atividades reconciliadas'};
  let state = null, page = 0, submitting = false;
  let pendingRefreshId = null, pollTimer;
  const selectedSeverities = new Set(), selectedTypes = new Set(), selectedRegionals = new Set();
  const regional = activity => state?.payload?.regional_source === 'activity.stateProvince' ? activity.regional || 'Sem estado' : 'Sem estado';
  const activities = row => row.orders.flatMap(order => order.activities);
  const pageSize = 30;
  function el(tag, value, className) {
    const node = document.createElement(tag);
    if (value !== undefined) node.textContent = value;
    if (className) node.className = className;
    return node;
  }
  const formatTime = value => value ? new Date(value).toLocaleString('pt-BR', {timeZone: 'America/Sao_Paulo'}) : '—';
  function activityCount(contracts) {
    const ids = new Set();
    for (const row of contracts) for (const order of row.orders) for (const activity of order.activities) ids.add(activity.activityId);
    return ids.size;
  }
  function renderTypeButtons() {
    const types = [...new Set((state?.payload?.contracts || []).flatMap(row => row.types))].sort();
    for (const selected of selectedTypes) if (!types.includes(selected)) selectedTypes.delete(selected);
    get('types').replaceChildren(...['', ...types].map(type => {
      const button = el('button', type || 'Todos', 'ro-filter-button');
      button.type = 'button';
      button.dataset.type = type;
      button.addEventListener('click', () => {
        if (!type) selectedTypes.clear();
        else if (selectedTypes.has(type)) selectedTypes.delete(type);
        else selectedTypes.add(type);
        page = 0; render();
      });
      return button;
    }));
  }
  function updateRefreshState() {
    const run = state?.run;
    const busy = ['queued', 'running'].includes(run?.status);
    const active = submitting || busy || pendingRefreshId !== null;
    const seconds = state?.remaining_seconds || 0;
    if (get('refresh')) {
      get('refresh').disabled = active || seconds > 0;
      get('refresh').textContent = active ? 'Atualização em andamento' : seconds ? `Atualizar em ${Math.ceil(seconds / 60)} min` : 'Atualizar análise';
    }
    get('progress').hidden = !active;
    get('progress').setAttribute('aria-busy', String(active));
    const title = submitting ? 'Solicitando atualização' : run?.status === 'queued' ? 'Atualização na fila' : 'Atualizando análise';
    const detail = submitting ? 'Registrando a solicitação no painel.' : run?.status === 'queued' ? 'Aguardando o worker iniciar.' : run?.status === 'running' ?
      run.pages ? `${run.pages} páginas · ${run.received} atividades recebidas. Último progresso: ${formatTime(run.heartbeat_at)}.` : 'Lendo a hierarquia e preparando a coleta.' : 'Aguardando confirmação do estado da atualização.';
    get('progress-title').textContent = title;
    get('progress-detail').textContent = detail;
    get('progress').querySelector('[role="progressbar"]').setAttribute('aria-valuetext', `${title}. ${detail}`);
  }
  function renderRegionalButtons() {
    const regionals = [...new Set((state?.payload?.contracts || []).flatMap(row => activities(row).map(regional)))].sort((a,b) => a.localeCompare(b, 'pt-BR'));
    for (const selected of selectedRegionals) if (!regionals.includes(selected)) selectedRegionals.delete(selected);
    get('regionals').replaceChildren(...['', ...regionals].map(value => {
      const button = el('button', value || 'Todos', 'ro-filter-button');
      button.type = 'button'; button.dataset.regional = value;
      button.addEventListener('click', () => {
        if (!value) selectedRegionals.clear();
        else if (selectedRegionals.has(value)) selectedRegionals.delete(value);
        else selectedRegionals.add(value);
        page = 0; render();
      });
      return button;
    }));
    const clear = el('button', 'Limpar seleção', 'om-state-reset');
    clear.type = 'button'; clear.id = 'ro-clear-regionals';
    clear.addEventListener('click', () => {selectedRegionals.clear(); page = 0; render();});
    get('regionals').append(clear);
  }
  function render() {
    const payload = state?.payload, run = state?.run;
    updateRefreshState();
    get('state').textContent = `${payload ? `Janela: ${payload.date_from} a ${payload.date_to}. Publicado em ${formatTime(state.published_at)}.` : 'Ainda não há análise publicada.'} ${state?.stale && payload ? 'Snapshot de outro dia; solicite atualização.' : ''}`;
    const failed = ['failed', 'interrupted'].includes(run?.status);
    get('error').hidden = !failed;
    get('error').textContent = failed ? `Atualização ${run.status === 'interrupted' ? 'interrompida' : 'falhou'}: ${run.error_text || 'Verifique o worker.'} ${payload ? 'A análise anterior foi preservada.' : 'Nenhum resultado parcial foi publicado.'}` : '';
    const contracts = payload?.contracts || [];
    get('total-count').textContent = activityCount(contracts);
    for (const button of root.querySelectorAll('[data-severity]')) {
      const severity = button.dataset.severity;
      button.querySelector('[data-severity-count]').textContent = payload?.summary[severity] || 0;
      button.setAttribute('aria-pressed', String(selectedSeverities.has(severity)));
      button.classList.toggle('is-active', selectedSeverities.has(severity));
    }
    for (const button of get('types').querySelectorAll('button')) {
      const active = button.dataset.type ? selectedTypes.has(button.dataset.type) : selectedTypes.size === 0;
      button.setAttribute('aria-pressed', String(active));
      button.classList.toggle('is-active', active);
    }
    for (const button of get('regionals').querySelectorAll('button[data-regional]')) {
      const active = button.dataset.regional ? selectedRegionals.has(button.dataset.regional) : selectedRegionals.size === 0;
      button.setAttribute('aria-pressed', String(active));
      button.classList.toggle('is-active', active);
    }
    const needle = get('contract').value.trim().toLowerCase();
    const allSelected = !needle && !selectedSeverities.size && !selectedTypes.size && !selectedRegionals.size;
    get('total').setAttribute('aria-pressed', String(allSelected));
    get('total').classList.toggle('is-active', allSelected);
    const rows = contracts.filter(row => row.contract.toLowerCase().includes(needle) &&
      (!selectedSeverities.size || selectedSeverities.has(row.severity)) &&
      (!selectedTypes.size || row.types.some(type => selectedTypes.has(type))) &&
      (!selectedRegionals.size || activities(row).some(activity => selectedRegionals.has(regional(activity)))));
    const pages = Math.max(1, Math.ceil(rows.length / pageSize));
    page = Math.min(page, pages - 1);
    get('count').textContent = `${rows.length} contratos · ${activityCount(rows)} atividades`;
    get('page').textContent = `${page + 1} / ${pages}`;
    get('prev').disabled = page === 0;
    get('next').disabled = page + 1 >= pages;
    const list = get('list');
    const expanded = new Set([...list.querySelectorAll('details[open]')].map(node => node.dataset.contract));
    list.replaceChildren();
    for (const row of rows.slice(page * pageSize, (page + 1) * pageSize)) {
      const detail = el('details', undefined, `om-card ro-case ro-${row.severity}`);
      detail.dataset.contract = row.contract;
      const summary = el('summary');
      summary.append(el('strong', `Contrato ${row.contract}`), el('span', `${row.count} OS`));
      summary.title = labels[row.severity];
      if (symbols[row.severity]) {
        const symbol = el('span', symbols[row.severity], 'ro-badge');
        symbol.setAttribute('aria-label', labels[row.severity]);
        summary.append(symbol);
      }
      detail.append(summary);
      detail.addEventListener('toggle', () => {
        if (!detail.open || detail.children.length > 1) return;
        const wrap = el('div', undefined, 'ro-table-wrap'), table = el('table');
        const head = el('thead'), tr = el('tr');
        for (const label of ['OS / grupo', 'Origem', 'Número da OS', 'Atividade', 'Tipo', 'Date', 'Status', 'ResourceId', 'Estado', 'timeSlot']) tr.append(el('th', label));
        head.append(tr);
        const body = el('tbody');
        for (const order of row.orders) for (const activity of order.activities) {
          const line = el('tr');
          for (const value of [order.key, order.origin, activity.apptNumber, activity.activityId, activity.activityType, activity.date, activity.status, activity.resourceId, regional(activity), activity.timeSlot]) line.append(el('td', value || '—'));
          body.append(line);
        }
        table.append(head, body); wrap.append(table); detail.append(wrap);
      });
      list.append(detail);
      if (expanded.has(row.contract)) detail.open = true;
    }
    if (!rows.length) list.append(el('p', payload ? 'Nenhum contrato encontrado com estes filtros.' : 'Solicite a primeira atualização para gerar o snapshot.'));
    get('diagnostics').textContent = payload ? Object.entries(payload.diagnostics).map(([key, count]) => `${diagLabels[key] || key}: ${count}`).join(' · ') : 'Sem coleta concluída.';
  }
  async function read() {
    try {
      const response = await fetch(root.dataset.dataUrl, {headers: {Accept: 'application/json'}});
      const body = await response.json();
      if (!response.ok || !body.ok) throw new Error(body.error?.message || 'Falha ao ler análise.');
      const previous = state?.snapshot_run_id;
      const previousStatuses = JSON.stringify(state?.payload?.allowed_statuses);
      const previousRegionalSource = state?.payload?.regional_source;
      state = body.data;
      if (pendingRefreshId !== null && state.run?.id >= pendingRefreshId) pendingRefreshId = null;
      if (state.snapshot_run_id !== previous || JSON.stringify(state.payload?.allowed_statuses) !== previousStatuses || state.payload?.regional_source !== previousRegionalSource) {
        renderTypeButtons();
        renderRegionalButtons();
      }
      render();
    } catch (error) {get('error').hidden = false; get('error').textContent = error.message;}
  }
  get('refresh')?.addEventListener('click', async () => {
    submitting = true; render();
    try {
      const response = await fetch(root.dataset.refreshUrl, {method: 'POST', headers: {'X-Repeated-Orders-CSRF': root.dataset.csrf, Accept: 'application/json'}});
      const body = await response.json();
      if (!response.ok || !body.ok) throw new Error(body.error?.message || 'Falha ao solicitar atualização.');
      pendingRefreshId = body.data.id;
      await read();
    } catch (error) {get('error').hidden = false; get('error').textContent = error.message;}
    finally {submitting = false; updateRefreshState(); schedulePoll();}
  });
  get('contract').addEventListener('input', () => {page = 0; render();});
  for (const button of root.querySelectorAll('[data-severity]')) button.addEventListener('click', () => {
    const severity = button.dataset.severity;
    if (selectedSeverities.has(severity)) selectedSeverities.delete(severity);
    else selectedSeverities.add(severity);
    page = 0; render();
  });
  get('total').addEventListener('click', () => {
    selectedSeverities.clear(); selectedTypes.clear(); selectedRegionals.clear(); get('contract').value = ''; page = 0; render();
  });
  get('prev').addEventListener('click', () => {page--; render();});
  get('next').addEventListener('click', () => {page++; render();});
  renderTypeButtons();
  renderRegionalButtons();
  function schedulePoll() {
    clearTimeout(pollTimer);
    const active = submitting || pendingRefreshId !== null || ['queued', 'running'].includes(state?.run?.status);
    pollTimer = setTimeout(async () => {
      if (!document.hidden) await read();
      schedulePoll();
    }, active ? 2000 : 10000);
  }
  read().finally(schedulePoll);
})();
