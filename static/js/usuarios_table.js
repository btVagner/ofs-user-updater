// static/js/usuarios_table.js
$(function () {
  const table = $('#usuariosTable').DataTable({
    dom: 'Brtip',           // sem "f" => remove Search padrão
    buttons: ['csv'],
    paging: false
  });

  // ---- Helpers ----
  const esc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

  // Retorna valores únicos da coluna; se includeDash=true, inclui "-" (ou vazio) como opção
  function uniqValues(colIdx, includeDash = false) {
    const raw = table.column(colIdx).data().toArray().map(v => (v ?? '').toString().trim());
    const set = new Set();

    for (const v of raw) {
      if (v) set.add(v); // valores normais
    }

    // Se deve incluir "sem valor", verifica se existe "-" ou vazio na coluna
    if (includeDash) {
      const hasNoVal = raw.some(v => v === '-' || v === '');
      if (hasNoVal) set.add('-'); // usamos "-" como marcador na tabela
    }

    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }

  // Cria/atualiza menu para uma coluna
  function buildMenu(menuId, colIdx) {
    let $menu = $('#' + menuId);
    if ($menu.length === 0) {
      $menu = $('<div>', { id: menuId, class: 'fd-menu' }).appendTo('body');
    } else {
      $menu.empty();
    }

    const includeDash = (colIdx === 2); // Bucket
    const values = uniqValues(colIdx, includeDash);

    const $all = $(`
      <label class="fd-all">
        <input type="checkbox" class="fd-all-toggle" data-col="${colIdx}" checked> Selecionar todos
      </label>
    `);
    const $opts = $('<div class="fd-options"></div>');

    values.forEach(v => {
      const labelText = (colIdx === 2 && v === '-') ? 'Sem bucket' : (v || '—');
      $opts.append(
        `<label><input type="checkbox" class="fd-opt" data-col="${colIdx}" value="${v}" checked> ${labelText}</label>`
      );
    });

    // Caso a coluna tenha SOMENTE valores vazios/“-” e nenhum outro (ex.: tudo sem bucket),
    // ainda teremos a opção "Sem bucket" criada acima porque includeDash=true detecta e inclui "-".

    $menu.append($all).append($opts);
    return $menu;
  }

  // Posiciona menu abaixo do botão
  function placeMenu($btn, $menu) {
    const off = $btn.offset();
    $menu.css({ top: off.top + $btn.outerHeight() + 6, left: off.left });
  }

  // Abre/fecha menus
  $(document).on('click', '.fd-btn', function (e) {
    e.stopPropagation();
    const $btn = $(this);
    const col = parseInt($btn.data('col'), 10);
    const menuId = $btn.data('target');

    const $menu = buildMenu(menuId, col);
    $('.fd-menu').not($menu).removeClass('open');
    placeMenu($btn, $menu);
    $menu.toggleClass('open');
  });

  // Fecha ao clicar fora
  $(document).on('click', function () { $('.fd-menu').removeClass('open'); });
  $(document).on('click', '.fd-menu', function (e) { e.stopPropagation(); });

  // "Selecionar todos" por menu
  $(document).on('change', '.fd-all-toggle', function () {
    const col = parseInt($(this).data('col'), 10);
    const $menu = $(this).closest('.fd-menu');
    const checked = $(this).is(':checked');
    $menu.find(`.fd-opt[data-col="${col}"]`).prop('checked', checked);
    applyFilter(col, $menu);
  });

  // Seleção individual
  $(document).on('change', '.fd-opt', function () {
    const col = parseInt($(this).data('col'), 10);
    const $menu = $(this).closest('.fd-menu');
    const total = $menu.find(`.fd-opt[data-col="${col}"]`).length;
    const marc = $menu.find(`.fd-opt[data-col="${col}"]:checked`).length;
    $menu.find('.fd-all-toggle').prop('checked', marc === total);
    applyFilter(col, $menu);
  });

  function applyFilter(colIdx, $menu) {
    const vals = $menu.find(`.fd-opt[data-col="${colIdx}"]:checked`).map(function () {
      return '^' + esc($(this).val()) + '$';
    }).get();

    if (vals.length === 0) {
      table.column(colIdx).search('a^', true, false); // sem resultados
    } else {
      table.column(colIdx).search('(' + vals.join('|') + ')', true, false);
    }
    table.draw();
  }

  // Busca por NOME (coluna 0) - contém, sem regex
  $('#nameSearch').on('input', function () {
    table.column(0).search($(this).val(), false, true).draw();
  });

  // Estado inicial: nenhum filtro restritivo
  table.columns([1, 2, 4]).search('', true, false).draw();
});
