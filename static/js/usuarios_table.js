// static/js/usuarios_table.js
$(function () {
  const table = $('#usuariosTable').DataTable({
    dom: 'Brtip',           // Remove o "f" (search global padrão)
    buttons: ['csv'],
    paging: false
  });

  // ---- Helpers ----
  function uniqSorted(colIdx) {
    return Array.from(new Set(
      table.column(colIdx).data().toArray()
        .map(v => (v || '').toString().trim())
        .filter(v => v && v !== '-')
    )).sort();
  }
  const esc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

  // Cria/atualiza menu para uma coluna
  function buildMenu(menuId, colIdx) {
    let $menu = $('#' + menuId);
    if ($menu.length === 0) {
      $menu = $('<div>', { id: menuId, class: 'fd-menu' }).appendTo('body');
    } else {
      $menu.empty();
    }

    const list = uniqSorted(colIdx);
    const $all = $('<label class="fd-all"><input type="checkbox" class="fd-all-toggle" data-col="' + colIdx + '" checked> Selecionar todos</label>');
    const $opts = $('<div class="fd-options"></div>');

    list.forEach(v => {
      const safe = v.replace(/\W+/g, '_');
      const $lbl = $('<label/>').html(
        `<input type="checkbox" class="fd-opt" data-col="${colIdx}" value="${v}" checked> ${v}`
      );
      $opts.append($lbl);
    });

    $menu.append($all).append($opts);
    return $menu;
  }

  // Posiciona menu abaixo do botão
  function placeMenu($btn, $menu) {
    const off = $btn.offset();
    $menu.css({
      top: off.top + $btn.outerHeight() + 6,
      left: off.left
    });
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

  // Busca por Nome (coluna 0) - contém, sem regex
  $('#nameSearch').on('input', function () {
    const val = $(this).val();
    table.column(0).search(val, false, true).draw();
  });

  // Estado inicial: nenhum filtro restritivo
  table.columns([1, 2, 4]).search('', true, false).draw();
});
