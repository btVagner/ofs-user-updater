$(document).ready(function () {
  const table = $('#usuariosTable').DataTable({
    dom: 'Bfrtip',
    buttons: ['csv'],
    paging: false,
    initComplete: function () {
      // Aplica filtros apenas nas colunas 1 (userType) e 3 (status)
      this.api().columns([1, 3]).every(function () {
        const column = this;
        const headerCell = $('#usuariosTable thead tr:eq(1) th').eq(column.index());
        const select = $('<select><option value="">Todos</option></select>')
          .appendTo(headerCell.empty())
          .on('change', function () {
            const val = $.fn.dataTable.util.escapeRegex($(this).val());
            column.search(val ? '^' + val + '$' : '', true, false).draw();
          });

        column.data().unique().sort().each(function (d) {
          if (d !== '-') {
            select.append('<option value="' + d + '">' + d + '</option>');
          }
        });
      });
    }
  });
});
