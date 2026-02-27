document.addEventListener("DOMContentLoaded", function () {

  // UF: força 2 letras maiúsculas
  const uf = document.querySelector('input[name="uf"]');
  if (uf) {
    uf.addEventListener("input", function () {
      this.value = this.value.toUpperCase().replace(/[^A-Z]/g, "").slice(0, 3);
    });
  }

  // Campos numéricos: deixa só dígitos
  const onlyDigits = (el) => {
    el.addEventListener("input", function () {
      this.value = this.value.replace(/[^\d]/g, "");
    });
  };

  const idCidade = document.querySelector('input[name="idCidade"]');
  if (idCidade) onlyDigits(idCidade);

  const filialCidade = document.querySelector('input[name="filialCidade"]');
  if (filialCidade) onlyDigits(filialCidade);

});