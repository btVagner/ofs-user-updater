(function () {
  function atualizarStatusOnline() {
    fetch("/status-online", { cache: "no-store" })
      .then(response => {
        if (!response.ok) {
          throw new Error("Erro ao buscar status online");
        }
        return response.json();
      })
      .then(data => {
        const el = document.querySelector(".online-text");
        if (el && typeof data.usuarios_online !== "undefined") {
          el.textContent = data.usuarios_online + " online";
        }
      })
      .catch(err => {
        console.warn("[status_online] Falha ao atualizar:", err);
      });
  }

  // Atualiza ao carregar a página
  document.addEventListener("DOMContentLoaded", function () {
    atualizarStatusOnline();

    // Atualiza a cada 10 segundos
    setInterval(atualizarStatusOnline, 10000);
  });
})();
