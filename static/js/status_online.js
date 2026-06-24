(function () {
  function atualizarStatusOnline() {
    const sidebar = document.querySelector(".sidebar");
    const statusUrl = sidebar ? sidebar.dataset.onlineStatusUrl : "";

    if (!statusUrl) {
      return;
    }

    fetch(statusUrl, { cache: "no-store" })
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
