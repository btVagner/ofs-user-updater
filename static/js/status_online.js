(function () {
  function getSidebar() {
    return document.querySelector(".sidebar");
  }

  function atualizarStatusOnline() {
    const sidebar = getSidebar();
    const statusUrl = sidebar ? sidebar.dataset.onlineStatusUrl : "";

    if (!statusUrl) return;

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

  function renderUsuariosOnline(usuarios) {
    const list = document.querySelector("[data-online-users-list]");
    if (!list) return;

    if (!usuarios.length) {
      list.innerHTML = '<div class="online-users-empty">Nenhum usuário online agora.</div>';
      return;
    }

    list.innerHTML = usuarios.map(usuario => {
      const nome = escapeHtml(usuario.nome || usuario.username || "-");

      return `
        <div class="online-user-item">
          <span class="online-user-dot"></span>
          <span class="online-user-name">${nome}</span>
        </div>
      `;
    }).join("");
  }

  function carregarUsuariosOnline() {
    const sidebar = getSidebar();
    const usersUrl = sidebar ? sidebar.dataset.onlineUsersUrl : "";
    const list = document.querySelector("[data-online-users-list]");

    if (!usersUrl || !list) return;

    list.innerHTML = '<div class="online-users-empty">Carregando...</div>';

    fetch(usersUrl, {
      method: "GET",
      cache: "no-store",
      headers: {
        "Accept": "application/json"
      }
    })
      .then(response => {
        return response.json().catch(() => ({})).then(data => {
          if (!response.ok || data.ok === false) {
            throw new Error(data.error || "Erro ao buscar usuários online.");
          }

          return data;
        });
      })
      .then(data => {
        renderUsuariosOnline(Array.isArray(data.usuarios) ? data.usuarios : []);
      })
      .catch(err => {
        list.innerHTML = '<div class="online-users-empty">Não foi possível carregar.</div>';
        console.warn("[status_online] Falha ao listar usuários:", err);
      });
  }

  function configurarPopoverOnline() {
    const toggle = document.querySelector("[data-online-toggle]");
    const popover = document.querySelector("[data-online-popover]");

    if (!toggle || !popover) return;

    toggle.addEventListener("click", function (event) {
      event.stopPropagation();

      const willOpen = popover.classList.contains("hidden");
      popover.classList.toggle("hidden", !willOpen);
      toggle.setAttribute("aria-expanded", willOpen ? "true" : "false");

      if (willOpen) {
        carregarUsuariosOnline();
      }
    });

    popover.addEventListener("click", function (event) {
      event.stopPropagation();
    });

    document.addEventListener("click", function () {
      popover.classList.add("hidden");
      toggle.setAttribute("aria-expanded", "false");
    });

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape") {
        popover.classList.add("hidden");
        toggle.setAttribute("aria-expanded", "false");
      }
    });
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  document.addEventListener("DOMContentLoaded", function () {
    atualizarStatusOnline();
    configurarPopoverOnline();

    setInterval(atualizarStatusOnline, 10000);
  });
})();