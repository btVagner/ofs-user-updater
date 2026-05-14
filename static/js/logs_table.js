(function () {
  const modal = document.getElementById("apiRespModal");
  const modalContent = document.getElementById("apiRespContent");
  const modalCloseBtn = document.getElementById("apiRespClose");

  function openApiModal(text) {
    if (!modal || !modalContent) return;

    let pretty = text || "";

    try {
      const obj = JSON.parse(pretty);
      pretty = JSON.stringify(obj, null, 2);
    } catch (e) {
      // Keep original text when it is not pure JSON.
    }

    modalContent.textContent = pretty;
    modal.classList.add("is-open");
  }

  function closeApiModal() {
    if (!modal || !modalContent) return;

    modal.classList.remove("is-open");
    modalContent.textContent = "";
  }

  document.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-api-id]");
    if (!btn) return;

    const id = btn.getAttribute("data-api-id");
    const script = document.getElementById("apiresp-" + id);

    if (!script) {
      openApiModal("Resposta da API nao encontrada para este log.");
      return;
    }

    let txt = "";

    try {
      txt = JSON.parse(script.textContent);
    } catch (err) {
      txt = script.textContent || "";
    }

    openApiModal(txt);
  });

  if (modalCloseBtn) {
    modalCloseBtn.addEventListener("click", closeApiModal);
  }

  if (modal) {
    modal.addEventListener("click", (e) => {
      if (e.target === modal) closeApiModal();
    });
  }

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeApiModal();
  });
})();
