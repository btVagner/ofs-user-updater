(() => {
  const STORAGE_KEY = 'ofs.sidebar.collapsed';

  const readCollapsedState = () => {
    try {
      return window.localStorage.getItem(STORAGE_KEY) === '1';
    } catch (_) {
      return false;
    }
  };

  const persistCollapsedState = (collapsed) => {
    try {
      window.localStorage.setItem(STORAGE_KEY, collapsed ? '1' : '0');
    } catch (_) {
      // O menu continua funcional mesmo se o storage estiver indisponível.
    }
  };

  const setCollapsed = (collapsed) => {
    document.documentElement.classList.toggle('sidebar-collapsed', collapsed);

    const toggle = document.querySelector('[data-sidebar-toggle]');
    if (toggle) {
      toggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
      toggle.setAttribute('aria-label', collapsed ? 'Expandir menu lateral' : 'Recolher menu lateral');
    }

    document.querySelectorAll('.sidebar-link').forEach((link) => {
      const text = link.querySelector('.sidebar-text')?.textContent?.trim();
      if (!text) return;

      if (collapsed) {
        link.setAttribute('title', text);
      } else {
        link.removeAttribute('title');
      }
    });
  };

  const initialCollapsed = readCollapsedState();
  document.documentElement.classList.toggle('sidebar-collapsed', initialCollapsed);

  document.addEventListener('DOMContentLoaded', () => {
    setCollapsed(initialCollapsed);

    const toggle = document.querySelector('[data-sidebar-toggle]');
    if (!toggle) return;

    toggle.addEventListener('click', () => {
      const collapsed = !document.documentElement.classList.contains('sidebar-collapsed');
      setCollapsed(collapsed);
      persistCollapsedState(collapsed);
    });
  });
})();
