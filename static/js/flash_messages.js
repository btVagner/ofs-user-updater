setTimeout(() => {
  const flashContainer = document.getElementById("flash-container");
  if (flashContainer) {
    flashContainer.style.opacity = '0';
    setTimeout(() => flashContainer.remove(), 500); // remove após o fade
  }
}, 3000); // 3 segundos
