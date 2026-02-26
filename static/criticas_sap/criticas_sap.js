(function () {
  const form = document.getElementById("dashFilters");
  const dateFromEl = document.getElementById("dateFrom");
  const dateToEl = document.getElementById("dateTo");
  const activityTypeEl = document.getElementById("activityType");

  const kpiTotal = document.getElementById("kpiTotal");
  const kpiRange = document.getElementById("kpiRange");

  const canvas = document.getElementById("chartCriticas");
  const ctx = canvas.getContext("2d");

  // Endpoint vindo do template (respeita APP_ROOT)
  const DATA_URL = window.DASH_DATA_URL || "/sap/acompanhamento-critica/dashboard/data";

  // ===== Buckets modal state =====
  const btnOpenBuckets = document.getElementById("btnOpenBuckets");
  const bucketBackdrop = document.getElementById("bucketBackdrop");
  const bucketModal = document.getElementById("bucketModal");
  const btnBucketsClose = document.getElementById("btnBucketsClose");
  const btnBucketsCancel = document.getElementById("btnBucketsCancel");
  const btnBucketsSave = document.getElementById("btnBucketsSave");
  const btnBucketsAll = document.getElementById("btnBucketsAll");
  const btnBucketsClear = document.getElementById("btnBucketsClear");
  const bucketSearch = document.getElementById("bucketSearch");
  const bucketCountEl = document.getElementById("bucketCount");

  const bucketChecks = Array.from(document.querySelectorAll(".bucket-check"));

  let selectedBuckets = [];
  let snapshotBuckets = [];

  function computeTickStep(dayCount) {
    if (dayCount <= 10) return 1;
    return Math.ceil((dayCount - 9) / 5) + 1;
  }

  let chart = new Chart(ctx, {
    type: "line",
    data: {
      labels: [],
      datasets: [{
        label: "Críticas SAP (qtd)",
        data: [],
        tension: 0.25,
        pointRadius: 3,
        pointHoverRadius: 5,
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true },
        tooltip: { enabled: true }
      },
      scales: {
        x: {
          ticks: {
            autoSkip: false,
            callback: function (val, index) {
              const labels = this.getLabels();
              const n = labels.length;
              if (!n) return "";

              const step = computeTickStep(n);
              if (index === 0 || index === n - 1) return labels[index];
              return (index % step === 0) ? labels[index] : "";
            },
            maxRotation: 0,
            minRotation: 0
          }
        },
        y: {
          beginAtZero: true,
          ticks: { precision: 0 }
        }
      }
    }
  });

  function filterBucketList(q) {
    const needle = (q || "").trim().toLowerCase();
    bucketChecks.forEach(ch => {
      const label = ch.closest(".bucket-item");
      const text = (label ? label.innerText : ch.value).toLowerCase();
      label.style.display = (!needle || text.includes(needle)) ? "flex" : "none";
    });
  }

  function updateBucketSummary() {
    bucketCountEl.textContent = String(selectedBuckets.length);
  }

  function openBucketModal() {
    snapshotBuckets = [...selectedBuckets];
    const setSel = new Set(selectedBuckets);
    bucketChecks.forEach(ch => ch.checked = setSel.has(ch.value));

    bucketSearch.value = "";
    filterBucketList("");

    bucketBackdrop.style.display = "block";
    bucketModal.style.display = "block";
  }

  function closeBucketModal() {
    bucketBackdrop.style.display = "none";
    bucketModal.style.display = "none";
  }

  btnOpenBuckets?.addEventListener("click", openBucketModal);
  btnBucketsClose?.addEventListener("click", closeBucketModal);
  bucketBackdrop?.addEventListener("click", closeBucketModal);

  btnBucketsCancel?.addEventListener("click", () => {
    selectedBuckets = [...snapshotBuckets];
    updateBucketSummary();
    closeBucketModal();
  });

  btnBucketsAll?.addEventListener("click", () => {
    bucketChecks.forEach(ch => {
      const label = ch.closest(".bucket-item");
      if (label && label.style.display !== "none") ch.checked = true;
    });
  });

  btnBucketsClear?.addEventListener("click", () => {
    bucketChecks.forEach(ch => ch.checked = false);
  });

  bucketSearch?.addEventListener("input", (e) => {
    filterBucketList(e.target.value);
  });

  btnBucketsSave?.addEventListener("click", () => {
    selectedBuckets = bucketChecks.filter(ch => ch.checked).map(ch => ch.value);
    updateBucketSummary();
    closeBucketModal();
    refresh();
  });

  updateBucketSummary();

  async function fetchData() {
    const dateFrom = (dateFromEl.value || "").trim();
    const dateTo = (dateToEl.value || "").trim();
    const activityType = (activityTypeEl.value || "").trim();

    if (!dateFrom || !dateTo) throw new Error("Informe De e Até.");
    if (dateTo < dateFrom) throw new Error("O campo 'Até' não pode ser menor que 'De'.");

    const params = new URLSearchParams();
    params.set("dateFrom", dateFrom);
    params.set("dateTo", dateTo);
    if (activityType) params.set("activityType", activityType);

    if (selectedBuckets.length) {
      for (const b of selectedBuckets) params.append("buckets", b);
    }

    const url = `${DATA_URL}?${params.toString()}`;

    const resp = await fetch(url, {
      headers: { "Accept": "application/json" },
      credentials: "same-origin" // ✅ garante cookies de sessão
    });

    let data;
    try {
      data = await resp.json();
    } catch (e) {
      throw new Error(`Resposta inválida do servidor (não é JSON). HTTP ${resp.status}`);
    }

    if (!resp.ok || !data.ok) {
      throw new Error((data && data.error) ? data.error : `Erro HTTP ${resp.status}`);
    }

    return { data, dateFrom, dateTo };
  }

  function updateKpis(labels, values, dateFrom, dateTo) {
    const total = values.reduce((acc, v) => acc + (Number(v) || 0), 0);
    kpiTotal.innerHTML = `Total no período: <b>${total}</b>`;
    kpiRange.innerHTML = `Período: <b>${dateFrom}</b> → <b>${dateTo}</b> | Dias com dados: <b>${labels.length}</b>`;
  }

  async function refresh() {
    try {
      kpiTotal.innerHTML = `Total no período: <b>Carregando...</b>`;
      kpiRange.innerHTML = `Período: <b>-</b>`;

      const { data, dateFrom, dateTo } = await fetchData();

      const labels = data.labels || [];
      const values = data.values || [];

      chart.data.labels = labels;
      chart.data.datasets[0].data = values;
      chart.update();

      updateKpis(labels, values, dateFrom, dateTo);

    } catch (e) {
      kpiTotal.innerHTML = `Total no período: <b>Erro</b>`;
      kpiRange.innerHTML = `<b>${String(e)}</b>`;

      chart.data.labels = [];
      chart.data.datasets[0].data = [];
      chart.update();
    }
  }

  form?.addEventListener("submit", function (ev) {
    ev.preventDefault();
    refresh();
  });

  refresh();
})();