(function () {
  const lineCanvas = document.getElementById("pendingCloseChart");
  const barCanvas = document.getElementById("pendingCloseActivityTypeChart");
  const rawDataEl = document.getElementById("pendingCloseChartData");
  const toggleIntegration = document.getElementById("toggleIntegration");
  const togglePending = document.getElementById("togglePending");
  const filterActions = document.getElementById("activityTypeFilterActions");
  const metricTotalIntegration = document.getElementById("metricTotalIntegration");
  const metricTotalPending = document.getElementById("metricTotalPending");

  if (!lineCanvas || !barCanvas || !rawDataEl || typeof Chart === "undefined") return;

  let parsed = {
    line_default: { labels: [], integration: [], pending: [], total_integration: 0, total_pending: 0 },
    activity_type_bar: { labels: [], integration: [], pending: [] },
    daily_by_activity_type: {}
  };

  try {
    parsed = JSON.parse(rawDataEl.textContent || "{}");
  } catch (e) {
    console.error("Falha ao ler dados dos gráficos:", e);
    return;
  }

  let currentActivityType = "__ALL__";

  function getCurrentLineData() {
    if (currentActivityType === "__ALL__") {
      return parsed.line_default || { labels: [], integration: [], pending: [], total_integration: 0, total_pending: 0 };
    }
    return parsed.daily_by_activity_type?.[currentActivityType] || {
      labels: [],
      integration: [],
      pending: [],
      total_integration: 0,
      total_pending: 0
    };
  }

  const valueLabelsPlugin = {
    id: "valueLabelsPlugin",
    afterDatasetsDraw(chart) {
      const { ctx } = chart;

      chart.data.datasets.forEach((dataset, datasetIndex) => {
        const meta = chart.getDatasetMeta(datasetIndex);
        if (meta.hidden) return;

        meta.data.forEach((point, index) => {
          const value = Number(dataset.data[index] || 0);
          if (!value) return;

          ctx.save();
          ctx.font = "11px sans-serif";
          ctx.textAlign = "center";
          ctx.textBaseline = "bottom";
          ctx.fillStyle = dataset.borderColor;
          ctx.fillText(String(value), point.x, point.y - 10);
          ctx.restore();
        });
      });
    }
  };

  const barValueLabelsPlugin = {
    id: "barValueLabelsPlugin",
    afterDatasetsDraw(chart) {
      const { ctx } = chart;

      chart.data.datasets.forEach((dataset, datasetIndex) => {
        const meta = chart.getDatasetMeta(datasetIndex);
        if (meta.hidden) return;

        meta.data.forEach((bar, index) => {
          const value = Number(dataset.data[index] || 0);
          if (!value) return;

          ctx.save();
          ctx.font = "11px sans-serif";
          ctx.textAlign = "center";
          ctx.textBaseline = "bottom";
          ctx.fillStyle = dataset.borderColor;
          ctx.fillText(String(value), bar.x, bar.y - 6);
          ctx.restore();
        });
      });
    }
  };

  const noDataPlugin = {
    id: "noDataPendingClose",
    afterDraw(chart) {
      const hasVisibleData = chart.data.datasets.some((dataset, index) => {
        const meta = chart.getDatasetMeta(index);
        if (meta.hidden) return false;
        return (dataset.data || []).some(v => Number(v) > 0);
      });

      if (hasVisibleData) return;

      const { ctx, chartArea } = chart;
      if (!chartArea) return;

      ctx.save();
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = "#9fa4ab";
      ctx.font = "14px sans-serif";
      ctx.fillText(
        "Sem dados para exibir",
        (chartArea.left + chartArea.right) / 2,
        (chartArea.top + chartArea.bottom) / 2
      );
      ctx.restore();
    }
  };

  function createGradient(context, colorHex) {
    const chart = context.chart;
    const { ctx, chartArea } = chart;

    if (!chartArea) {
      return colorHex + "22";
    }

    const gradient = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
    gradient.addColorStop(0, colorHex + "40");
    gradient.addColorStop(1, colorHex + "05");
    return gradient;
  }

  function updateMetrics(lineData) {
    metricTotalIntegration.textContent = lineData.total_integration || 0;
    metricTotalPending.textContent = lineData.total_pending || 0;
  }

  const lineChart = new Chart(lineCanvas, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Erros de integração",
          data: [],
          borderColor: "#FF8AA9",
          backgroundColor: (context) => createGradient(context, "#FF8AA9"),
          pointBackgroundColor: "#FF8AA9",
          pointBorderColor: "#FF8AA9",
          pointRadius: 4,
          pointHoverRadius: 6,
          borderWidth: 3,
          tension: 0.35,
          fill: true,
          hidden: false
        },
        {
          label: "Erros ainda pendentes",
          data: [],
          borderColor: "#F3953F",
          backgroundColor: (context) => createGradient(context, "#F3953F"),
          pointBackgroundColor: "#F3953F",
          pointBorderColor: "#F3953F",
          pointRadius: 4,
          pointHoverRadius: 6,
          borderWidth: 3,
          tension: 0.35,
          fill: true,
          hidden: false
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false
      },
      animation: {
        duration: 350
      },
      plugins: {
        legend: {
          display: false
        },
        tooltip: {
          backgroundColor: "#111",
          borderColor: "#333",
          borderWidth: 1,
          titleColor: "#fff",
          bodyColor: "#ddd",
          displayColors: true,
          callbacks: {
            label(context) {
              return `${context.dataset.label}: ${context.raw}`;
            }
          }
        }
      },
      scales: {
        x: {
          grid: {
            color: "rgba(255,255,255,0.06)"
          },
          ticks: {
            color: "#cfd3d8",
            maxRotation: 0,
            autoSkip: true
          }
        },
        y: {
          beginAtZero: true,
          grid: {
            color: "rgba(255,255,255,0.06)"
          },
          ticks: {
            color: "#cfd3d8",
            precision: 0
          }
        }
      }
    },
    plugins: [valueLabelsPlugin, noDataPlugin]
  });

  const barChart = new Chart(barCanvas, {
    type: "bar",
    data: {
      labels: parsed.activity_type_bar?.labels || [],
      datasets: [
        {
          label: "Erros de integração",
          data: parsed.activity_type_bar?.integration || [],
          backgroundColor: "#E56B8ACC",
          borderColor: "#E56B8A",
          borderWidth: 2,
          borderRadius: 8,
          borderSkipped: false
        },
        {
          label: "Erros ainda pendentes",
          data: parsed.activity_type_bar?.pending || [],
          backgroundColor: "#F4A261CC",
          borderColor: "#F4A261",
          borderWidth: 2,
          borderRadius: 8,
          borderSkipped: false
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: {
        duration: 350
      },
      onClick(event, elements) {
        if (!elements.length) return;
        const index = elements[0].index;
        const activityType = barChart.data.labels[index];
        if (!activityType) return;

        currentActivityType = activityType;
        syncFilterButtons();
        refreshLineChart();
      },
      plugins: {
        legend: {
          labels: {
            color: "#e5e7eb"
          }
        },
        tooltip: {
          backgroundColor: "#111",
          borderColor: "#333",
          borderWidth: 1,
          titleColor: "#fff",
          bodyColor: "#ddd",
          displayColors: true
        }
      },
      scales: {
        x: {
          stacked: false,
          grid: {
            color: "rgba(255,255,255,0.06)"
          },
          ticks: {
            color: "#cfd3d8",
            maxRotation: 35,
            minRotation: 0
          }
        },
        y: {
          stacked: false,
          beginAtZero: true,
          grid: {
            color: "rgba(255,255,255,0.06)"
          },
          ticks: {
            color: "#cfd3d8",
            precision: 0
          }
        }
      }
    },
    plugins: [noDataPlugin]
  });

  function refreshLineChart() {
    const lineData = getCurrentLineData();

    lineChart.data.labels = lineData.labels || [];
    lineChart.data.datasets[0].data = lineData.integration || [];
    lineChart.data.datasets[1].data = lineData.pending || [];

    lineChart.setDatasetVisibility(0, !!toggleIntegration.checked);
    lineChart.setDatasetVisibility(1, !!togglePending.checked);

    updateMetrics(lineData);
    lineChart.update();
  }

  function syncFilterButtons() {
    const buttons = document.querySelectorAll(".pending-close-filter-btn");
    buttons.forEach(btn => {
      const isActive = btn.dataset.activityType === currentActivityType;
      btn.classList.toggle("active", isActive);
    });
  }

  function buildFilterButtons() {
    const labels = parsed.activity_type_bar?.labels || [];
    const fragment = document.createDocumentFragment();

    labels.forEach(label => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "btn-secondary pending-close-filter-btn";
      btn.dataset.activityType = label;
      btn.textContent = label;

      btn.addEventListener("click", function () {
        currentActivityType = label;
        syncFilterButtons();
        refreshLineChart();
      });

      fragment.appendChild(btn);
    });

    filterActions.appendChild(fragment);

    const allBtn = filterActions.querySelector('[data-activity-type="__ALL__"]');
    if (allBtn) {
      allBtn.addEventListener("click", function () {
        currentActivityType = "__ALL__";
        syncFilterButtons();
        refreshLineChart();
      });
    }

    syncFilterButtons();
  }

  if (toggleIntegration) {
    toggleIntegration.addEventListener("change", refreshLineChart);
  }

  if (togglePending) {
    togglePending.addEventListener("change", refreshLineChart);
  }

  buildFilterButtons();
  refreshLineChart();
})();