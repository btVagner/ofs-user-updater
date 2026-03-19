(function () {
    const dataEl = document.getElementById("errosAgendamentoChartData");
    const ctx = document.getElementById("chartErrosAgendamentoByDay");

    if (!dataEl || !ctx) return;

    let chartInstance = null;

    function defaultNoDataPlugin() {
        return {
            id: "noDataPlugin",
            afterDraw(chart) {
                const hasData = chart.data?.datasets?.some(ds =>
                    Array.isArray(ds.data) && ds.data.some(v => Number(v) > 0)
                );

                if (hasData) return;

                const { ctx, chartArea } = chart;
                if (!chartArea) return;

                ctx.save();
                ctx.textAlign = "center";
                ctx.textBaseline = "middle";
                ctx.font = "14px sans-serif";
                ctx.fillStyle = "#9aa0a6";
                ctx.fillText(
                    "Sem dados no período",
                    (chartArea.left + chartArea.right) / 2,
                    (chartArea.top + chartArea.bottom) / 2
                );
                ctx.restore();
            }
        };
    }

    Chart.register(defaultNoDataPlugin());
    const glowPlugin = {
        id: "glowPlugin",
        beforeDatasetsDraw(chart) {
            const { ctx } = chart;
            ctx.save();
            ctx.shadowColor = "rgba(255, 159, 185, 0.4)";
            ctx.shadowBlur = 12;
        },
        afterDatasetsDraw(chart) {
            chart.ctx.restore();
        }
    };

    Chart.register(glowPlugin);
    const commonOptions = {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
            mode: "index",
            intersect: false
        },
        plugins: {
            legend: {
                labels: {
                    color: "#d8d8d8",
                    font: { size: 12 }
                }
            },
            tooltip: {
                backgroundColor: "rgba(20,20,20,0.95)",
                titleColor: "#ffffff",
                bodyColor: "#d8d8d8",
                borderColor: "#3a3a3a",
                borderWidth: 1,
                padding: 10,
                displayColors: false
            }
        },
        scales: {
            x: {
                ticks: {
                    color: "#cfcfcf",
                    maxRotation: 0
                },
                grid: {
                    color: "rgba(255,255,255,0.04)"
                }
            },
            y: {
                beginAtZero: true,
                ticks: {
                    color: "#cfcfcf",
                    precision: 0
                },
                grid: {
                    color: "rgba(255,255,255,0.06)"
                }
            }
        }
    };
    let dashboardData = {};
    try {
        dashboardData = JSON.parse(dataEl.textContent || "{}");
    } catch (e) {
        dashboardData = {};
    }

    const labels = Array.isArray(dashboardData.labels) ? dashboardData.labels : [];
    const values = Array.isArray(dashboardData.values) ? dashboardData.values : [];

    if (chartInstance) chartInstance.destroy();

    chartInstance = new Chart(ctx, {
        type: "line",
        data: {
            labels,
            datasets: [{
                label: "Erros de agendamento",
                data: values,
                tension: 0.35,
                fill: true,
                borderColor: "#ff9fb9",
                backgroundColor: "rgba(255, 159, 185, 0.15)",
                pointBackgroundColor: "#ff9fb9",
                pointBorderColor: "#fff",
                pointRadius: 4,
                pointHoverRadius: 6,
                pointHoverBackgroundColor: "#ff9fb9",
                pointHoverBorderColor: "#fff",
                borderWidth: 2
            }]
        },
        options: {
            ...commonOptions,
            plugins: {
                ...commonOptions.plugins,
                legend: { display: true }
            }
        }
    });
})();