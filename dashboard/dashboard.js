/**
 * E-Commerce Analytics Dashboard
 * Interactive Chart.js visualizations powered by pre-computed JSON data.
 */

// ─── Chart.js Global Configuration ─────────────────────────────────
Chart.defaults.color = '#a0a0c0';
Chart.defaults.borderColor = 'rgba(99, 102, 241, 0.1)';
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.plugins.legend.display = false;
Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(13, 12, 29, 0.95)';
Chart.defaults.plugins.tooltip.borderColor = 'rgba(99, 102, 241, 0.3)';
Chart.defaults.plugins.tooltip.borderWidth = 1;
Chart.defaults.plugins.tooltip.cornerRadius = 8;
Chart.defaults.plugins.tooltip.padding = 12;
Chart.defaults.plugins.tooltip.titleFont = { weight: '600', size: 12 };
Chart.defaults.plugins.tooltip.bodyFont = { size: 11 };
Chart.defaults.animation.duration = 1200;
Chart.defaults.animation.easing = 'easeOutQuart';

// ─── Color Palette ──────────────────────────────────────────────────
const COLORS = {
    indigo: '#6366f1',
    violet: '#8b5cf6',
    cyan: '#06b6d4',
    emerald: '#10b981',
    amber: '#f59e0b',
    rose: '#f43f5e',
    pink: '#ec4899',
    teal: '#14b8a6',
    orange: '#f97316',
    blue: '#3b82f6',
    lime: '#84cc16',
    red: '#ef4444',
    sky: '#0ea5e9',
    fuchsia: '#d946ef',
    yellow: '#eab308',
};

const PALETTE = [
    COLORS.indigo, COLORS.cyan, COLORS.emerald, COLORS.amber, COLORS.rose,
    COLORS.violet, COLORS.pink, COLORS.teal, COLORS.orange, COLORS.blue,
    COLORS.lime, COLORS.red, COLORS.sky, COLORS.fuchsia, COLORS.yellow,
];

const SEGMENT_COLORS = {
    'Champions': COLORS.emerald,
    'Loyal': COLORS.indigo,
    'Recent': COLORS.cyan,
    'Potential': COLORS.violet,
    'At Risk': COLORS.amber,
    'Lost': COLORS.rose,
    'Others': '#6b6b8d',
    'Loyal Customers': COLORS.indigo,
    'Recent Customers': COLORS.cyan,
    'Potential Loyalists': COLORS.violet,
};

// ─── Utility Functions ──────────────────────────────────────────────
function formatCurrency(value) {
    if (value >= 1e6) return `R$ ${(value / 1e6).toFixed(1)}M`;
    if (value >= 1e3) return `R$ ${(value / 1e3).toFixed(1)}K`;
    return `R$ ${value.toFixed(2)}`;
}

function formatNumber(value) {
    if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`;
    if (value >= 1e3) return `${(value / 1e3).toFixed(1)}K`;
    return value.toLocaleString();
}

function hexToRgba(hex, alpha) {
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function createGradient(ctx, color1, color2, vertical = true) {
    const gradient = vertical
        ? ctx.createLinearGradient(0, 0, 0, ctx.canvas.height)
        : ctx.createLinearGradient(0, 0, ctx.canvas.width, 0);
    gradient.addColorStop(0, hexToRgba(color1, 0.4));
    gradient.addColorStop(1, hexToRgba(color2 || color1, 0.02));
    return gradient;
}

// ─── KPI Rendering ──────────────────────────────────────────────────
function renderKPIs(data) {
    const kpis = data.kpis;
    const delivery = data.delivery_metrics;

    // Animate count-up
    function animateValue(el, target, prefix = '', suffix = '', decimals = 0) {
        const duration = 1500;
        const start = performance.now();
        const update = (now) => {
            const elapsed = now - start;
            const progress = Math.min(elapsed / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 3);
            const current = target * eased;
            el.textContent = prefix + current.toFixed(decimals).replace(/\B(?=(\d{3})+(?!\d))/g, ',') + suffix;
            if (progress < 1) requestAnimationFrame(update);
        };
        requestAnimationFrame(update);
    }

    const revEl = document.getElementById('kpi-revenue-value');
    const ordEl = document.getElementById('kpi-orders-value');
    const custEl = document.getElementById('kpi-customers-value');
    const avgEl = document.getElementById('kpi-avg-value');
    const revwEl = document.getElementById('kpi-review-value');
    const delEl = document.getElementById('kpi-delivery-value');

    animateValue(revEl, kpis.total_revenue / 1e6, 'R$ ', 'M', 1);
    animateValue(ordEl, kpis.total_orders, '', '', 0);
    animateValue(custEl, kpis.unique_customers, '', '', 0);
    animateValue(avgEl, kpis.avg_order_value, 'R$ ', '', 2);
    animateValue(revwEl, kpis.avg_review_score, '', ' / 5', 2);
    animateValue(delEl, delivery.on_time_rate, '', '%', 1);

    // Sub labels
    document.getElementById('kpi-revenue-sub').textContent = `GMV incl. freight: R$ ${((kpis.total_revenue + kpis.total_freight) / 1e6).toFixed(1)}M`;
    document.getElementById('kpi-orders-sub').textContent = `${formatNumber(kpis.unique_products)} products | ${formatNumber(kpis.active_sellers)} sellers`;
    document.getElementById('kpi-customers-sub').textContent = `Across all Brazilian states`;
    document.getElementById('kpi-avg-sub').textContent = `Avg delivery: ${kpis.avg_delivery_days} days`;
    document.getElementById('kpi-review-sub').textContent = `From ${formatNumber(data.review_distribution.count.reduce((a, b) => a + b, 0))} reviews`;
    document.getElementById('kpi-delivery-sub').textContent = `${formatNumber(delivery.on_time)} on-time / ${formatNumber(delivery.late)} late`;
}

// ─── Chart: Monthly Revenue Trends ──────────────────────────────────
function renderRevenueTrend(data) {
    const ctx = document.getElementById('canvas-revenue-trend').getContext('2d');
    const months = data.monthly_trends.month;
    const revenue = data.monthly_trends.revenue;
    const orders = data.monthly_trends.orders;

    const gradient = createGradient(ctx, COLORS.indigo, COLORS.violet);

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: months.map(m => {
                const parts = m.split('-');
                return parts.length === 2 ? `${parts[1]}/${parts[0].slice(2)}` : m;
            }),
            datasets: [
                {
                    label: 'Revenue (R$)',
                    data: revenue,
                    borderColor: COLORS.indigo,
                    backgroundColor: gradient,
                    borderWidth: 2.5,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 3,
                    pointHoverRadius: 6,
                    pointBackgroundColor: COLORS.indigo,
                    pointBorderColor: '#0d0c1d',
                    pointBorderWidth: 2,
                    yAxisID: 'y',
                },
                {
                    label: 'Orders',
                    data: orders,
                    borderColor: COLORS.cyan,
                    backgroundColor: 'transparent',
                    borderWidth: 2,
                    borderDash: [6, 4],
                    tension: 0.4,
                    pointRadius: 2,
                    pointHoverRadius: 5,
                    pointBackgroundColor: COLORS.cyan,
                    yAxisID: 'y1',
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { maxRotation: 45, font: { size: 9 } }
                },
                y: {
                    position: 'left',
                    grid: { color: 'rgba(99, 102, 241, 0.06)' },
                    ticks: {
                        callback: v => formatCurrency(v),
                        font: { size: 9 }
                    }
                },
                y1: {
                    position: 'right',
                    grid: { drawOnChartArea: false },
                    ticks: {
                        callback: v => formatNumber(v),
                        font: { size: 9 }
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: (ctx) => {
                            if (ctx.datasetIndex === 0) return `Revenue: ${formatCurrency(ctx.parsed.y)}`;
                            return `Orders: ${formatNumber(ctx.parsed.y)}`;
                        }
                    }
                }
            }
        }
    });
}

// ─── Chart: Yearly Summary ──────────────────────────────────────────
function renderYearlySummary(data) {
    const ctx = document.getElementById('canvas-yearly').getContext('2d');
    const years = data.yearly_summary.year.map(String);
    const revenue = data.yearly_summary.revenue;
    const orders = data.yearly_summary.orders;

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: years,
            datasets: [
                {
                    label: 'Revenue',
                    data: revenue,
                    backgroundColor: [
                        hexToRgba(COLORS.indigo, 0.5),
                        hexToRgba(COLORS.indigo, 0.7),
                        hexToRgba(COLORS.indigo, 0.9),
                    ],
                    borderColor: COLORS.indigo,
                    borderWidth: 1,
                    borderRadius: 6,
                    yAxisID: 'y',
                },
                {
                    label: 'Orders',
                    data: orders,
                    type: 'line',
                    borderColor: COLORS.amber,
                    backgroundColor: 'transparent',
                    borderWidth: 2.5,
                    pointRadius: 5,
                    pointBackgroundColor: COLORS.amber,
                    pointBorderColor: '#0d0c1d',
                    pointBorderWidth: 2,
                    tension: 0.3,
                    yAxisID: 'y1',
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, position: 'bottom', labels: { boxWidth: 10, padding: 15, font: { size: 10 } } },
                tooltip: {
                    callbacks: {
                        label: (ctx) => {
                            if (ctx.datasetIndex === 0) return `Revenue: ${formatCurrency(ctx.parsed.y)}`;
                            return `Orders: ${formatNumber(ctx.parsed.y)}`;
                        }
                    }
                }
            },
            scales: {
                x: { grid: { display: false } },
                y: {
                    position: 'left',
                    grid: { color: 'rgba(99, 102, 241, 0.06)' },
                    ticks: { callback: v => formatCurrency(v), font: { size: 9 } }
                },
                y1: {
                    position: 'right',
                    grid: { drawOnChartArea: false },
                    ticks: { callback: v => formatNumber(v), font: { size: 9 } }
                }
            }
        }
    });
}

// ─── Chart: Top Categories ──────────────────────────────────────────
function renderCategories(data) {
    const ctx = document.getElementById('canvas-categories').getContext('2d');
    const categories = data.category_performance.category.slice().reverse();
    const revenue = data.category_performance.revenue.slice().reverse();

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: categories.map(c => c.length > 22 ? c.slice(0, 20) + '...' : c),
            datasets: [{
                data: revenue,
                backgroundColor: PALETTE.slice().reverse().map(c => hexToRgba(c, 0.75)),
                borderColor: PALETTE.slice().reverse(),
                borderWidth: 1,
                borderRadius: 4,
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    grid: { color: 'rgba(99, 102, 241, 0.06)' },
                    ticks: { callback: v => formatCurrency(v), font: { size: 9 } }
                },
                y: {
                    grid: { display: false },
                    ticks: { font: { size: 9 } }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: (ctx) => `Revenue: ${formatCurrency(ctx.parsed.x)}`
                    }
                }
            }
        }
    });
}

// ─── Chart: Payment Distribution ────────────────────────────────────
function renderPayments(data) {
    const ctx = document.getElementById('canvas-payments').getContext('2d');
    const types = data.payment_distribution.type;
    const values = data.payment_distribution.value;

    const formattedLabels = types.map(t => t.charAt(0).toUpperCase() + t.slice(1).replace('_', ' '));
    const total = values.reduce((a, b) => a + b, 0);
    const bgColors = [COLORS.indigo, COLORS.cyan, COLORS.emerald, COLORS.amber, COLORS.rose].map(c => hexToRgba(c, 0.75));
    const borderColors = [COLORS.indigo, COLORS.cyan, COLORS.emerald, COLORS.amber, COLORS.rose];

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: formattedLabels,
            datasets: [{
                data: values,
                backgroundColor: bgColors,
                borderColor: borderColors,
                borderWidth: 1,
                borderRadius: 4,
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    grid: { color: 'rgba(99, 102, 241, 0.06)' },
                    ticks: { callback: v => formatCurrency(v), font: { size: 9 } }
                },
                y: {
                    grid: { display: false },
                    ticks: { font: { size: 9 } }
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: (ctx) => `Revenue: ${formatCurrency(ctx.parsed.x)}`,
                        afterLabel: (ctx) => {
                            const percent = (ctx.parsed.x / total * 100).toFixed(1);
                            return `Share: ${percent}%`;
                        }
                    }
                }
            }
        }
    });
}

// ─── Chart: Regional Performance ────────────────────────────────────
function renderRegional(data) {
    const ctx = document.getElementById('canvas-regional').getContext('2d');
    const states = data.regional_data.state;
    const revenue = data.regional_data.revenue;

    const maxRev = Math.max(...revenue);
    const bgColors = revenue.map(r => {
        const intensity = 0.3 + (r / maxRev) * 0.6;
        return hexToRgba(COLORS.indigo, intensity);
    });

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: states,
            datasets: [{
                data: revenue,
                backgroundColor: bgColors,
                borderColor: COLORS.indigo,
                borderWidth: 1,
                borderRadius: 4,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { font: { size: 9 } }
                },
                y: {
                    grid: { color: 'rgba(99, 102, 241, 0.06)' },
                    ticks: { callback: v => formatCurrency(v), font: { size: 9 } }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        afterLabel: (ctx) => {
                            const i = ctx.dataIndex;
                            return `Orders: ${formatNumber(data.regional_data.orders[i])}\nCustomers: ${formatNumber(data.regional_data.customers[i])}`;
                        }
                    }
                }
            }
        }
    });
}

// ─── Chart: Customer Segments ───────────────────────────────────────
function renderSegments(data) {
    const ctx = document.getElementById('canvas-segments').getContext('2d');
    const segments = data.customer_segments.segment;
    const counts = data.customer_segments.count;
    const revenues = data.customer_segments.revenue;

    const bgColors = segments.map(s => {
        return SEGMENT_COLORS[s] || '#6b6b8d';
    });

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: segments,
            datasets: [
                {
                    label: 'Customers',
                    data: counts,
                    backgroundColor: bgColors.map(c => hexToRgba(c, 0.7)),
                    borderColor: bgColors,
                    borderWidth: 1,
                    borderRadius: 6,
                    yAxisID: 'y',
                },
                {
                    label: 'Revenue',
                    data: revenues,
                    type: 'line',
                    borderColor: COLORS.amber,
                    backgroundColor: 'transparent',
                    borderWidth: 2,
                    pointRadius: 4,
                    pointBackgroundColor: COLORS.amber,
                    pointBorderColor: '#0d0c1d',
                    pointBorderWidth: 2,
                    tension: 0.3,
                    yAxisID: 'y1',
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, position: 'bottom', labels: { boxWidth: 10, padding: 12, font: { size: 10 } } },
                tooltip: {
                    callbacks: {
                        label: (ctx) => {
                            if (ctx.datasetIndex === 0) return `Customers: ${formatNumber(ctx.parsed.y)}`;
                            return `Revenue: ${formatCurrency(ctx.parsed.y)}`;
                        }
                    }
                }
            },
            scales: {
                x: { grid: { display: false }, ticks: { font: { size: 9 } } },
                y: {
                    position: 'left',
                    grid: { color: 'rgba(99, 102, 241, 0.06)' },
                    ticks: { callback: v => formatNumber(v), font: { size: 9 } }
                },
                y1: {
                    position: 'right',
                    grid: { drawOnChartArea: false },
                    ticks: { callback: v => formatCurrency(v), font: { size: 9 } }
                }
            }
        }
    });
}

// ─── Chart: Reviews ─────────────────────────────────────────────────
function renderReviews(data) {
    const ctx = document.getElementById('canvas-reviews').getContext('2d');
    const scores = data.review_distribution.score;
    const counts = data.review_distribution.count;

    const starColors = [COLORS.rose, COLORS.orange, COLORS.amber, '#a3e635', COLORS.emerald];

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: scores.map(s => `${s} Star`),
            datasets: [{
                data: counts,
                backgroundColor: starColors.map(c => hexToRgba(c, 0.7)),
                borderColor: starColors,
                borderWidth: 1,
                borderRadius: 6,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { grid: { display: false } },
                y: {
                    grid: { color: 'rgba(99, 102, 241, 0.06)' },
                    ticks: { callback: v => formatNumber(v), font: { size: 9 } }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        afterLabel: (ctx) => {
                            const total = counts.reduce((a, b) => a + b, 0);
                            return `${(ctx.parsed.y / total * 100).toFixed(1)}% of all reviews`;
                        }
                    }
                }
            }
        }
    });
}

// ─── Chart: Day of Week ─────────────────────────────────────────────
function renderDOW(data) {
    const ctx = document.getElementById('canvas-dow').getContext('2d');
    const days = data.dow_patterns.day;
    const orders = data.dow_patterns.orders;

    const isWeekend = d => d === 'Saturday' || d === 'Sunday';
    const barColors = days.map(d => isWeekend(d) ? hexToRgba(COLORS.cyan, 0.7) : hexToRgba(COLORS.indigo, 0.7));
    const borderColors = days.map(d => isWeekend(d) ? COLORS.cyan : COLORS.indigo);

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: days.map(d => d.slice(0, 3)),
            datasets: [{
                data: orders,
                backgroundColor: barColors,
                borderColor: borderColors,
                borderWidth: 1,
                borderRadius: 6,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { grid: { display: false } },
                y: {
                    grid: { color: 'rgba(99, 102, 241, 0.06)' },
                    ticks: { callback: v => formatNumber(v), font: { size: 9 } }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: (ctx) => `Orders: ${formatNumber(ctx.parsed.y)}`
                    }
                }
            }
        }
    });
}

// ─── Initialize Dashboard ───────────────────────────────────────────
let globalDashboardData = null;
let charts = {}; // Store chart instances to destroy them later

// Initialize theme before loading data
function initTheme() {
    const savedTheme = localStorage.getItem('theme') || 'light';
    if (savedTheme === 'dark') {
        document.documentElement.setAttribute('data-theme', 'dark');
        document.querySelector('.sun-icon').style.display = 'block';
        document.querySelector('.moon-icon').style.display = 'none';
        
        Chart.defaults.color = '#94a3b8';
        Chart.defaults.borderColor = 'rgba(255, 255, 255, 0.05)';
    } else {
        document.documentElement.removeAttribute('data-theme');
        document.querySelector('.sun-icon').style.display = 'none';
        document.querySelector('.moon-icon').style.display = 'block';
        
        Chart.defaults.color = '#64748b';
        Chart.defaults.borderColor = 'rgba(0, 0, 0, 0.05)';
    }
}

async function initDashboard() {
    initTheme();
    try {
        if (!window.DASHBOARD_DATA) throw new Error('DASHBOARD_DATA not injected. Please ensure dashboard_data.js loaded properly.');
        globalDashboardData = window.DASHBOARD_DATA;

        console.log('Dashboard data loaded:', Object.keys(globalDashboardData));

        renderDashboard(globalDashboardData);
        setupFilters();

        console.log('Dashboard rendered successfully');
    } catch (error) {
        console.error('Failed to load dashboard data:', error);
        document.querySelector('.main-content').innerHTML = `
            <div style="text-align:center; padding:80px 20px;">
                <h2 style="color:#f43f5e; margin-bottom:12px;">Failed to Load Dashboard Data</h2>
                <p style="color:#a0a0c0;">Error: ${error.message}</p>
                <p style="color:#6b6b8d; margin-top:8px; font-size:0.85rem;">
                    Make sure <code>dashboard_data.json</code> exists in the same directory.<br>
                    Run <code>python scripts/generate_dashboard_data.py</code> to generate it.
                </p>
            </div>
        `;
    }
}

function renderDashboard(data, filteredYear = 'all') {
    // Render all components
    renderKPIs(data);
    
    // For trends, we can actually slice the data based on year!
    let trendsData = JSON.parse(JSON.stringify(data)); // deep copy
    if (filteredYear !== 'all') {
        const indices = trendsData.monthly_trends.month
            .map((m, i) => m.startsWith(filteredYear) ? i : -1)
            .filter(i => i !== -1);
            
        trendsData.monthly_trends.month = indices.map(i => trendsData.monthly_trends.month[i]);
        trendsData.monthly_trends.revenue = indices.map(i => trendsData.monthly_trends.revenue[i]);
        trendsData.monthly_trends.orders = indices.map(i => trendsData.monthly_trends.orders[i]);
    }
    
    renderRevenueTrend(trendsData);
    renderYearlySummary(data);
    renderCategories(data);
    renderPayments(data);
    renderRegional(data);
    renderSegments(data);
    renderReviews(data);
    renderDOW(data);
}

// ─── Interactive Filters ────────────────────────────────────────────
function setupFilters() {
    const btnParams = document.getElementById('btn-apply-filters');
    const yearSelect = document.getElementById('filter-year');
    
    btnParams.addEventListener('click', () => {
        const originalText = btnParams.innerText;
        btnParams.innerHTML = '<span class="pulse-dot" style="display:inline-block; margin-right:8px; background:white;"></span> Updating...';
        btnParams.disabled = true;
        
        // Ensure chart instances are destroyed before re-rendering
        for (let key in Chart.instances) {
            Chart.instances[key].destroy();
        }
        
        setTimeout(() => {
            const year = yearSelect.value;
            renderDashboard(globalDashboardData, year);
            
            btnParams.innerHTML = 'Filters Applied';
            setTimeout(() => {
                btnParams.innerText = originalText;
                btnParams.disabled = false;
            }, 1000);
        }, 600);
    });
}

// ─── Theme Toggle Logic ─────────────────────────────────────────────
document.getElementById('theme-toggle').addEventListener('click', () => {
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    const newTheme = isDark ? 'light' : 'dark';
    
    if (newTheme === 'dark') {
        document.documentElement.setAttribute('data-theme', 'dark');
        document.querySelector('.sun-icon').style.display = 'block';
        document.querySelector('.moon-icon').style.display = 'none';
        Chart.defaults.color = '#94a3b8';
        Chart.defaults.borderColor = 'rgba(255, 255, 255, 0.05)';
    } else {
        document.documentElement.removeAttribute('data-theme');
        document.querySelector('.sun-icon').style.display = 'none';
        document.querySelector('.moon-icon').style.display = 'block';
        Chart.defaults.color = '#64748b';
        Chart.defaults.borderColor = 'rgba(0, 0, 0, 0.05)';
    }
    
    localStorage.setItem('theme', newTheme);
    
    // Update all charts without fully redrawing
    for (let id in Chart.instances) {
        const chart = Chart.instances[id];
        
        // Update scales if they exist
        if (chart.options.scales) {
            for (let axis in chart.options.scales) {
                if (chart.options.scales[axis].grid) {
                    chart.options.scales[axis].grid.color = Chart.defaults.borderColor;
                }
                if (chart.options.scales[axis].ticks) {
                    chart.options.scales[axis].ticks.color = Chart.defaults.color;
                }
            }
        }
        chart.update();
    }
});

// Launch
document.addEventListener('DOMContentLoaded', initDashboard);
