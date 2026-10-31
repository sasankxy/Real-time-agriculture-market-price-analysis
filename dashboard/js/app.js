/**
 * KrishiStream Live Analytics Frontend Controller
 * WebSocket stream handler, Leaflet map engine, Chart.js visualizer,
 * Anomaly alert feed, and Cross-Market arbitrage matrix.
 */

document.addEventListener("DOMContentLoaded", () => {
    // STATE
    let socket = null;
    let isPaused = false;
    let currentSpeed = 2.0;
    let selectedCommodity = "Tomato";
    const chartHistory = [];
    const maxChartPoints = 30;

    // DOM ELEMENTS
    const streamStatusPill = document.getElementById("stream-status-pill");
    const streamStatusText = document.getElementById("stream-status-text");
    const streamHzLabel = document.getElementById("stream-hz-label");
    const toggleBtn = document.getElementById("toggle-stream-btn");
    const toggleIcon = document.getElementById("toggle-icon");
    const toggleText = document.getElementById("toggle-text");
    const speedSelect = document.getElementById("speed-select");
    const tickerMarquee = document.getElementById("ticker-marquee");

    // KPI Elements
    const kpiTotal = document.getElementById("kpi-total");
    const kpiNormal = document.getElementById("kpi-normal");
    const kpiWarning = document.getElementById("kpi-warning");
    const kpiCritical = document.getElementById("kpi-critical");
    const kpiArbitrage = document.getElementById("kpi-arbitrage");
    const kpiSpeed = document.getElementById("kpi-speed");

    // Alert & Table Elements
    const alertFeedContainer = document.getElementById("alert-feed-container");
    const alertCounterBadge = document.getElementById("alert-counter-badge");
    const arbitrageTableBody = document.getElementById("arbitrage-table-body");
    const audioAlert = document.getElementById("audio-alert");

    // Demo Buttons
    const btnSpikeTomato = document.getElementById("btn-spike-tomato");
    const btnCrashOnion = document.getElementById("btn-crash-onion");
    const commodityFilter = document.getElementById("commodity-filter");

    // ==========================================
    // 1. LEAFLET MAP INITIALIZATION
    // ==========================================
    const map = L.map("mandi-map", {
        center: [21.5, 78.5],
        zoom: 5,
        zoomControl: true,
        attributionControl: false
    });

    // Pristine Dark Basemap (Esri World Dark Gray - No API key required, zero watermarks)
    L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}", {
        maxZoom: 16,
        attribution: ""
    }).addTo(map);

    // Reference layer with city/district boundaries and labels
    L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}", {
        maxZoom: 16,
        attribution: ""
    }).addTo(map);

    const mandiMarkers = new Map();

    function updateMandiMarker(event) {
        const market = event.Market || "Unknown";
        const state = event.State || "";
        const lat = parseFloat(event.Latitude) || 20.5937;
        const lng = parseFloat(event.Longitude) || 78.9629;
        const price = event.Modal_Price;
        const baseline = event.Baseline_Mean;
        const zScore = event.Z_Score;
        const severity = event.Severity || "NORMAL";

        let color = "#10b981"; // Normal emerald
        let radius = 6;
        if (severity === "WARNING") {
            color = "#f59e0b"; // Warning amber
            radius = 8;
        } else if (severity === "CRITICAL") {
            color = "#f43f5e"; // Critical rose
            radius = 11;
        }

        const popupContent = `
            <div style="font-family: 'Plus Jakarta Sans', sans-serif; font-size: 12px; color: #0f172a; padding: 4px;">
                <div style="font-weight: 800; font-size: 13px; color: #0284c7; margin-bottom: 2px;">${market}</div>
                <div style="font-size: 11px; color: #64748b; margin-bottom: 6px;">${state}</div>
                <div><b>Commodity:</b> ${event.Commodity} (${event.Variety})</div>
                <div><b>Observed Price:</b> <span style="font-family: monospace; font-weight: 700; color: #0f172a;">₹${price}/Qtl</span></div>
                <div><b>Baseline μ:</b> ₹${baseline}/Qtl</div>
                <div><b>Z-Score:</b> <span style="color: ${severity === 'CRITICAL' ? '#e11d48' : '#059669'}; font-weight: 700;">${zScore}</span></div>
                <div style="margin-top: 4px; display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: 700; background: ${severity === 'CRITICAL' ? '#ffe4e6' : '#dcfce7'}; color: ${severity === 'CRITICAL' ? '#be123c' : '#15803d'};">${severity}</div>
            </div>
        `;

        if (mandiMarkers.has(market)) {
            const marker = mandiMarkers.get(market);
            marker.setStyle({
                fillColor: color,
                color: color,
                radius: radius
            });
            marker.setPopupContent(popupContent);
        } else {
            const marker = L.circleMarker([lat, lng], {
                radius: radius,
                fillColor: color,
                color: color,
                weight: 2,
                opacity: 0.9,
                fillOpacity: 0.75
            }).addTo(map);

            marker.bindPopup(popupContent);
            mandiMarkers.set(market, marker);
        }
    }

    // ==========================================
    // 2. CHART.JS STREAM VISUALIZER
    // ==========================================
    const ctx = document.getElementById("price-stream-chart").getContext("2d");
    const priceChart = new Chart(ctx, {
        type: "line",
        data: {
            labels: [],
            datasets: [
                {
                    label: "Observed Price (₹)",
                    data: [],
                    borderColor: "#06b6d4",
                    backgroundColor: "rgba(6, 182, 212, 0.1)",
                    borderWidth: 2.5,
                    pointRadius: 4,
                    pointBackgroundColor: "#06b6d4",
                    tension: 0.25,
                    fill: false
                },
                {
                    label: "Expected Upper (+2σ)",
                    data: [],
                    borderColor: "rgba(168, 85, 247, 0.4)",
                    backgroundColor: "rgba(168, 85, 247, 0.08)",
                    borderWidth: 1,
                    pointRadius: 0,
                    borderDash: [4, 4],
                    fill: "+1",
                    tension: 0.2
                },
                {
                    label: "Expected Lower (-2σ)",
                    data: [],
                    borderColor: "rgba(168, 85, 247, 0.4)",
                    backgroundColor: "transparent",
                    borderWidth: 1,
                    pointRadius: 0,
                    borderDash: [4, 4],
                    fill: false,
                    tension: 0.2
                },
                {
                    label: "Adaptive EWMA Drift",
                    data: [],
                    borderColor: "#10b981",
                    borderWidth: 2,
                    borderDash: [6, 4],
                    pointRadius: 0,
                    fill: false,
                    tension: 0.3
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 300 },
            interaction: { mode: "index", intersect: false },
            scales: {
                x: {
                    grid: { color: "rgba(255, 255, 255, 0.05)" },
                    ticks: { color: "#64748b", font: { family: "JetBrains Mono", size: 10 } }
                },
                y: {
                    grid: { color: "rgba(255, 255, 255, 0.05)" },
                    ticks: {
                        color: "#94a3b8",
                        font: { family: "JetBrains Mono", size: 11 },
                        callback: val => `₹${val}`
                    }
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: "rgba(15, 21, 35, 0.95)",
                    titleFont: { family: "Plus Jakarta Sans", weight: "700" },
                    bodyFont: { family: "JetBrains Mono" },
                    borderColor: "rgba(255, 255, 255, 0.1)",
                    borderWidth: 1
                }
            }
        }
    });

    function updateChart(event) {
        if (selectedCommodity !== "ALL" && event.Commodity !== selectedCommodity) {
            return;
        }

        const timeLabel = new Date().toLocaleTimeString();
        const price = event.Modal_Price;
        const upper = event.Expected_Max;
        const lower = event.Expected_Min;
        const ewma = event.Adaptive_Mean || event.Commodity_EWMA;

        priceChart.data.labels.push(timeLabel);
        priceChart.data.datasets[0].data.push(price);
        priceChart.data.datasets[1].data.push(upper);
        priceChart.data.datasets[2].data.push(lower);
        priceChart.data.datasets[3].data.push(ewma);

        if (priceChart.data.labels.length > maxChartPoints) {
            priceChart.data.labels.shift();
            priceChart.data.datasets.forEach(ds => ds.data.shift());
        }

        priceChart.update("none");
    }

    // ==========================================
    // 3. ALERT FEED & TICKER
    // ==========================================
    let alertCount = 0;

    function addAlertToFeed(alert) {
        alertCount++;
        alertCounterBadge.innerText = `${alertCount} Alerts`;

        const isCritical = alert.severity === "CRITICAL";
        const item = document.createElement("div");
        item.className = `alert-item ${isCritical ? "severity-critical" : "severity-warning"}`;

        const tagBadges = (alert.tags || []).map(t =>
            `<span class="tag-badge ${isCritical ? "tag-critical" : "tag-warning"}">${t}</span>`
        ).join(" ");

        item.innerHTML = `
            <div class="alert-top">
                <span class="alert-title">
                    <span>${isCritical ? "🚨" : "⚠"}</span>
                    <span>${alert.commodity} @ ${alert.market}</span>
                    ${tagBadges}
                </span>
                <span class="alert-time">${alert.timestamp}</span>
            </div>
            <div class="alert-details">${alert.reason}</div>
            <div class="alert-meta">
                <span>Observed: ₹${alert.modal_price}/Qtl</span>
                <span>Expected: ₹${alert.baseline_mean}</span>
                <span>Z-Score: ${alert.z_score}</span>
            </div>
        `;

        if (alertFeedContainer.querySelector(".empty-state")) {
            alertFeedContainer.innerHTML = "";
        }

        alertFeedContainer.prepend(item);

        // Keep maximum 25 alerts in feed
        while (alertFeedContainer.children.length > 25) {
            alertFeedContainer.removeChild(alertFeedContainer.lastChild);
        }

        // Chime for critical alerts
        if (isCritical) {
            try {
                audioAlert.play().catch(() => {});
            } catch (e) {}
        }
    }

    function addTickerItem(event) {
        const item = document.createElement("span");
        item.className = "ticker-item";
        const isUp = event.Percent_Deviation > 0;
        item.innerHTML = `
            <span><b>${event.Commodity}</b> (${event.Market})</span>
            <span class="ticker-price">₹${event.Modal_Price}</span>
            <span style="color: ${isUp ? '#10b981' : '#f43f5e'}; font-size: 11px;">
                ${isUp ? "▲" : "▼"} ${Math.abs(event.Percent_Deviation)}%
            </span>
        `;

        if (tickerMarquee.querySelector(".placeholder-item")) {
            tickerMarquee.innerHTML = "";
        }

        tickerMarquee.prepend(item);
        while (tickerMarquee.children.length > 12) {
            tickerMarquee.removeChild(tickerMarquee.lastChild);
        }
    }

    // ==========================================
    // 4. CROSS-MARKET ARBITRAGE TABLE
    // ==========================================
    const recentArbitrageList = [];

    function updateArbitrageTable(event) {
        const arb = event.Cross_Market_Arbitrage;
        if (!arb || !arb.compare_market) return;

        recentArbitrageList.unshift({
            commodity: event.Commodity,
            reportedMarket: event.Market,
            reportedPrice: event.Modal_Price,
            compareMarket: arb.compare_market,
            spread: arb.price_spread,
            spreadPercent: arb.spread_percent,
            isArbitrage: arb.arbitrage_flag
        });

        if (recentArbitrageList.length > 8) {
            recentArbitrageList.pop();
        }

        arbitrageTableBody.innerHTML = recentArbitrageList.map(row => `
            <tr>
                <td><b>${row.commodity}</b></td>
                <td>${row.reportedMarket}</td>
                <td class="spread-val">₹${row.reportedPrice}</td>
                <td>${row.compareMarket}</td>
                <td class="spread-val" style="color: ${row.spread > 0 ? '#10b981' : '#f43f5e'}">
                    ${row.spread > 0 ? "+" : ""}₹${row.spread}
                </td>
                <td>${row.spreadPercent}%</td>
                <td>
                    ${row.isArbitrage 
                        ? `<span class="spread-badge-arb">⚡ ARBITRAGE</span>` 
                        : `<span style="color: #64748b; font-size: 11px;">Balanced</span>`}
                </td>
            </tr>
        `).join("");
    }

    // ==========================================
    // 5. WEBSOCKET CLIENT
    // ==========================================
    function connectWebSocket() {
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = `${protocol}//${window.location.host}/ws/live-stream`;

        socket = new WebSocket(wsUrl);

        socket.onopen = () => {
            console.log("[WebSocket] Connected to KrishiStream live pipeline.");
            streamStatusPill.className = "live-pill active";
            streamStatusText.innerText = "STREAM LIVE";
        };

        socket.onmessage = (message) => {
            const data = JSON.parse(message.data);

            if (data.type === "INITIAL_SNAPSHOT") {
                if (data.stats) updateStatsCounters(data.stats);
                if (data.recent_alerts) {
                    data.recent_alerts.forEach(a => addAlertToFeed(a));
                }
            } else if (data.type === "STREAM_EVENT") {
                const event = data.event;
                const alert = data.alert;

                // Update Map
                updateMandiMarker(event);

                // Update Chart
                updateChart(event);

                // Update Marquee
                addTickerItem(event);

                // Update Stats
                if (data.stats) updateStatsCounters(data.stats);

                // Update Alerts
                if (alert) addAlertToFeed(alert);

                // Update Arbitrage
                updateArbitrageTable(event);
            }
        };

        socket.onclose = () => {
            console.warn("[WebSocket] Disconnected. Reconnecting in 2 seconds...");
            streamStatusPill.className = "live-pill paused";
            streamStatusText.innerText = "RECONNECTING...";
            setTimeout(connectWebSocket, 2000);
        };
    }

    function updateStatsCounters(stats) {
        kpiTotal.innerText = (stats.total_processed || 0).toLocaleString();
        kpiNormal.innerText = (stats.normal_count || 0).toLocaleString();
        kpiWarning.innerText = (stats.warning_count || 0).toLocaleString();
        kpiCritical.innerText = (stats.critical_count || 0).toLocaleString();
        kpiArbitrage.innerText = (stats.arbitrage_alerts || 0).toLocaleString();
    }

    // ==========================================
    // 6. USER CONTROLS & DEMO TRIGGERS
    // ==========================================
    toggleBtn.addEventListener("click", async () => {
        isPaused = !isPaused;
        const action = isPaused ? "stop" : "start";

        await fetch("/api/producer/control", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ action: action })
        });

        if (isPaused) {
            streamStatusPill.className = "live-pill paused";
            streamStatusText.innerText = "STREAM PAUSED";
            toggleIcon.innerText = "▶";
            toggleText.innerText = "Resume";
        } else {
            streamStatusPill.className = "live-pill active";
            streamStatusText.innerText = "STREAM LIVE";
            toggleIcon.innerText = "⏸";
            toggleText.innerText = "Pause";
        }
    });

    speedSelect.addEventListener("change", async (e) => {
        const speed = parseFloat(e.target.value);
        currentSpeed = speed;
        streamHzLabel.innerText = `${speed.toFixed(1)} Hz`;
        kpiSpeed.innerText = `${speed.toFixed(1)} events/sec`;

        await fetch("/api/producer/control", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ action: "speed", speed: speed })
        });
    });

    const sourceSelect = document.getElementById("source-select");
    if (sourceSelect) {
        sourceSelect.addEventListener("change", async (e) => {
            const mode = e.target.value;
            await fetch("/api/producer/control", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ action: "source", source: mode })
            });
            console.log("[Producer] Switched streaming source to:", mode);
        });
    }

    commodityFilter.addEventListener("change", (e) => {
        selectedCommodity = e.target.value;
        // Clear chart for new series
        priceChart.data.labels = [];
        priceChart.data.datasets.forEach(ds => ds.data = []);
        priceChart.update();
    });

    btnSpikeTomato.addEventListener("click", () => {
        if (socket && socket.readyState === WebSocket.OPEN) {
            socket.send(JSON.stringify({
                action: "inject",
                type: "PRICE_SPIKE",
                commodity: "Tomato",
                market: "Kolar APMC"
            }));
            btnSpikeTomato.innerText = "Queued Spike! ⚡";
            setTimeout(() => { btnSpikeTomato.innerText = "⚡ Spike Tomato"; }, 1500);
        }
    });

    btnCrashOnion.addEventListener("click", () => {
        if (socket && socket.readyState === WebSocket.OPEN) {
            socket.send(JSON.stringify({
                action: "inject",
                type: "PRICE_CRASH",
                commodity: "Onion",
                market: "Lasalgaon"
            }));
            btnCrashOnion.innerText = "Queued Crash! 📉";
            setTimeout(() => { btnCrashOnion.innerText = "📉 Crash Onion"; }, 1500);
        }
    });

    // Start WebSocket
    connectWebSocket();
});
