/* Marketplace Control Tower Client — 100% Offline-Safe, Multi-Layer H3 Geospatial UI */

const state = {
  city: "nyc",
  service: "",
  layer: "pricing",
  geoData: null,
  selectedCellId: null,
};

const LAYER_CONFIG = {
  pricing: {
    label: "Dynamic Multiplier",
    getValue: (p) => Number(p.pricing_multiplier || 1.0),
    format: (v) => `${v.toFixed(2)}x`,
    color: (v) => {
      if (v >= 2.4) return "#ef4444";
      if (v >= 1.8) return "#f97316";
      if (v >= 1.35) return "#f59e0b";
      if (v > 1.05) return "#eab308";
      return "#10b981";
    },
    legend: [
      { color: "#10b981", label: "1.00x Baseline" },
      { color: "#eab308", label: "1.06–1.34x Mild" },
      { color: "#f59e0b", label: "1.35–1.79x Moderate" },
      { color: "#f97316", label: "1.80–2.39x High" },
      { color: "#ef4444", label: "2.40–3.00x Cap Surge" },
    ],
  },
  imbalance: {
    label: "Imbalance Ratio (D / Eff S)",
    getValue: (p) => Number(p.imbalance_ratio || 0),
    format: (v) => `${v.toFixed(2)}x`,
    color: (v) => {
      if (v >= 3.0) return "#dc2626";
      if (v >= 1.8) return "#f97316";
      if (v >= 1.15) return "#f59e0b";
      if (v >= 0.7) return "#38bdf8";
      return "#14b8a6";
    },
    legend: [
      { color: "#14b8a6", label: "<0.7 Surplus" },
      { color: "#38bdf8", label: "0.7–1.14 Balanced" },
      { color: "#f59e0b", label: "1.15–1.79 Deficit" },
      { color: "#f97316", label: "1.80–2.99 Shortage" },
      { color: "#dc2626", label: ">=3.0 Critical" },
    ],
  },
  demand: {
    label: "Demand Intensity",
    getValue: (p) => Number(p.demand_count || 0),
    format: (v) => `${v} reqs`,
    color: (v) => {
      if (v >= 20) return "#e11d48";
      if (v >= 10) return "#f43f5e";
      if (v >= 5) return "#fb7185";
      if (v >= 2) return "#fda4af";
      return "#334155";
    },
    legend: [
      { color: "#334155", label: "0–1 reqs" },
      { color: "#fda4af", label: "2–4 reqs" },
      { color: "#fb7185", label: "5–9 reqs" },
      { color: "#f43f5e", label: "10–19 reqs" },
      { color: "#e11d48", label: ">=20 Hotzone" },
    ],
  },
  supply: {
    label: "Available Driver Supply",
    getValue: (p) => Number(p.supply_count || 0),
    format: (v) => `${v} drivers`,
    color: (v) => {
      if (v >= 6) return "#059669";
      if (v >= 4) return "#10b981";
      if (v >= 2) return "#34d399";
      if (v >= 1) return "#6ee7b7";
      return "#1e293b";
    },
    legend: [
      { color: "#1e293b", label: "0 drivers" },
      { color: "#6ee7b7", label: "1 driver" },
      { color: "#34d399", label: "2–3 drivers" },
      { color: "#10b981", label: "4–5 drivers" },
      { color: "#059669", label: ">=6 Dense Supply" },
    ],
  },
};

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}`);
  return res.json();
}

function buildQuery(extra = {}) {
  const params = new URLSearchParams();
  if (state.city) params.set("city_id", state.city);
  if (state.service) params.set("service_type", state.service);
  Object.entries(extra).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") params.set(k, String(v));
  });
  const q = params.toString();
  return q ? `?${q}` : "";
}

function escapeHtml(str) {
  return String(str ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function renderSummary(summary, health) {
  const k = summary.kpis;
  document.getElementById("kpi-cells").textContent = k.active_h3_cells.toLocaleString();
  document.getElementById("kpi-surged").textContent = `${k.surged_cells_gt_1_2x} surged (>1.2x)`;
  document.getElementById("kpi-drivers").textContent = k.available_drivers.toLocaleString();
  document.getElementById("kpi-requests").textContent = `${k.total_requests.toLocaleString()} total requests`;
  document.getElementById("kpi-match").textContent = `${k.match_rate_pct.toFixed(1)}%`;
  document.getElementById("kpi-matched-cnt").textContent = `${k.matched_requests.toLocaleString()} matched`;
  document.getElementById("kpi-completion").textContent = `${k.completion_rate_pct.toFixed(1)}%`;
  document.getElementById("kpi-completed-cnt").textContent = `${k.completed_trips.toLocaleString()} completed trips`;
  document.getElementById("kpi-multiplier").textContent = `${k.avg_pricing_multiplier.toFixed(2)}x`;
  document.getElementById("kpi-max-mult").textContent = `Peak ${k.max_pricing_multiplier.toFixed(2)}x (Policy Cap 3.00x)`;
  document.getElementById("kpi-liquidity").textContent = `${k.avg_liquidity_score.toFixed(1)} / 100`;
  document.getElementById("kpi-lookup-ms").textContent = `Serving lookup ${summary.lookup_latency_ms} ms`;

  const healthy = health.status === "healthy";
  document.getElementById("health-status").textContent = healthy ? "HEALTHY" : "DEGRADED";
  document.getElementById("health-dot").className = `dot ${healthy ? "dot-live" : "dot-warn"}`;
  const ts = new Date(summary.freshness_utc);
  document.getElementById("freshness-ts").textContent = `Updated ${ts.toISOString().slice(11, 19)} UTC`;
}

function renderCellInspector(props) {
  const el = document.getElementById("cell-inspector-body");
  if (!props) {
    el.innerHTML = `<p class="muted">No H3 cell selected.</p>`;
    return;
  }
  state.selectedCellId = props.h3_cell;
  const reasons = Array.isArray(props.reason_codes) ? props.reason_codes : [];
  const chips = reasons
    .map((rc) => {
      const ok = rc.includes("BALANCED") || rc.includes("BASELINE");
      return `<span class="chip ${ok ? "ok" : ""}">${escapeHtml(rc)}</span>`;
    })
    .join("");

  el.innerHTML = `
    <div class="mono" style="font-size:12px;color:#93c5fd;margin-bottom:4px;">${escapeHtml(props.h3_cell)}</div>
    <div style="font-size:11.5px;color:#94a3b8;">City: <strong>${escapeHtml(props.city_id.toUpperCase())}</strong> · Policy: <span class="mono">${escapeHtml(props.policy_version)}</span> · Variant: <span class="mono">${escapeHtml(props.experiment_variant)}</span></div>
    <div class="inspector-kv">
      <div class="ikv-item"><div class="ikv-label">Applied Multiplier</div><div class="ikv-val accent-amber">${Number(props.pricing_multiplier).toFixed(2)}x</div></div>
      <div class="ikv-item"><div class="ikv-label">Raw Unsmoothed</div><div class="ikv-val">${Number(props.raw_multiplier).toFixed(2)}x</div></div>
      <div class="ikv-item"><div class="ikv-label">Demand / Supply</div><div class="ikv-val">${props.demand_count} / ${props.supply_count}</div></div>
      <div class="ikv-item"><div class="ikv-label">1-Ring Neighbor Supply</div><div class="ikv-val accent-teal">${props.neighbor_supply_count} drivers</div></div>
      <div class="ikv-item"><div class="ikv-label">Imbalance Ratio</div><div class="ikv-val">${Number(props.imbalance_ratio).toFixed(2)}x</div></div>
      <div class="ikv-item"><div class="ikv-label">Est. Wait Time</div><div class="ikv-val">${Math.round(props.estimated_wait_seconds)} s</div></div>
    </div>
    <div class="reason-chips">${chips || '<span class="chip ok">BASELINE_EQUILIBRIUM</span>'}</div>
  `;
}

function renderMap(geoData) {
  state.geoData = geoData;
  const svg = document.getElementById("h3-map-svg");
  const tooltip = document.getElementById("map-tooltip");
  const layerCfg = LAYER_CONFIG[state.layer];

  // Update legend
  document.getElementById("map-legend").innerHTML =
    `<strong style="color:#fff">${layerCfg.label}:</strong> ` +
    layerCfg.legend
      .map((l) => `<span class="legend-swatch"><span class="legend-box" style="background:${l.color}"></span>${l.label}</span>`)
      .join("");

  // If "All Cities" is selected, display NYC on the focused map with a clear badge while KPIs aggregate all 3 cities
  const mapCity = state.city || "nyc";
  const features = geoData.features.filter((f) => f.properties.city_id === mapCity);
  const cityMeta = geoData.cities[mapCity];

  if (!features.length || !cityMeta) {
    svg.innerHTML = `<text x="460" y="250" text-anchor="middle" fill="#94a3b8" font-size="15">No H3 cells available for selection</text>`;
    return;
  }

  // Compute tight bounding box around actual H3 vertices so hexagons look crisp and spacious
  let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
  for (const f of features) {
    for (const [lon, lat] of f.geometry.coordinates[0]) {
      if (lon < minLon) minLon = lon;
      if (lon > maxLon) maxLon = lon;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    }
  }
  const padLon = (maxLon - minLon) * 0.08 || 0.02;
  const padLat = (maxLat - minLat) * 0.09 || 0.02;
  minLon -= padLon; maxLon += padLon;
  minLat -= padLat; maxLat += padLat;

  const W = 920, H = 500;
  // Cosine latitude correction so hexagons preserve true geometric proportions!
  const midLatRad = (((minLat + maxLat) / 2) * Math.PI) / 180;
  const cosLat = Math.cos(midLatRad);
  const spanX = (maxLon - minLon) * cosLat;
  const spanY = maxLat - minLat;
  const scale = Math.min((W - 60) / spanX, (H - 76) / spanY);
  const offsetX = (W - spanX * scale) / 2;
  const offsetY = (H - spanY * scale) / 2 - 10;

  const proj = (lon, lat) => [
    offsetX + (lon - minLon) * cosLat * scale,
    offsetY + (maxLat - lat) * scale,
  ];

  // Background gridlines + city title watermark
  let html = `<defs>
    <radialGradient id="mapGlow" cx="50%" cy="50%" r="60%">
      <stop offset="0%" stop-color="#13233b" />
      <stop offset="100%" stop-color="#080e17" />
    </radialGradient>
  </defs>
  <rect x="0" y="0" width="${W}" height="${H}" fill="url(#mapGlow)" />`;

  for (let gx = 80; gx < W; gx += 120) {
    html += `<line x1="${gx}" y1="0" x2="${gx}" y2="${H}" stroke="#162438" stroke-width="1" />`;
  }
  for (let gy = 60; gy < H; gy += 90) {
    html += `<line x1="0" y1="${gy}" x2="${W}" y2="${gy}" stroke="#162438" stroke-width="1" />`;
  }

  html += `<text x="18" y="28" fill="#cbd5e1" font-size="14" font-weight="700">${escapeHtml(cityMeta.display_name.toUpperCase())} · H3 RES-7 LIVE HEX GRID (${features.length} CELLS)</text>`;
  if (!state.city) {
    html += `<text x="18" y="45" fill="#64748b" font-size="11">Showing NYC focused viewport (global KPIs &amp; tables below aggregate NYC + SF + London)</text>`;
  }

  // Draw hexagons
  const byCell = new Map();
  for (const f of features) {
    const p = f.properties;
    byCell.set(p.h3_cell, p);
    const pts = f.geometry.coordinates[0]
      .map(([lon, lat]) => proj(lon, lat).map((n) => n.toFixed(1)).join(","))
      .join(" ");
    const val = layerCfg.getValue(p);
    const fill = layerCfg.color(val);
    const isSel = p.h3_cell === state.selectedCellId ? " selected" : "";
    html += `<polygon class="hex-poly${isSel}" data-cell="${p.h3_cell}" points="${pts}" fill="${fill}" fill-opacity="0.82" />`;
  }

  // Draw hot-zone landmark beacons (Airports, Stadiums, Downtown, Nightlife) with staggered offsets
  const labelOffsets = { downtown: -22, stadium: 6, nightlife: 22, airport: 0 };
  for (const hz of cityMeta.hot_zones || []) {
    const [hx, hy] = proj(hz.lon, hz.lat);
    const dy = labelOffsets[hz.zone_type] ?? 0;
    const ly = Math.max(18, Math.min(H - 18, hy + dy));
    if (hx >= 20 && hx <= W - 20 && hy >= 20 && hy <= H - 20) {
      const boxW = Math.min(195, hz.name.length * 6.5 + 16);
      html += `
        <g pointer-events="none">
          <line x1="${hx.toFixed(1)}" y1="${hy.toFixed(1)}" x2="${(hx + 10).toFixed(1)}" y2="${ly.toFixed(1)}" stroke="#38bdf8" stroke-width="1" stroke-opacity="0.7" />
          <circle cx="${hx.toFixed(1)}" cy="${hy.toFixed(1)}" r="4.5" fill="#0f172a" stroke="#38bdf8" stroke-width="2" />
          <rect x="${(hx + 10).toFixed(1)}" y="${(ly - 10).toFixed(1)}" width="${boxW}" height="19" rx="4" fill="rgba(11,17,27,0.92)" stroke="#294060" />
          <text x="${(hx + 16).toFixed(1)}" y="${(ly + 3).toFixed(1)}" fill="#e2e8f0" font-size="10.5" font-weight="600">${escapeHtml(hz.name)}</text>
        </g>`;
    }
  }

  svg.innerHTML = html;

  // Bind hover & click interactions
  svg.querySelectorAll(".hex-poly").forEach((poly) => {
    poly.addEventListener("mouseenter", (ev) => {
      const p = byCell.get(poly.dataset.cell);
      if (!p) return;
      tooltip.innerHTML = `
        <div style="color:#93c5fd;font-weight:700">${p.h3_cell}</div>
        <div>${layerCfg.label}: <strong>${layerCfg.format(layerCfg.getValue(p))}</strong></div>
        <div>Demand: ${p.demand_count} · Supply: ${p.supply_count} (Nb: ${p.neighbor_supply_count})</div>
        <div>Multiplier: ${Number(p.pricing_multiplier).toFixed(2)}x · Policy: ${p.policy_version}</div>
      `;
      tooltip.classList.remove("hidden");
    });
    poly.addEventListener("mousemove", (ev) => {
      const rect = svg.parentElement.getBoundingClientRect();
      tooltip.style.left = `${Math.min(rect.width - 255, ev.clientX - rect.left + 14)}px`;
      tooltip.style.top = `${Math.max(10, ev.clientY - rect.top - 10)}px`;
    });
    poly.addEventListener("mouseleave", () => tooltip.classList.add("hidden"));
    poly.addEventListener("click", () => {
      const p = byCell.get(poly.dataset.cell);
      if (!p) return;
      svg.querySelectorAll(".hex-poly.selected").forEach((n) => n.classList.remove("selected"));
      poly.classList.add("selected");
      renderCellInspector(p);
    });
  });

  // Populate top hotspots list and auto-select highest-surge cell
  const sorted = [...features]
    .map((f) => f.properties)
    .sort((a, b) => layerCfg.getValue(b) - layerCfg.getValue(a));

  if (!state.selectedCellId || !byCell.has(state.selectedCellId)) {
    renderCellInspector(sorted[0]);
    const firstPoly = svg.querySelector(`.hex-poly[data-cell="${sorted[0].h3_cell}"]`);
    if (firstPoly) firstPoly.classList.add("selected");
  }

  const hotEl = document.getElementById("hotspots-list");
  hotEl.innerHTML = sorted
    .slice(0, 8)
    .map(
      (p) => `
      <div class="hotspot-row" data-cell="${p.h3_cell}">
        <div>
          <div class="mono" style="color:#93c5fd">${p.h3_cell}</div>
          <div class="muted" style="font-size:10.5px">D:${p.demand_count} / S:${p.supply_count} (Nb:${p.neighbor_supply_count})</div>
        </div>
        <div class="mono" style="font-weight:700;color:#fbbf24">${layerCfg.format(layerCfg.getValue(p))}</div>
      </div>`
    )
    .join("");

  hotEl.querySelectorAll(".hotspot-row").forEach((row) => {
    row.addEventListener("click", () => {
      const p = byCell.get(row.dataset.cell);
      if (!p) return;
      renderCellInspector(p);
      svg.querySelectorAll(".hex-poly.selected").forEach((n) => n.classList.remove("selected"));
      const poly = svg.querySelector(`.hex-poly[data-cell="${p.h3_cell}"]`);
      if (poly) poly.classList.add("selected");
    });
  });
}

function renderFunnel(matching) {
  const f = matching.aggregate_funnel;
  const tot = Math.max(1, f.requests_total);
  const steps = [
    { label: "1. Ride Requested", val: f.requests_total, pct: 100, color: "#3b82f6" },
    { label: "2. Dispatch Offered", val: f.requests_offered, pct: f.offer_rate_pct, color: "#06b6d4" },
    { label: "3. Driver Matched", val: f.requests_matched, pct: f.match_rate_pct, color: "#14b8a6" },
    { label: "4. Trip Completed", val: f.trips_completed, pct: f.completion_rate_pct, color: "#22c55e" },
    { label: "Rider / Driver Cancelled", val: f.rider_cancellations + f.driver_cancellations, pct: ((f.rider_cancellations + f.driver_cancellations) / tot) * 100, color: "#f59e0b" },
    { label: "Unfulfilled / Expired", val: f.unfulfilled_requests, pct: (f.unfulfilled_requests / tot) * 100, color: "#ef4444" },
  ];

  document.getElementById("funnel-bars").innerHTML = steps
    .map(
      (s) => `
      <div class="funnel-row">
        <div class="funnel-meta">
          <span>${s.label}</span>
          <span class="mono"><strong>${s.val.toLocaleString()}</strong> (${s.pct.toFixed(1)}%)</span>
        </div>
        <div class="bar-track">
          <div class="bar-fill" style="width:${Math.max(2, Math.min(100, s.pct))}%;background:${s.color}"></div>
        </div>
      </div>`
    )
    .join("");

  document.getElementById("funnel-latencies").innerHTML = `
    <div class="mini-box"><div class="k">Avg Req → Offer</div><div class="v">${f.avg_request_to_offer_ms} ms</div></div>
    <div class="mini-box"><div class="k">Avg Req → Match</div><div class="v accent-teal">${f.avg_request_to_match_ms} ms</div></div>
    <div class="mini-box"><div class="k">Avg Pickup ETA</div><div class="v">${Math.round(f.avg_estimated_pickup_s)} s</div></div>
  `;

  // Populate Trip Lifecycle table
  const tbody = document.querySelector("#trips-table tbody");
  tbody.innerHTML = matching.by_city_service
    .map(
      (r) => `
      <tr>
        <td>${escapeHtml(r.city_id.toUpperCase())}</td>
        <td>${escapeHtml(r.service_type)}</td>
        <td>${r.requests_total}</td>
        <td style="color:#2dd4bf">${Number(r.match_success_rate_pct).toFixed(1)}%</td>
        <td>${Number(r.driver_acceptance_rate_pct).toFixed(1)}%</td>
        <td style="color:#34d399">${r.trips_completed}</td>
        <td>${r.rider_cancellations} / ${r.driver_cancellations}</td>
        <td>${Math.round(r.avg_estimated_pickup_s)} s</td>
      </tr>`
    )
    .join("");
}

function renderUtilizationAndDistricts(supply, regions) {
  const dist = supply.driver_state_distribution || {};
  const ordered = ["AVAILABLE", "ON_TRIP", "RESERVED", "OFFLINE"];
  const totalDrv = Object.values(dist).reduce((a, b) => a + b, 0) || 1;
  document.getElementById("driver-state-bars").innerHTML = ordered
    .map((st) => {
      const cnt = dist[st] || 0;
      const pct = ((cnt / totalDrv) * 100).toFixed(1);
      return `<div class="mini-box"><div class="k">${st}</div><div class="v">${cnt} <span class="muted" style="font-size:11px">(${pct}%)</span></div></div>`;
    })
    .join("");

  const tbody = document.querySelector("#districts-table tbody");
  tbody.innerHTML = (regions.h3_res5_districts || [])
    .slice(0, 7)
    .map(
      (d) => `
      <tr>
        <td>${escapeHtml(d.city_id.toUpperCase())}</td>
        <td style="color:#93c5fd">${escapeHtml(d.district_h3_res5)}</td>
        <td>${d.active_res7_cells}</td>
        <td>${d.district_demand}</td>
        <td>${d.district_supply}</td>
        <td style="color:#fbbf24">${Number(d.avg_multiplier).toFixed(2)}x / ${Number(d.max_multiplier).toFixed(2)}x</td>
      </tr>`
    )
    .join("");

  const ptbody = document.querySelector("#policy-table tbody");
  ptbody.innerHTML = (regions.policy_scd2_history || [])
    .map(
      (p) => `
      <tr>
        <td>${escapeHtml(p.region_id)}</td>
        <td>${escapeHtml(p.policy_version)}</td>
        <td>${Number(p.min_multiplier).toFixed(1)}–${Number(p.max_multiplier).toFixed(2)}x</td>
        <td>${Number(p.smoothing_alpha).toFixed(2)}</td>
        <td>${Number(p.neighbor_supply_weight).toFixed(2)}</td>
        <td><span class="pill ${p.is_current ? "pill-pass" : "pill-info"}">${p.is_current ? "CURRENT" : "HIST"}</span></td>
      </tr>`
    )
    .join("");
}

function renderExperiments(exp) {
  const cardsEl = document.getElementById("experiment-cards");
  cardsEl.innerHTML = (exp.variants || [])
    .map(
      (v) => `
      <div class="exp-card ${v.experiment_variant === "treatment" ? "treatment" : ""}">
        <div class="exp-title">
          <span>${escapeHtml(v.experiment_variant.toUpperCase())}</span>
          <span class="mono muted">${escapeHtml(v.policy_version)}</span>
        </div>
        <div class="exp-row"><span class="muted">H3 Cells Exposed</span><span class="mono">${v.cells_exposed}</span></div>
        <div class="exp-row"><span class="muted">Requests Exposed</span><span class="mono">${v.requests_exposed}</span></div>
        <div class="exp-row"><span class="muted">Match Rate</span><span class="mono accent-teal">${Number(v.match_rate_pct).toFixed(2)}%</span></div>
        <div class="exp-row"><span class="muted">Completion Rate</span><span class="mono accent-green">${Number(v.completion_rate_pct).toFixed(2)}%</span></div>
        <div class="exp-row"><span class="muted">Avg Multiplier</span><span class="mono accent-amber">${Number(v.avg_pricing_multiplier).toFixed(3)}x</span></div>
      </div>`
    )
    .join("");

  const lift = exp.lift_summary || {};
  const srmBadge = document.getElementById("srm-badge");
  const srmOk = lift.srm_status !== "FAIL_SRM";
  srmBadge.textContent = `SRM ${lift.srm_status || "PASS"} (χ²=${lift.srm_chi2_stat ?? 0})`;
  srmBadge.className = `pill ${srmOk ? "pill-pass" : "pill-fail"}`;

  const sign = (n) => (n >= 0 ? `+${n}` : `${n}`);
  document.getElementById("experiment-lift").innerHTML = `
    <span>Match Lift: <strong>${sign(lift.match_rate_delta_pp ?? 0)} pp</strong></span>
    <span>Completion Lift: <strong>${sign(lift.completion_rate_delta_pp ?? 0)} pp</strong></span>
    <span>Avg Mult Δ: <strong>${sign(lift.avg_multiplier_delta ?? 0)}x</strong></span>
    <span>Match Latency Δ: <strong>${sign(lift.match_latency_delta_ms ?? 0)} ms</strong></span>
  `;
}

function renderPlatformAndQuality(platform, dq) {
  const ck = platform.stream_checkpoint || {};
  const cnt = ck.counters || {};
  document.getElementById("platform-summary-grid").innerHTML = `
    <div class="mini-box"><div class="k">Serving Store (${platform.serving_store.mode})</div><div class="v accent-teal">${platform.serving_store.keys} keys</div></div>
    <div class="mini-box"><div class="k">Checkpoint / H3 Transfers</div><div class="v">#${ck.checkpoint_id ?? 0} / ${(cnt.cell_transfers_processed ?? 0).toLocaleString()}</div></div>
    <div class="mini-box"><div class="k">Stale / Dup Suppressed</div><div class="v">${(cnt.stale_out_of_order_ignored ?? 0).toLocaleString()} / ${(cnt.duplicates_suppressed ?? 0).toLocaleString()}</div></div>
    <div class="mini-box"><div class="k">Iceberg V2 Tables</div><div class="v">${platform.iceberg_tables.length} tables</div></div>
  `;

  document.getElementById("footer-lineage").textContent =
    `OpenLineage v2 Events Emitted: ${platform.openlineage_events_emitted} · Profile: ${platform.execution_profile}`;

  document.querySelector("#topics-table tbody").innerHTML = platform.topics
    .filter((t) => t.records_retained > 0 || t.topic.includes("dead-letter"))
    .slice(0, 9)
    .map(
      (t) => `
      <tr>
        <td>${escapeHtml(t.topic.replace("marketplace.", ""))}</td>
        <td>${t.partitions}</td>
        <td>${t.records_retained.toLocaleString()}</td>
        <td style="color:${t.consumer_lag > 0 ? "#f59e0b" : "#34d399"}">${t.consumer_lag}</td>
      </tr>`
    )
    .join("");

  // DQ Suite
  const dqBadge = document.getElementById("dq-overall-badge");
  dqBadge.textContent = `${dq.summary.passed}/${dq.summary.total} ${dq.overall_status}`;
  dqBadge.className = `pill ${dq.overall_status === "PASS" ? "pill-pass" : "pill-fail"}`;

  document.getElementById("dq-list").innerHTML = (dq.checks || [])
    .map(
      (c) => `
      <div class="dq-item">
        <div>
          <div class="dq-name">${escapeHtml(c.check_name)}</div>
          <div class="dq-meta">Layer: ${escapeHtml(c.layer)} · Observed: ${c.observed_value} (Threshold: ${c.threshold})</div>
        </div>
        <span class="pill ${c.status === "PASS" ? "pill-pass" : "pill-fail"}">${c.status}</span>
      </div>`
    )
    .join("");

  // Incident & Reconciliation Monitor
  const recon = dq.reconciliation_status || platform.reconciliation || {};
  const reconBadge = document.getElementById("recon-badge");
  reconBadge.textContent = recon.status || "CONSISTENT";
  reconBadge.className = `pill ${recon.status === "CONSISTENT" || recon.status === "REPAIRED" ? "pill-pass" : "pill-warn"}`;

  const dlqCount = (dq.dlq_summary || []).reduce((acc, r) => acc + Number(r.cnt || 0), 0);
  const chaos = dq.chaos_drill_summary;
  const chaosHtml = chaos
    ? `<div class="inc-card">
         <div class="inc-title"><span>Chaos &amp; Fault-Injection Drill</span><span class="pill pill-pass">${chaos.passed}/${chaos.total} PASS</span></div>
         <div class="muted">Verified broker restart, stream checkpoint parity, serving KV outage → safe 1.0x fallback → full rebuild from durable log, out-of-order rejection, and drift repair.</div>
       </div>`
    : `<div class="inc-card">
         <div class="inc-title"><span>Chaos &amp; Fault-Injection Suite</span><span class="pill pill-info">READY</span></div>
         <div class="muted">Run <code>pytest tests/chaos/</code> to execute and record automated fault-injection drills.</div>
       </div>`;

  document.getElementById("incident-monitor-body").innerHTML = `
    <div class="inc-card">
      <div class="inc-title"><span>Serving Store ↔ Iceberg Reconciliation</span><span class="pill pill-pass">${escapeHtml(recon.status || "CONSISTENT")}</span></div>
      <div class="muted">Drifted cells: <strong class="mono">${recon.drifted_cells_detected ?? 0}</strong> · Missing: <strong class="mono">${recon.missing_cells_detected ?? 0}</strong> · Repaired: <strong class="mono">${recon.cells_repaired ?? 0}</strong></div>
    </div>
    <div class="inc-card">
      <div class="inc-title"><span>Pricing Safety Guardrails</span><span class="pill pill-pass">ARMED</span></div>
      <div class="muted">Stale/missing signal fallback → <strong>1.00x baseline</strong> · Policy floor <strong>1.00x</strong> · Ceiling <strong>3.00x</strong> · Monotonic version-aware KV writes enforced.</div>
    </div>
    <div class="inc-card">
      <div class="inc-title"><span>Dead-Letter Quarantine (DLQ)</span><span class="pill ${dlqCount > 0 ? "pill-warn" : "pill-pass"}">${dlqCount} QUARANTINED</span></div>
      <div class="muted">Malformed / poison Avro payloads isolated to <code>marketplace.dead-letter</code> and <code>bronze.dead_letter</code> without stalling streaming consumers.</div>
    </div>
    ${chaosHtml}
  `;
}

async function refreshAll() {
  const qCity = state.city ? `?city_id=${encodeURIComponent(state.city)}` : "";
  const qBoth = buildQuery();
  try {
    const [health, summary, geo, matching, supply, regions, experiments, platform, dq] = await Promise.all([
      fetchJson("/health"),
      fetchJson(`/marketplace/summary${qCity}`),
      fetchJson(`/api/map/h3-cells.geojson`),
      fetchJson(`/marketplace/matching${qBoth}`),
      fetchJson(`/marketplace/supply${qCity}`),
      fetchJson(`/marketplace/regions${qCity}`),
      fetchJson("/marketplace/experiments"),
      fetchJson("/platform/metrics"),
      fetchJson("/platform/data-quality"),
    ]);

    renderSummary(summary, health);
    renderMap(geo);
    renderFunnel(matching);
    renderUtilizationAndDistricts(supply, regions);
    renderExperiments(experiments);
    renderPlatformAndQuality(platform, dq);
  } catch (err) {
    console.error("Control Tower refresh error:", err);
  }
}

document.getElementById("city-select").addEventListener("change", (e) => {
  state.city = e.target.value;
  state.selectedCellId = null;
  refreshAll();
});

document.getElementById("service-select").addEventListener("change", (e) => {
  state.service = e.target.value;
  refreshAll();
});

document.querySelectorAll(".tab-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    state.layer = btn.dataset.layer;
    if (state.geoData) renderMap(state.geoData);
  });
});

document.getElementById("refresh-btn").addEventListener("click", refreshAll);

refreshAll();
