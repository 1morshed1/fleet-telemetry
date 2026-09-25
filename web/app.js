// Fleet Telemetry dashboard front-end.
// - Draws geofence polygons (fetched once as GeoJSON).
// - Opens a WebSocket to /api/v1/live and updates vehicle markers from the
//   coalesced position batches the server broadcasts.
// - Polls /api/v1/alerts for the recent-alerts feed.

const CENTER = [52.52, 13.405];
const map = L.map('map', { zoomControl: true }).setView(CENTER, 11);

L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  attribution: '&copy; OpenStreetMap &copy; CARTO',
  maxZoom: 19,
}).addTo(map);

// vehicleId -> Leaflet circle marker
const markers = new Map();
let violationCount = 0;

const colorFor = (status) => (status === 'violation' ? '#ff5c5c' : '#4da3ff');

function upsertVehicle(p) {
  const existing = markers.get(p.vehicle_id);
  if (existing) {
    existing.setLatLng([p.lat, p.lng]);
    existing.setStyle({ color: colorFor(p.status), fillColor: colorFor(p.status) });
    existing._status = p.status;
    existing.setTooltipContent(tooltip(p));
  } else {
    const m = L.circleMarker([p.lat, p.lng], {
      radius: 5, weight: 1, color: colorFor(p.status),
      fillColor: colorFor(p.status), fillOpacity: 0.85,
    }).addTo(map);
    m.bindTooltip(tooltip(p), { direction: 'top' });
    m._status = p.status;
    markers.set(p.vehicle_id, m);
  }
}

function tooltip(p) {
  return `<b>${p.vehicle_id}</b><br>${p.speed_kmh.toFixed(0)} km/h · ${p.status}`;
}

function refreshStats() {
  document.getElementById('stat-vehicles').textContent = markers.size;
  let viol = 0;
  for (const m of markers.values()) if (m._status === 'violation') viol++;
  document.getElementById('stat-violations').textContent = viol;
}

// --- geofences ---
fetch('/api/v1/geofences')
  .then((r) => r.json())
  .then((fc) => {
    L.geoJSON(fc, {
      style: (f) => {
        const t = f.properties.zone_type;
        const color = t === 'restricted' ? '#ff5c5c' : t === 'warehouse' ? '#ffb020' : '#39d98a';
        return { color, weight: 2, fillOpacity: 0.08 };
      },
      onEachFeature: (f, layer) => {
        const pr = f.properties;
        layer.bindPopup(`<b>${pr.name}</b><br>${pr.zone_type}` +
          (pr.speed_limit ? `<br>limit ${pr.speed_limit} km/h` : ''));
      },
    }).addTo(map);
  })
  .catch((e) => console.warn('geofences load failed', e));

// --- live positions via WebSocket ---
function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/api/v1/live`);
  const status = document.getElementById('status');

  ws.onopen = () => { status.className = 'live'; status.textContent = 'live'; };
  ws.onclose = () => {
    status.className = 'down'; status.textContent = 'disconnected — retrying…';
    setTimeout(connect, 2000);
  };
  ws.onerror = () => ws.close();
  ws.onmessage = (ev) => {
    let batch;
    try { batch = JSON.parse(ev.data); } catch { return; }
    if (!Array.isArray(batch)) return;
    for (const p of batch) upsertVehicle(p);
    refreshStats();
  };
}
connect();

// --- alerts feed ---
function loadAlerts() {
  fetch('/api/v1/alerts?type=all')
    .then((r) => r.json())
    .then((alerts) => {
      if (!Array.isArray(alerts)) return;
      alerts.sort((a, b) => new Date(b.time) - new Date(a.time));
      document.getElementById('stat-alerts').textContent = alerts.length;
      const box = document.getElementById('alerts');
      box.innerHTML = '';
      for (const a of alerts.slice(0, 50)) {
        const el = document.createElement('div');
        el.className = `alert ${a.type}`;
        el.innerHTML = `<div class="who">${a.vehicle_id}</div>` +
          `<div>${a.detail}</div>` +
          `<div class="when">${new Date(a.time).toLocaleTimeString()}</div>`;
        box.appendChild(el);
      }
    })
    .catch((e) => console.warn('alerts load failed', e));
}
loadAlerts();
setInterval(loadAlerts, 5000);
