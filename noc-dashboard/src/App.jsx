import { useEffect, useMemo, useState } from "react";
import { GeoJSON, MapContainer, TileLayer } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import API_BASE_URL from "./config";
import "./App.css";

const number = (value) => Number(value || 0).toLocaleString(undefined, { maximumFractionDigits: 0 });
const date = (value) => value ? new Date(value).toLocaleString([], { dateStyle: "medium", timeStyle: "short" }) : "-";
const DEFAULT_LIMIT = 10;
const DEFAULT_SEVERITY = "ALL";

function toDateTimeLocal(value) {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "";
  const offset = parsed.getTimezoneOffset();
  return new Date(parsed.getTime() - offset * 60000).toISOString().slice(0, 16);
}

async function request(path, options) {
  const response = await fetch(`${API_BASE_URL}${path}`, options);
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `Request failed (${response.status})`);
  }
  return response.json();
}

function StatusPill({ value = "NORMAL" }) {
  const normalized = String(value).toUpperCase();
  const label = normalized.includes("HIGH") ? "HIGH" : normalized.includes("ATTENTION") || normalized.includes("MEDIUM") || normalized.includes("ALERT") ? "ATTENTION" : "NORMAL";
  return <span className={`status-pill ${label.toLowerCase()}`}>{label}</span>;
}

function App() {
  const [page, setPage] = useState("overview");
  const [gridId, setGridId] = useState("");
  const [gridData, setGridData] = useState(null);
  const [summary, setSummary] = useState(null);
  const [hotspots, setHotspots] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [geojson, setGeojson] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [gridError, setGridError] = useState("");
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [severity, setSeverity] = useState(DEFAULT_SEVERITY);
  const [prediction, setPrediction] = useState(null);
  const [predicting, setPredicting] = useState(false);

  useEffect(() => {
    const query = new URLSearchParams({ limit: String(limit), severity });
    Promise.allSettled([
      request("/network/summary"),
      request(`/network/hotspots?limit=${limit}`).catch(() => request(`/network/hotspot?limit=${limit}`)),
      request(`/network/alerts?${query}`),
    ]).then(([summaryResult, hotspotResult, alertResult]) => {
      if (summaryResult.status === "fulfilled") {
        setSummary(summaryResult.value);
        setError("");
      } else {
        setError(summaryResult.reason?.message || "The summary API is unavailable.");
      }
      setHotspots(hotspotResult.status === "fulfilled" ? hotspotResult.value.results || [] : []);
      setAlerts(alertResult.status === "fulfilled" ? alertResult.value.results || [] : []);
    })
      .finally(() => setLoading(false));
  }, [limit, severity]);

  useEffect(() => {
    fetch("/reference/milano-grid.geojson")
      .then((response) => response.ok ? response.json() : null)
      .then(setGeojson)
      .catch(() => setGeojson(null));
  }, []);

  const searchGrid = async () => {
    if (!gridId) {
      setGridError("Enter a grid ID to load activity.");
      return;
    }

    setLoading(true);
    setGridError("");
    setGridData(null);

    try {
      setGridData(await request(`/network/grid/${gridId}`));

    } catch (err) {
      setGridError(`Grid ${gridId} could not be found. ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const rankedRows = useMemo(() => [...hotspots, ...alerts].sort((a, b) => Number(b.total_activity || 0) - Number(a.total_activity || 0)).slice(0, limit), [hotspots, alerts, limit]);

  const predictRisk = async (event) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const selectedGridId = form.get("grid_id");
    const selectedTimestamp = form.get("timestamp");
    setPredicting(true);
    try {
      const featureTimestamp = new Date(selectedTimestamp);
      if (!selectedGridId || Number(selectedGridId) < 1 || Number.isNaN(featureTimestamp.getTime())) {
        throw new Error("Enter a valid grid ID and timestamp.");
      }

      const features = await request(
        `/network/grid/${selectedGridId}/features?timestamp=${encodeURIComponent(featureTimestamp.toISOString())}`,
      );
      const result = await request("/network/predict-risk", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...features,
          feature_timestamp: features.feature_timestamp,
        }),
      });
      setPrediction(result);
    } catch (requestError) { setPrediction({ error: requestError.message }); }
    finally { setPredicting(false); }
  };

  const openGrid = (id) => { setGridId(String(id)); setPage("explorer"); };

  return (
    <div className="app-shell">
      <aside className="sidebar"><div className="brand"><span className="brand-mark">N</span><div><strong>NOC / MILANO</strong><small>Network operations center</small></div></div><nav>{[["overview", "Overview"], ["explorer", "Grid explorer"], ["watchlist", "Hotspots & alerts"], ["risk", "Risk model"]].map(([id, label]) => <button className={page === id ? "active" : ""} onClick={() => setPage(id)} key={id}>{label}</button>)}</nav><div className="sidebar-footer"><span className="live-dot" />Historical telemetry<br /><small>Read-only operations view</small></div></aside>
      <main className="main-content"><header className="topbar"><div><span className="eyebrow">MILANO TELECOM ANALYTICS</span><h1>{page === "overview" ? "Network overview" : page === "explorer" ? "Grid explorer" : page === "watchlist" ? "Hotspots & alerts" : "Risk model"}</h1></div><div className="timestamp"><span>REPORTING TIMESTAMP</span><strong>{summary ? date(summary.as_of) : "Awaiting API"}</strong></div></header>
        {error && <div className="error-banner"><strong>API unavailable</strong><span>{error}</span></div>}
        {loading && !summary ? <div className="loading-state">Loading network telemetry...</div> : <>
          {page === "overview" && <section className="page-stack"><div className="section-heading"><div><span className="eyebrow">LIVE SNAPSHOT</span><h2>Network at a glance</h2></div><span className="muted">Source: analytics warehouse</span></div><div className="metrics">{[["Total activity", summary?.total_activity, "events"], ["Active grids", summary?.active_grids, "grids reporting"], ["Peak hour", summary?.peak_hour == null ? "-" : `${String(summary.peak_hour).padStart(2, "0")}:00`, "local time"], ["Top grid", summary?.top_grid, "highest volume"]].map(([label, value, note]) => <article className="metric-card" key={label}><span>{label}</span><strong>{typeof value === "number" ? number(value) : value}</strong><small>{note}</small></article>)}</div><div className="content-grid"><section className="panel"><div className="panel-heading"><div><span className="eyebrow">PRIORITY QUEUE</span><h2>Highest activity grids</h2></div><button className="link-button" onClick={() => setPage("watchlist")}>View all</button></div><RankedTable rows={hotspots.slice(0, 5)} onSelect={openGrid} /></section><section className="panel"><div className="panel-heading"><div><span className="eyebrow">SYSTEM NOTE</span><h2>Operational status</h2></div></div><div className="status-card"><span className="live-dot" /><div><strong>Data pipeline connected</strong><p>Summary, grid activity, alerts and hotspot feeds are available for this reporting window.</p></div></div><div className="legend"><span><i className="normal" />Normal</span><span><i className="attention" />Attention</span><span><i className="high" />High</span></div></section></div><MapPanel geojson={geojson} rows={rankedRows} onSelect={openGrid} /></section>}
          {page === "explorer" && <section className="page-stack"><section className="panel"><div className="section-heading"><div><span className="eyebrow">24-HOUR WINDOW</span><h2>Inspect activity by grid</h2></div></div><div className="search-box"><input type="number" min="1" placeholder="Grid ID" value={gridId} onChange={(event) => setGridId(event.target.value)} onKeyDown={(event) => event.key === "Enter" && searchGrid()} /><button onClick={searchGrid}>Search grid</button></div>{gridError && <div className="inline-error">{gridError}</div>}{loading && <p className="muted">Loading grid activity...</p>}{gridData && !loading && <><div className="grid-result-heading"><div><h2>Grid {gridData.grid_id}</h2><span className="muted">Reporting time: {date(gridData.as_of)}</span></div><button className="secondary-button" onClick={() => setPage("risk")}>Assess risk</button></div><ActivityTable rows={gridData.data || []} /></>}</section></section>}
          {page === "watchlist" && <section className="page-stack"><section className="panel"><div className="toolbar"><div><span className="eyebrow">MONITORING</span><h2>Ranked watchlist</h2></div><div className="filters"><select value={limit} onChange={(event) => setLimit(Number(event.target.value))}><option value="10">10 rows</option><option value="15">15 rows</option><option value="25">25 rows</option></select><select value={severity} onChange={(event) => setSeverity(event.target.value)}><option value="ALL">All severity</option><option value="HIGH">High</option><option value="ATTENTION">Attention</option></select></div></div><RankedTable rows={rankedRows} onSelect={openGrid} /></section><MapPanel geojson={geojson} rows={rankedRows} onSelect={openGrid} /></section>}
          {page === "risk" && <section className="page-stack"><section className="risk-layout"><form className="panel risk-form" onSubmit={predictRisk}><div><span className="eyebrow">MODEL INPUT</span><h2>Assess grid risk</h2><p className="muted">Features are loaded automatically from the telemetry table.</p></div><label>Grid ID<input name="grid_id" type="number" min="1" defaultValue={gridId} placeholder="1" required /></label><label>Timestamp<input name="timestamp" type="datetime-local" defaultValue={toDateTimeLocal(summary?.as_of)} required /></label><button type="submit" disabled={predicting}>{predicting ? "Loading telemetry..." : "Run risk assessment"}</button></form><section className="panel result-panel"><div><span className="eyebrow">MODEL OUTPUT</span><h2>Prediction result</h2></div>{prediction?.error ? <div className="inline-error">{prediction.error}</div> : prediction ? <div className="prediction"><strong>{Math.round(prediction.risk_score * 100)}%</strong><StatusPill value={prediction.risk_level} /><dl><div><dt>Grid</dt><dd>{prediction.grid_id}</dd></div><div><dt>Model version</dt><dd>{prediction.model_version}</dd></div></dl><p>{prediction.explanation_note}</p><button type="button" className="secondary-button">Explain with AI</button></div> : <p className="empty-state">Enter a grid ID and timestamp to load stored features and see a risk score.</p>}</section></section></section>}
        </>}</main>
    </div>
  );
}

function RankedTable({ rows, onSelect }) { return rows.length ? <div className="table-wrap"><table><thead><tr><th>Grid</th><th>Activity</th><th>Status</th><th>Hourly timestamp</th><th /></tr></thead><tbody>{rows.map((row, index) => <tr key={`${row.grid_id}-${row.timestamp}-${index}`}><td><strong>#{row.grid_id}</strong></td><td>{number(row.total_activity)}</td><td><StatusPill value={row.severity || row.status} /></td><td>{date(row.timestamp)}</td><td><button className="row-action" onClick={() => onSelect(row.grid_id)}>Open</button></td></tr>)}</tbody></table></div> : <p className="empty-state">No rows are available for this filter.</p>; }
function ActivityTable({ rows }) { return <div className="table-wrap"><table><thead><tr><th>Timestamp</th><th>Calls</th><th>SMS</th><th>Internet</th><th>Total</th></tr></thead><tbody>{rows.map((row, index) => <tr key={`${row.timestamp}-${index}`}><td>{date(row.timestamp)}</td><td>{number(row.total_calls)}</td><td>{number(row.total_sms)}</td><td>{number(row.internet)}</td><td><strong>{number(row.total_activity)}</strong></td></tr>)}</tbody></table></div>; }
function MapPanel({ geojson, rows, onSelect }) {
  const statusFor = (id) => rows.find((item) => String(item.grid_id) === String(id));
  const styleFor = (feature) => {
    const row = statusFor(feature.properties?.cellId ?? feature.properties?.grid_id ?? feature.id);
    const label = String(row?.severity || row?.status || "NORMAL").toUpperCase();
    const color = label.includes("HIGH") ? "#e26f67" : label.includes("ATTENTION") || label.includes("ALERT") || label.includes("MEDIUM") ? "#e7b455" : "#69bd91";
    return { color, fillColor: color, fillOpacity: row ? 0.68 : 0.2, weight: row ? 2 : 1 };
  };
  const onEachFeature = (feature, layer) => {
    const id = feature.properties?.cellId ?? feature.properties?.grid_id ?? feature.id;
    const row = statusFor(id);
    layer.bindTooltip(`Grid ${id}${row ? ` | ${number(row.total_activity)} activity` : " | NORMAL"}`, { sticky: true });
    layer.on({ click: () => onSelect(id), mouseover: (event) => event.target.setStyle({ weight: 3, fillOpacity: 0.9 }), mouseout: (event) => event.target.setStyle(styleFor(feature)) });
  };
  return <section className="panel map-panel"><div className="panel-heading"><div><span className="eyebrow">GEOSPATIAL VIEW</span><h2>Milan grid status</h2></div><span className="muted">{geojson ? `${geojson.features?.length || 0} polygons loaded` : "Loading map data"}</span></div><div className="map-canvas">{geojson ? <MapContainer className="leaflet-map" center={[45.4642, 9.19]} zoom={11} scrollWheelZoom><TileLayer attribution='&copy; Esri' url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}" /><TileLayer attribution="&copy; OpenStreetMap contributors" url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" opacity={0.2} /><GeoJSON data={geojson} style={styleFor} onEachFeature={onEachFeature} /></MapContainer> : <p className="empty-state">GeoJSON could not be loaded.</p>}</div><div className="legend"><span><i className="normal" />Normal</span><span><i className="attention" />Attention</span><span><i className="high" />High</span></div></section>;
}

export default App;