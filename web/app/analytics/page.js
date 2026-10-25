import { getDatabase } from "@/../src/db.js";

export const dynamic = "force-dynamic";

export default async function AnalyticsPage({ searchParams }) {
  const filters = await searchParams;
  const zone = filters?.zona ?? "";
  const hub = filters?.hub ?? "";
  const from = filters?.desde ?? "";
  const through = filters?.hasta ?? "";
  const database = await getDatabase();
  const query = {};
  if (zone) query.zonaId = zone;
  if (hub) query.hubId = hub;
  if (from || through) {
    query.fecha = {
      ...(from ? { $gte: from } : {}),
      ...(through ? { $lte: through } : {}),
    };
  }
  const metrics = await database.collection("metricas_diarias")
    .find(query).sort({ fecha: -1 }).limit(300).toArray();

  const totals = metrics.reduce((result, item) => {
    result.fallos += item.fallos ?? 0;
    result.primerIntentoTotal += item.primerIntentoTotal ?? 0;
    result.primerIntentoExitoso += item.primerIntentoExitoso ?? 0;
    result.enviosConPromesa += item.enviosConPromesa ?? 0;
    result.entregasEnSla += item.entregasEnSla ?? 0;
    result.tiempoHubTotalSegundos += (item.tiempoHubPromedioSegundos ?? 0)
      * (item.tiempoHubMuestras ?? 0);
    result.tiempoHubMuestras += item.tiempoHubMuestras ?? 0;
    return result;
  }, {
    fallos: 0,
    primerIntentoTotal: 0,
    primerIntentoExitoso: 0,
    enviosConPromesa: 0,
    entregasEnSla: 0,
    tiempoHubTotalSegundos: 0,
    tiempoHubMuestras: 0,
  });
  const firstAttemptRate = totals.primerIntentoTotal
    ? totals.primerIntentoExitoso / totals.primerIntentoTotal
    : 0;
  const slaRate = totals.enviosConPromesa
    ? totals.entregasEnSla / totals.enviosConPromesa
    : 0;
  const reasons = {};
  for (const metric of metrics) {
    for (const [reason, count] of Object.entries(metric.motivos ?? {})) {
      reasons[reason] = (reasons[reason] ?? 0) + count;
    }
  }
  const maxReason = Math.max(1, ...Object.values(reasons));
  const zones = [...new Set(metrics.map((item) => item.zonaId))].sort();
  const hubs = [...new Set(metrics.map((item) => item.hubId))].sort();
  const latest = metrics.slice(0, 12);

  return (
    <div className="page-shell">
      <section className="page-heading">
        <div><p className="eyebrow">MEJORA CONTINUA · INDICADORES</p><h1>Analítica de operación</h1><p className="muted">Agregados incrementales por zona, centro, fecha y franja.</p></div>
      </section>

      <form className="filters panel" action="/analytics">
        <label>Zona<select name="zona" defaultValue={zone}><option value="">Todas las zonas</option>{zones.map((item) => <option key={item}>{item}</option>)}</select></label>
        <label>Centro<select name="hub" defaultValue={hub}><option value="">Todos los centros</option>{hubs.map((item) => <option key={item}>{item}</option>)}</select></label>
        <label>Desde<input name="desde" type="date" defaultValue={from} /></label>
        <label>Hasta<input name="hasta" type="date" defaultValue={through} /></label>
        <button className="button button-dark">Aplicar filtros</button>
      </form>

      <section className="metric-grid">
        <article className="metric-card"><span>Tasa de entrega en primer intento</span><strong>{(firstAttemptRate * 100).toFixed(1)}%</strong><small>{totals.primerIntentoExitoso} de {totals.primerIntentoTotal} primeros intentos</small></article>
        <article className="metric-card"><span>Cumplimiento de SLA</span><strong>{(slaRate * 100).toFixed(1)}%</strong><small>{totals.entregasEnSla} entregas a tiempo de {totals.enviosConPromesa} casos</small></article>
        <article className="metric-card"><span>Motivos de fallo</span><strong>{totals.fallos}</strong><small>fallos en las dimensiones seleccionadas</small></article>
        <article className="metric-card"><span>Tiempo medio en el centro</span><strong>{totals.tiempoHubMuestras ? `${Math.round(totals.tiempoHubTotalSegundos / totals.tiempoHubMuestras / 60)} min` : "Pendiente"}</strong><small>{totals.tiempoHubMuestras} mediciones reprocesadas</small></article>
      </section>

      <div className="content-grid analytics-grid">
        <section className="panel">
          <div className="panel-heading"><div><p className="eyebrow">CU2 · CAUSAS</p><h2>Distribución de motivos</h2></div></div>
          {Object.keys(reasons).length === 0 ? <p className="muted">Todavía no hay fallos para analizar.</p> : (
            <div className="reason-list">
              {Object.entries(reasons).sort((a, b) => b[1] - a[1]).map(([reason, count]) => (
                <div className="reason-row" key={reason}>
                  <div><span>{reason.replaceAll("_", " ")}</span><strong>{count}</strong></div>
                  <div className="bar-track"><span style={{ width: `${(count / maxReason) * 100}%` }} /></div>
                </div>
              ))}
            </div>
          )}
        </section>
        <section className="panel">
          <div className="panel-heading"><div><p className="eyebrow">CU3 · COMPARACIÓN</p><h2>Rendimiento por centro</h2></div></div>
          {metrics.length === 0 ? <p className="muted">Sin métricas todavía.</p> : (
            <div className="table-wrap">
              <table><thead><tr><th>Centro</th><th>Fecha</th><th>En SLA</th><th>1er intento</th></tr></thead>
                <tbody>{latest.map((item, index) => (
                  <tr key={`${item._id}-${index}`}><td>{item.hubId}</td><td>{item.fecha}</td><td>{item.entregasEnSla ?? 0}/{item.enviosConPromesa ?? 0}</td><td>{item.primerIntentoExitoso ?? 0}/{item.primerIntentoTotal ?? 0}</td></tr>
                ))}</tbody>
              </table>
            </div>
          )}
        </section>
      </div>
      <p className="analytics-note">El cuarto indicador se reconstruye desde el principio del log con un grupo nuevo sin alterar los demás consumidores. Ejecutá <code>docker compose run --rm replay</code> para actualizarlo.</p>
    </div>
  );
}
