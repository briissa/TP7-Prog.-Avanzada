import { getDatabase } from "@/../src/db.js";

export const dynamic = "force-dynamic";

const statusLabels = {
  CREADO: "Creado",
  INGRESADO_HUB: "En centro",
  CLASIFICADO: "Clasificado",
  EN_REPARTO: "En reparto",
  ENTREGADO: "Entregado",
  ENTREGA_FALLIDA: "Entrega fallida",
  SIN_EVENTOS: "Sin eventos",
};

function dateLabel(value) {
  if (!value) return "—";
  return new Intl.DateTimeFormat("es-AR", {
    dateStyle: "short",
    timeStyle: "short",
  }).format(new Date(value));
}

export default async function ControlPage() {
  const database = await getDatabase();
  const [shipmentCounts, alerts, shipments, totalShipments, openAlertCount] = await Promise.all([
    database.collection("envios").aggregate([
      { $group: { _id: "$estado", total: { $sum: 1 } } },
    ]).toArray(),
    database.collection("alertas").find({ estado: "abierta" })
      .sort({ prioridadOrden: -1, creadaEn: 1 }).limit(8).toArray(),
    database.collection("envios").find({})
      .sort({ actualizadoEn: -1 }).limit(10).toArray(),
    database.collection("envios").countDocuments(),
    database.collection("alertas").countDocuments({ estado: "abierta" }),
  ]);
  const counts = Object.fromEntries(shipmentCounts.map(({ _id, total }) => [_id, total]));
  const cards = [
    ["Creados", counts.CREADO ?? 0, "blue"],
    ["En centro", counts.INGRESADO_HUB ?? 0, "slate"],
    ["Clasificados", counts.CLASIFICADO ?? 0, "slate"],
    ["En reparto", counts.EN_REPARTO ?? 0, "orange"],
    ["Entregados", counts.ENTREGADO ?? 0, "green"],
    ["Entrega fallida", counts.ENTREGA_FALLIDA ?? 0, "red"],
    ["Alertas abiertas", openAlertCount, "red"],
  ];

  return (
    <div className="page-shell">
      <section className="page-heading">
        <div>
          <p className="eyebrow">CENTRO DE OPERACIONES · HOY</p>
          <h1>Torre de control</h1>
          <p className="muted">Seguimiento operativo, alertas y actividad de envíos.</p>
        </div>
        <a className="button button-primary" href="/demo">Abrir panel de demo <span>↗</span></a>
      </section>

      <section className="stat-grid" aria-label="Resumen de envíos">
        {cards.map(([label, value, color]) => (
          <article className="stat-card" key={label}>
            <span className={`stat-indicator ${color}`} />
            <p>{label}</p>
            <strong>{value.toLocaleString("es-AR")}</strong>
          </article>
        ))}
      </section>

      <div className="content-grid">
        <section className="panel">
          <div className="panel-heading">
            <div><p className="eyebrow">ACTIVIDAD RECIENTE</p><h2>Últimos movimientos</h2></div>
            <span className="live-pill">EN VIVO</span>
          </div>
          {shipments.length === 0 ? (
            <div className="empty-state"><span>◷</span><h3>Esperando los primeros envíos</h3><p>Iniciá un escenario desde el panel de demo o aguardá al simulador.</p></div>
          ) : (
            <div className="table-wrap">
              <table>
                <thead><tr><th>Envío</th><th>Estado</th><th>Zona</th><th>Actualización</th><th /></tr></thead>
                <tbody>
                  {shipments.map((shipment) => (
                    <tr key={shipment._id}>
                      <td><strong>{shipment.shipmentId}</strong></td>
                      <td><span className={`status status-${shipment.estado?.toLowerCase()}`}>{statusLabels[shipment.estado] ?? shipment.estado}</span></td>
                      <td>{shipment.zonaId ?? "—"}</td>
                      <td>{dateLabel(shipment.actualizadoEn)}</td>
                      <td><a className="row-link" href={`/shipments/${encodeURIComponent(shipment.shipmentId)}`}>Ver →</a></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <aside className="panel alert-panel">
          <div className="panel-heading">
            <div><p className="eyebrow">PRIORIZACIÓN AUTOMÁTICA</p><h2>Alertas SLA</h2></div>
            <span className="alert-count">{openAlertCount}</span>
          </div>
          {alerts.length === 0 ? (
            <div className="empty-state compact"><span>✓</span><p>Sin alertas abiertas</p></div>
          ) : (
            <ul className="alert-list">
              {alerts.map((alert) => (
                <li key={alert._id}>
                  <span className={`priority priority-${alert.prioridad}`}>{alert.prioridad}</span>
                  <div>
                    <a href={`/shipments/${encodeURIComponent(alert.shipmentId)}`}><strong>{alert.shipmentId}</strong></a>
                    <p>{alert.causa}</p>
                    <small>{dateLabel(alert.creadaEn)}</small>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </aside>
      </div>

      <section className="status-strip">
        <span><i className="dot-green" /> Kafka y proyecciones activos</span>
        <span><i className="dot-blue" /> {totalShipments.toLocaleString("es-AR")} envíos proyectados</span>
        <a href="/analytics">Ver analítica de operación →</a>
      </section>
    </div>
  );
}
