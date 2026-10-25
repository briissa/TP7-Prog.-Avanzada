import { notFound } from "next/navigation";
import { getDatabase } from "@/../src/db.js";

export const dynamic = "force-dynamic";

function dateLabel(value) {
  return value
    ? new Intl.DateTimeFormat("es-AR", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value))
    : "—";
}

export default async function ShipmentDetailPage({ params }) {
  const { id } = await params;
  const database = await getDatabase();
  const shipment = await database.collection("envios").findOne({ shipmentId: id });
  if (!shipment) notFound();

  const [alerts, notifications] = await Promise.all([
    database.collection("alertas").find({ shipmentId: id }).sort({ creadaEn: -1 }).toArray(),
    database.collection("notificaciones").find({ shipmentId: id }).sort({ ocurridoEn: -1 }).toArray(),
  ]);
  const history = [...(shipment.historial ?? [])].sort(
    (left, right) => new Date(left.ocurridoEn) - new Date(right.ocurridoEn),
  );

  return (
    <div className="page-shell">
      <a className="back-link" href="/">← Volver a la torre de control</a>
      <section className="page-heading detail-heading">
        <div>
          <p className="eyebrow">DETALLE DEL ENVÍO</p>
          <h1>{shipment.shipmentId}</h1>
          <p className="muted">Zona {shipment.zonaId ?? "—"} · Centro {shipment.hubId ?? "—"} · Repartidor {shipment.repartidorId ?? "—"}</p>
        </div>
        <div className="detail-actions">
          <span className="status status-large">{shipment.estado}</span>
          <a className="button button-light" href={`/tracking/${encodeURIComponent(shipment.shipmentId)}`}>Vista pública ↗</a>
        </div>
      </section>

      <section className="stat-grid detail-stats">
        <article className="stat-card"><p>Intentos fallidos</p><strong>{shipment.intentos?.length ?? 0}</strong></article>
        <article className="stat-card"><p>Promesa de entrega</p><strong className="stat-date">{dateLabel(shipment.promesa?.venceEn)}</strong></article>
        <article className="stat-card"><p>Próxima acción</p><strong className="stat-date">{shipment.proximaAccion ?? "Sin acción pendiente"}</strong></article>
      </section>

      <div className="content-grid detail-grid">
        <section className="panel">
          <div className="panel-heading"><div><p className="eyebrow">EVENTOS ORDENADOS</p><h2>Línea de tiempo</h2></div></div>
          {history.length === 0 ? <p className="muted">Sin eventos.</p> : (
            <ol className="timeline">
              {history.map((entry, index) => (
                <li key={entry.eventId ?? `${entry.tipo}-${index}`}>
                  <span className="timeline-mark" />
                  <div className="timeline-content">
                    <div><strong>{entry.tipo}</strong><small>{dateLabel(entry.ocurridoEn)}</small></div>
                    <p>
                      {entry.motivo && <>Motivo: {entry.motivo} · </>}
                      {entry.intento && <>Intento {entry.intento} · </>}
                      {entry.programadoPara && <>Programado: {dateLabel(entry.programadoPara)} ({entry.franja}) · </>}
                      Secuencia {entry.secuencia}
                    </p>
                  </div>
                </li>
              ))}
            </ol>
          )}
        </section>

        <div className="side-stack">
          <section className="panel">
            <div className="panel-heading"><div><p className="eyebrow">RIESGO</p><h2>Alertas SLA</h2></div></div>
            {alerts.length ? alerts.map((alert) => (
              <div className="record-row" key={alert._id}>
                <span className={`priority priority-${alert.prioridad}`}>{alert.prioridad}</span>
                <div><strong>{alert.regla}</strong><p>{alert.causa}</p><small>{alert.estado}</small></div>
              </div>
            )) : <p className="muted">Este envío no tiene alertas.</p>}
          </section>
          <section className="panel">
            <div className="panel-heading"><div><p className="eyebrow">COMUNICACIÓN</p><h2>Notificaciones</h2></div></div>
            {notifications.length ? notifications.map((notification) => (
              <div className="notification-item" key={notification.eventId}>
                <span className="notification-icon">↗</span>
                <div><strong>{notification.canal}</strong><p>{notification.mensaje}</p><small>{dateLabel(notification.ocurridoEn)}</small></div>
              </div>
            )) : <p className="muted">Sin notificaciones emitidas.</p>}
          </section>
        </div>
      </div>
    </div>
  );
}
