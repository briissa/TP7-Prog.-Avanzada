import { notFound } from "next/navigation";
import { getDatabase } from "@/../src/db.js";

export const dynamic = "force-dynamic";

function dateLabel(value) {
  if (!value) return "A confirmar";
  return new Intl.DateTimeFormat("es-AR", { dateStyle: "long", timeStyle: "short" }).format(new Date(value));
}

export default async function PublicTrackingPage({ params }) {
  const { id } = await params;
  const database = await getDatabase();
  const shipment = await database.collection("envios").findOne(
    { shipmentId: id },
    { projection: { shipmentId: 1, estado: 1, promesa: 1, historial: 1 } },
  );
  if (!shipment) notFound();
  const history = [...(shipment.historial ?? [])]
    .filter((item) => ["CREADO", "INGRESADO_HUB", "EN_REPARTO", "ENTREGADO", "ENTREGA_FALLIDA"].includes(item.tipo))
    .sort((left, right) => new Date(left.ocurridoEn) - new Date(right.ocurridoEn));

  return (
    <div className="tracking-page">
      <a className="brand tracking-brand" href="/"><span className="brand-mark">RS</span><span>RutaSur<span className="brand-sub">TRACKING</span></span></a>
      <section className="tracking-card">
        <p className="eyebrow">SEGUIMIENTO DE PAQUETE</p>
        <h1>{shipment.shipmentId}</h1>
        <span className="tracking-status">{shipment.estado}</span>
        <p className="tracking-promise">Entrega estimada<br /><strong>{dateLabel(shipment.promesa?.venceEn)}</strong></p>
        <ol className="timeline public-timeline">
          {history.map((entry, index) => (
            <li key={entry.eventId ?? `${entry.tipo}-${index}`}>
              <span className="timeline-mark" />
              <div className="timeline-content">
                <strong>{entry.tipo.replaceAll("_", " ")}</strong>
                <small>{dateLabel(entry.ocurridoEn)}</small>
              </div>
            </li>
          ))}
        </ol>
        <p className="public-note">La información se actualiza automáticamente.</p>
      </section>
    </div>
  );
}
