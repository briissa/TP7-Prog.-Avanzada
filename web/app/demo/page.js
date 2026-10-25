import ScenarioButtons from "@/components/ScenarioButtons.js";
import LagPanel from "@/components/LagPanel.js";

export const dynamic = "force-dynamic";

export default function DemoPage() {
  return (
    <div className="page-shell">
      <section className="page-heading">
        <div><p className="eyebrow">PRESENTACIÓN · 15 MINUTOS</p><h1>Panel de demo</h1><p className="muted">Escenarios deterministas para recorrer los cuatro casos de uso.</p></div>
        <a className="button button-dark" href="/">Volver al control</a>
      </section>
      <ScenarioButtons />
      <LagPanel />
      <section className="demo-script panel">
        <div><p className="eyebrow">RECORRIDO SUGERIDO</p><h2>De la publicación al reprocesamiento</h2></div>
        <ol>
          <li>Envío feliz y tracking público.</li>
          <li>Ausente: aviso, reprogramación y analítica en paralelo.</li>
          <li>Demorado: alerta de riesgo SLA en tiempo real.</li>
          <li>Detener un consumidor, ver crecer su lag y reiniciarlo.</li>
          <li>Evento malformado a DLQ; luego, reprocesar el histórico.</li>
        </ol>
      </section>
    </div>
  );
}
