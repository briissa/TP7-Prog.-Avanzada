"use client";

import { useState } from "react";

const scenarios = [
  ["feliz", "Envío feliz", "Recorre el ciclo completo hasta la entrega."],
  ["ausente", "Entrega fallida", "Activa aviso y reprogramación independientes."],
  ["demorado", "Envío demorado", "Provoca una alerta de riesgo de SLA."],
  ["tercer-fallo", "Tercer intento fallido", "Agota intentos y marca devolución."],
  ["rafaga", "Ráfaga · 500 envíos", "Carga eventos en paralelo para observar el lag."],
  ["malformado", "Evento inválido", "Envía un registro que debe terminar en la DLQ."],
];

export default function ScenarioButtons() {
  const [active, setActive] = useState("");
  const [message, setMessage] = useState("");

  async function trigger(name) {
    setActive(name);
    setMessage("");
    try {
      const response = await fetch("/api/scenarios", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error ?? "No se pudo iniciar el escenario");
      setMessage(name === "rafaga"
        ? "La ráfaga de 500 envíos fue publicada."
        : `Escenario iniciado · envío ${result.shipmentId}`);
    } catch (error) {
      setMessage(error.message);
    } finally {
      setActive("");
    }
  }

  return (
    <>
      <div className="scenario-grid">
        {scenarios.map(([name, title, description]) => (
          <article className="scenario-card" key={name}>
            <span className="scenario-icon">{name === "rafaga" ? "500" : "↗"}</span>
            <h3>{title}</h3>
            <p>{description}</p>
            <button disabled={Boolean(active)} onClick={() => trigger(name)}>
              {active === name ? "Publicando…" : "Ejecutar escenario"}
            </button>
          </article>
        ))}
      </div>
      {message && <p className="notice" role="status">{message}</p>}
    </>
  );
}
