"use client";

import { useCallback, useEffect, useState } from "react";

export default function LagPanel() {
  const [groups, setGroups] = useState([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const response = await fetch("/api/lag", { cache: "no-store" });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error ?? "No se pudo obtener el lag");
      setGroups(result);
      setError("");
    } catch (caught) {
      setError(caught.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, [refresh]);

  return (
    <section className="panel lag-panel">
      <div className="panel-heading"><div><p className="eyebrow">OBSERVABILIDAD</p><h2>Lag de consumidores</h2></div><button className="button button-light" onClick={refresh}>{loading ? "Consultando…" : "Actualizar"}</button></div>
      {error ? <p className="error-message">{error}</p> : (
        <div className="lag-grid">
          {groups.map((group) => (
            <article className="lag-card" key={group.groupId}>
              <span>{group.groupId}</span>
              <strong>{Number(group.lag).toLocaleString("es-AR")}</strong>
              <small>{group.state}</small>
            </article>
          ))}
          {groups.length === 0 && <p className="muted">Los grupos aparecerán cuando comiencen a consumir.</p>}
        </div>
      )}
    </section>
  );
}
