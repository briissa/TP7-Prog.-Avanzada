import "./styles.css";
import LiveRefresh from "@/components/LiveRefresh.js";

export const metadata = {
  title: "RutaSur | Torre de control",
  description: "Seguimiento de envíos y operación logística en tiempo real.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="es">
      <body>
        <LiveRefresh />
        <header className="topbar">
          <a className="brand" href="/">
            <span className="brand-mark">RS</span>
            <span>RutaSur<span className="brand-sub">OPERACIONES</span></span>
          </a>
          <nav aria-label="Navegación principal">
            <a href="/">Control</a>
            <a href="/analytics">Analítica</a>
            <a href="/demo">Demo</a>
          </nav>
          <span className="live-status"><i /> Operación en vivo</span>
        </header>
        <main>{children}</main>
        <footer className="footer">
          <span>RutaSur · Simulador académico de logística orientada a eventos</span>
          <span>Kafka · MongoDB · Next.js</span>
        </footer>
      </body>
    </html>
  );
}
