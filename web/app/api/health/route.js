import { getDatabase } from "@/../src/db.js";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const database = await getDatabase();
    await database.command({ ping: 1 });
    const simulator = await fetch(
      `${process.env.SIMULATOR_URL ?? "http://localhost:4000"}/health`,
      { cache: "no-store" },
    );
    if (!simulator.ok) throw new Error(`Simulador HTTP ${simulator.status}`);
    return Response.json({ status: "ok" });
  } catch (error) {
    return Response.json({ status: "error", error: error.message }, { status: 503 });
  }
}
