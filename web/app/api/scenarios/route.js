export const dynamic = "force-dynamic";

export async function POST(request) {
  let body;
  try {
    body = await request.json();
  } catch {
    return Response.json({ error: "El cuerpo debe ser JSON válido" }, { status: 400 });
  }

  try {
    const result = await fetch(`${process.env.SIMULATOR_URL ?? "http://localhost:4000"}/scenario`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      cache: "no-store",
    });
    const payload = await result.json();
    return Response.json(payload, { status: result.status });
  } catch (error) {
    return Response.json(
      { error: `No se pudo contactar al simulador: ${error.message}` },
      { status: 502 },
    );
  }
}
