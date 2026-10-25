export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const response = await fetch(
      `${process.env.SIMULATOR_URL ?? "http://localhost:4000"}/lag`,
      { cache: "no-store" },
    );
    const groups = await response.json();
    return Response.json(groups, { status: response.status });
  } catch (error) {
    return Response.json(
      { error: `No se pudo consultar el lag: ${error.message}` },
      { status: 502 },
    );
  }
}
