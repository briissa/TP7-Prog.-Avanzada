import { getDatabase } from "@/../src/db.js";

export const dynamic = "force-dynamic";

export async function GET(request) {
  const database = await getDatabase();
  const stream = database.watch([
    {
      $match: {
        "ns.coll": { $in: ["envios", "alertas", "metricas_diarias"] },
        operationType: { $in: ["insert", "update", "replace", "delete"] },
      },
    },
  ], { fullDocument: "updateLookup" });
  const encoder = new TextEncoder();
  let closed = false;

  const body = new ReadableStream({
    async start(controller) {
      controller.enqueue(encoder.encode("retry: 2000\n\n"));
      try {
        while (!closed && !request.signal.aborted) {
          const change = await stream.tryNext();
          if (change) {
            controller.enqueue(encoder.encode(
              `event: update\ndata: ${JSON.stringify({ collection: change.ns.coll })}\n\n`,
            ));
          } else {
            controller.enqueue(encoder.encode(": keep-alive\n\n"));
            await new Promise((resolve) => setTimeout(resolve, 1000));
          }
        }
      } catch (error) {
        if (!closed && !request.signal.aborted) {
          controller.error(error);
        }
      } finally {
        await stream.close();
        if (!closed) controller.close();
      }
    },
    async cancel() {
      closed = true;
      await stream.close();
    },
  });

  return new Response(body, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  });
}
