import { randomUUID } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Kafka } from "kafkajs";
import { TOPICS } from "../constants.js";
import { getDatabase, getMongoClient } from "../db.js";
import { stateEventSchema } from "../events.js";

function offsetMap(offsets) {
  return new Map(offsets.map((item) => [item.partition, BigInt(item.offset)]));
}

export async function runReplay() {
  const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
  const kafka = new Kafka({ clientId: "rutasur-analytics-replay", brokers });
  const admin = kafka.admin();
  const database = await getDatabase();
  const staging = database.collection("metricas_transito_replay");
  const shipments = new Map();
  const groupId = `analitica-replay-${Date.now()}-${randomUUID().slice(0, 8)}`;
  let consumer;
  let totalEvents = 0;

  await admin.connect();
  try {
    const targetOffsets = offsetMap(await admin.fetchTopicOffsets(TOPICS.states));
    if ([...targetOffsets.values()].some((offset) => offset > 0n)) {
      consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
      const reached = new Set(
        [...targetOffsets.entries()]
          .filter(([, offset]) => offset === 0n)
          .map(([partition]) => partition),
      );
      await consumer.connect();
      await consumer.subscribe({ topic: TOPICS.states, fromBeginning: true });
      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
          const target = targetOffsets.get(partition) ?? 0n;
          const nextOffset = BigInt(message.offset) + 1n;
          if (nextOffset <= target) {
            totalEvents += 1;
            let event;
            try {
              event = JSON.parse(message.value?.toString() ?? "{}");
            } catch {
              event = null;
            }
            const parsed = stateEventSchema.safeParse(event);
            if (parsed.success) {
              event = parsed.data;
              const previous = shipments.get(event.shipmentId);
              if (!previous || event.secuencia > previous.secuencia) {
                shipments.set(event.shipmentId, {
                  secuencia: event.secuencia,
                  ingresadoEn: event.tipo === "INGRESADO_HUB"
                    ? new Date(event.ocurridoEn)
                    : previous?.ingresadoEn,
                  salidaEn: event.tipo === "EN_REPARTO"
                    ? new Date(event.ocurridoEn)
                    : previous?.salidaEn,
                  zonaId: event.tipo === "EN_REPARTO"
                    ? event.zonaId
                    : previous?.zonaId,
                  hubId: event.tipo === "EN_REPARTO"
                    ? event.hubId
                    : previous?.hubId,
                });
              }
            }
          }
          if (nextOffset >= target) reached.add(partition);
        },
      });
      await new Promise((resolve, reject) => {
        const timer = setInterval(() => {
          if (![...targetOffsets.keys()].every((item) => reached.has(item))) return;
          clearInterval(timer);
          consumer.stop().then(resolve, reject);
        }, 100);
      });
      await consumer.disconnect();
      consumer = undefined;
    }

    const aggregates = new Map();
    for (const shipment of shipments.values()) {
      if (!shipment.ingresadoEn || !shipment.salidaEn) continue;
      const dimension = {
        fecha: shipment.salidaEn.toISOString().slice(0, 10),
        zonaId: shipment.zonaId,
        hubId: shipment.hubId,
        franja: shipment.salidaEn.getUTCHours() < 12 ? "MANANA" : "TARDE",
      };
      const key = JSON.stringify(dimension);
      const current = aggregates.get(key) ?? { ...dimension, suma: 0, cantidad: 0 };
      current.suma += Math.max(
        0,
        (shipment.salidaEn.getTime() - shipment.ingresadoEn.getTime()) / 1000,
      );
      current.cantidad += 1;
      aggregates.set(key, current);
    }

    await staging.deleteMany({});
    if (aggregates.size > 0) {
      await staging.insertMany([...aggregates.values()]);
    }

    await database.collection("metricas_diarias").updateMany(
      {},
      { $unset: { tiempoHubPromedioSegundos: "", tiempoHubMuestras: "" } },
    );
    for (const metric of aggregates.values()) {
      await database.collection("metricas_diarias").updateOne(
        {
          fecha: metric.fecha,
          zonaId: metric.zonaId,
          hubId: metric.hubId,
          franja: metric.franja,
        },
        {
          $set: {
            tiempoHubPromedioSegundos: metric.suma / metric.cantidad,
            tiempoHubMuestras: metric.cantidad,
          },
          $setOnInsert: {
            fecha: metric.fecha,
            zonaId: metric.zonaId,
            hubId: metric.hubId,
            franja: metric.franja,
          },
        },
        { upsert: true },
      );
    }
    await staging.deleteMany({});

    return {
      groupId,
      eventosLeidos: totalEvents,
      enviosConMuestra: [...aggregates.values()].reduce((total, item) => total + item.cantidad, 0),
      indicadores: aggregates.size,
    };
  } finally {
    if (consumer) {
      await consumer.stop();
      await consumer.disconnect();
    }
    await admin.disconnect();
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  (async () => {
    try {
      const result = await runReplay();
      console.info(JSON.stringify({ serviceName: "replay", ...result }));
    } catch (error) {
      console.error(JSON.stringify({ serviceName: "replay", level: "fatal", message: error.message }));
      process.exitCode = 1;
    } finally {
      await getMongoClient().close();
    }
  })();
}
