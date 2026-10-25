import { Kafka } from "kafkajs";
import { TOPICS } from "../constants.js";
import { getDatabase, getMongoClient } from "../db.js";

const consumerTopics = {
  proyector: [
    TOPICS.states,
    TOPICS.promises,
    TOPICS.reschedules,
    TOPICS.alerts,
    TOPICS.notifications,
  ],
  notificaciones: [TOPICS.states],
  reprogramacion: [TOPICS.states],
  "monitor-sla": [TOPICS.states, TOPICS.promises],
  analitica: [TOPICS.states],
};

async function rebuild() {
  const kafka = new Kafka({
    clientId: "rutasur-rebuild",
    brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  });
  const admin = kafka.admin();
  const database = await getDatabase();
  let connected = false;

  try {
    await admin.connect();
    connected = true;
    const groups = new Set((await admin.listGroups()).groups.map((group) => group.groupId));
    const active = [];
    for (const groupId of Object.keys(consumerTopics)) {
      if (!groups.has(groupId)) continue;
      const details = await admin.describeGroups([groupId]);
      if ((details.groups[0]?.members.length ?? 0) > 0) active.push(groupId);
    }
    if (active.length > 0) {
      throw new Error(`Detené estos grupos antes de reconstruir: ${active.join(", ")}`);
    }

    for (const [groupId, topics] of Object.entries(consumerTopics)) {
      if (!groups.has(groupId)) continue;
      for (const topic of topics) {
        await admin.resetOffsets({ groupId, topic, earliest: true });
      }
    }

    const collections = [
      "envios",
      "alertas",
      "notificaciones",
      "metricas_diarias",
      "eventos_procesados",
      "analytics_state",
      "sla_monitor_state",
      "sla_alert_state",
      "avisos_emitidos",
      "outbox",
      "metricas_transito_replay",
    ];
    const existing = new Set(
      (await database.listCollections(
        { name: { $in: collections } },
        { nameOnly: true },
      ).toArray()).map((collection) => collection.name),
    );
    for (const name of collections) {
      if (existing.has(name)) {
        await database.collection(name).deleteMany({});
      }
    }

    console.info(JSON.stringify({
      serviceName: "rebuild",
      status: "completed",
      gruposReiniciados: Object.keys(consumerTopics).filter((groupId) => groups.has(groupId)),
      vistasEliminadas: collections,
    }));
  } finally {
    if (connected) await admin.disconnect();
    await getMongoClient().close();
  }
}

rebuild().catch((error) => {
  console.error(JSON.stringify({ serviceName: "rebuild", level: "fatal", message: error.message }));
  process.exitCode = 1;
});
