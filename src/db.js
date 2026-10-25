import { MongoClient } from "mongodb";

const client = new MongoClient(
  process.env.MONGO_URL ?? "mongodb://localhost:27017/rutasur?replicaSet=rs0",
  { serverSelectionTimeoutMS: 10000 },
);

let connection;

export async function getDatabase() {
  if (!connection) {
    connection = client.connect().then(async () => {
      const database = client.db();
      await Promise.all([
        database.collection("envios").createIndex({ shipmentId: 1 }, { unique: true }),
        database.collection("envios").createIndex({ estado: 1, zonaId: 1 }),
        database.collection("envios").createIndex({ "promesa.venceEn": 1 }),
        database.collection("alertas").createIndex({ estado: 1, prioridadOrden: -1 }),
        database.collection("alertas").createIndex({ shipmentId: 1 }),
        database.collection("notificaciones").createIndex({ eventId: 1 }, { unique: true }),
        database.collection("notificaciones").createIndex({ shipmentId: 1 }),
        database.collection("metricas_diarias").createIndex(
          { fecha: 1, zonaId: 1, hubId: 1, franja: 1 },
          { unique: true },
        ),
        database.collection("eventos_procesados").createIndex(
          { consumidor: 1, eventId: 1 },
          { unique: true },
        ),
        database.collection("eventos_procesados").createIndex(
          { createdAt: 1 },
          { expireAfterSeconds: 604800 },
        ),
        database.collection("outbox").createIndex({ topicOwner: 1, sentAt: 1, createdAt: 1 }),
      ]);
      return database;
    });
  }

  return connection;
}

export function getMongoClient() {
  return client;
}
