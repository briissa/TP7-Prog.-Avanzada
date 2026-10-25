import { Kafka, logLevel } from "kafkajs";
import { getDatabase, getMongoClient } from "./db.js";
import { schemasByTopic } from "./events.js";
import { TOPICS } from "./constants.js";

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const kafka = new Kafka({
  clientId: `rutasur-${process.env.SERVICE_NAME ?? "app"}`,
  brokers,
  logLevel: logLevel.NOTHING,
  retry: { initialRetryTime: 300, retries: 20 },
});

export function createProducer() {
  return kafka.producer({ allowAutoTopicCreation: false });
}

export async function publish(producer, topic, events) {
  if (events.length === 0) return;
  await publishRaw(producer, topic, events.map((event) => ({
    key: event.shipmentId,
    value: JSON.stringify(event),
  })));
}

export async function publishRaw(producer, topic, messages) {
  if (messages.length === 0) return;
  await producer.send({ topic, messages });
}

export async function connectProducer() {
  const producer = createProducer();
  await producer.connect();
  return producer;
}

export async function processOnce(database, consumerName, event, handler) {
  const ledger = database.collection("eventos_procesados");
  const ledgerEntry = { consumidor: consumerName, eventId: event.eventId };
  if (await ledger.findOne(ledgerEntry, { projection: { _id: 1 } })) return false;

  const session = getMongoClient().startSession();
  let applied = false;
  try {
    await session.withTransaction(async () => {
      await ledger.insertOne({
          consumidor: consumerName,
          eventId: event.eventId,
          createdAt: new Date(),
      }, { session });

      const output = await handler(session);
      for (const item of output ?? []) {
        await database.collection("outbox").updateOne(
          { _id: item.id },
          {
            $setOnInsert: {
              topicOwner: item.topicOwner,
              topic: item.topic,
              event: item.event,
              createdAt: new Date(),
              sentAt: null,
            },
          },
          { upsert: true, session },
        );
      }
      applied = true;
    }, {
      readConcern: { level: "snapshot" },
      writeConcern: { w: "majority" },
    });
  } catch (error) {
    if (error.code === 11000 && await ledger.findOne(ledgerEntry, { projection: { _id: 1 } })) {
      return false;
    }
    throw error;
  } finally {
    await session.endSession();
  }
  return applied;
}

export async function startOutboxPublisher(database, producer, serviceName) {
  let running = true;
  const work = async () => {
    while (running) {
      try {
        const pending = await database.collection("outbox")
          .find({ topicOwner: serviceName, sentAt: null })
          .sort({ createdAt: 1 })
          .limit(50)
          .toArray();
        for (const item of pending) {
          try {
            await publish(producer, item.topic, [item.event]);
            await database.collection("outbox").updateOne(
              { _id: item._id, sentAt: null },
              { $set: { sentAt: new Date() } },
            );
          } catch (error) {
            console.error(JSON.stringify({
              level: "error",
              serviceName,
              outboxId: item._id,
              message: error.message,
            }));
            break;
          }
        }
      } catch (error) {
        console.error(JSON.stringify({ level: "error", serviceName, message: error.message }));
      }
      await new Promise((resolve) => setTimeout(resolve, running ? 1000 : 0));
    }
  };
  const promise = work();
  return async () => {
    running = false;
    await promise;
  };
}

export function outboxRecord(serviceName, sourceEvent, topic, event) {
  return {
    id: `${serviceName}:${event.eventId}`,
    topicOwner: serviceName,
    topic,
    event,
  };
}

export async function startConsumer({ serviceName, groupId, topics, handler }) {
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
  const database = await getDatabase();
  const producer = await connectProducer();
  await consumer.connect();
  await consumer.subscribe({ topics, fromBeginning: true });
  const runPromise = consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      let event;
      try {
        const raw = JSON.parse(message.value?.toString() ?? "");
        event = schemasByTopic[topic]?.parse(raw);
      } catch (error) {
        const rawPayload = message.value?.toString() ?? "";
        let original;
        try {
          original = JSON.parse(rawPayload);
        } catch {
          original = {};
        }
        const rejected = {
          eventId: `dlq-${topic}-${partition}-${message.offset}`,
          shipmentId: original.shipmentId ?? "desconocido",
          ocurridoEn: new Date().toISOString(),
          secuencia: 1,
          version: 1,
          topicOriginal: topic,
          particionOriginal: partition,
          offsetOriginal: message.offset,
          motivo: error.message,
          payload: rawPayload,
        };
        const source = { eventId: rejected.eventId, shipmentId: rejected.shipmentId };
        await processOnce(database, "dead-letter", source, async () => [
          outboxRecord("dead-letter", source, TOPICS.deadLetter, rejected),
        ]);
        console.error(JSON.stringify({
          level: "error",
          serviceName,
          message: "Evento inválido detectado; se registró en la DLQ",
          topic,
          partition,
          offset: message.offset,
          reason: error.message,
        }));
      }

      if (event) {
        console.info(JSON.stringify({
          level: "info",
          serviceName,
          eventId: event.eventId,
          partition,
          offset: message.offset,
          topic,
        }));
        let processed = false;
        while (!processed) {
          try {
            await handler({ database, producer, topic, event });
            processed = true;
          } catch (error) {
            console.error(JSON.stringify({
              level: "error",
              serviceName,
              eventId: event.eventId,
              partition,
              offset: message.offset,
              message: error.message,
            }));
            await new Promise((resolve) => setTimeout(resolve, 2000));
          }
        }
      }

      await consumer.commitOffsets([{
        topic,
        partition,
        offset: (BigInt(message.offset) + 1n).toString(),
      }]);
    },
  });

  runPromise.catch((error) => {
    console.error(JSON.stringify({
      level: "error",
      serviceName,
      message: "El consumidor Kafka se detuvo",
      error: error.message,
    }));
    process.exitCode = 1;
  });
  return { consumer, database, producer, runPromise };
}
