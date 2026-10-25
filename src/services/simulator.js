import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { HUBS, TOPICS, ZONES } from "../constants.js";
import { simulatedNow } from "../clock.js";
import { createEvent } from "../events.js";
import { connectProducer, publish, publishRaw } from "../kafka.js";

const port = Number(process.env.PORT ?? 4000);
const shipmentsPerMinute = Number(process.env.SHIPMENTS_PER_MINUTE ?? 20);
if (!Number.isFinite(shipmentsPerMinute) || shipmentsPerMinute < 1) {
  throw new Error("SHIPMENTS_PER_MINUTE debe ser un número mayor o igual a 1");
}
const rate = {
  shipmentsPerMinute,
  failureByZone: {
    "zona-rio": 0.12,
    "zona-centro": 0.08,
    "zona-oeste": 0.2,
    "zona-norte": 0.1,
  },
  duplicateProbability: 0.02,
  outOfOrderProbability: 0.02,
  malformedProbability: 0.01,
  delayedHubProbability: 0.1,
};
let producer;
let sequence = 0;
let randomState = Number(process.env.SEED ?? 712026);

function random() {
  randomState += 0x6D2B79F5;
  let value = randomState;
  value = Math.imul(value ^ (value >>> 15), value | 1);
  value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
  return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
}

function pick(values) {
  return values[Math.floor(random() * values.length)];
}

function createShipment(started) {
  sequence += 1;
  const shipmentId = `RS-${Date.now().toString(36).toUpperCase()}-${sequence.toString().padStart(6, "0")}`;
  const hubId = pick(HUBS);
  const zonaId = pick(ZONES);
  const venceEn = new Date(started.getTime() + 12 * 60 * 60 * 1000).toISOString();
  return {
    shipmentId,
    hubId,
    zonaId,
    repartidorId: `REP-${String(Math.floor(random() * 120) + 1).padStart(3, "0")}`,
    ocurridoEn: started.toISOString(),
    venceEn,
    destinatario: `destinatario-${sequence}@rutasur.local`,
  };
}

function stateEvents(shipment, statuses) {
  let previous = new Date(shipment.ocurridoEn);
  return statuses.map((status, index) => {
    const occurredAt = new Date(previous.getTime() + (index ? 30 * 60 * 1000 : 0));
    previous = occurredAt;
    return createEvent(shipment, status.tipo, index + 1, {
      ocurridoEn: occurredAt.toISOString(),
      intento: status.intento ?? 1,
      ...(status.motivo ? { motivo: status.motivo } : {}),
      venceEn: shipment.venceEn,
      destinatario: shipment.destinatario,
    });
  });
}

async function publishShipment(shipment, statuses, { duplicate = false, outOfOrder = false } = {}) {
  const promise = createEvent(shipment, "PROMESA_ACTUALIZADA", 1, {
    ocurridoEn: shipment.ocurridoEn,
    venceEn: shipment.venceEn,
  });
  await publish(producer, TOPICS.promises, [promise]);

  let events = stateEvents(shipment, statuses);
  if (outOfOrder && events.length > 2) {
    events = [...events.slice(0, 2), events[3], events[2], ...events.slice(4)];
  }
  if (duplicate && events.length > 0) events.splice(2, 0, events[1]);
  for (const event of events) {
    await publish(producer, TOPICS.states, [event]);
    await new Promise((resolve) => setTimeout(resolve, 80));
  }
  return shipment.shipmentId;
}

const failureReasons = [
  ["AUSENTE", 0.6],
  ["DIRECCION_INCORRECTA", 0.2],
  ["RECHAZADO", 0.15],
  ["ZONA_INACCESIBLE", 0.05],
];

function chooseFailureReason() {
  const result = random();
  let cumulative = 0;
  for (const [reason, weight] of failureReasons) {
    cumulative += weight;
    if (result <= cumulative) return reason;
  }
  return "AUSENTE";
}

async function runScenario(name) {
  if (name === "rafaga") {
    const promises = [];
    const events = [];
    const started = simulatedNow();
    for (let index = 0; index < 500; index += 1) {
      const shipment = createShipment(started);
      shipment.ocurridoEn = new Date(started.getTime() + index).toISOString();
      shipment.venceEn = new Date(started.getTime() + 12 * 60 * 60 * 1000).toISOString();
      const promise = createEvent(shipment, "PROMESA_ACTUALIZADA", 1, {
        ocurridoEn: shipment.ocurridoEn,
        venceEn: shipment.venceEn,
      });
      promises.push({ key: shipment.shipmentId, value: JSON.stringify(promise) });
      const isFailure = random() < (rate.failureByZone[shipment.zonaId] ?? 0.1);
      const last = isFailure
        ? { tipo: "ENTREGA_FALLIDA", motivo: chooseFailureReason() }
        : { tipo: "ENTREGADO" };
      for (const event of stateEvents(shipment, [
        { tipo: "CREADO" },
        { tipo: "INGRESADO_HUB" },
        { tipo: "CLASIFICADO" },
        { tipo: "EN_REPARTO" },
        last,
      ])) {
        events.push({ key: shipment.shipmentId, value: JSON.stringify(event) });
      }
    }
    await Promise.all([
      producer.send({ topic: TOPICS.promises, messages: promises }),
      producer.send({ topic: TOPICS.states, messages: events }),
    ]);
    return { escenario: name, envios: 500 };
  }

  const shipment = createShipment(simulatedNow());
  if (name === "feliz") {
    await publishShipment(shipment, [
      { tipo: "CREADO" },
      { tipo: "INGRESADO_HUB" },
      { tipo: "CLASIFICADO" },
      { tipo: "EN_REPARTO" },
      { tipo: "ENTREGADO" },
    ]);
  } else if (name === "ausente") {
    await publishShipment(shipment, [
      { tipo: "CREADO" },
      { tipo: "INGRESADO_HUB" },
      { tipo: "CLASIFICADO" },
      { tipo: "EN_REPARTO" },
      { tipo: "ENTREGA_FALLIDA", motivo: "AUSENTE" },
    ]);
  } else if (name === "demorado") {
    shipment.venceEn = new Date(
      simulatedNow().getTime() + 2 * 60 * 60 * 1000,
    ).toISOString();
    await publishShipment(shipment, [
      { tipo: "CREADO" },
      { tipo: "INGRESADO_HUB" },
      { tipo: "CLASIFICADO" },
    ]);
  } else if (name === "tercer-fallo") {
    const statuses = [
      { tipo: "CREADO" },
      { tipo: "INGRESADO_HUB" },
      { tipo: "CLASIFICADO" },
      { tipo: "EN_REPARTO" },
      { tipo: "ENTREGA_FALLIDA", intento: 1, motivo: "AUSENTE" },
      { tipo: "CLASIFICADO", intento: 2 },
      { tipo: "EN_REPARTO", intento: 2 },
      { tipo: "ENTREGA_FALLIDA", intento: 2, motivo: "AUSENTE" },
      { tipo: "CLASIFICADO", intento: 3 },
      { tipo: "EN_REPARTO", intento: 3 },
      { tipo: "ENTREGA_FALLIDA", intento: 3, motivo: "AUSENTE" },
    ];
    await publishShipment(shipment, statuses);
  } else if (name === "malformado") {
    await publishRaw(producer, TOPICS.states, [{
      key: shipment.shipmentId,
      value: JSON.stringify({
        eventId: randomUUID(),
        shipmentId: shipment.shipmentId,
        tipo: "ENTREGA_FALLIDA",
        ocurridoEn: shipment.ocurridoEn,
        secuencia: "no-es-numero",
      }),
    }]);
    return { escenario: name, shipmentId: shipment.shipmentId };
  } else {
    throw new Error(`Escenario desconocido: ${name}`);
  }
  return { escenario: name, shipmentId: shipment.shipmentId };
}

async function readBody(request) {
  let body = "";
  for await (const chunk of request) body += chunk;
  return body ? JSON.parse(body) : {};
}

async function consumerLags() {
  const admin = (await import("kafkajs")).Kafka;
  const client = new admin({
    clientId: "rutasur-lag-inspector",
    brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  }).admin();
  await client.connect();
  try {
    const groups = await client.listGroups();
    const result = [];
    for (const group of groups.groups) {
      const summary = await client.describeGroups([group.groupId]);
      const offsets = await client.fetchOffsets({ groupId: group.groupId, topic: TOPICS.states });
      const end = await client.fetchTopicOffsets(TOPICS.states);
      const lag = new Map(end.map((item) => [item.partition, BigInt(item.offset)]));
      const consumed = offsets.reduce((sum, item) => (
        sum + (lag.get(item.partition) > BigInt(item.offset) ? lag.get(item.partition) - BigInt(item.offset) : 0n)
      ), 0n);
      result.push({
        groupId: group.groupId,
        state: summary.groups[0]?.state ?? "unknown",
        lag: consumed.toString(),
      });
    }
    return result;
  } finally {
    await client.disconnect();
  }
}

async function start() {
  producer = await connectProducer();
  const server = createServer(async (request, response) => {
    response.setHeader("content-type", "application/json; charset=utf-8");
    try {
      const url = new URL(request.url ?? "/", `http://localhost:${port}`);
      if (request.method === "GET" && url.pathname === "/health") {
        response.end(JSON.stringify({ status: "ok", simulatedNow: simulatedNow().toISOString() }));
        return;
      }
      if (request.method === "GET" && url.pathname === "/config") {
        response.end(JSON.stringify(rate));
        return;
      }
      if (request.method === "PATCH" && url.pathname === "/config") {
        const patch = await readBody(request);
        for (const key of ["duplicateProbability", "outOfOrderProbability", "malformedProbability", "delayedHubProbability"]) {
          if (key in patch) {
            if (typeof patch[key] !== "number" || patch[key] < 0 || patch[key] > 1) {
              throw new Error(`${key} debe estar entre 0 y 1`);
            }
            rate[key] = patch[key];
          }
        }
        if (patch.failureByZone) {
          for (const [zone, probability] of Object.entries(patch.failureByZone)) {
            if (!ZONES.includes(zone) || typeof probability !== "number" || probability < 0 || probability > 1) {
              throw new Error(`Probabilidad inválida para ${zone}`);
            }
            rate.failureByZone[zone] = probability;
          }
        }
        response.end(JSON.stringify(rate));
        return;
      }
      if (request.method === "GET" && url.pathname === "/lag") {
        response.end(JSON.stringify(await consumerLags()));
        return;
      }
      if (request.method === "POST" && url.pathname === "/scenario") {
        const body = await readBody(request);
        response.statusCode = 202;
        response.end(JSON.stringify(await runScenario(body.name)));
        return;
      }
      response.statusCode = 404;
      response.end(JSON.stringify({ error: "No encontrado" }));
    } catch (error) {
      response.statusCode = 400;
      response.end(JSON.stringify({ error: error.message }));
    }
  });

  await new Promise((resolve) => server.listen(port, "0.0.0.0", resolve));
  console.info(JSON.stringify({
    level: "info",
    serviceName: "simulator",
    message: "Simulador iniciado",
    port,
    seed: process.env.SEED ?? 712026,
  }));

  const emit = async () => {
    try {
      const shipment = createShipment(simulatedNow());
      const failure = random() < (rate.failureByZone[shipment.zonaId] ?? 0.1);
      const delayed = random() < rate.delayedHubProbability;
      if (delayed) {
        shipment.venceEn = new Date(
          simulatedNow().getTime() + 2 * 60 * 60 * 1000,
        ).toISOString();
      }
      const statuses = delayed
        ? [{ tipo: "CREADO" }, { tipo: "INGRESADO_HUB" }, { tipo: "CLASIFICADO" }]
        : [
          { tipo: "CREADO" },
          { tipo: "INGRESADO_HUB" },
          { tipo: "CLASIFICADO" },
          { tipo: "EN_REPARTO" },
          failure
            ? { tipo: "ENTREGA_FALLIDA", motivo: chooseFailureReason() }
            : { tipo: "ENTREGADO" },
        ];
      const duplicate = random() < rate.duplicateProbability;
      const outOfOrder = random() < rate.outOfOrderProbability;
      const malformed = random() < rate.malformedProbability;
      if (malformed) {
        await publishRaw(producer, TOPICS.states, [{
          key: shipment.shipmentId,
          value: JSON.stringify({
            eventId: randomUUID(),
            shipmentId: shipment.shipmentId,
            tipo: "ENTREGA_FALLIDA",
            ocurridoEn: shipment.ocurridoEn,
            secuencia: "invalida",
          }),
        }]);
      }
      await publishShipment(shipment, statuses, { duplicate, outOfOrder });
    } catch (error) {
      console.error(JSON.stringify({ level: "error", serviceName: "simulator", message: error.message }));
    }
  };
  const interval = setInterval(emit, Math.max(1000, 60_000 / rate.shipmentsPerMinute));
  await emit();

  const shutdown = async () => {
    clearInterval(interval);
    await new Promise((resolve) => server.close(resolve));
    await producer.disconnect();
    process.exit(0);
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

start().catch((error) => {
  console.error(JSON.stringify({ level: "fatal", serviceName: "simulator", message: error.message }));
  process.exitCode = 1;
});
