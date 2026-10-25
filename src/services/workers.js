import { createEvent } from "../events.js";
import { ALERT_PRIORITIES, TOPICS } from "../constants.js";
import { monitorSimulatedNow } from "../clock.js";
import { isTerminalShipmentEvent, rescheduleDecision, timeWindow, evaluateSlaRules } from "../domain.js";
import {
  connectProducer,
  outboxRecord,
  processOnce,
  startConsumer,
  startOutboxPublisher,
} from "../kafka.js";
import { getDatabase, getMongoClient } from "../db.js";

function alertEvent(shipment, rule, estado) {
  const shipmentId = shipment.shipmentId ?? shipment._id;
  return {
    ...createEvent({
      ...shipment,
      shipmentId,
    }, "ALERTA_SLA", shipment.secuencia ?? shipment.ultimaSecuencia ?? 1, {
      eventId: `${shipmentId}:${rule.regla}:${estado}`,
      regla: rule.regla,
      prioridad: rule.prioridad,
      estado,
      causa: rule.causa,
    }),
    shipmentId,
  };
}

async function projectEvent({ database, event, topic }) {
  await processOnce(database, "proyector", event, async (session) => {
    const shipments = database.collection("envios");
    const current = await shipments.findOne(
      { _id: event.shipmentId },
      { session },
    );

    if (topic === TOPICS.states) {
      if (current && event.secuencia <= (current.ultimaSecuenciaAplicada ?? 0)) {
        return [];
      }
      const update = {
        shipmentId: event.shipmentId,
        estado: event.tipo,
        zonaId: event.zonaId,
        hubId: event.hubId,
        repartidorId: event.repartidorId,
        promesa: { venceEn: new Date(event.venceEn) },
        destinatario: event.destinatario,
        ultimaSecuenciaAplicada: event.secuencia,
        actualizadoEn: new Date(event.ocurridoEn),
      };
      const historyItem = {
        eventId: event.eventId,
        tipo: event.tipo,
        ocurridoEn: new Date(event.ocurridoEn),
        secuencia: event.secuencia,
        intento: event.intento,
        motivo: event.motivo,
      };
      const mutation = {
        $set: update,
        $push: { historial: historyItem },
        $setOnInsert: { creadaEn: new Date(event.ocurridoEn), alertas: [], notificaciones: [] },
      };
      if (event.tipo === "ENTREGA_FALLIDA") {
        mutation.$push.intentos = {
          eventId: event.eventId,
          numero: event.intento,
          motivo: event.motivo,
          ocurridoEn: new Date(event.ocurridoEn),
        };
      }
      await shipments.updateOne(
        { _id: event.shipmentId },
        mutation,
        { upsert: true, session },
      );
    } else if (topic === TOPICS.promises) {
      await shipments.updateOne(
        { _id: event.shipmentId },
        {
          $set: {
            shipmentId: event.shipmentId,
            promesa: { venceEn: new Date(event.venceEn) },
          },
          $setOnInsert: {
            estado: "SIN_EVENTOS",
            historial: [],
            intentos: [],
            alertas: [],
            notificaciones: [],
          },
        },
        { upsert: true, session },
      );
    } else if (topic === TOPICS.reschedules) {
      await shipments.updateOne(
        { _id: event.shipmentId },
        {
          $push: {
            historial: {
              eventId: event.eventId,
              tipo: event.tipo,
              ocurridoEn: new Date(event.ocurridoEn),
              secuencia: event.secuencia,
              motivo: event.motivo,
              programadoPara: event.programadoPara
                ? new Date(event.programadoPara)
                : undefined,
              franja: event.franja,
            },
          },
          $set: { proximaAccion: event.tipo },
          $setOnInsert: {
            shipmentId: event.shipmentId,
            estado: "SIN_EVENTOS",
            intentos: [],
            alertas: [],
            notificaciones: [],
          },
        },
        { upsert: true, session },
      );
    } else if (topic === TOPICS.notifications) {
      await database.collection("notificaciones").updateOne(
        { eventId: event.eventId },
        { $setOnInsert: { ...event, creadoEn: new Date(event.ocurridoEn) } },
        { upsert: true, session },
      );
      await shipments.updateOne(
        { _id: event.shipmentId },
        { $addToSet: { notificaciones: event.eventId } },
        { session },
      );
    } else if (topic === TOPICS.alerts) {
      const id = `${event.shipmentId}:${event.regla}`;
      await database.collection("alertas").updateOne(
        { _id: id },
        {
          $set: {
            shipmentId: event.shipmentId,
            regla: event.regla,
            prioridad: event.prioridad,
            prioridadOrden: ALERT_PRIORITIES[event.prioridad],
            estado: event.estado,
            causa: event.causa,
            actualizadoEn: new Date(event.ocurridoEn),
          },
          $setOnInsert: { creadaEn: new Date(event.ocurridoEn) },
        },
        { upsert: true, session },
      );
      await shipments.updateOne(
        { _id: event.shipmentId },
        { $addToSet: { alertas: id } },
        { session },
      );
    }
    return [];
  });
}

async function handleNotifications({ database, event }) {
  if (event.tipo !== "ENTREGA_FALLIDA") return;
  const notification = createEvent(event, "NOTIFICACION_REGISTRADA", event.secuencia, {
    eventId: `notificacion-${event.eventId}`,
    canal: "SIMULADO",
    mensaje: `El intento ${event.intento} no pudo completarse (${event.motivo}).`,
  });
  await processOnce(database, "notificaciones", event, async (session) => {
    await database.collection("avisos_emitidos").updateOne(
      { eventId: event.eventId },
      { $setOnInsert: { eventId: event.eventId, creadoEn: new Date() } },
      { upsert: true, session },
    );
    return [
      outboxRecord("notificaciones", event, TOPICS.notifications, notification),
    ];
  });
}

async function handleRescheduling({ database, event }) {
  if (event.tipo !== "ENTREGA_FALLIDA") return;
  const decision = rescheduleDecision(event);
  const reschedule = createEvent(event, decision.tipo, event.secuencia, {
    eventId: `reprogramacion-${event.eventId}`,
    intento: event.intento,
    motivo: decision.motivo,
    ...(decision.programadoPara ? { programadoPara: decision.programadoPara } : {}),
    ...(decision.franja ? { franja: decision.franja } : {}),
  });
  await processOnce(database, "reprogramacion", event, async () => [
    outboxRecord("rescheduler", event, TOPICS.reschedules, reschedule),
  ]);
}

async function handleAnalytics({ database, event, topic }) {
  if (topic !== TOPICS.states) return;
  const date = new Date(event.ocurridoEn);
  const dimensions = {
    fecha: date.toISOString().slice(0, 10),
    zonaId: event.zonaId,
    hubId: event.hubId,
    franja: timeWindow(date),
  };

  await processOnce(database, "analitica", event, async (session) => {
    const stateCollection = database.collection("analytics_state");
    const current = await stateCollection.findOne(
      { _id: event.shipmentId },
      { session },
    );
    if (current && event.secuencia <= (current.ultimaSecuencia ?? 0)) return [];

    const increments = {};
    if (event.tipo === "ENTREGA_FALLIDA") {
      increments.fallos = 1;
      increments[`motivos.${event.motivo}`] = 1;
    }
    if (event.intento === 1 && ["ENTREGADO", "ENTREGA_FALLIDA"].includes(event.tipo)) {
      increments.primerIntentoTotal = 1;
      if (event.tipo === "ENTREGADO") increments.primerIntentoExitoso = 1;
    }
    if (isTerminalShipmentEvent(event)) {
      increments.enviosConPromesa = 1;
      if (event.tipo === "ENTREGADO"
          && new Date(event.ocurridoEn).getTime() <= new Date(event.venceEn).getTime()) {
        increments.entregasEnSla = 1;
      }
    }
    if (Object.keys(increments).length > 0) {
      await database.collection("metricas_diarias").updateOne(
        dimensions,
        { $inc: increments, $setOnInsert: dimensions },
        { upsert: true, session },
      );
    }
    await stateCollection.updateOne(
      { _id: event.shipmentId },
      {
        $set: {
          ultimaSecuencia: event.secuencia,
          ...(event.tipo === "INGRESADO_HUB"
            ? { ingresadoEn: new Date(event.ocurridoEn), hubId: event.hubId }
            : {}),
          promesaVenceEn: new Date(event.venceEn),
        },
      },
      { upsert: true, session },
    );
    return [];
  });
}

async function updateMonitorState({ database, event, topic }) {
  await processOnce(database, "monitor-sla", event, async (session) => {
    const states = database.collection("sla_monitor_state");
    const current = await states.findOne({ _id: event.shipmentId }, { session });
    if (topic === TOPICS.states
        && current
        && event.secuencia <= (current.ultimaSecuencia ?? 0)) {
      return [];
    }
    await states.updateOne(
      { _id: event.shipmentId },
      {
        $set: topic === TOPICS.promises
          ? { venceEn: new Date(event.venceEn) }
          : {
            tipo: event.tipo,
            ocurridoEn: new Date(event.ocurridoEn),
            secuencia: event.secuencia,
            hubId: event.hubId,
            zonaId: event.zonaId,
            repartidorId: event.repartidorId,
            venceEn: new Date(event.venceEn),
            ultimaSecuencia: event.secuencia,
          },
      },
      { upsert: true, session },
    );
    return [];
  });
}

async function emitSlaAlerts(database, shipment) {
  const now = await monitorSimulatedNow(database);
  const alertState = database.collection("sla_alert_state");
  if (shipment.tipo === "ENTREGADO") {
    const existing = await alertState.find({
      shipmentId: shipment._id,
      estado: "abierta",
    }).toArray();
    for (const alert of existing) {
      const closedEvent = alertEvent(shipment, alert, "cerrada");
      const session = getMongoClient().startSession();
      try {
        await session.withTransaction(async () => {
          const result = await alertState.updateOne(
            { _id: alert._id, estado: "abierta" },
            { $set: { estado: "cerrada", actualizadoEn: now } },
            { session },
          );
          if (!result.modifiedCount) return;
          await database.collection("outbox").updateOne(
            { _id: `sla-monitor:${closedEvent.eventId}` },
            {
              $setOnInsert: {
                topicOwner: "sla-monitor",
                topic: TOPICS.alerts,
                event: closedEvent,
                createdAt: now,
                sentAt: null,
              },
            },
            { upsert: true, session },
          );
        });
      } finally {
        await session.endSession();
      }
    }
    return;
  }

  if (!shipment.venceEn) return;
  const rules = evaluateSlaRules({
    tipo: shipment.tipo,
    ocurridoEn: shipment.ocurridoEn,
    venceEn: shipment.venceEn,
    now,
  });
  for (const rule of rules) {
    const id = `${shipment._id}:${rule.regla}`;
    const alert = alertEvent(shipment, rule, "abierta");
    const session = getMongoClient().startSession();
    try {
      await session.withTransaction(async () => {
        await alertState.insertOne({
          _id: id,
          shipmentId: shipment._id,
          regla: rule.regla,
          prioridad: rule.prioridad,
          estado: "abierta",
          causa: rule.causa,
          creadaEn: now,
          actualizadoEn: now,
        }, { session });
        await database.collection("outbox").insertOne({
          _id: `sla-monitor:${alert.eventId}`,
          topicOwner: "sla-monitor",
          topic: TOPICS.alerts,
          event: alert,
          createdAt: now,
          sentAt: null,
        }, { session });
      });
    } catch (error) {
      if (error.code !== 11000) throw error;
    } finally {
      await session.endSession();
    }
  }
}

async function startSlaMonitor() {
  const database = await getDatabase();
  const producer = await connectProducer();
  startOutboxPublisher(database, producer, "sla-monitor");
  await startConsumer({
    serviceName: "monitor-sla",
    groupId: "monitor-sla",
    topics: [TOPICS.states, TOPICS.promises],
    handler: async (context) => {
      await updateMonitorState(context);
      const shipment = await database.collection("sla_monitor_state")
        .findOne({ _id: context.event.shipmentId });
      if (shipment) await emitSlaAlerts(database, shipment);
    },
  });
  setInterval(async () => {
    try {
      const states = database.collection("sla_monitor_state");
      const cursor = states.find({ tipo: { $ne: "ENTREGADO" } });
      for await (const shipment of cursor) {
        await emitSlaAlerts(database, shipment);
      }
      const delivered = states.find({ tipo: "ENTREGADO" });
      for await (const shipment of delivered) {
        await emitSlaAlerts(database, shipment);
      }
    } catch (error) {
      console.error(JSON.stringify({ level: "error", serviceName: "monitor-sla", message: error.message }));
    }
  }, 60_000);
}

async function startService(serviceName) {
  const database = await getDatabase();

  if (serviceName === "sla-monitor") {
    await startSlaMonitor();
    return;
  }

  const settings = {
    projector: {
      groupId: "proyector",
      topics: [
        TOPICS.states,
        TOPICS.promises,
        TOPICS.reschedules,
        TOPICS.alerts,
        TOPICS.notifications,
      ],
      handler: projectEvent,
    },
    notifications: {
      groupId: "notificaciones",
      topics: [TOPICS.states],
      handler: handleNotifications,
    },
    rescheduler: {
      groupId: "reprogramacion",
      topics: [TOPICS.states],
      handler: handleRescheduling,
    },
    analytics: {
      groupId: "analitica",
      topics: [TOPICS.states],
      handler: handleAnalytics,
    },
  }[serviceName];

  if (!settings) throw new Error(`Servicio desconocido: ${serviceName}`);
  const producer = await connectProducer();
  if (serviceName === "notifications" || serviceName === "rescheduler") {
    startOutboxPublisher(database, producer, serviceName === "notifications" ? "notificaciones" : "rescheduler");
  }
  if (serviceName === "projector") {
    startOutboxPublisher(database, producer, "dead-letter");
  }
  await startConsumer({
    serviceName,
    groupId: settings.groupId,
    topics: settings.topics,
    handler: (context) => settings.handler({ ...context, producer }),
  });
}

const serviceName = process.argv[2];
if (!serviceName) {
  throw new Error("Uso: node src/services/workers.js <projector|notifications|rescheduler|sla-monitor|analytics>");
}

startService(serviceName).catch((error) => {
  console.error(JSON.stringify({ level: "fatal", serviceName, message: error.message }));
  process.exitCode = 1;
});
