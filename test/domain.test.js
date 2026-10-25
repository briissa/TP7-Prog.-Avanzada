import test from "node:test";
import assert from "node:assert/strict";
import { evaluateSlaRules, nextBusinessDay, rescheduleDecision } from "../src/domain.js";
import { stateEventSchema } from "../src/events.js";

const state = {
  eventId: "event-1",
  shipmentId: "shipment-1",
  tipo: "ENTREGA_FALLIDA",
  ocurridoEn: "2026-10-23T10:00:00.000Z",
  secuencia: 5,
  hubId: "hub-sur",
  zonaId: "zona-centro",
  repartidorId: "REP-001",
  version: 1,
  intento: 1,
  motivo: "AUSENTE",
  venceEn: "2026-10-24T10:00:00.000Z",
  destinatario: "destinatario@rutasur.local",
};

test("valida estados y exige motivo en entrega fallida", () => {
  assert.equal(stateEventSchema.safeParse(state).success, true);
  const { motivo, ...invalid } = state;
  assert.equal(stateEventSchema.safeParse(invalid).success, false);
});

test("programa ausencias el siguiente día hábil y alterna franja", () => {
  const result = rescheduleDecision(state);
  assert.equal(result.tipo, "REINTENTO_PROGRAMADO");
  assert.equal(result.franja, "TARDE");
  assert.equal(new Date(result.programadoPara).getUTCDay(), 1);
  assert.equal(nextBusinessDay(new Date("2026-10-23T10:00:00.000Z")).getDay(), 1);
});

test("retiene direcciones incorrectas y devuelve rechazos o tercer intento", () => {
  assert.equal(rescheduleDecision({ ...state, motivo: "DIRECCION_INCORRECTA" }).tipo, "RETENIDO");
  assert.equal(rescheduleDecision({ ...state, motivo: "RECHAZADO" }).tipo, "RETORNO_COMERCIO");
  assert.equal(rescheduleDecision({ ...state, intento: 3, motivo: "AUSENTE" }).motivo, "TERCER_FALLO");
});

test("detecta riesgo sin salida y prioriza vencidos", () => {
  const alerts = evaluateSlaRules({
    tipo: "CLASIFICADO",
    ocurridoEn: "2026-10-24T08:00:00.000Z",
    venceEn: "2026-10-24T11:00:00.000Z",
    now: new Date("2026-10-24T08:00:00.000Z"),
  });
  assert.deepEqual(alerts.map((alert) => alert.prioridad), ["alta"]);
  const overdue = evaluateSlaRules({
    tipo: "CREADO",
    ocurridoEn: "2026-10-23T08:00:00.000Z",
    venceEn: "2026-10-24T07:00:00.000Z",
    now: new Date("2026-10-24T08:00:00.000Z"),
  });
  assert.equal(overdue[0].regla, "vencido");
  assert.equal(overdue[0].prioridad, "critica");
});

test("alerta entrega fallida el día de la promesa y cierra al entregar", () => {
  const failure = evaluateSlaRules({
    tipo: "ENTREGA_FALLIDA",
    ocurridoEn: "2026-10-24T08:00:00.000Z",
    venceEn: "2026-10-24T10:00:00.000Z",
    now: new Date("2026-10-24T08:00:00.000Z"),
  });
  assert.ok(failure.some((alert) => alert.regla === "fallo_en_dia_limite"));
  assert.deepEqual(evaluateSlaRules({
    tipo: "ENTREGADO",
    ocurridoEn: "2026-10-24T08:00:00.000Z",
    venceEn: "2026-10-24T11:00:00.000Z",
    now: new Date("2026-10-24T12:00:00.000Z"),
  }), []);
});
