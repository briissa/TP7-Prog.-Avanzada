import { ALERT_PRIORITIES } from "./constants.js";

export function oppositeShift(date) {
  const hour = date.getUTCHours();
  return hour < 12 ? "TARDE" : "MANANA";
}

export function nextBusinessDay(date) {
  const result = new Date(date);
  result.setUTCDate(result.getUTCDate() + 1);
  while (result.getUTCDay() === 0 || result.getUTCDay() === 6) {
    result.setUTCDate(result.getUTCDate() + 1);
  }
  return result;
}

export function rescheduleDecision(event) {
  if (event.intento >= 3) {
    return { tipo: "RETORNO_COMERCIO", motivo: "TERCER_FALLO" };
  }

  switch (event.motivo) {
    case "AUSENTE": {
      const failedAt = new Date(event.ocurridoEn);
      return {
        tipo: "REINTENTO_PROGRAMADO",
        motivo: event.motivo,
        programadoPara: nextBusinessDay(failedAt).toISOString(),
        franja: oppositeShift(failedAt),
      };
    }
    case "DIRECCION_INCORRECTA":
      return { tipo: "RETENIDO", motivo: event.motivo };
    case "RECHAZADO":
      return { tipo: "RETORNO_COMERCIO", motivo: event.motivo };
    default:
      return { tipo: "RETENIDO", motivo: event.motivo };
  }
}

export function evaluateSlaRules({ tipo, ocurridoEn, venceEn, now }) {
  if (tipo === "ENTREGADO") return [];

  const remaining = new Date(venceEn).getTime() - now.getTime();
  const rules = [];
  if (tipo === "CREADO" && remaining < 10 * 60 * 60 * 1000 && remaining >= 0) {
    rules.push({
      regla: "sin_ingreso",
      prioridad: "media",
      causa: "Faltan menos de 10 horas y el envío no ingresó al hub",
    });
  }
  if (!["EN_REPARTO", "ENTREGADO"].includes(tipo)
      && remaining < 4 * 60 * 60 * 1000 && remaining >= 0) {
    rules.push({
      regla: "sin_salida",
      prioridad: "alta",
      causa: "Faltan menos de 4 horas y el envío no está en reparto",
    });
  }
  if (tipo === "ENTREGA_FALLIDA"
      && new Date(ocurridoEn).toISOString().slice(0, 10)
        === new Date(venceEn).toISOString().slice(0, 10)) {
    rules.push({
      regla: "fallo_en_dia_limite",
      prioridad: "alta",
      causa: "La entrega falló el día del vencimiento de la promesa",
    });
  }
  if (now.getTime() > new Date(venceEn).getTime()) {
    rules.push({
      regla: "vencido",
      prioridad: "critica",
      causa: "La promesa venció sin una entrega confirmada",
    });
  }

  return rules.sort(
    (left, right) => ALERT_PRIORITIES[right.prioridad] - ALERT_PRIORITIES[left.prioridad],
  );
}

export function timeWindow(date) {
  return date.getUTCHours() < 12 ? "MANANA" : "TARDE";
}

export function isTerminalShipmentEvent(event) {
  return event.tipo === "ENTREGADO"
    || (event.tipo === "ENTREGA_FALLIDA"
      && (event.motivo !== "AUSENTE" || event.intento >= 3));
}
