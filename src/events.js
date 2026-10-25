import { randomUUID } from "node:crypto";
import { z } from "zod";
import { SHIPMENT_STATES, TOPICS } from "./constants.js";

const common = {
  eventId: z.string().min(1),
  shipmentId: z.string().min(1),
  ocurridoEn: z.string().datetime(),
  secuencia: z.number().int().positive(),
  hubId: z.string().min(1),
  zonaId: z.string().min(1),
  repartidorId: z.string().min(1),
  version: z.number().int().positive(),
};

export const stateEventSchema = z.object({
  ...common,
  tipo: z.enum(SHIPMENT_STATES),
  intento: z.number().int().positive(),
  motivo: z.enum([
    "AUSENTE",
    "DIRECCION_INCORRECTA",
    "RECHAZADO",
    "ZONA_INACCESIBLE",
  ]).optional(),
  venceEn: z.string().datetime(),
  destinatario: z.string().min(1),
}).superRefine((event, context) => {
  if (event.tipo === "ENTREGA_FALLIDA" && !event.motivo) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ["motivo"],
      message: "ENTREGA_FALLIDA requiere un motivo",
    });
  }
});

export const promiseEventSchema = z.object({
  ...common,
  tipo: z.literal("PROMESA_ACTUALIZADA"),
  venceEn: z.string().datetime(),
});

export const rescheduleEventSchema = z.object({
  ...common,
  tipo: z.enum(["REINTENTO_PROGRAMADO", "RETORNO_COMERCIO", "RETENIDO"]),
  intento: z.number().int().positive(),
  motivo: z.string().min(1),
  programadoPara: z.string().datetime().optional(),
  franja: z.enum(["MANANA", "TARDE"]).optional(),
});

export const notificationEventSchema = z.object({
  ...common,
  tipo: z.literal("NOTIFICACION_REGISTRADA"),
  canal: z.literal("SIMULADO"),
  mensaje: z.string().min(1),
});

export const alertEventSchema = z.object({
  ...common,
  tipo: z.literal("ALERTA_SLA"),
  regla: z.enum(["sin_ingreso", "sin_salida", "fallo_en_dia_limite", "vencido"]),
  prioridad: z.enum(["media", "alta", "critica"]),
  estado: z.enum(["abierta", "cerrada"]),
  causa: z.string().min(1),
});

export const schemasByTopic = {
  [TOPICS.states]: stateEventSchema,
  [TOPICS.promises]: promiseEventSchema,
  [TOPICS.reschedules]: rescheduleEventSchema,
  [TOPICS.notifications]: notificationEventSchema,
  [TOPICS.alerts]: alertEventSchema,
};

export function createEvent(shipment, tipo, secuencia, extra = {}) {
  return {
    eventId: randomUUID(),
    shipmentId: shipment.shipmentId,
    tipo,
    ocurridoEn: shipment.ocurridoEn ?? new Date().toISOString(),
    secuencia,
    hubId: shipment.hubId,
    zonaId: shipment.zonaId,
    repartidorId: shipment.repartidorId,
    version: 1,
    ...extra,
  };
}
