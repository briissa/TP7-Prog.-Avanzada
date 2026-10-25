export const TOPICS = {
  states: "envios.estados",
  promises: "envios.promesas",
  reschedules: "envios.reprogramaciones",
  alerts: "alertas.sla",
  notifications: "notificaciones.enviadas",
  deadLetter: "envios.estados.dlq",
};

export const SHIPMENT_STATES = [
  "CREADO",
  "INGRESADO_HUB",
  "CLASIFICADO",
  "EN_REPARTO",
  "ENTREGADO",
  "ENTREGA_FALLIDA",
];

export const ALERT_PRIORITIES = {
  media: 1,
  alta: 2,
  critica: 3,
};

export const HUBS = ["hub-sur", "hub-centro", "hub-norte"];
export const ZONES = ["zona-rio", "zona-centro", "zona-oeste", "zona-norte"];
