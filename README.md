# RutaSur · logística orientada a eventos

Implementación local del caso práctico de Programación Avanzada. El proyecto conecta un simulador, Kafka, cinco consumidores Node.js, MongoDB y una interfaz Next.js. Los servicios no se llaman entre sí: publican y consumen eventos; la interfaz consulta las proyecciones MongoDB y recibe cambios por SSE.

## Requisitos

- Docker Desktop con Docker Compose.
- Node.js 22 o superior para ejecutar las pruebas locales.

## Arranque

Desde esta carpeta, ejecutar:

```powershell
docker compose up --build
```

La primera vez se descargan las imágenes y dependencias; puede tardar varios minutos. La aplicación queda disponible en <http://localhost:3100>, el simulador en el puerto `4000`, Kafka en `29092` y MongoDB en `27017`. Para apagar los servicios, usar `Ctrl+C` y luego `docker compose down`. Los datos del replica set se conservan en el volumen `mongo-data`.

Se pueden sobrescribir la velocidad del reloj y el caudal inicial copiando `.env.example` como `.env` antes de arrancar Compose.

El Compose crea Kafka en modo KRaft, los seis topics requeridos, MongoDB como replica set de un nodo, el simulador, los cinco grupos consumidores y Next.js. Next.js solo consulta las vistas MongoDB; las llamadas HTTP del panel al simulador son controles de demo, y el reprocesamiento se ejecuta como proceso independiente de Kafka. La interfaz usa el puerto `3100` y Kafka usa `29092` para conexiones desde la máquina anfitriona, evitando conflictos con servicios comunes en `3000` y `9092`; dentro de Compose, Next.js queda en `3000` y Kafka en `9092`. La aplicación publica los escenarios iniciales automáticamente mientras permanece levantada.

## Recorrido de la demo

1. **Torre de control:** contadores, envíos recientes y alertas prioritarias.
2. **Envío feliz:** abrir `/demo`, ejecutar el escenario y seguir el identificador desde la torre de control hasta `/tracking/<shipmentId>`.
3. **Entrega fallida:** ejecutar **Entrega fallida**; notificaciones y reprogramación se procesan en grupos independientes.
4. **Riesgo de SLA:** ejecutar **Envío demorado**; la promesa cercana activa una alerta alta.
5. **Tercer fallo:** verifica la devolución al comercio.
6. **Aislamiento:** detener, por ejemplo, `notifications` con `docker compose stop notifications`; otros consumidores continúan. El panel de demo muestra el lag. Reiniciar con `docker compose start notifications`.
7. **DLQ:** ejecutar **Evento inválido** y consultar `envios.estados.dlq` en Kafka.
8. **Analítica y replay:** revisar `/analytics`; desde otra terminal ejecutar `docker compose run --rm replay` para añadir el tiempo medio en hub con un grupo nuevo `analitica-replay-*`. Los otros servicios siguen procesando.
9. **Ráfaga:** publica 500 envíos y permite observar acumulación y recuperación.

Para demostrar una reconstrucción completa, detener primero simulador y consumidores para que no publiquen ni apliquen eventos durante la operación:

```powershell
docker compose stop simulator projector notifications rescheduler sla-monitor analytics
docker compose run --rm rebuild
docker compose start projector notifications rescheduler sla-monitor analytics simulator
```

La tarea comprueba que no queden consumidores de los grupos de la aplicación activos, lleva sus offsets al inicio y vacía únicamente las vistas derivadas de MongoDB. El log Kafka no se borra.

## Servicios y topics

| Servicio | Grupo | Topics de entrada | Responsabilidad |
|---|---|---|---|
| Simulador | — | — | Genera promesas, estados y escenarios reproducibles. |
| Proyector | `proyector` | estados, promesas, reprogramaciones, alertas y notificaciones | Mantiene `envios`, `alertas` y `notificaciones`. |
| Notificaciones | `notificaciones` | estados | Emite una notificación simulada por fallo. |
| Reprogramación | `reprogramacion` | estados | Programa ausencias al siguiente día hábil, retiene direcciones incorrectas, devuelve rechazos y todo tercer fallo. Zona inaccesible queda retenida para revisión manual. |
| Monitor SLA | `monitor-sla` | estados y promesas | Evalúa riesgos por evento y cada minuto usando el reloj acelerado. |
| Analítica | `analitica` | estados | Actualiza incrementos diarios e indicadores de primer intento, motivos y SLA. |

Los topics son `envios.estados`, `envios.promesas`, `envios.reprogramaciones`, `alertas.sla`, `notificaciones.enviadas` y `envios.estados.dlq`. Todos particionan por `shipmentId`. Los consumidores confirman manualmente el offset después de persistir; un registro idempotente y transacciones Mongo evitan repetir efectos al recuperarse.

El simulador usa una semilla fija (`712026`), publica 20 envíos por minuto por defecto y acelera el reloj a 120x (una hora simulada equivale a 30 segundos). El reloj del simulador vive en un volumen privado y el monitor conserva su checkpoint de tiempo y reglas en MongoDB, por lo que pueden reiniciarse sin perder el avance. Los escenarios se pueden disparar en cualquier momento desde el panel.

La tasa de fallos por zona y las probabilidades de prueba se pueden consultar y modificar mientras corre la demo:

```powershell
Invoke-RestMethod http://localhost:4000/config
$config = @{ failureByZone = @{ "zona-oeste" = 0.3 }; duplicateProbability = 0.05 } | ConvertTo-Json
Invoke-RestMethod http://localhost:4000/config -Method Patch -ContentType "application/json" -Body $config
```

## Pruebas

```powershell
npm install
npm test
npm run build
```

`npm run build` comprueba el frontend y los módulos de servidor. Los servicios de infraestructura y consumidores se ejecutan mediante Docker Compose.
