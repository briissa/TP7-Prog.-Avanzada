import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";

const speed = Number(process.env.SIMULATION_SPEED ?? 120);
if (!Number.isFinite(speed) || speed < 1) {
  throw new Error("SIMULATION_SPEED debe ser un número mayor o igual a 1");
}

const clockPath = process.env.SIMULATION_CLOCK_FILE
  ?? path.join(process.cwd(), ".simulation-clock.json");
let localClock;

function persistLocalClock() {
  mkdirSync(path.dirname(clockPath), { recursive: true });
  writeFileSync(clockPath, JSON.stringify(localClock));
}

function initializeLocalClock(wallAt) {
  if (existsSync(clockPath)) {
    const saved = JSON.parse(readFileSync(clockPath, "utf8"));
    if (!Number.isFinite(saved.simulationAt) || !Number.isFinite(saved.wallAt)) {
      throw new Error(`El archivo de reloj simulado está dañado: ${clockPath}`);
    }
    localClock = {
      simulationAt: saved.simulationAt + Math.max(0, wallAt - saved.wallAt) * speed,
      wallAt,
    };
  } else {
    localClock = { simulationAt: wallAt, wallAt };
  }
}

export function simulatedNow() {
  const wallAt = Date.now();
  if (!localClock) initializeLocalClock(wallAt);
  localClock.simulationAt += Math.max(0, wallAt - localClock.wallAt) * speed;
  localClock.wallAt = wallAt;
  persistLocalClock();
  return new Date(localClock.simulationAt);
}

export async function monitorSimulatedNow(database) {
  const clock = database.collection("sla_clock");
  const wallAt = new Date();
  await clock.updateOne(
    { _id: "rutasur" },
    { $setOnInsert: { simulationAt: wallAt, wallAt, speed } },
    { upsert: true },
  );
  await clock.updateOne(
    { _id: "rutasur" },
    [{
      $set: {
        simulationAt: {
          $add: [
            "$simulationAt",
            { $multiply: [{ $subtract: ["$$NOW", "$wallAt"] }, speed] },
          ],
        },
        wallAt: "$$NOW",
        speed,
      },
    }],
  );
  const current = await clock.findOne({ _id: "rutasur" });
  if (!current?.simulationAt) {
    throw new Error("No se pudo leer el reloj simulado del monitor");
  }
  return current.simulationAt;
}
