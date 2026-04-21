import { sql } from "../config/database.js";

export async function ejecutarConsulta(query, params = {}) {
  const request = new sql.Request();
  for (const key in params) {
    request.input(key, params[key]);
  }
  const result = await request.query(query);
  return result.recordset;
}

export function limpiarValor(valor) {
  return valor ? String(valor).trim() : "";
}

export function parseNumberFromString(v) {
  if (v === null || v === undefined || v === "") return 0;
  if (typeof v === "number") return v;
  const s = String(v).replace(/\$/g, "").replace(/,/g, "").trim();
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

export function obtenerFechaVenezuelaISO() {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en", {
    timeZone: "America/Caracas",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  })
    .formatToParts(now)
    .reduce((acc, p) => {
      acc[p.type] = p.value;
      return acc;
    }, {});
  const { year = "0000", month = "01", day = "01", hour = "00", minute = "00", second = "00" } = parts;
  return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
}
