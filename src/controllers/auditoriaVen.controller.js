import { sql, getMysqlPool } from "../config/database.js";

import { DateTime } from "luxon";

// Helpers



export const remoteConfig = {
  user: 'profit',
  password: 'profit',
  server: '192.168.4.20',
  port: 1433,
  database: 'CRISTM25',
  options: {
    encrypt: false,
    trustServerCertificate: true
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  },
  connectionTimeout: 300000,
  requestTimeout: 300000
};

const logRequest = (fnName, req) => {
  try {
    const safe = {
      method: req?.method,

      path: req?.path ?? req?.originalUrl,

      params: req?.params ?? null,

      query: req?.query ?? null,

      body: req?.body ?? null,

      headers: req?.headers ?? null,
    };

    console.log(`[${fnName}] recibida: ${JSON.stringify(safe, null, 2)}`);
  } catch (e) {
    console.log(`[${fnName}] error al serializar request: ${e?.message}`);
  }
};

const toNumber = (v) =>
  v === undefined || v === null || v === "" ? null : Number(v);

const toInt = (v) => {
  const n = toNumber(v);

  return n === null ? 0 : Math.round(n);
};

const cleanDate = (d) => {
  if (!d) return null;

  if (typeof d === "string") return d.split("T")[0];

  try {
    return d.toISOString().split("T")[0];
  } catch {
    return null;
  }
};

const cleanString = (v) => (typeof v === "string" ? v.trim() : v);

// ===============================

// POST /vendedores-pagina

// ===============================

export const getVendedoresPagina = async (req, res) => {
  logRequest("getVendedoresPagina", req);

  try {
    const pool = getMysqlPool();

    if (!pool)
      return res

        .status(500)

        .json({ error: "No hay conexi\ufffdn a la base de datos local." });

    const [rows] = await pool.query(
      "SELECT id, nombre, co_ven FROM usuarios WHERE rol = 'vendedor'",
    );

    res.json(rows);
  } catch (error) {
    res

      .status(500)

      .json({
        error: "Error al consultar los vendedores",

        details: error.message,
      });
  }
};

// GET /vendedores-mercado
// Consulta la tabla `vendedores_mercado` en la base de datos local 'app'.
// Query params / body:
//   - usuario: filtra por la columna `usuario` (opcional, exact match)
//   - limit: cantidad máxima de filas a devolver (opcional, por defecto 100)
export const getVendedoresMercado = async (req, res) => {
  logRequest("getVendedoresMercado", req);

  try {
    const pool = getMysqlPool();

    if (!pool)
      return res.status(500).json({ error: "No hay conexión a la base de datos local." });

    // aceptar tanto GET query como POST body
    const usuario = req.query?.usuario ?? req.body?.usuario;
    const limitRaw = req.query?.limit ?? req.body?.limit ?? 100;

    const params = [];
    let sqlQuery = "SELECT * FROM vendedores_mercado";

    if (usuario !== undefined && usuario !== null && String(usuario).trim() !== "") {
      sqlQuery += " WHERE usuario = ?";
      params.push(String(usuario).trim());
    }

    sqlQuery += " ORDER BY id DESC LIMIT ?";
    params.push(Number(limitRaw) || 100);

    const [rows] = await pool.query(sqlQuery, params);

    res.json(rows || []);
  } catch (error) {
    console.error("Error en getVendedoresMercado:", error);
    res.status(500).json({ error: "Error al consultar vendedores_mercado", detalle: error.message });
  }
};

// ===============================

export const getGestionesPorDia = async (req, res) => {
  logRequest("getGestionesPorDia", req);

  try {
    const pool = getMysqlPool();

    if (!pool)
      return res

        .status(500)

        .json({ error: "No hay conexi\ufffdn a la base de datos local." });

    let { anio, mes } = req.query;

    if (!anio || !mes) {
      const venezuelaNow = DateTime.now().setZone("America/Caracas");

      anio = venezuelaNow.year;

      mes = venezuelaNow.month;
    } else {
      anio = parseInt(anio, 10);

      mes = parseInt(mes, 10);
    }

    const [rows] = await pool.query(
      `SELECT usuario_id, fecha_registro, COUNT(*) AS cantidad

       FROM gestiones

       WHERE YEAR(fecha_registro) = ? AND MONTH(fecha_registro) = ?

       GROUP BY usuario_id, fecha_registro

       ORDER BY fecha_registro ASC, usuario_id ASC`,

      [anio, mes],
    );

    res.json(rows);
  } catch (error) {
    res

      .status(500)

      .json({
        error: "Error al consultar las gestiones por d\ufffda",

        details: error.message,
      });
  }
};

// ===============================

// POST /segmentos

// ===============================

export const getSegmentos = async (req, res) => {
  logRequest("getSegmentos", req);

  try {
    const request = new sql.Request();

    const result = await request.query(
      "SELECT co_seg, seg_des FROM segmento ORDER BY co_seg",
    );

    const rows = (result.recordset || []).map((r) => ({
      co_seg: cleanString(r.co_seg),

      seg_des: cleanString(r.seg_des),
    }));

    res.json(rows);
  } catch (err) {
    console.error("Error en getSegmentos:", err);

    res

      .status(500)

      .json({ error: "Error al obtener segmentos", detalle: err.message });
  }
};

// ===============================

// POST /zonas

// ===============================

export const getZonas = async (req, res) => {
  logRequest("getZonas", req);

  try {
    const request = new sql.Request();

    const result = await request.query(
      "SELECT co_zon, zon_des FROM zona ORDER BY co_zon",
    );

    const rows = (result.recordset || []).map((r) => ({
      co_zon: cleanString(r.co_zon),

      zon_des: cleanString(r.zon_des),
    }));

    res.json(rows);
  } catch (err) {
    console.error("Error en getZonas:", err);

    res

      .status(500)

      .json({ error: "Error al obtener zonas", detalle: err.message });
  }
};

// ===============================

// POST /kpi-vendedores

// ===============================

export const insertarKpiVendedor = async (req, res) => {
  logRequest("insertarKpiVendedor", req);

  try {
    const pool = getMysqlPool();

    if (!pool)
      return res

        .status(500)

        .json({ error: "No hay conexi\ufffdn a la base de datos local." });

    // Permitir recibir { configuracin: {...} } o un array de ellos

    let payload = req.body;

    if (payload.configuración) payload = payload.configuración;

    const entries = Array.isArray(payload) ? payload : [payload];

    if (!entries.length)
      return res.status(400).json({ error: "No se recibieron registros." });

    const columns = [
      "co_ven",

      "nombre",

      "rol",

      "ruta",

      "segmento",

      "comision",

      "comisionPorDia",

      "valorVehiculo",

      "valorMeta",

      "valorCarteraAct",

      "valorClientesRec",

      "valorSku",

      "valorGrupoNeg",

      "clientesConve",

      "valorCobranza",

      "diaLunes",

      "diaMartes",

      "diaMiercoles",

      "diaJueves",

      "diaViernes",

      "diaSabado",

      "totVisitas",

      "total",

      "fecha",

      "vehiculo",

      "metaVentas",

      "metaCarteraAct",

      "metaClientesRec",

      "metaSku",

      "metaGrupoNeg",

      "metaClientesConve",

      "metaCobranza",

      "metaSaldoVencidoInicio",

      "valorSaldoVencidoInicio",

      "pctSaldoVencidoInicio",
    ];

    const columnsWithId = ["id", ...columns];

    const placeholdersWithId = columnsWithId.map(() => "?").join(", ");

    const insertSqlWithId = `INSERT INTO kpi_vendedores (${columnsWithId.join(
      ", ",
    )}) VALUES (${placeholdersWithId})`;

    const parseDay = (d, key, alt) => {
      const v = d && (d[key] ?? (alt ? d[alt] : undefined));

      if (v === undefined || v === null || v === "") return 0;

      const n = Number(v);

      return isNaN(n) ? 0 : n;
    };

    const results = [];

    for (const itemOrig of entries) {
      try {
        const item = { ...itemOrig };

        // Mapeo robusto para campos alternativos

        item.valorCarteraAct =
          item.valorCarteraActiva ?? item.valorCarteraAct ?? 0;

        item.metaCarteraAct =
          item.metaCarteraActiva ?? item.metaCarteraAct ?? 0;

        item.clientesConve = item.valorClientesConve ?? item.clientesConve ?? 0;

        item.metaClientesConve =
          item.metaClientesConve ?? item.metaClientesConve ?? 0;

        // ...resto del c\ufffddigo...

        const {
          co_ven = null,

          nombre,

          rol = null,

          ruta = null,

          segmento = null,

          comision,

          comisionPorDia,

          valorVehiculo,

          valorMeta,

          valorCarteraAct,

          valorClientesRec,

          valorSku,

          valorGrupoNeg,

          clientesConve,

          valorCobranza,

          dias = {},

          totVisitas,

          total,

          fecha: fechaRaw,

          vehiculo,

          metaVentas,

          metaCarteraAct,

          metaClientesRec,

          metaSku,

          metaGrupoNeg,

          metaClientesConve,

          metaCobranza,

          metaSaldoVencidoInicio,

          valorSaldoVencidoInicio,

          pctSaldoVencidoInicio,
        } = item || {};

        if (!nombre) {
          results.push({ success: false, error: "Campo requerido: nombre" });

          continue;
        }

        let fecha = new Date();

        if (fechaRaw) {
          const tmp = new Date(fechaRaw);

          if (!isNaN(tmp)) fecha = tmp;
        }

        const clientesConveVal = (() => {
          const v = clientesConve;

          if (v === undefined || v === null || v === "") return 0;

          const n = Number(v);

          return Number.isFinite(n) ? Number(n.toFixed(2)) : 0;
        })();

        const params = [
          co_ven ? String(co_ven).trim() : null,

          String(nombre).trim(),

          rol ?? null,

          ruta ?? null,

          segmento ?? null,

          toNumber(comision),

          toNumber(comisionPorDia),

          toNumber(valorVehiculo),

          toNumber(valorMeta),

          toNumber(valorCarteraAct),

          toNumber(valorClientesRec),

          toNumber(valorSku),

          toNumber(valorGrupoNeg),

          clientesConveVal,

          toNumber(valorCobranza),

          parseDay(dias, "Lunes"),

          parseDay(dias, "Martes"),

          parseDay(dias, "Mi\ufffdrcoles", "Miercoles"),

          parseDay(dias, "Jueves"),

          parseDay(dias, "Viernes"),

          parseDay(dias, "S\ufffdbado", "Sabado"),

          toNumber(totVisitas),

          toNumber(total),

          fecha,

          vehiculo ? String(vehiculo).trim() : null,

          toNumber(metaVentas),

          toNumber(metaCarteraAct),

          toNumber(metaClientesRec),

          toNumber(metaSku),

          toNumber(metaGrupoNeg),

          toNumber(metaClientesConve),

          toNumber(metaCobranza),

          toNumber(metaSaldoVencidoInicio),

          toNumber(valorSaldoVencidoInicio),

          toNumber(pctSaldoVencidoInicio),
        ];

        const conn = await pool.getConnection();

        try {
          await conn.beginTransaction();

          // 1. Verificar si existe registro (co_ven, fecha)

          // params[0] = co_ven, params[23] = fecha

          console.log(
            `\U0001f50d Buscando registro existente: co_ven="${params[0]}", fecha="${params[23]}"`,
          );

          const [existing] = await conn.query(
            "SELECT * FROM kpi_vendedores WHERE co_ven = ? AND DATE(fecha) = DATE(?) FOR UPDATE",

            [params[0], params[23]],
          );

          console.log(
            `\u2713 Registros encontrados:`,
            existing.length,
            existing.length > 0 ? existing[0] : "ninguno",
          );

          if (existing.length > 0) {
            // UPDATE: solo modificar campos que realmente cambiaron

            const existingRow = existing[0];

            // Construir objeto entrante con los nombres de columnas

            const incoming = Object.fromEntries(
              columns.map((c, i) => [c, params[i]]),
            );

            const isDate = (v) =>
              v instanceof Date ||
              (!isNaN(Date.parse(v)) && typeof v === "string");

            const valuesEqual = (a, b) => {
              if (a === null || a === undefined) a = null;

              if (b === null || b === undefined) b = null;

              if (a === b) return true;

              // Date comparison

              if (isDate(a) && isDate(b)) {
                try {
                  const ta = new Date(a).getTime();

                  const tb = new Date(b).getTime();

                  return ta === tb;
                } catch {}
              }

              // Numeric comparison

              const na =
                typeof a === "string" && a.trim() !== "" && !isNaN(Number(a))
                  ? Number(a)
                  : typeof a === "number"
                    ? a
                    : null;

              const nb =
                typeof b === "string" && b.trim() !== "" && !isNaN(Number(b))
                  ? Number(b)
                  : typeof b === "number"
                    ? b
                    : null;

              if (na !== null && nb !== null) return Number(na) === Number(nb);

              // String comparison (trim, case sensitive)

              if (typeof a === "string" || typeof b === "string") {
                const sa = a === null ? null : String(a).trim();

                const sb = b === null ? null : String(b).trim();

                return sa === sb;
              }

              // Fallback strict

              return a === b;
            };

            const changedCols = [];

            const changedVals = [];

            for (let i = 0; i < columns.length; i++) {
              const col = columns[i];

              const incomingVal = incoming[col];

              const existingVal = existingRow[col];

              if (!valuesEqual(existingVal, incomingVal)) {
                changedCols.push(`${col} = ?`);

                changedVals.push(incomingVal);
              }
            }

            if (changedCols.length === 0) {
              // No hay cambios: no hacemos UPDATE

              await conn.commit();

              conn.release();

              results.push({
                id: existingRow.id,

                success: true,

                affectedRows: 0,

                action: "NOOP",

                message: "Registro existe y no hubo cambios",
              });
            } else {
              const updateSql = `UPDATE kpi_vendedores SET ${changedCols.join(", ")} WHERE id = ?`;

              const [resUpd] = await conn.query(updateSql, [
                ...changedVals,
                existingRow.id,
              ]);

              await conn.commit();

              conn.release();

              results.push({
                id: existingRow.id,

                success: true,

                affectedRows: resUpd.affectedRows,

                action: "UPDATE",

                updatedColumns: changedCols.map((s) => s.split(" = ")[0]),
              });
            }
          } else {
            // INSERT

            const [mxRows] = await conn.query(
              "SELECT COALESCE(MAX(id), 0) AS mx FROM kpi_vendedores",
            );

            const newId =
              (mxRows && mxRows[0] && mxRows[0].mx ? Number(mxRows[0].mx) : 0) +
              1;

            const paramsWithId = [newId, ...params];

            const [result] = await conn.query(insertSqlWithId, paramsWithId);

            await conn.commit();

            conn.release();

            results.push({
              id: newId,

              success: true,

              affectedRows: result.affectedRows ?? 0,

              insertId: newId,

              action: "INSERT",
            });
          }
        } catch (txErr) {
          try {
            await conn.rollback();
          } catch {}

          conn.release();

          throw txErr;
        }
      } catch (errItem) {
        console.error("Error procesando item kpi-vendedores:", errItem);

        results.push({ id: null, success: false, error: errItem.message });
      }
    }

    const insertedCount = results.filter((r) => r && r.success).length;

    if (insertedCount > 0) {
      return res

        .status(200)

        .json({ success: true, inserted: insertedCount, processed: results });
    }

    return res

      .status(400)

      .json({
        success: false,

        inserted: 0,

        processed: results,

        error: "No se insert\ufffd ning\ufffdn registro",
      });
  } catch (err) {
    console.error("Error en insertarKpiVendedor:", err);

    res.status(500).json({ error: err.message });
  }
};

// ===============================

// GET /kpi-vendedores

// ===============================

export const getKpiVendedores = async (req, res) => {
  logRequest("getKpiVendedores", req);

  try {
    const pool = getMysqlPool();

    if (!pool)
      return res

        .status(500)

        .json({ error: "No hay conexi\ufffdn a la base de datos local." });

    const {
      id,

      nombre,

      segmento,

      fecha,

      fecha_from,

      fecha_to,

      limit = 100,
    } = req.query || {};

    // Construir filtros comunes
    const where = [];
    const params = [];

    // Si solicitan por id, mantenemos comportamiento cl\ufffdsico (filtrar por id)
    if (id) {
      where.push("id = ?");
      params.push(id);
      // Consulta directa por id
      var sqlQuery =
        "SELECT * FROM kpi_vendedores WHERE id = ? ORDER BY fecha DESC LIMIT ?";
      params.push(Number(limit) || 100);
    } else {
      if (nombre) {
        where.push("nombre LIKE ?");
        params.push(`%${nombre}%`);
      }
      if (segmento) {
        where.push("segmento = ?");
        params.push(segmento);
      }
      if (fecha) {
        where.push("DATE(fecha) = ?");
        params.push(fecha);
      } else if (fecha_from && fecha_to) {
        where.push("DATE(fecha) BETWEEN ? AND ?");
        params.push(fecha_from, fecha_to);
      } else if (fecha_from) {
        where.push("DATE(fecha) >= ?");
        params.push(fecha_from);
      } else if (fecha_to) {
        where.push("DATE(fecha) <= ?");
        params.push(fecha_to);
      }

      // Queremos \ufffdnicamente el \ufffdltimo registro por vendedor (co_ven)
      const subWhere = where.length ? ` WHERE ${where.join(" AND ")}` : "";
      const subParams = [...params];

      const subQuery = `SELECT co_ven, MAX(fecha) AS max_fecha FROM kpi_vendedores${subWhere} GROUP BY co_ven`;

      sqlQuery = `SELECT t.* FROM kpi_vendedores t INNER JOIN (${subQuery}) m ON t.co_ven = m.co_ven AND t.fecha = m.max_fecha ORDER BY t.fecha DESC LIMIT ?`;
      // params para la consulta final: primero los params usados en subconsulta, luego LIMIT
      params.length = 0; // limpia params, usaremos subParams + limit
      params.push(...subParams);
      params.push(Number(limit) || 100);
    }

    const [rows] = await pool.query(sqlQuery, params);

    const numericKeys = [
      "metaVentas",

      "metaClientesAct",

      "metaClientesRec",

      "metaSku",

      "metaGrupoNeg",

      "metaClientesConve",

      "metaCobranza",

      "comision",

      "comisionPorDia",

      "valorVehiculo",

      "valorMeta",

      "valorClientesAct",

      "valorClientesRec",

      "valorSku",

      "valorGrupoNeg",

      "clientesConve",

      "valorCobranza",
    ];

    const cleaned = (rows || []).map((r) => {
      const obj = Object.fromEntries(
        Object.keys(r).map((k) => [k, cleanString(r[k])]),
      );

      const dias = {
        Lunes: toInt(obj.diaLunes),

        Martes: toInt(obj.diaMartes),

        Miércoles: toInt(obj.diaMiercoles),

        Jueves: toInt(obj.diaJueves),

        Viernes: toInt(obj.diaViernes),

        Sábado: toInt(obj.diaSabado),
      };

      numericKeys.forEach((k) => {
        if (obj[k] !== undefined) obj[k] = toNumber(obj[k]);
      });

      delete obj.diaLunes;

      delete obj.diaMartes;

      delete obj.diaMiercoles;

      delete obj.diaJueves;

      delete obj.diaViernes;

      delete obj.diaSabado;

      obj.dias = dias;

      return obj;
    });

    res.json(cleaned);
  } catch (err) {
    console.error("Error en getKpiVendedores:", err);

    res

      .status(500)

      .json({ error: "Error al obtener kpi_vendedores", detalle: err.message });
  }
};

// ===============================

// GET /kpi-metas

// ===============================

export const getKpiMetas = async (req, res) => {
  logRequest("getKpiMetas", req);

  try {
    const pool = getMysqlPool();

    if (!pool)
      return res

        .status(500)

        .json({ error: "No hay conexi\ufffdn a la base de datos local." });

    // Fechas en formato Profit (YYYYMMDD)

    const startRaw =
      req.query?.startDate ??
      req.body?.startDate ??
      req.query?.fechaInicio ??
      req.body?.fechaInicio;

    const endRaw =
      req.query?.endDate ??
      req.body?.endDate ??
      req.query?.fechaFin ??
      req.body?.fechaFin;

    // Detectar si el usuario proporcion\ufffd fechas expl\ufffdcitamente

    const userProvidedStart = Boolean(
      req.query?.startDate ??
      req.body?.startDate ??
      req.query?.fechaInicio ??
      req.body?.fechaInicio,
    );

    const userProvidedEnd = Boolean(
      req.query?.endDate ??
      req.body?.endDate ??
      req.query?.fechaFin ??
      req.body?.fechaFin,
    );

    // Parsear fechas con Luxon en la zona de Caracas para evitar shifts por timezone
    let startDate = null;
    let endDate = null;
    if (startRaw) {
      const dt = DateTime.fromISO(String(startRaw), {
        zone: "America/Caracas",
      });
      if (dt.isValid) startDate = dt.startOf("day").toJSDate();
    }
    if (endRaw) {
      const dt2 = DateTime.fromISO(String(endRaw), { zone: "America/Caracas" });
      if (dt2.isValid) endDate = dt2.startOf("day").toJSDate();
    }
    if (startDate && !endDate) endDate = startDate;
    if (!startDate && endDate) startDate = endDate;
    if (!startDate || !endDate) {
      // Por defecto, mes actual

      const venezuelaNow = DateTime.now().setZone("America/Caracas");

      const year = venezuelaNow.year;

      const month = venezuelaNow.month;

      const pad = (n) => (n < 10 ? `0${n}` : `${n}`);

      startDate = new Date(`${year}-${pad(month)}-01`);

      endDate = new Date(
        month === 12 ? `${year + 1}-01-01` : `${year}-${pad(month + 1)}-01`,
      );
    }

    // Si el usuario envi\ufffd ambas fechas, tratamos la fechaFin como inclusiva
    // NOTA: no sumamos un d\ufffda aqu\ufffd porque la consulta MSSQL ya aplica DATEADD(DAY, 1, @fechaFin).
    // Esto evita incluir un d\ufffda extra por duplicar el ajuste de fecha.
    // (Se elimin\ufffd el endDate = new Date(endDate.getTime() + 24 * 60 * 60 * 1000);)

    // Profit: YYYYMMDD
    const formatProfitDate = (d) =>
      DateTime.fromJSDate(d).setZone("America/Caracas").toFormat("yyyyLLdd");
    const fechaInicioProfit = formatProfitDate(startDate);
    const fechaFinProfit = formatProfitDate(endDate);

    // Log temporal para debugging de fechas

    console.log(
      `[getKpiMetas] fechaInicioProfit=${fechaInicioProfit}, fechaFinProfit=${fechaFinProfit}, userProvidedStart=${userProvidedStart}, userProvidedEnd=${userProvidedEnd}`,
    );

    // Filtros adicionales

    const { id, segmento, nombre, co_ven, limit = 100 } = req.query || {};

    const where = [];

    const params = [];

    if (id) {
      where.push("id = ?");

      params.push(id);
    }

    if (co_ven) {
      where.push("co_ven = ?");

      params.push(co_ven);
    }

    if (segmento) {
      where.push("segmento = ?");

      params.push(segmento);
    }

    if (nombre) {
      where.push("nombre LIKE ?");

      params.push(`%${nombre}%`);
    }

    const finalSql = where.length
      ? `SELECT * FROM kpi_vendedores WHERE ${where.join(
          " AND ",
        )} ORDER BY fecha DESC LIMIT ?`
      : `SELECT * FROM kpi_vendedores ORDER BY fecha DESC LIMIT ?`;

    params.push(Number(limit) || 100);

    const [rows] = await pool.query(finalSql, params);

    const coVens = Array.from(
      new Set((rows || []).map((r) => cleanString(r.co_ven)).filter(Boolean)),
    );

    // Si no hay vendedores, responde vac\ufffdo

    if (coVens.length === 0) return res.json([]);

    // Prepara IN para MSSQL

    const inList = coVens.map((_, i) => `@p${i}`).join(",");

    const request = new sql.Request();

    coVens.forEach((cv, i) => request.input(`p${i}`, sql.VarChar(100), cv));

    request.input("fechaInicio", sql.VarChar(8), fechaInicioProfit);

    request.input("fechaFin", sql.VarChar(8), fechaFinProfit);

    // START cobrado_dia fix
    let startCobrado = null;
    let endCobrado = null;
    let startDateCobrado =
      req.body.startDateCobrado || req.query.startDateCobrado;
    let endDateCobrado = req.body.endDateCobrado || req.query.endDateCobrado;

    if (startDateCobrado) {
      const dt = DateTime.fromISO(String(startDateCobrado), {
        zone: "America/Caracas",
      });
      if (dt.isValid) startCobrado = dt.toJSDate();
    }

    if (endDateCobrado) {
      const dt2 = DateTime.fromISO(String(endDateCobrado), {
        zone: "America/Caracas",
      });
      if (dt2.isValid) endCobrado = dt2.toJSDate();
    }

    const fechaInicioCobradoProfit = startCobrado
      ? formatProfitDate(startCobrado)
      : fechaInicioProfit;
    const fechaFinCobradoProfit = endCobrado
      ? formatProfitDate(endCobrado)
      : fechaFinProfit;

    request.input(
      "fechaInicioCobrado",
      sql.VarChar(8),
      fechaInicioCobradoProfit,
    );
    request.input("fechaFinCobrado", sql.VarChar(8), fechaFinCobradoProfit);
    // END cobrado_dia fix

    // ===============================
    // CONSULTA PEDIDOS (APP)
    // ===============================
    // Filtrar por fechaCobrado (si existe) o fecha general
    const startPedidos = startCobrado || startDate;
    const endPedidos = endCobrado || endDate;

    // Formato MySQL YYYY-MM-DD
    const fIniMysql = DateTime.fromJSDate(startPedidos)
      .setZone("America/Caracas")
      .toFormat("yyyy-MM-dd");
    const fFinMysql = DateTime.fromJSDate(endPedidos)
      .setZone("America/Caracas")
      .toFormat("yyyy-MM-dd");

    const placeholdersMysql = coVens.map(() => "?").join(",");
    const pedidosQuery = `
      SELECT cod_prov, COUNT(*) as total_pedidos
      FROM pedidos
      WHERE cod_prov IN (${placeholdersMysql})
        AND DATE(created_at) >= ?
        AND DATE(created_at) <= ?
      GROUP BY cod_prov
    `;

    const [pedidosRows] = await pool.query(pedidosQuery, [
      ...coVens,
      fIniMysql,
      fFinMysql,
    ]);
    const pedidosPorCoVen = new Map();
    for (const r of pedidosRows) {
      pedidosPorCoVen.set(
        cleanString(r.cod_prov),
        Number(r.total_pedidos) || 0,
      );
    }

    // ===============================

    // Consultar neto_facturas_total (mes actual y anteriores con saldo)

    // ===============================

    const mssqlNetoMesQuery = `

      SELECT f.co_ven, SUM(CASE WHEN TRY_CAST(f.tasa AS FLOAT) IS NULL OR TRY_CAST(f.tasa AS FLOAT) = 0 THEN 0 ELSE f.tot_neto / TRY_CAST(f.tasa AS FLOAT) END) AS neto_mes

      FROM factura f

      WHERE f.co_ven IN (${inList})

        AND f.anulada = 0

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) >= @fechaInicio

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) <= @fechaFin

      GROUP BY f.co_ven

    `;

    const netoMesResult = await request.query(mssqlNetoMesQuery);

    const netoMesPorCoVen = new Map();

    for (const r of netoMesResult.recordset || []) {
      const key = cleanString(r.co_ven);

      netoMesPorCoVen.set(key, Number(r.neto_mes) || 0);
    }

    const mssqlNetoAntQuery = `

      SELECT f.co_ven, SUM(CASE WHEN TRY_CAST(f.tasa AS FLOAT) IS NULL OR TRY_CAST(f.tasa AS FLOAT) = 0 THEN 0 ELSE f.tot_neto / TRY_CAST(f.tasa AS FLOAT) END) AS neto_ant

      FROM factura f

      WHERE f.co_ven IN (${inList})

        AND f.anulada = 0

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) < @fechaInicio

        AND ISNULL(f.saldo, 0) > 0

      GROUP BY f.co_ven

    `;

    const netoAntResult = await request.query(mssqlNetoAntQuery);

    const netoAntPorCoVen = new Map();

    for (const r of netoAntResult.recordset || []) {
      const key = cleanString(r.co_ven);

      netoAntPorCoVen.set(key, Number(r.neto_ant) || 0);
    }

    // Consulta agregada para ventas, clientes activos y SKUs \ufffdnicos

    const mssqlQuery = `

      SELECT

        f.co_ven,

        SUM(CASE WHEN TRY_CAST(f.tasa AS FLOAT) IS NULL OR TRY_CAST(f.tasa AS FLOAT) = 0 THEN 0 ELSE ISNULL(f.saldo, 0) / TRY_CAST(f.tasa AS FLOAT) END) AS ventas_sum,

        COUNT(DISTINCT LTRIM(RTRIM(f.co_cli))) AS clientes_activos,

        COUNT(DISTINCT LTRIM(RTRIM(rf.co_art))) AS skus_unicos

      FROM factura f

      INNER JOIN reng_fac rf ON rf.fact_num = f.fact_num

      WHERE f.co_ven IN (${inList})

        AND f.anulada = 0

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) >= @fechaInicio

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) <= @fechaFin

      GROUP BY f.co_ven

    `;

    const result = await request.query(mssqlQuery);

    // Consulta ventas_factura_metas: tot_neto por vendedor (usar la f\ufffdrmula indicada por el usuario)

    const mssqlMetasQuery = `

      SELECT

        f.co_ven,

        v.ven_des,

        ROUND(SUM((f.tot_neto - f.iva) / NULLIF(f.tasa,0)), 2) AS tot_neto

      FROM factura f

      JOIN vendedor v ON f.co_ven = v.co_ven

      WHERE f.co_ven IN (${inList})

        AND f.anulada = 0

        AND f.fec_emis >= CONVERT(datetime, @fechaInicio, 112)

        AND f.fec_emis <  DATEADD(DAY, 1, CONVERT(datetime, @fechaFin, 112))

      GROUP BY f.co_ven, v.ven_des

    `;

    const metasResult = await request.query(mssqlMetasQuery);

    // DEBUG: si se solicita ?debug=metas devolver el recordset y los par\ufffdmetros usados

    if (req.query && String(req.query.debug) === "metas") {
      return res.json({
        fechaInicioProfit,

        fechaFinProfit,

        inList,

        mssqlMetasQuery: mssqlMetasQuery,

        metasRaw: metasResult.recordset || [],
      });
    }

    // Mapear resultados

    const ventasPorCoVen = new Map();

    const clientesActivosPorCoVen = new Map();

    const skusUnicosPorCoVen = new Map();

    for (const r of result.recordset || []) {
      const key = cleanString(r.co_ven);

      ventasPorCoVen.set(key, Number(r.ventas_sum) || 0);

      clientesActivosPorCoVen.set(key, Number(r.clientes_activos) || 0);

      skusUnicosPorCoVen.set(key, Number(r.skus_unicos) || 0);
    }

    // Mapear resultados de ventas_factura_metas

    const metasPorCoVen = new Map();

    for (const r of metasResult.recordset || []) {
      const key = cleanString(r.co_ven);

      metasPorCoVen.set(key, {
        ven_des: cleanString(r.ven_des),

        tot_neto: Number(r.tot_neto) || 0,
      });
    }

    // Consultar clientes recuperados

    const mssqlRecQuery = `

      SELECT

        f.co_ven,

        COUNT(DISTINCT LTRIM(RTRIM(f.co_cli))) AS clientes_recuperados

      FROM factura f

      WHERE f.co_ven IN (${inList})

        AND f.anulada = 0

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) >= @fechaInicio

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) <= @fechaFin

      GROUP BY f.co_ven

    `;

    const recResult = await request.query(mssqlRecQuery);

    const clientesRecuperadosPorCoVen = new Map();

    for (const r of recResult.recordset || []) {
      const key = cleanString(r.co_ven);

      clientesRecuperadosPorCoVen.set(key, Number(r.clientes_recuperados) || 0);
    }

    // Consultar clientes con convenio (los que tienen comentario en la tabla clientes)

    const mssqlConveQuery = `

      SELECT

        f.co_ven,

        COUNT(DISTINCT LTRIM(RTRIM(f.co_cli))) AS clientes_convenio

      FROM factura f

      INNER JOIN clientes c ON c.co_cli = f.co_cli

      WHERE f.co_ven IN (${inList})

        AND f.anulada = 0

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) >= @fechaInicio

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) <= @fechaFin

        AND ISNULL(LTRIM(RTRIM(CAST(c.comentario AS VARCHAR(MAX)))), '') <> ''

      GROUP BY f.co_ven

    `;

    const conveResult = await request.query(mssqlConveQuery);

    const clientesConvePorCoVen = new Map();

    for (const r of conveResult.recordset || []) {
      const key = cleanString(r.co_ven);

      clientesConvePorCoVen.set(key, Number(r.clientes_convenio) || 0);
    }

    // Consulta de clientes_new (clientes registrados en el mes en curso)
    const mssqlClientesNewQuery = `
      SELECT
        c.co_ven,
        COUNT(*) AS clientes_new
      FROM clientes c
      WHERE c.co_ven IN (${inList})
        AND MONTH(c.fecha_reg) = MONTH(GETDATE())
        AND YEAR(c.fecha_reg) = YEAR(GETDATE())
      GROUP BY c.co_ven
    `;

    const clientesNewResult = await request.query(mssqlClientesNewQuery);
    const clientesNewPorCoVen = new Map();
    for (const r of clientesNewResult.recordset || []) {
      clientesNewPorCoVen.set(
        cleanString(r.co_ven),
        Number(r.clientes_new) || 0,
      );
    }

    // Consultar NEGOCIACION (reg. de factura donde campo2 = vendedor)

    const mssqlNegociacionQuery = `

      SELECT

        LTRIM(RTRIM(f.campo2)) AS co_ven,

        COUNT(*) AS negociacion

      FROM factura f

      WHERE LTRIM(RTRIM(f.campo2)) IN (${inList})

        AND f.anulada = 0

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) >= @fechaInicio

        AND CONVERT(VARCHAR(8), f.fec_emis, 112) <= @fechaFin

      GROUP BY LTRIM(RTRIM(f.campo2))

    `;

    const negociacionResult = await request.query(mssqlNegociacionQuery);

    const negociacionPorCoVen = new Map();

    for (const r of negociacionResult.recordset || []) {
      const key = cleanString(r.co_ven);

      negociacionPorCoVen.set(key, Number(r.negociacion) || 0);
    }

    // ===============================

    // Consultar saldo_clientes_activos

    // ===============================

    // Sumatoria de saldo de factura, con saldo mayor a 0, tomando la fecha de vencimiento desde el origen,

    // y sumar todas hasta el 31 del mes actual (fechaFin), las que vencen para el siguiente mes no tomarlas.

    const mssqlSaldoActivosQuery = `

      SELECT

        f.co_ven,

        ROUND(SUM(CASE WHEN TRY_CAST(f.tasa AS FLOAT) IS NULL OR TRY_CAST(f.tasa AS FLOAT) = 0 THEN 0 ELSE ISNULL(f.saldo, 0) / TRY_CAST(f.tasa AS FLOAT) END), 2) AS saldo_activos

      FROM factura f

      WHERE f.co_ven IN (${inList})

        AND f.anulada = 0

        AND ISNULL(f.saldo, 0) > 0

        AND CONVERT(VARCHAR(8), f.fec_venc, 112) >= @fechaInicio

        AND CONVERT(VARCHAR(8), f.fec_venc, 112) <= @fechaFin

      GROUP BY f.co_ven

    `;

    const saldoActivosResult = await request.query(mssqlSaldoActivosQuery);

    const saldoClientesActivosPorCoVen = new Map();

    for (const r of saldoActivosResult.recordset || []) {
      const key = cleanString(r.co_ven);

      saldoClientesActivosPorCoVen.set(key, Number(r.saldo_activos) || 0);
    }

    // ===============================

    // 1. SALDO VENCIDO INICIO DE MES

    // ===============================

    // Suma del saldo de facturas donde fec_venc es antes del d\ufffda 01 del mes actual

    // Solo facturas con saldo > 0 y anulada = 0, dividido entre tasa

    const mssqlSaldoVencidoInicioQuery = `

      SELECT

        f.co_ven,

        ROUND(SUM(CASE WHEN TRY_CAST(f.tasa AS FLOAT) IS NULL OR TRY_CAST(f.tasa AS FLOAT) = 0 THEN 0 ELSE ISNULL(f.saldo, 0) / TRY_CAST(f.tasa AS FLOAT) END), 2) AS saldo_vencido_inicio

      FROM factura f

      WHERE f.co_ven IN (${inList})

        AND f.anulada = 0

        AND ISNULL(f.saldo, 0) > 0

        AND f.fec_venc < CONVERT(datetime, @fechaInicio, 112)

      GROUP BY f.co_ven

    `;

    const saldoVencidoInicioResult = await request.query(
      mssqlSaldoVencidoInicioQuery,
    );

    const saldoVencidoInicioPorCoVen = new Map();

    for (const r of saldoVencidoInicioResult.recordset || []) {
      const key = cleanString(r.co_ven);

      saldoVencidoInicioPorCoVen.set(key, Number(r.saldo_vencido_inicio) || 0);
    }

    // ===============================

    // 2. NETO DE LAS FACTURAS DEL MES EN TR\ufffdNSITO

    // ===============================

    // tot_neto de facturas cuyo fec_venc cae dentro del rango consultado (del 01 al \ufffdltimo d\ufffda)

    const mssqlNetoMesTransitoQuery = `

      SELECT

        f.co_ven,

        CAST(SUM(f.tot_neto / NULLIF(TRY_CAST(f.tasa AS FLOAT), 0)) AS DECIMAL(18,2)) AS neto_mes_transito

      FROM factura f

      WHERE f.co_ven IN (${inList})

        AND f.anulada = 0

        AND TRY_CAST(f.fec_venc AS DATETIME) >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)

        AND TRY_CAST(f.fec_venc AS DATETIME) <= DATEADD(SECOND, 86399, CAST(EOMONTH(GETDATE()) AS DATETIME))

      GROUP BY f.co_ven

      ORDER BY f.co_ven

    `;

    const netoMesTransitoResult = await request.query(
      mssqlNetoMesTransitoQuery,
    );

    const netoMesTransitoPorCoVen = new Map();

    for (const r of netoMesTransitoResult.recordset || []) {
      const key = cleanString(r.co_ven);

      netoMesTransitoPorCoVen.set(key, Number(r.neto_mes_transito) || 0);
    }

    // ===============================

    // 3. SALDO DE LAS FACTURAS DEL MES

    // ===============================

    // Suma del saldo de todas las facturas del mes consultado y de meses anteriores

    // Solo facturas con saldo > 0, anulada = 0, dividido entre tasa

    const mssqlSaldoFacturasMesQuery = `

      SELECT

        f.co_ven,

        ROUND(SUM(CASE WHEN TRY_CAST(f.tasa AS FLOAT) IS NULL OR TRY_CAST(f.tasa AS FLOAT) = 0 THEN 0 ELSE ISNULL(f.saldo, 0) / TRY_CAST(f.tasa AS FLOAT) END), 2) AS saldo_facturas_mes

      FROM factura f

      WHERE f.co_ven IN (${inList})

        AND f.anulada = 0

        AND ISNULL(f.saldo, 0) > 0

        AND f.fec_venc < DATEADD(DAY, 1, CONVERT(datetime, @fechaFin, 112))

      GROUP BY f.co_ven

    `;

    const saldoFacturasMesResult = await request.query(
      mssqlSaldoFacturasMesQuery,
    );

    const saldoFacturasMesPorCoVen = new Map();

    for (const r of saldoFacturasMesResult.recordset || []) {
      const key = cleanString(r.co_ven);

      saldoFacturasMesPorCoVen.set(key, Number(r.saldo_facturas_mes) || 0);
    }

    const mssqlCobradoDiaQuery = `
      SELECT 
          c.co_ven,
          CAST(
              SUM(TRY_CAST(cob.campo2 AS FLOAT) / NULLIF(TRY_CAST(cob.campo3 AS FLOAT), 0)) 
          AS DECIMAL(18,2)) AS cobrado_dia
      FROM clientes AS c
      INNER JOIN cobros AS cob ON c.co_cli = cob.co_cli
      WHERE c.co_ven IN (${inList})
          AND cob.anulado = 0
          AND cob.campo2 IS NOT NULL 
          AND cob.campo3 IS NOT NULL 
          AND TRY_CAST(cob.campo3 AS FLOAT) <> 0
          AND CONVERT(VARCHAR(8), cob.fec_cob, 112) >= @fechaInicioCobrado
          AND CONVERT(VARCHAR(8), cob.fec_cob, 112) <= @fechaFinCobrado
      GROUP BY c.co_ven
    `;

    const cobradoDiaResult = await request.query(mssqlCobradoDiaQuery);
    const cobradoDiaPorCoVen = new Map();
    for (const r of cobradoDiaResult.recordset || []) {
      cobradoDiaPorCoVen.set(cleanString(r.co_ven), Number(r.cobrado_dia) || 0);
    }

    // ===============================
    // Consultar pedidos por vendedor en la base de datos 'app' (local mysql)
    // Filtra por fecha (created_at) usando las fechas de cobrado
    // Y ahora tambien CONSULTAR VISITAS (gestiones)
    // ===============================
    
    let mapPedidosPorCoVen = new Map();
    let mapVisitasPorCoVen = new Map();
    let horasPorCoVen = new Map(); 

    if (coVens.length > 0) {
      const placeholdersM = coVens.map(() => "?").join(",");
      const startParaMysql = startCobrado || startDate;
      const endParaMysql = endCobrado || endDate;
      
      // Fecha actual para filtrar EXCLUSIVAMENTE el día de hoy
      const actualNowInVenezuela = DateTime.now().setZone("America/Caracas");
      const todayMysql = actualNowInVenezuela.toFormat("yyyy-MM-dd");
      const todayMysqlStart = todayMysql + ' 00:00:00';
      const todayMysqlEnd = todayMysql + ' 23:59:59';

      // Importante: toFormat('yyyy-MM-dd')
      const fechaIniMysql = DateTime.fromJSDate(startParaMysql)
        .setZone("America/Caracas")
        .toFormat("yyyy-MM-dd");
      const fechaFinMysql = DateTime.fromJSDate(endParaMysql)
        .setZone("America/Caracas")
        .toFormat("yyyy-MM-dd");
    
      // Log para debug
      console.log(`[getKpiMetas] Filtro Fechas MySQL: ${fechaIniMysql} a ${fechaFinMysql}`);
      console.log(`[getKpiMetas] Filtro 'HOY' para visitas/horas: ${todayMysql}`);

      // 1. Pedidos (Mantiene rango de fechas seleccionado por el usuario o por defecto)
      const pedidosQueryFix = `
        SELECT cod_prov as cod_ven, COUNT(*) AS total_pedidos
        FROM pedidos
        WHERE cod_prov IN (${placeholdersM})
          AND created_at >= ?
          AND created_at <= ?
        GROUP BY cod_prov
      `;
      // Ajuste para incluir todo el día final: 'YYYY-MM-DD 23:59:59'
      const fechaFinMysqlFull = fechaFinMysql + ' 23:59:59';
      const fechaIniMysqlFull = fechaIniMysql + ' 00:00:00';

      try {
        const [pedidosRows] = await pool.query(pedidosQueryFix, [
          ...coVens,
          fechaIniMysqlFull,
          fechaFinMysqlFull,
        ]);

        for (const r of pedidosRows) {
          mapPedidosPorCoVen.set(
            cleanString(r.cod_ven),
            Number(r.total_pedidos) || 0,
          );
        }
      } catch (error) {
        console.error("Error consultando pedidos en MySQL:", error);
      }

      // 2. Visitas (gestiones) y Horas - Lógica corregida basada en getGestiones
      // Se obtienen todas las gestiones de hoy y se procesan en JS para deduplicar y mapear correctamente
      try {
        // A. Obtener usuarios para mapear id -> co_ven
        const [usersRows] = await pool.query("SELECT id, co_ven FROM usuarios WHERE co_ven IS NOT NULL");
        const userIdToCoVen = new Map();
        usersRows.forEach(u => { 
            userIdToCoVen.set(u.id, cleanString(u.co_ven)); 
        });

        // B. Consultar gestiones 'de hoy' (usando fecha_registro para coincidir con getGestiones)
        const gestionesQuery = `SELECT * FROM gestiones WHERE fecha_registro >= ? AND fecha_registro <= ?`;
        const [gestionesRows] = await pool.query(gestionesQuery, [todayMysqlStart, todayMysqlEnd]);
        
        console.log(`[getKpiMetas] Gestiones del día encontradas: ${gestionesRows.length}`);

        // C. Deduplicación (lógica tomada de getGestiones)
        const seen = new Set();
        const gestionesUnicas = gestionesRows.filter((row) => {
             const coCliStr = row.co_cli == null ? "" : String(row.co_cli).trim();
             let fechaMomentIso = "";
             const fechaFuente = row.fecha_registro || row.fecha || null;
             
             if (fechaFuente) {
                try {
                  // Intentamos normalizar a minuto en zona CCS
                  let d = DateTime.fromJSDate(new Date(fechaFuente)).setZone("America/Caracas");
                  if (!d.isValid) {
                    // Fallback para string SQL
                    d = DateTime.fromSQL(String(fechaFuente), { zone: "America/Caracas" });
                  }
                  
                  if (d.isValid) {
                      fechaMomentIso = d.startOf("minute").toISO();
                  } else {
                      fechaMomentIso = String(fechaFuente);
                  }
                } catch (e) {
                   fechaMomentIso = String(fechaFuente);
                }
             }
             
             const key = row.gestion_id
               ? `g:${String(row.gestion_id).trim()}`
               : `u:${row.usuario_id || ""}|c:${coCliStr}|f:${fechaMomentIso}`;
             
             if (seen.has(key)) return false;
             seen.add(key);
             return true;
        });

        console.log(`[getKpiMetas] Gestiones únicas tras deduplicar: ${gestionesUnicas.length}`);

        // D. Contar visitas y calcular horas por vendedor
        for (const g of gestionesUnicas) {
            const coVen = userIdToCoVen.get(g.usuario_id);
            if (!coVen) continue; // Si el usuario no tiene co_ven, no cuenta para KPI
            
            // Contar Visita
            mapVisitasPorCoVen.set(coVen, (mapVisitasPorCoVen.get(coVen) || 0) + 1);

            // Calcular Horas (min/max)
            const fechaEvento = g.fecha || g.fecha_registro;
            if (fechaEvento) {
                const dt = DateTime.fromJSDate(new Date(fechaEvento)).setZone("America/Caracas");
                if (dt.isValid) {
                    const timeStr = dt.toFormat('HH:mm:ss');
                    if (!horasPorCoVen.has(coVen)) {
                        horasPorCoVen.set(coVen, { min: timeStr, max: timeStr });
                    } else {
                        const entry = horasPorCoVen.get(coVen);
                        if (timeStr < entry.min) entry.min = timeStr;
                        if (timeStr > entry.max) entry.max = timeStr;
                    }
                }
            }
        }

      } catch (error) {
          console.error("Error procesando gestiones (visitas/horas) en MySQL:", error);
      }
    } 

   const numericKeys = [
      "metaVentas",
      "metaClientesAct",
      "metaClientesRec",
      "metaSku",
      "metaGrupoNeg",
      "metaClientesConve",
      "metaCobranza",
      "comision",
      "comisionPorDia",
      "valorVehiculo",
      "valorMeta",
      "valorClientesAct",
      "valorClientesRec",
      "valorSku",
      "valorGrupoNeg",
      "clientesConve",
      "valorCobranza",
      "metaSaldoVencidoInicio",
    ];

    // Mapear resultado final

    const cleaned = (rows || []).map((r) => {
      const obj = Object.fromEntries(
        Object.keys(r).map((k) => [k, cleanString(r[k])]),
      );

      // Usando el co_ven original
      const coVenKey = cleanString(obj.co_ven);

      const metas = metasPorCoVen.get(coVenKey) || {};

      obj.ventas_factura_sum = metas.tot_neto || 0;

      obj.clientes_activos_factura = clientesActivosPorCoVen.get(coVenKey) || 0;

      obj.skus_unicos_factura = skusUnicosPorCoVen.get(coVenKey) || 0;

      obj.clientes_recuperados = clientesRecuperadosPorCoVen.get(coVenKey) || 0;

      obj.clientes_convenio = clientesConvePorCoVen.get(coVenKey) || 0;

      obj.clientes_new = clientesNewPorCoVen.get(coVenKey) || 0;

      obj.ven_des = metas.ven_des || null;

      obj.negociacion = negociacionPorCoVen.get(coVenKey) || 0;

      obj.saldo_clientes_activos =
        saldoClientesActivosPorCoVen.get(coVenKey) || 0;

      // neto_facturas_total = neto_mes + neto_ant

      const netoMes = netoMesPorCoVen.get(coVenKey) || 0;

      const netoAnt = netoAntPorCoVen.get(coVenKey) || 0;

      obj.neto_facturas_total = netoMes + netoAnt;

      // Nuevos campos agregados

      obj.saldo_vencido_inicio_mes =
        saldoVencidoInicioPorCoVen.get(coVenKey) || 0;

      obj.neto_facturas_mes_transito =
        netoMesTransitoPorCoVen.get(coVenKey) || 0;

      obj.saldo_facturas_mes = saldoFacturasMesPorCoVen.get(coVenKey) || 0;

      obj.cobrado_dia = cobradoDiaPorCoVen.get(coVenKey) || 0;

      // PEDIDOS (APP)
      obj.pedidos_app = mapPedidosPorCoVen.get(coVenKey) || 0;
      
      // Visitas corregidas
      obj.visitas_app = mapVisitasPorCoVen.get(coVenKey) || 0;

      // Horas (si se desean devolver, agregar aquí)
      const horas = horasPorCoVen.get(coVenKey);
      if (horas) {
          obj.hora_1_ven = horas.min;
          obj.hora_2_ven = horas.max;
      } else {
          obj.hora_1_ven = null;
          obj.hora_2_ven = null;
      }

      numericKeys.forEach((k) => {
        if (obj[k] !== undefined) obj[k] = toNumber(obj[k]);
      });

      return obj;
    });

    res.json(cleaned);
  } catch (err) {
    res

      .status(500)

      .json({
        error: "Error al obtener metas de kpi_vendedores",

        detalle: err.message,
      });
  }
};

export const getGestionesConPromedioHoras = async (req, res) => {
  logRequest("getGestionesConPromedioHoras", req);

  try {
    const pool = getMysqlPool();

    if (!pool)
      return res

        .status(500)

        .json({ error: "No hay conexi\ufffdn a la base de datos local." });

    const {
      fecha_from = null,
      fecha_to = null,
      returnDetails = false,
    } = req.query || {};

    // Obtener la cantidad mensual de gestiones por usuario
    const refDateForMonth = fecha_to || fecha_from;
    let monthlyQuery = "";
    let monthlyParams = [];
    if (refDateForMonth) {
      monthlyQuery = `SELECT usuario_id, COUNT(*) AS total_mensual FROM gestiones WHERE YEAR(fecha_registro) = YEAR(?) AND MONTH(fecha_registro) = MONTH(?) GROUP BY usuario_id`;
      monthlyParams = [refDateForMonth, refDateForMonth];
    } else {
      monthlyQuery = `SELECT usuario_id, COUNT(*) AS total_mensual FROM gestiones WHERE YEAR(fecha_registro) = YEAR(CURDATE()) AND MONTH(fecha_registro) = MONTH(CURDATE()) GROUP BY usuario_id`;
      monthlyParams = [];
    }
    const [monthlyRows] = await pool.query(monthlyQuery, monthlyParams);
    const monthlyMap = new Map();
    for (const r of monthlyRows) {
      monthlyMap.set(Number(r.usuario_id), Number(r.total_mensual));
    }

    // Obtener las gestiones agrupadas por usuario y d\ufffda, con segundos entre primer y \ufffdltimo registro
    const [innerRows] = await pool.query(
      `SELECT 
         usuario_id,
         DATE(fecha_registro) AS dia,
         COUNT(*) AS gestiones_dia,
         TIMESTAMPDIFF(SECOND, MIN(fecha_registro), MAX(fecha_registro)) AS segundos,
         MIN(fecha_registro) AS min_fecha,
         MAX(fecha_registro) AS max_fecha
       FROM gestiones
       WHERE (? IS NULL OR DATE(fecha_registro) >= ?)
         AND (? IS NULL OR DATE(fecha_registro) <= ?)
       GROUP BY usuario_id, DATE(fecha_registro)
       ORDER BY usuario_id, DATE(fecha_registro)`,
      [fecha_from, fecha_from, fecha_to, fecha_to],
    );

    const map = new Map();
    for (const r of innerRows) {
      const uid = Number(r.usuario_id);
      let diaStr = r.dia
        ? r.dia instanceof Date
          ? r.dia.toISOString().split("T")[0]
          : String(r.dia).split("T")[0]
        : null;
      const gestionesDia = Number(r.gestiones_dia || 0);
      const segundos = Number(r.segundos || 0);
      const horasDia = segundos / 3600;

      if (!map.has(uid)) {
        map.set(uid, {
          usuario_id: uid,
          total_gestiones: 0,
          dias_trabajados: 0,
          sum_horas_por_dia: 0,
          dias: [],
        });
      }
      const entry = map.get(uid);
      entry.total_gestiones += gestionesDia;
      entry.dias_trabajados += 1;
      entry.sum_horas_por_dia += horasDia;
      entry.dias.push({
        dia: diaStr,
        gestiones: gestionesDia,
        horas: Number(horasDia.toFixed(4)),
        min_fecha: r.min_fecha ?? null,
        max_fecha: r.max_fecha ?? null,
      });
    }

    const userIds = Array.from(map.keys());
    let usersMap = new Map();
    if (userIds.length > 0) {
      const placeholders = userIds.map(() => "?").join(",");
      const [usersRows] = await pool.query(
        `SELECT id, nombre, rol FROM usuarios WHERE id IN (${placeholders})`,
        userIds,
      );
      for (const u of usersRows) {
        usersMap.set(Number(u.id), {
          nombre: cleanString(u.nombre),
          rol: cleanString(u.rol),
        });
      }
    }

    const output = Array.from(map.values()).map((e) => {
      const avg =
        e.dias_trabajados > 0 ? e.sum_horas_por_dia / e.dias_trabajados : 0;
      const userInfo = usersMap.get(e.usuario_id) || {
        nombre: null,
        rol: null,
      };
      const base = {
        usuario_id: e.usuario_id,
        nombre: userInfo.nombre,
        rol: userInfo.rol,
        total_gestiones: monthlyMap.get(e.usuario_id) || 0,
        dias_trabajados: e.dias_trabajados,
        avg_horas_por_dia: Number(avg.toFixed(4)),
      };
      if (returnDetails) base.detalles = e.dias;
      return base;
    });

    res.json(output);
  } catch (err) {
    console.error("Error en getGestionesConPromedioHoras:", err);

    res

      .status(500)

      .json({
        error: "Error al obtener gestiones y promedio de horas",

        detalle: err.message,
      });
  }
};

export const registrarComisionVendedor = async (req, res) => {
  logRequest("registrarComisionVendedor", req);

  try {
    const pool = getMysqlPool();

    if (!pool)
      return res
        .status(500)
        .json({ error: "No hay conexi\ufffdn a la base de datos local." });

    const data = req.body;

    if (!data || !data.vendedor_id) {
      return res
        .status(400)
        .json({ error: "Falta vendedor_id en el cuerpo del request" });
    }

    const {
      vendedor_id,

      inputs = {},

      totales = {},

      calculos_detalle = {},

      fecha_calculo,
    } = data;

    const { base_nacional = 0, base_casa = 0, porcentajes_pago = {} } = inputs;

    const {
      visitas = 0,
      vehiculo = 0,
      meta = 0,
      cartera = 0,
      recup = 0,

      sku = 0,
      neg = 0,
      conv = 0,
      cob = 0,
    } = porcentajes_pago;

    const {
      promedio_efectividad = 0,

      total_pagar = 0,

      diferencia_efectividad = 0,
    } = totales;

    const {
      comisiones_generales = {},

      montos_base = {},

      pagos_individuales = {},
    } = calculos_detalle;

    const {
      nacional = 0,
      casa = 0,
      dif_nacional = 0,
      dif_casa = 0,
    } = comisiones_generales;

    // Montos base

    const {
      visitas: monto_visitas = 0,

      vehiculo: monto_vehiculo = 0,

      meta: monto_meta = 0,

      cartera: monto_cartera = 0,

      recup: monto_recup = 0,

      sku: monto_sku = 0,

      neg: monto_neg = 0,

      conv: monto_conv = 0,

      cob: monto_cob = 0,
    } = montos_base;

    // Pagos individuales

    const {
      visitas: pago_visitas = 0,

      vehiculo: pago_vehiculo = 0,

      meta: pago_meta = 0,

      cartera: pago_cartera = 0,

      recup: pago_recup = 0,

      sku: pago_sku = 0,

      neg: pago_neg = 0,

      conv: pago_conv = 0,

      cob: pago_cob = 0,
    } = pagos_individuales;

    const insertSql = `

      INSERT INTO comisiones_vendedor (

        vendedor_id, base_nacional, base_casa,

        porcentaje_visitas, porcentaje_vehiculo, porcentaje_meta, porcentaje_cartera, porcentaje_recup, porcentaje_sku, porcentaje_neg, porcentaje_conv, porcentaje_cob,

        promedio_efectividad, total_pagar, diferencia_efectividad,

        comision_nacional, comision_casa, dif_nacional, dif_casa,

        monto_visitas, monto_vehiculo, monto_meta, monto_cartera, monto_recup, monto_sku, monto_neg, monto_conv, monto_cob,

        pago_visitas, pago_vehiculo, pago_meta, pago_cartera, pago_recup, pago_sku, pago_neg, pago_conv, pago_cob,

        fecha_calculo, json_original

      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, ?)

    `;

    const params = [
      vendedor_id,

      base_nacional,
      base_casa,

      visitas,
      vehiculo,
      meta,
      cartera,
      recup,
      sku,
      neg,
      conv,
      cob,

      promedio_efectividad,
      total_pagar,
      diferencia_efectividad,

      nacional,
      casa,
      dif_nacional,
      dif_casa,

      monto_visitas,
      monto_vehiculo,
      monto_meta,
      monto_cartera,
      monto_recup,
      monto_sku,
      monto_neg,
      monto_conv,
      monto_cob,

      pago_visitas,
      pago_vehiculo,
      pago_meta,
      pago_cartera,
      pago_recup,
      pago_sku,
      pago_neg,
      pago_conv,
      pago_cob,

      fecha_calculo ? new Date(fecha_calculo) : new Date(),

      JSON.stringify(data),
    ];

    await pool.query(insertSql, params);

    res.json({ success: true });
  } catch (err) {
    console.error("Error en registrarComisionVendedor:", err);

    res.status(500).json({ error: err.message });
  }
};

// ===============================

// GET/POST /matrix-excel-datos

// Consulta la tabla auditoria.dbo.matrix_excel_datos en SQL Server

// Opcional: ?limit=100 para limitar resultados

// ===============================

export const getMatrixExcelDatos = async (req, res) => {
  logRequest("getMatrixExcelDatos", req);

  try {
    const limitParam = req.query.limit || req.body?.limit;
    const limit = limitParam ? parseInt(limitParam, 10) : null;

    // Filtro por usuario (requerido o opcional según tu lógica)
    const usuarioParam = req.query.usuario || req.body?.usuario;
    const usuario = usuarioParam != null ? String(usuarioParam).trim() : null;

    const pool = getMysqlPool();

    if (!pool) {
      return res.status(500).json({ error: "MySQL no conectado" });
    }

    // Construir consulta sobre app.excel_data_potencial con las columnas definidas
    let query = `
      SELECT
        id,
        ciudad,
        cod_ejecutiva,
        ejecutiva,
        potencial,
        farmacias,
        mesual_unid,
        mesual_crist,
        peso_empresa,
        vendedor,
        usuario,
        created_at
      FROM app.excel_data_potencial
    `;

    const params = [];

    // Filtro por usuario (exact match, case-insensitive)
    if (usuario) {
      query += ` WHERE LOWER(TRIM(usuario)) = ?`;
      params.push(usuario.toLowerCase());
    }

    if (Number.isInteger(limit) && limit > 0) {
      query += " LIMIT ?";
      params.push(limit);
    }

    // Log para depuración
    console.log("[getMatrixExcelDatos] SQL:", query, "PARAMS:", params);

    const [rows] = await pool.query(query, params);

    return res.json({ total: (rows || []).length, rows: rows || [] });

  } catch (err) {
    console.error("Error en getMatrixExcelDatos (MySQL):", err);
    return res.status(500).json({
      error: "Error al consultar excel_data_potencial",
      detalle: err.message,
    });
  }
};
// ===============================
// GET /matrix-excel-datos
// Recibe segmento_bitrix_excel en req.query o req.body
// Filtra excel_data_potencial por vendedor_bitrix
// ===============================
export const getExcelDataPotencial = async (req, res) => {
  logRequest("getExcelDataPotencial", req);

  let mssqlPool = null;

  try {
    let raw_cod = req.body.cod_profit_vendedor || req.query.cod_profit_vendedor;

    if (!raw_cod) {
      return res
        .status(400)
        .json({ error: "Falta el parametro cod_profit_vendedor" });
    }

    // --- LIMPIEZA DEL CÓDIGO ---
    // Convertimos a número para quitar ceros a la izquierda y luego a string
    const cod_profit_vendedor = String(Number(raw_cod));

    // ── 1. MySQL: obtener farmacias del vendedor ──────────────────────────────
    const pool = getMysqlPool();
    if (!pool) {
      return res
        .status(500)
        .json({ error: "No hay conexión a la base de datos local." });
    }

    // Usamos TRIM en el campo de la DB para asegurar la coincidencia
    const query = `
      SELECT *
      FROM auditoria.farmacia_datos
      WHERE TRIM(user_app) = ?
    `;
    const [farmacias] = await pool.query(query, [cod_profit_vendedor]);

    if (farmacias.length === 0) {
      return res.json([]);
    }

    // ── 2. SQL Server: buscar último fec_emis por codigo_profit ──────────────
    mssqlPool = await sql.connect(remoteConfig);

    const result = await Promise.all(
      farmacias.map(async (farmacia) => {
        const codigoProfit = (farmacia.codigo_profit || '').trim();

        if (!codigoProfit) {
          return { ...farmacia, ultima_fec_emis: null };
        }

        const sqlResult = await mssqlPool
          .request()
          .input('co_cli', sql.VarChar, codigoProfit)
          .query(`
            SELECT TOP 1
              CONVERT(VARCHAR(10), fec_emis, 120) AS fec_emis
            FROM factura
            WHERE LTRIM(RTRIM(co_cli)) = @co_cli
            ORDER BY fec_emis DESC
          `);

        const ultimaFecEmis =
          sqlResult.recordset.length > 0
            ? sqlResult.recordset[0].fec_emis
            : null;

        return { ...farmacia, ultima_fec_emis: ultimaFecEmis };
      })
    );

    return res.json(result);

  } catch (error) {
    console.error("Error en getExcelDataPotencial:", error);
    return res
      .status(500)
      .json({ error: "Error interno del servidor", detalle: error.message });
  } finally {
    if (mssqlPool) {
      await mssqlPool.close();
    }
  }
};

/**
 * Endpoint GET para devolver los datos de comisiones almacenados.
 * Permite filtrar por co_ven o devolver todos (opcional paginación si se requiere).
 * GET /comisiones-vendedor
 * Query params opcionales: co_ven, startDate, endDate
 */
export const getComisionesVendedores = async (req, res) => {
  try {
    const { co_ven, startDate, endDate } = req.query; // Filtros opcionales
    const pool = getMysqlPool();
    if (!pool) {
      return res
        .status(500)
        .json({ error: "No hay conexi\ufffdn a la base de datos" });
    }

    let sql = "SELECT * FROM comisiones_vendedor";
    const params = [];
    const where = [];

    if (co_ven) {
      where.push("vendedor_id = ?"); // Corregido: co_ven -> vendedor_id seg\ufffdn la tabla
      params.push(String(co_ven).trim());
    }

    if (startDate) {
      where.push("fecha_calculo >= ?"); // Corregido: fecha -> fecha_calculo
      params.push(String(startDate).trim());
    }

    if (endDate) {
      where.push("fecha_calculo <= ?"); // Corregido: fecha -> fecha_calculo
      params.push(String(endDate).trim());
    }

    if (where.length > 0) {
      sql += " WHERE " + where.join(" AND ");
    }

    sql += " ORDER BY id DESC, fecha_calculo DESC"; // Corregido: fecha -> fecha_calculo

    const [rows] = await pool.execute(sql, params);

    res.json(rows);
  } catch (err) {
    console.error("Error en getComisionesVendedores:", err);
    res
      .status(500)
      .json({ error: "Error al consultar comisiones", detalle: err.message });
  }
};
// Nuevo endpoint GET/POST /vendedores-rutas
export const getVendedoresRutas = async (req, res) => {
  logRequest("getVendedoresRutas", req);

  try {
    const pool = getMysqlPool();
    if (!pool) return res.status(500).json({ error: "No hay conexión a la base de datos local." });

    // aceptar co_ven por query (GET) o body (POST)
    const co_ven = req.query?.co_ven ?? req.body?.co_ven;

    let sql = 'SELECT * FROM `vendedores_rutas`';
    const params = [];

    if (co_ven && String(co_ven).trim() !== "") {
      sql += ' WHERE `co_ven` = ?';
      params.push(String(co_ven).trim());
    }

    sql += ' ORDER BY `vendedor` ASC';

    let conn;
    try {
      conn = await pool.getConnection();
      const [rows] = await conn.execute(sql, params);
      return res.json({ success: true, total: (rows || []).length, data: rows || [] });
    } catch (err) {
      console.error('[rutas] Error consulta:', err);
      return res.status(500).json({ error: err?.message || 'Error interno del servidor.' });
    } finally {
      if (conn) try { conn.release(); } catch (e) { /* ignore */ }
    }
  } catch (err) {
    console.error('Error en getVendedoresRutas:', err);
    return res.status(500).json({ error: err?.message || 'Error interno del servidor.' });
  }
};

// Nuevo endpoint GET/POST /cobertura-vendedores
export const getCoberturaVendedores = async (req, res) => {
  logRequest("getCoberturaVendedores", req);

  try {
    const pool = getMysqlPool();
    if (!pool) return res.status(500).json({ error: "No hay conexión a la base de datos local." });

    const co_ven = req.query?.co_ven ?? req.body?.co_ven;

    let sql = 'SELECT * FROM `cobertura_vendedores`';
    const params = [];

    if (co_ven && String(co_ven).trim() !== "") {
      sql += ' WHERE `co_ven` = ?';
      params.push(String(co_ven).trim());
    }

    sql += ' ORDER BY `vendedor` ASC';

    let conn;
    try {
      conn = await pool.getConnection();
      const [rows] = await conn.execute(sql, params);
      return res.json({ success: true, total: (rows || []).length, data: rows || [] });
    } catch (err) {
      console.error('[cobertura] Error consulta:', err);
      return res.status(500).json({ error: err?.message || 'Error interno del servidor.' });
    } finally {
      if (conn) try { conn.release(); } catch(e) { /* ignore */ }
    }
  } catch (err) {
    console.error('Error en getCoberturaVendedores:', err);
    return res.status(500).json({ error: err?.message || 'Error interno del servidor.' });
  }
};