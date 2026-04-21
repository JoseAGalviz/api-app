import { getMysqlPool, sql } from "../config/database.js";
import { DateTime } from "luxon";
import fetch from "node-fetch";
import jwt from "jsonwebtoken";
import bcrypt from "bcryptjs";

// POST /facturas/locales
export const saveFacturasLocales = async (req, res) => {
  const pool = getMysqlPool();
  if (!pool) {
    return res.status(500).json({ error: "MySQL no conectado" });
  }

  const { facturas } = req.body;

  if (!Array.isArray(facturas) || facturas.length === 0) {
    return res
      .status(400)
      .json({ error: "El campo facturas debe ser un array no vacío" });
  }

  const insertQuery = `
    INSERT INTO facturas_cargadas (
      fact_num, co_cli, cli_des, tipo, dias_credito,
      fec_emis, fec_venc_antes, fec_venc_despues, fecha_escaneo,
      co_ven, co_zon, zon_des, co_seg, seg_des, coordenadas, observacion_logistica
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    for (const factura of facturas) {
      const {
        fact_num,
        co_cli,
        cli_des,
        tipo,
        dias_credito,
        fec_emis,
        fec_venc_antes,
        fec_venc_despues,
        fecha_escaneo,
        co_ven,
        co_zon,
        zon_des,
        co_seg,
        seg_des,
        coordenadas,
        comentario_rango
      } = factura;

      // Si fec_venc_despues es null o undefined, usar fec_venc_antes como valor por defecto
      const fecVencDespuesFinal = fec_venc_despues ?? fec_venc_antes;

      // Usar fecha_escaneo del JSON si existe, si no la actual de Caracas
      let fechaEscaneoFinal;
      if (fecha_escaneo) {
        const raw = String(fecha_escaneo).trim();
        // Detectar si la cadena incluye hora (HH:mm o HH:mm:ss o THH:mm)
        const hasTime = /\d{2}:\d{2}(:\d{2})?/.test(raw) || /T\d{2}:\d{2}/.test(raw);

        // Intentar parsear como ISO primero (respetando zona/offset si viene)
        let dt = DateTime.fromISO(raw, { setZone: true });
        if (!dt.isValid) {
          // Intentar parsear como SQL
          dt = DateTime.fromSQL(raw, { setZone: true });
        }

        if (dt.isValid) {
          if (!hasTime) {
            // Si llegó sólo la fecha (ej. '2026-02-04'), combinar con la hora actual en Caracas
            const nowVzla = DateTime.now().setZone("America/Caracas");
            const combined = DateTime.fromObject(
              {
                year: dt.year,
                month: dt.month,
                day: dt.day,
                hour: nowVzla.hour,
                minute: nowVzla.minute,
                second: nowVzla.second,
                millisecond: nowVzla.millisecond,
              },
              { zone: "America/Caracas" }
            );
            fechaEscaneoFinal = combined.toFormat("yyyy-MM-dd HH:mm:ss.SSS");
          } else {
            // Si trae hora, preservamos offset/Z si existe, si no lo normalizamos a Caracas
            const hasOffset = /[Zz]|[+\-]\d{2}:?\d{2}/.test(raw);
            if (hasOffset) {
              // Guardar ISO completo con offset (ej. 2026-02-04T12:34:56.789-04:00)
              fechaEscaneoFinal = dt.toISO();
            } else {
              // Normalizar a zona Caracas y mantener milisegundos
              fechaEscaneoFinal = dt.setZone("America/Caracas").toFormat("yyyy-MM-dd HH:mm:ss.SSS");
            }
          }
        } else {
          // Si no podemos parsear, guardamos tal cual (mejor que insertar 00:00:00)
          fechaEscaneoFinal = raw;
        }
      } else {
        // Si no viene fecha_escaneo, usar la fecha/hora actual de Caracas con milisegundos
        fechaEscaneoFinal = DateTime.now()
          .setZone("America/Caracas")
          .toFormat("yyyy-MM-dd HH:mm:ss.SSS");
      }


      await conn.query(insertQuery, [
        fact_num,
        co_cli,
        cli_des,
        tipo,
        dias_credito,
        fec_emis,
        fec_venc_antes,
        fecVencDespuesFinal,
        fechaEscaneoFinal,
        co_ven,
        co_zon,
        zon_des,
        co_seg,
        seg_des,
        coordenadas || null,
        comentario_rango == null ? " " : comentario_rango
      ]);
    }

    await conn.commit();
    res.status(201).json({ message: "Facturas guardadas correctamente" });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error("Error al guardar facturas:", err);
    res.status(500).json({ error: "Error al guardar facturas" });
  } finally {
    if (conn) conn.release();
  }
};

// GET /gestiones
export const getGestiones = async (req, res) => {
  const pool = getMysqlPool();
  if (!pool) {
    return res.status(500).json({ error: "MySQL no conectado" });
  }

  // Parámetros de filtro por fecha
  const { fechaInicio, fechaFin } = req.query;

  let whereClause = [];
  const whereParams = [];

  function parseFecha(fecha, isInicio) {
    if (!fecha) return null;
    if (/\d{2}:\d{2}/.test(fecha)) return fecha;
    return isInicio ? fecha + " 00:00:00" : fecha + " 23:59:59";
  }

  const fechaInicioSQL = parseFecha(fechaInicio, true);
  const fechaFinSQL = parseFecha(fechaFin, false);

  if (fechaInicioSQL && fechaFinSQL) {
    whereClause.push("fecha_registro BETWEEN ? AND ?");
    whereParams.push(fechaInicioSQL, fechaFinSQL);
  } else if (fechaInicioSQL) {
    whereClause.push("fecha_registro >= ?");
    whereParams.push(fechaInicioSQL);
  } else if (fechaFinSQL) {
    whereClause.push("fecha_registro <= ?");
    whereParams.push(fechaFinSQL);
  }

  const whereSQL = whereClause.length
    ? "WHERE " + whereClause.join(" AND ")
    : "";

  let conn;
  try {
    conn = await pool.getConnection();

    // Si no hay filtro de fechas, limitar a 100 registros más recientes
    let queryStr = `SELECT * FROM gestiones ${whereSQL} ORDER BY fecha_registro DESC`;
    if (!fechaInicioSQL && !fechaFinSQL) {
      queryStr += " LIMIT 100";
    }
    const [result] = await conn.query(queryStr + ";", whereParams);
    // Resultado bruto
    let rows = result;

    // Eliminar registros duplicados: preferimos deduplicar por gestion_id cuando exista,
    // si no, por usuario_id + co_cli + fecha_registro (normalizada a minuto en zona Caracas)
    const seen = new Set();
    rows = rows.filter((row) => {
      const coCliStr = row.co_cli == null ? "" : String(row.co_cli).trim();

      // Normalizar fecha_registro (o fecha) a minuto en zona "America/Caracas"
      let fechaMomentIso = "";
      const fechaFuente = row.fecha_registro || row.fecha || null;
      if (fechaFuente) {
        try {
          // fromSQL acepta formatos 'yyyy-MM-dd HH:mm:ss' o 'yyyy-MM-dd HH:mm'
          fechaMomentIso = DateTime.fromSQL(String(fechaFuente), {
            zone: "America/Caracas",
          })
            .startOf("minute")
            .toISO(); // ISO normalizada hasta minuto
        } catch (e) {
          // fallback: intentar parsear como ISO o usar toString
          try {
            fechaMomentIso = DateTime.fromISO(String(fechaFuente), {
              zone: "America/Caracas",
            })
              .startOf("minute")
              .toISO();
          } catch (e2) {
            fechaMomentIso = String(fechaFuente);
          }
        }
      }

      const key = row.gestion_id
        ? `g:${String(row.gestion_id).trim()}`
        : `u:${row.usuario_id || ""}|c:${coCliStr}|f:${fechaMomentIso}`;

      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    // Obtener todos los usuario_id únicos de las gestiones
    const usuarioIds = [
      ...new Set(rows.map((row) => row.usuario_id).filter(Boolean)),
    ];
    let coVenMap = {};
    if (usuarioIds.length > 0) {
      const [usuariosRows] = await conn.query(
        `SELECT id, co_ven FROM usuarios WHERE id IN (${usuarioIds
          .map(() => "?")
          .join(",")})`,
        usuarioIds
      );
      coVenMap = Object.fromEntries(usuariosRows.map((u) => [u.id, u.co_ven]));
    }

    // Obtener todos los co_ven únicos para buscar en Profit (limpiando espacios)
    const coVens = [
      ...new Set(
        Object.values(coVenMap)
          .filter(Boolean)
          .map((v) => v.trim())
      ),
    ];
    let venDesMap = {};
    if (coVens.length > 0) {
      const request = new sql.Request();
      coVens.forEach((co_ven, idx) => {
        request.input(`co_ven${idx}`, sql.VarChar, co_ven);
      });
      const query = `
        SELECT RTRIM(LTRIM(co_ven)) AS co_ven, ven_des FROM vendedor WHERE RTRIM(LTRIM(co_ven)) IN (${coVens
          .map((_, idx) => `@co_ven${idx}`)
          .join(",")})
      `;
      const result = await request.query(query);
      venDesMap = Object.fromEntries(
        result.recordset.map((v) => [v.co_ven.trim(), v.ven_des])
      );
    }

    // --- Optimización: consulta a Bitrix en lotes de hasta 50 co_cli únicos ---
    const bitrixApiUrl =
      process.env.BITRIX_MAIN_URL;
    const bitrixCache = {};
    const allCoCli = [
      ...new Set(rows.map((row) => row.co_cli).filter(Boolean)),
    ];
    const batchSize = 50;
    let bitrixResults = {};

    async function fetchBitrixBatch(coCliBatch) {
      // Intentar enviar el filtro como array
      // Limpiar espacios en los co_cli antes de enviar a Bitrix
      const cleanBatch = coCliBatch.map(c => c && typeof c === 'string' ? c.trim() : c);
      const filterObj = {
        filter: { UF_CRM_1634787828: cleanBatch },
        select: ["UF_CRM_1651251237102", "UF_CRM_1634787828"],
      };
      const endpoint = bitrixApiUrl;
      async function fetchWithRetry(
        url,
        options = {},
        retries = 3,
        delayMs = 2000
      ) {
        for (let i = 0; i < retries; i++) {
          try {
            const response = await fetch(url, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(filterObj),
              timeout: 15000,
            });
            if (!response.ok) throw new Error("HTTP " + response.status);
            return await response.json();
          } catch (err) {
            if (i === retries - 1) throw err;
            await new Promise((res) => setTimeout(res, delayMs));
          }
        }
      }
      try {
        const data = await fetchWithRetry(endpoint, {}, 3, 2000);
        let result = data.result;
        if (Array.isArray(result)) {
          for (const item of result) {
            const co_cli = item.UF_CRM_1634787828 ? item.UF_CRM_1634787828.trim() : null;
            const bitrixId = item.ID;
            const coordenadas = item.UF_CRM_1651251237102
              ? item.UF_CRM_1651251237102.trim()
              : null;
            if (co_cli) {
              bitrixResults[co_cli] = {
                bitrix_id: bitrixId,
                bitrix_coords: coordenadas,
              };
            }
          }
        }
      } catch (error) {
        console.error("Bitrix batch fetch error:", error);
      }
    }

    // Procesar en lotes
    for (let i = 0; i < allCoCli.length; i += batchSize) {
      const batch = allCoCli.slice(i, i + batchSize);
      await fetchBitrixBatch(batch);
    }

    // Asignar resultados a cada gestión
    const gestionesFormateadas = rows.map((row) => {
      const co_ven = coVenMap[row.usuario_id]
        ? coVenMap[row.usuario_id].trim()
        : null;
      const ven_des = co_ven ? venDesMap[co_ven] || null : null;
      // Limpiar espacios en co_cli antes de buscar en bitrixResults
      const cleanCoCli = row.co_cli && typeof row.co_cli === 'string' ? row.co_cli.trim() : row.co_cli;
      const bitrixData = bitrixResults[cleanCoCli] || {
        bitrix_id: "N/A",
        bitrix_coords: null,
      };

      // Formatear fecha y hora por separado en zona Caracas
      let fechaRegistroFecha = null;
      let fechaRegistroHora = null;
      if (row.fecha_registro) {
        const dtReg = DateTime.fromJSDate(new Date(row.fecha_registro)).setZone("America/Caracas");
        fechaRegistroFecha = dtReg.toFormat("dd/LL/yyyy");
        fechaRegistroHora = dtReg.toFormat("HH:mm");
      }

      let fechaFecha = null;
      let fechaHora = null;
      if (row.fecha) {
        const dt = DateTime.fromJSDate(new Date(row.fecha)).setZone("America/Caracas");
        fechaFecha = dt.toFormat("dd/LL/yyyy");
        fechaHora = dt.toFormat("HH:mm");
      }

      return {
        ...row,
        // fecha_registro: ahora sólo la fecha en formato dd/MM/yyyy
        fecha_registro: fechaRegistroFecha,
        // fecha_registro_hora: hora separada (HH:mm)
        fecha_registro_hora: fechaRegistroHora,
        // fecha: ahora sólo la fecha en formato dd/MM/yyyy
        fecha: fechaFecha,
        // fecha_hora: hora separada (HH:mm)
        fecha_hora: fechaHora,
        co_ven,
        ven_des,
        bitrix_id: bitrixData.bitrix_id,
        bitrix_coords: bitrixData.bitrix_coords,
      };
    });

    // Respuesta: siempre todos los datos filtrados
    res.json({
      total: gestionesFormateadas.length,
      gestiones: gestionesFormateadas,
    });
  } catch (err) {
    console.error("Error al consultar gestiones:", err);
    res.status(500).json({ error: "Error al consultar gestiones" });
  } finally {
    if (conn) conn.release();
  }
};

// POST /gestiones
export const saveGestiones = async (req, res) => {
  let { gestiones, usuario } = req.body;

  if (!gestiones || !usuario) {
    return res
      .status(400)
      .json({ error: "Faltan datos requeridos: usuario o gestiones" });
  }

  // Si gestiones no es un array, lo convertimos para unificar el manejo
  if (!Array.isArray(gestiones)) {
    gestiones = [gestiones];
  }

  const gestionesConUsuario = gestiones.map((gestion) => ({
    usuario,
    gestion,
  }));

  const pool = getMysqlPool();
  if (!pool) {
    return res.status(500).json({ error: "MySQL no conectado" });
  }

  let conn;
  let gestionesInsertadas = 0;
  let gestionesOmitidas = 0;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    for (const item of gestionesConUsuario) {
      const { usuario, gestion } = item;
      // Verificar si ya existe una gestión con ese gestion.id
      const [existe] = await conn.query(
        "SELECT 1 FROM gestiones WHERE gestion_id = ? LIMIT 1",
        [gestion.id]
      );
      if (existe.length > 0) {
        gestionesOmitidas++;
        continue; // Saltar si ya existe
      }
      const esNuevoCliente =
        gestion.tipos && gestion.tipos.includes("nuevo_cliente");
      // Obtener fecha/hora actual de Venezuela
      const fechaRegistroVzla = DateTime.now()
        .setZone("America/Caracas")
        .toFormat("yyyy-LL-dd HH:mm:ss");
      await conn.query(
        `INSERT INTO gestiones (
          usuario_id, usuario_nombre, gestion_id, cliente, co_cli, tipos,
          venta_tipoGestion, venta_descripcion,
          cobranza_tipoGestion, cobranza_descripcion,
          nuevo_cliente_nombreFarmacia, nuevo_cliente_responsable, nuevo_cliente_telefono, nuevo_cliente_codigoSim,
          fecha, ubicacion_lat, ubicacion_lng, fecha_registro
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          usuario.id,
          usuario.nombre,
          gestion.id,
          gestion.cliente,
          gestion.co_cli || null,
          JSON.stringify(gestion.tipos),
          gestion.venta?.tipoGestion || null,
          gestion.venta?.descripcion || null,
          gestion.cobranza?.tipoGestion || null,
          gestion.cobranza?.descripcion || null,
          esNuevoCliente ? gestion.nuevo_cliente?.nombreFarmacia || null : null,
          esNuevoCliente ? gestion.nuevo_cliente?.responsable || null : null,
          esNuevoCliente ? gestion.nuevo_cliente?.telefono || null : null,
          esNuevoCliente ? gestion.nuevo_cliente?.codigoSim || null : null,
          gestion.fecha,
          gestion.ubicacion?.lat || null,
          gestion.ubicacion?.lng || null,
          fechaRegistroVzla, // Aquí va la fecha/hora de Venezuela
        ]
      );
      gestionesInsertadas++;
    }

    await conn.commit();
    res.status(201).json({
      message: "Gestiones procesadas",
      insertadas: gestionesInsertadas,
      omitidas: gestionesOmitidas,
    });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error("Error al guardar gestiones:", err);
    res.status(500).json({ error: "Error al guardar gestiones" });
  } finally {
    if (conn) conn.release();
  }
};

// POST /login
export const loginUser = async (req, res) => {
  const pool = getMysqlPool();
  if (!pool) {
    return res.status(500).json({ error: "MySQL no conectado" });
  }

  let conn;
  try {
    conn = await pool.getConnection();
    const { username, password } = req.body;
    if (!username || !password) {
      return res.status(400).json({ error: "Usuario y contraseña requeridos" });
    }

    const [rows] = await conn.query(
      "SELECT * FROM usuarios WHERE usuario = ? LIMIT 1",
      [username]
    );

    if (rows.length === 0) {
      return res.status(401).json({ error: "Credenciales incorrectas" });
    }

    const user = rows[0];

    // Detecta si el password almacenado es bcrypt o texto plano (migración automática)
    const isHashed = user.password.startsWith('$2');
    let validPassword = false;
    if (isHashed) {
      validPassword = await bcrypt.compare(password, user.password);
    } else {
      validPassword = user.password === password;
      if (validPassword) {
        // Auto-migrar a bcrypt en el primer login exitoso
        const hashed = await bcrypt.hash(password, 10);
        await conn.query("UPDATE usuarios SET password = ? WHERE id = ?", [hashed, user.id]);
      }
    }

    if (!validPassword) {
      return res.status(401).json({ error: "Credenciales incorrectas" });
    }
    // Obtiene los segmentos asociados al usuario
    const [segmentosRows] = await conn.query(
      "SELECT segmento_id FROM usuarios_segmentos WHERE usuario_id = ?",
      [user.id]
    );
    const segmentos = segmentosRows.map((row) => row.segmento_id);

    // Generar token JWT con los datos del usuario y segmentos
    const payload = {
      id: user.id,
      usuario: user.usuario,
      nombre: user.nombre,
      rol: user.rol,
      co_ven: user.co_ven,
      segmentos,
    };
    const secret = process.env.JWT_SECRET;
    const token = jwt.sign(payload, secret, { expiresIn: "12h" });

    // Devuelve el usuario y el token
    res.json({
      message: "Login exitoso",
      user: {
        ...user,
        segmentos,
      },
      token, // Incluye el token en la respuesta
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Error en el servidor" });
  } finally {
    if (conn) conn.release();
  }
};

// POST /register
export const registerUser = async (req, res) => {
  const pool = getMysqlPool();
  if (!pool) {
    return res.status(500).json({ error: "MySQL no conectado" });
  }

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const { nombre, usuario, password, rol, segmentos, co_ven } = req.body;

    if (
      !nombre ||
      !usuario ||
      !password ||
      !rol ||
      !Array.isArray(segmentos) ||
      !co_ven
    ) {
      return res.status(400).json({ error: "Datos incompletos" });
    }

    const hashedPassword = await bcrypt.hash(password, 10);
    const [userResult] = await conn.query(
      "INSERT INTO usuarios (nombre, usuario, password, rol, fecha_registro, estado, co_ven) VALUES (?, ?, ?, ?, NOW(), 1, ?)",
      [nombre, usuario, hashedPassword, rol, co_ven]
    );
    const userId = userResult.insertId;

    for (const segmentoId of segmentos) {
      await conn.query(
        "INSERT INTO usuarios_segmentos (usuario_id, segmento_id) VALUES (?, ?)",
        [userId, segmentoId]
      );
    }

    await conn.commit();
    res.status(201).json({
      name: nombre,
      email: usuario,
      role: rol,
      segmentos: segmentos,
      co_ven: co_ven,
    });
  } catch (err) {
    if (conn) await conn.rollback();
    console.error(err);
    res.status(500).json({ error: "Error al registrar usuario" });
  } finally {
    if (conn) conn.release();
  }
};

export const getFacturasCargadas = async (req, res) => {
  const pool = getMysqlPool();
  if (!pool) {
    return res.status(500).json({ error: "MySQL no conectado" });
  }
  let conn;
  try {
    conn = await pool.getConnection();
    const [rows] = await conn.query("SELECT * FROM facturas_cargadas");
    res.json(rows);
  } catch (err) {
    console.error("Error al consultar facturas_cargadas:", err);
    res.status(500).json({ error: "Error al consultar facturas_cargadas" });
  } finally {
    if (conn) conn.release();
  }
};

// Limita la concurrencia de promesas (máximo 5 a la vez)
async function promisePool(tasks, poolLimit = 5) {
  const results = [];
  const executing = [];
  for (const task of tasks) {
    const p = Promise.resolve().then(() => task());
    results.push(p);

    if (poolLimit <= tasks.length) {
      const e = p.then(() => executing.splice(executing.indexOf(e), 1));
      executing.push(e);
      if (executing.length >= poolLimit) {
        await Promise.race(executing);
      }
    }
  }
  return Promise.all(results);
}

async function fetchAllBitrixData(co_cli_list) {
  const bitrixApiUrl =
    process.env.BITRIX_ALT_URL;
  const bitrixCache = {};
  const co_cli_set = new Set(co_cli_list.filter(Boolean));
  const co_cli_array = Array.from(co_cli_set);
  const resultMap = {};

  // Bitrix solo permite filtrar por un valor, así que hay que hacer una petición por cada co_cli
  // (Si tuvieras un endpoint que acepte varios, sería más eficiente)
  for (const co_cli of co_cli_array) {
    if (bitrixCache[co_cli]) {
      resultMap[co_cli] = bitrixCache[co_cli];
      continue;
    }
    const endpoint = `${bitrixApiUrl}?filter[UF_CRM_1634787828]=${co_cli}&select[]=UF_CRM_1651251237102`;
    try {
      const response = await fetch(endpoint, { timeout: 15000 });
      const data = await response.json();
      let result = data.result;
      if (Array.isArray(result) && result.length > 0) {
        const bitrixId = result[0].ID;
        const coordenadas = result[0].UF_CRM_1651251237102
          ? result[0].UF_CRM_1651251237102.trim()
          : null;
        bitrixCache[co_cli] = {
          bitrix_id: bitrixId,
          bitrix_coords: coordenadas,
        };
        resultMap[co_cli] = bitrixCache[co_cli];
      } else {
        bitrixCache[co_cli] = { bitrix_id: "N/A", bitrix_coords: null };
        resultMap[co_cli] = bitrixCache[co_cli];
      }
    } catch (error) {
      console.error("Bitrix fetch error:", error);
      bitrixCache[co_cli] = { bitrix_id: "N/A", bitrix_coords: null };
      resultMap[co_cli] = bitrixCache[co_cli];
    }
  }
  return resultMap;
}

// POST/GET /redirect-to-ip
// Recibe `ip` por query (?ip=1.2.3.4:8080) o en body { ip: '1.2.3.4:8080' }
export const redirectToIp = (req, res) => {
  const ipInput = (req.query && req.query.ip) || (req.body && req.body.ip);
  if (!ipInput) {
    return res.status(400).json({ error: "Parámetro 'ip' requerido" });
  }

  let target = String(ipInput).trim();

  // Si no incluye esquema, asumimos http://
  if (!/^https?:\/\//i.test(target)) {
    target = `http://${target}`;
  }

  // Validación básica de host (IPv4 o hostname) usando URL
  try {
    const urlObj = new URL(target);
    const host = urlObj.hostname;

    const ipv4Regex = /^(25[0-5]|2[0-4]\d|[01]?\d\d?)(\.(25[0-5]|2[0-4]\d|[01]?\d\d?)){3}$/;
    const hostnameRegex = /^(([a-zA-Z0-9-]+\.)*[a-zA-Z0-9-]+)$/;

    if (!ipv4Regex.test(host) && !hostnameRegex.test(host)) {
      return res.status(400).json({ error: "Host inválido" });
    }

    // Permitir sólo http/https
    if (!/^https?:$/i.test(urlObj.protocol.replace(':', ''))) {
      // ningún otro esquema permitido
      return res.status(400).json({ error: "Esquema no permitido" });
    }
  } catch (err) {
    return res.status(400).json({ error: "URL inválida" });
  }

  return res.redirect(302, target);
}

// Redirige a la URL fija añadiendo el co_ven como parámetro `ven`
// Espera JSON en el body: { co_ven: '10' } (también acepta ?co_ven=10)
export const redirectToFixedIp = (req, res) => {
  // Preferir body (JSON) pero aceptar query como fallback
  const coVenInput = (req.body && req.body.co_ven) || (req.query && req.query.co_ven);
  if (!coVenInput) {
    return res.status(400).json({ error: "Parámetro 'co_ven' requerido en body (JSON) o query" });
  }

  let coVenStr = String(coVenInput).trim();
  if (!coVenStr) {
    return res.status(400).json({ error: "'co_ven' no puede estar vacío" });
  }

  // Si es numérico, lo normalizamos a 5 dígitos con ceros a la izquierda (ej: 10 -> 00010)

  const base = process.env.DASHBOARD_BASE_URL;
  const target = `${base}?ven=${encodeURIComponent(coVenStr)}`;

  // Validación mínima de la URL resultante
  try {
    const urlObj = new URL(target);
    const protocol = urlObj.protocol.replace(':', '').toLowerCase();
    if (protocol !== 'http' && protocol !== 'https') {
      return res.status(400).json({ error: 'Esquema no permitido en URL fija' });
    }
  } catch (err) {
    return res.status(400).json({ error: 'URL fija inválida' });
  }

  return res.redirect(302, target);
}

// Redirige a la URL fija para vendedor añadiendo el parámetro `co_ven`
// Espera JSON en el body: { co_ven: '10' } (también acepta ?co_ven=10)
export const redirectToVendedorFixedIp = (req, res) => {
  // Preferir body (JSON) pero aceptar query como fallback
  const coVenInput = (req.body && req.body.co_ven) || (req.query && req.query.co_ven);
  if (!coVenInput) {
    return res.status(400).json({ error: "Parámetro 'co_ven' requerido en body (JSON) o query" });
  }

  let coVenStr = String(coVenInput).trim();
  if (!coVenStr) {
    return res.status(400).json({ error: "'co_ven' no puede estar vacío" });
  }

  // Construir URL target
  const base = process.env.VENDEDOR_BASE_URL;
  const target = `${base}?co_ven=${encodeURIComponent(coVenStr)}`;

  // Validación mínima de la URL resultante
  try {
    const urlObj = new URL(target);
    const protocol = urlObj.protocol.replace(':', '').toLowerCase();
    if (protocol !== 'http' && protocol !== 'https') {
      return res.status(400).json({ error: 'Esquema no permitido en URL fija' });
    }
  } catch (err) {
    return res.status(400).json({ error: 'URL fija inválida' });
  }

  return res.redirect(302, target);
}
