import 'dotenv/config';
import mysql from "mysql2/promise";
import { getMysqlPool } from "../config/database.js";
import bcrypt from "bcryptjs";
import { sql } from "../config/database.js";

// Pool específico para transferencias (inicialización lazy)
let _transferenciasPool = null;

function getTransferenciasPool() {
  if (_transferenciasPool) return _transferenciasPool;

  const host = process.env.DB_TRANS_HOST;
  const user = process.env.DB_TRANS_USER;
  const password = process.env.DB_TRANS_PASSWORD;

  if (!host || !user || !password) {
    throw new Error(
      `Variables de entorno de la BD de transferencias no configuradas. ` +
      `DB_TRANS_HOST=${host}, DB_TRANS_USER=${user}, DB_TRANS_PASSWORD=${password ? '***' : '(vacío)'}`
    );
  }

  _transferenciasPool = mysql.createPool({
    host,
    user,
    password,
    database: "transferencias",
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
  });

  return _transferenciasPool;
}

// Utilidad para limpiar los campos string de un objeto
function cleanStrings(obj) {
  return Object.fromEntries(
    Object.entries(obj).map(([key, value]) => [
      key,
      typeof value === "string" ? value.trim() : value,
    ])
  );
}

// --- FUNCIONES QUE USAN MYSQL (transferencias) ---

export const registrarUsuario = async (req, res) => {
  try {
    const {
      rol,
      usuario, // <-- Este viene del frontend, pero la columna es 'user'
      password,
      telefono,
      catalogo,
      estado,
      segmento,
      proveedor,
      status,
      session_id,
    } = req.body;

    if (!usuario || !password || !rol) {
      return res
        .status(400)
        .json({ error: "usuario, password y rol son requeridos." });
    }

    const salt = await bcrypt.genSalt(10);
    const hashedPassword = await bcrypt.hash(password.trim(), salt);

    // Convertir arrays o strings a string separados por coma
    const segmentoStr = Array.isArray(segmento)
      ? segmento.join(",")
      : segmento
        ? segmento
        : null;
    const proveedorStr = Array.isArray(proveedor)
      ? proveedor.join(",")
      : proveedor
        ? proveedor
        : null;
    const catalogoStr = Array.isArray(catalogo)
      ? catalogo.join(",")
      : catalogo
        ? catalogo
        : null;
    const fecha = new Date();

    const pool = getTransferenciasPool();


    const query = `
      INSERT INTO usuarios (
        user, password, rol, telefono, estado, segmento, session_id, proveedor, status, fecha, catalogo
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

    try {
      await pool.execute(query, [
        usuario.trim(),
        hashedPassword,
        rol.trim(),
        telefono ? telefono.trim() : '',       // <- usar '' en vez de null
        estado ? estado.trim() : '',          // <- usar '' en vez de null
        segmentoStr || '',
        session_id ? session_id.trim() : '',
        proveedorStr || '',
        status ? status.trim() : 'A',
        fecha,
        catalogoStr || ''
      ]);
      res.json({ success: true, message: "Usuario registrado correctamente." });
    } catch (mysqlErr) {
      console.error("Error MySQL:", mysqlErr);
      res.status(500).json({ error: mysqlErr.message });
    }
  } catch (err) {
    console.error("Error general:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getArticulos = async (req, res) => {
  try {
    const { co_prov, co_art } = req.query;

    let query = `
      SELECT co_art, co_prov, art_des, stock_act, stock_com, 
        (stock_act - stock_com) AS stock_real,
        co_lin, co_cat, co_subl, co_color, uni_venta, prec_vta1, prec_vta2, prec_vta3, prec_vta4, prec_vta5, prec_agr1, prec_agr2, prec_agr3, prec_agr4, tipo_imp
      FROM art
      WHERE stock_act > 0
        AND stock_com < stock_act
    `;

    const request = new sql.Request();

    if (co_prov) {
      query += " AND co_prov = @co_prov";
      request.input("co_prov", sql.VarChar, co_prov);
    }
    if (co_art) {
      query += " AND co_art = @co_art";
      request.input("co_art", sql.VarChar, co_art);
    }

    const result = await request.query(query);

    const cleanedRecordset = result.recordset.map(cleanStrings);

    res.json(cleanedRecordset);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

// Nueva función getPrecios
export const getPrecios = async (req, res) => {
  try {
    // Ahora recibe los datos desde el body (JSON)
    const { co_prov, precio_num } = req.body;

    let precioCampo = "";
    let precioVtaCampo = "";
    switch (parseInt(precio_num, 10)) {
      case 1:
        precioCampo = "prec_agr1";
        precioVtaCampo = "prec_vta1";
        break;
      case 2:
        precioCampo = "prec_agr2";
        precioVtaCampo = "prec_vta2";
        break;
      case 3:
        precioCampo = "prec_agr3";
        precioVtaCampo = "prec_vta3";
        break;
      case 4:
        precioCampo = "prec_agr4";
        precioVtaCampo = "prec_vta4";
        break;
      default:
        precioCampo = "prec_agr5";
        precioVtaCampo = "prec_vta5";
    }

    let query = `
      SELECT 
        a.co_art AS imagen, 
        a.co_prov,
        a.art_des AS descripcion, 
        (a.stock_act - a.stock_com) AS stock,
        ${precioCampo} AS Precio,
        ${precioVtaCampo} AS precio_venta,
        a.tipo_imp,
        ISNULL((SELECT TOP 1 porc1 FROM descuen WHERE co_desc = a.co_art), 0) AS descuento_por_art,
        ISNULL((SELECT TOP 1 porc1 FROM descuen WHERE co_desc = a.co_cat), 0) AS descuento_por_categoria,
        ISNULL((SELECT TOP 1 porc1 FROM descuen WHERE co_desc = a.co_lin), 0) AS descuento_por_linea,
        ISNULL((
          SELECT TOP 1 (s.stock_act - s.stock_com)
          FROM st_almac s
          WHERE s.co_art = a.co_art AND s.co_alma = '01'
        ), 0) AS stock_tachira,
        ISNULL((
          SELECT TOP 1 (s.stock_act - s.stock_com)
          FROM st_almac s
          WHERE s.co_art = a.co_art AND s.co_alma = '04'
        ), 0) AS stock_barquisimeto
      FROM art a
      WHERE a.stock_act > 1
    `;

    if (co_prov) {
      query += ` AND a.co_prov = @co_prov`;
    }

    query += ` AND a.stock_com < a.stock_act`;

    const request = new sql.Request();
    if (co_prov) {
      request.input("co_prov", sql.VarChar, co_prov);
    }

    const result = await request.query(query);

    const cleanedRecordset = result.recordset.map((item) => {
      const cleanedItem = {};
      for (const key in item) {
        cleanedItem[key] =
          typeof item[key] === "string" ? item[key].trim() : item[key];
      }
      return cleanedItem;
    });

    res.json(cleanedRecordset);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

export const getClientes = async (req, res) => {
  try {
    // Ahora recibe los datos desde el body (JSON)
    const { rif, co_seg, nit } = req.body; // <-- agregado 'nit'

    let query = `
      SELECT 
        co_cli, tipo, cli_des, rif, direc1, dir_ent2, telefonos, respons, 
        co_zon, desc_ppago, plaz_pag, inactivo, 
        desc_glob, saldo, ciudad, website, login, password, mont_cre,
        co_seg,
        (SELECT precio_a FROM tipo_cli WHERE tipo_cli.tip_cli = clientes.tipo) AS precio_a
      FROM clientes WHERE inactivo = 0
    `;

    let whereClauses = [];
    let parameters = {};

    if (rif) {
      whereClauses.push(`rif LIKE @rif`);
      parameters.rif = `%${rif}%`;
    }

    if (nit) {
      whereClauses.push(`nit LIKE @nit`);
      parameters.nit = `%${nit}%`;
    }

    if (co_seg) {
      // Soporta uno o varios segmentos separados por coma
      const segmentos = co_seg
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      if (segmentos.length === 1) {
        whereClauses.push(`co_seg = @co_seg`);
        parameters.co_seg = segmentos[0];
      } else if (segmentos.length > 1) {
        // Genera parámetros dinámicos para el IN
        const inParams = segmentos.map((_, i) => `@co_seg${i}`);
        whereClauses.push(`co_seg IN (${inParams.join(",")})`);
        segmentos.forEach((seg, i) => {
          parameters[`co_seg${i}`] = seg;
        });
      }
    }

    if (whereClauses.length > 0) {
      query += " AND " + whereClauses.join(" AND ");
    }

    const request = new sql.Request();

    for (const param in parameters) {
      request.input(param, sql.VarChar, parameters[param]);
    }

    const result = await request.query(query);

    const cleanedRecordset = result.recordset.map((item) => {
      const cleanedItem = {};
      for (const key in item) {
        cleanedItem[key] =
          typeof item[key] === "string" ? item[key].trim() : item[key];
      }
      return cleanedItem;
    });

    res.json(cleanedRecordset);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
};

export const getLogin = async (req, res) => {
  try {
    const { login, password } = req.body;

    if (!login || !password) {
      return res
        .status(400)
        .json({ error: "Login y password son requeridos." });
    }

    const query = `
      SELECT 
        co_cli, cli_des, login, password
      FROM clientes
      WHERE login = @login AND password = @password AND inactivo = 0
    `;

    const request = new sql.Request();
    request.input("login", sql.VarChar, login.trim());
    request.input("password", sql.VarChar, password.trim());

    const result = await request.query(query);

    if (result.recordset.length === 0) {
      return res.status(401).json({ error: "Credenciales inválidas." });
    }

    const user = result.recordset[0];
    res.json({
      co_cli: user.co_cli,
      cli_des: user.cli_des,
      login: user.login,
      // Puedes agregar más campos si lo necesitas
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

export const getProveedores = async (req, res) => {
  try {
    const request = new sql.Request();
    const result = await request.query("SELECT co_prov, prov_des FROM prov");
    // Limpiar los campos string de cada proveedor
    const cleaned = result.recordset.map((item) => {
      const limpio = {};
      for (const key in item) {
        limpio[key] =
          typeof item[key] === "string" ? item[key].trim() : item[key];
      }
      return limpio;
    });
    res.json(cleaned);
  } catch (err) {
    console.error("Error al consultar la tabla prov:", err);
    res.status(500).json({ error: "Error al consultar la tabla prov" });
  }
};

export const loginUsuario = async (req, res) => {
  try {

    const { usuario, password } = req.body;

    if (!usuario || !password) {
      return res.status(400).json({ error: "usuario y password son requeridos." });
    }

    const query = `SELECT user, password, rol, telefono, estado, segmento, proveedor, status, fecha, catalogo FROM usuarios WHERE user = ? LIMIT 1`;
    const [rows] = await getTransferenciasPool().execute(query, [usuario.trim()]);

    if (rows.length === 0) {
      return res.status(401).json({ error: "Usuario no encontrado." });
    }

    const user = rows[0];

    const passwordMatch = await bcrypt.compare(password, user.password);

    if (!passwordMatch) {
      return res.status(401).json({ error: "Contraseña incorrecta." });
    }

    res.json({
      user: user.user,
      rol: user.rol,
      telefono: user.telefono,
      estado: user.estado,
      segmento: user.segmento,
      proveedor: user.proveedor,
      status: user.status,
      fecha: user.fecha,
      catalogo: user.catalogo,
    });
  } catch (err) {
    console.error("Error en loginUsuario:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getCatalogo = async (req, res) => {
  try {
    // Log para ver exactamente qué llega al endpoint

    // Soporta tanto query (GET) como body (POST)
    const co_prov_input = req.query.co_prov ?? req.body?.co_prov;
    const precio_num = parseInt(req.query.precio_num ?? req.body?.precio_num, 10);

    // Normalizar precio
    let precioCampo = "";
    let precioVtaCampo = "";
    switch (precio_num) {
      case 1:
        precioCampo = "prec_agr1";
        precioVtaCampo = "prec_vta3";
        break;
      case 2:
        precioCampo = "prec_agr2";
        precioVtaCampo = "prec_vta3";
        break;
      case 3:
        precioCampo = "prec_agr3";
        precioVtaCampo = "prec_vta4";
        break;
      case 4:
        precioCampo = "prec_agr4";
        precioVtaCampo = "prec_vta4";
        break;
      default:
        precioCampo = "prec_agr1";
        precioVtaCampo = "prec_vta3";
    }

    // Aceptar uno o varios proveedores:
    // - array en JSON: { "co_prov": ["43","44"] }
    // - string CSV: ?co_prov=43,44 o { "co_prov": "43,44" }
    // - único valor: ?co_prov=43
    let proveedores = [];
    if (Array.isArray(co_prov_input)) {
      proveedores = co_prov_input.map(String).map(s => s.trim()).filter(Boolean);
    } else if (typeof co_prov_input === "string" && co_prov_input.includes(",")) {
      proveedores = co_prov_input.split(",").map(s => s.trim()).filter(Boolean);
    } else if (co_prov_input) {
      proveedores = [String(co_prov_input).trim()];
    }

    let query = `
      SELECT 
        a.co_art AS imagen, 
        a.co_prov,
        a.art_des AS descripcion, 
        (a.stock_act - a.stock_com) AS stock,
        ${precioCampo} AS Precio,
        ${precioVtaCampo} AS precio_venta,
        a.tipo_imp,
        ISNULL((SELECT TOP 1 porc1 FROM descuen WHERE co_desc = a.co_art), 0) AS descuento_por_art,
        ISNULL((SELECT TOP 1 porc1 FROM descuen WHERE co_desc = a.co_cat), 0) AS descuento_por_categoria,
        ISNULL((SELECT TOP 1 porc1 FROM descuen WHERE co_desc = a.co_lin), 0) AS descuento_por_linea,
        ISNULL((
          SELECT TOP 1 (s.stock_act - s.stock_com)
          FROM st_almac s
          WHERE s.co_art = a.co_art AND s.co_alma = '01'
        ), 0) AS stock_tachira,
        ISNULL((
          SELECT TOP 1 (s.stock_act - s.stock_com)
          FROM st_almac s
          WHERE s.co_art = a.co_art AND s.co_alma = '04'
        ), 0) AS stock_barquisimeto
      FROM art a
      WHERE a.stock_act > 1
    `;

    const request = new sql.Request();

    // Si vienen proveedores, construir IN (...) con parámetros dinámicos
    if (proveedores.length > 0) {
      const inParams = proveedores.map((_, i) => `@prov${i}`);
      query += ` AND a.co_prov IN (${inParams.join(",")})`;
      proveedores.forEach((prov, i) => {
        request.input(`prov${i}`, sql.VarChar, prov);
      });
    }

    query += ` AND a.stock_com < a.stock_act`;

    const result = await request.query(query);

    const cleanedRecordset = result.recordset.map((item) => {
      const cleanedItem = {};
      for (const key in item) {
        cleanedItem[key] =
          typeof item[key] === "string" ? item[key].trim() : item[key];
      }
      return cleanedItem;
    });

    res.json(cleanedRecordset);
  } catch (err) {
    console.error("Error en getCatalogo:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getTiemposPago = async (req, res) => {
  try {
    // Usando el pool MySQL (transferenciasPool) para consultar la tabla tiempos_pago
    const query = "SELECT id, tiempo, porcentaje, columna, fecha FROM tiempos_pago ORDER BY id";
    const [rows] = await getTransferenciasPool().execute(query);

    // Limpiar strings y devolver resultado
    const cleaned = rows.map(cleanStrings);

    res.json(cleaned);
  } catch (err) {
    console.error("Error en getTiemposPago:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getTiemposPagoTransferencias = async (req, res) => {
  try {
    // Usar el pool MySQL 'transferenciasPool' ya definido en este archivo
    const sql = "SELECT id, tiempo, porcentaje, columna FROM tiempos_pago ORDER BY id";
    const [rows] = await getTransferenciasPool().execute(sql);

    // Limpiar strings (reutiliza la función cleanStrings definida arriba)
    const cleaned = (rows || []).map(cleanStrings);

    res.json(cleaned);
  } catch (err) {
    console.error("Error en getTiemposPagoTransferencias:", err);
    res.status(500).json({ error: "Error al obtener tiempos de pago", detalle: err.message });
  }
};

export const getTipoCli = async (req, res) => {
  try {
    const { tip_cli } = req.query;
    const request = new sql.Request();

    let query = "SELECT * FROM tipo_cli";
    if (tip_cli) {
      query += " WHERE tip_cli = @tip_cli";
      request.input("tip_cli", sql.VarChar, tip_cli);
    }

    const result = await request.query(query);
    const cleaned = result.recordset.map(cleanStrings);

    res.json(cleaned);
  } catch (err) {
    console.error("Error al consultar tipo_cli:", err);
    res.status(500).json({ error: "Error al consultar tipo_cli" });
  }
};

// Unidades vendidas por usuario (quien montó el pedido)
export const getUnidadesPorUsuario = async (req, res) => {
  try {
    const proveedor_codigo =
      req.query.proveedor_codigo ??
      req.body?.proveedor_codigo ??
      req.query.cod_prov ??
      req.body?.cod_prov;
    if (!proveedor_codigo) {
      return res.status(400).json({ error: "proveedor_codigo requerido" });
    }

    const sql = `
      SELECT u.usuario AS nombre_usuario, SUM(pp.cantidad) AS total_unidades
      FROM pedidos p
      JOIN pedido_productos pp ON p.id = pp.pedido_id
      JOIN usuarios u ON p.usuario_id = u.id
      WHERE p.cod_prov = ?
      GROUP BY u.usuario
    `;
    const [rows] = await getTransferenciasPool().execute(sql, [proveedor_codigo]);
    res.json(rows.map(cleanStrings));
  } catch (err) {
    console.error("Error getUnidadesPorUsuario:", err);
    res.status(500).json({ error: err.message });
  }
};

// Productos vendidos por el proveedor (agrupados por código)
export const getProductosVendidos = async (req, res) => {
  try {
    const proveedor_codigo =
      req.query.proveedor_codigo ??
      req.body?.proveedor_codigo ??
      req.query.cod_prov ??
      req.body?.cod_prov;
    if (!proveedor_codigo) {
      return res.status(400).json({ error: "proveedor_codigo requerido" });
    }

    const sqlProd = `
      SELECT pp.co_art AS codigo, SUM(pp.cantidad) AS cantidad_total
      FROM pedidos p
      JOIN pedido_productos pp ON p.id = pp.pedido_id
      WHERE p.cod_prov = ?
      GROUP BY pp.co_art
    `;
    const [rows] = await getTransferenciasPool().execute(sqlProd, [proveedor_codigo]);
    res.json(rows.map(cleanStrings));
  } catch (err) {
    console.error("Error getProductosVendidos:", err);
    res.status(500).json({ error: err.message });
  }
};

// Pedidos del proveedor logeado
export const getTransferenciasProveedor = async (req, res) => {
  try {
    const proveedor_codigo =
      req.query.proveedor_codigo ??
      req.body?.proveedor_codigo ??
      req.query.cod_prov ??
      req.body?.cod_prov;
    if (!proveedor_codigo) {
      return res.status(400).json({ error: "proveedor_codigo requerido" });
    }

    const sqlTrans = `
      SELECT cod_cliente, fecha, cod_ped_profit, tot_bruto
      FROM pedidos
      WHERE cod_prov = ?
    `;
    const [rows] = await getTransferenciasPool().execute(sqlTrans, [proveedor_codigo]);
    res.json(rows.map(cleanStrings));
  } catch (err) {
    console.error("Error getTransferenciasProveedor:", err);
    res.status(500).json({ error: err.message });
  }
};

// Total general de la columna TOTAL para el proveedor logeado
export const getTotalGeneralProveedor = async (req, res) => {
  try {
    const proveedor_codigo =
      req.query.proveedor_codigo ??
      req.body?.proveedor_codigo ??
      req.query.cod_prov ??
      req.body?.cod_prov;
    if (!proveedor_codigo) {
      return res.status(400).json({ error: "proveedor_codigo requerido" });
    }

    const sqlTotal = `SELECT SUM(tot_bruto) AS total FROM pedidos WHERE cod_prov = ?`;
    const [rows] = await getTransferenciasPool().execute(sqlTotal, [proveedor_codigo]);

    const total = rows && rows.length > 0 && rows[0].total !== null
      ? Number(rows[0].total)
      : 0;

    res.json({ total });
  } catch (err) {
    console.error("Error getTotalGeneralProveedor:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getRenglonesFactura = async (req, res) => {
  try {
    const nro_doc  = req.query?.nro_doc  ?? req.body?.nro_doc;
    const co_cli   = req.query?.co_cli   ?? req.body?.co_cli;
    const cod_prov = req.query?.cod_prov ?? req.body?.cod_prov;

    const startRaw = req.body?.startDate ?? req.query?.startDate;
    const endRaw   = req.body?.endDate   ?? req.query?.endDate;

    const parseDate = (v) => {
      if (v === undefined || v === null || v === "") return null;
      const d = new Date(String(v));
      if (isNaN(d)) return null;
      return d;
    };

    const now = new Date();
    const startDateObj = parseDate(startRaw) ?? new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0);
    const endDateObj   = parseDate(endRaw)   ?? new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59, 999);

    if (!cod_prov) {
      return res.status(400).json({ error: "cod_prov requerido para filtrar por campo5" });
    }

    const safeStr = (v) => (v != null ? String(v).trim() : null);

    const whereClauses = [
      "(f.campo5 = @cod_prov OR f.campo5 LIKE @cod_prov + ',%')",
      "f.campo6 IS NOT NULL",
      "LTRIM(RTRIM(f.campo6)) <> ''",
      "TRY_CAST(f.campo6 AS FLOAT) IS NOT NULL"
    ];
    if (nro_doc) whereClauses.push("f.nro_doc = @nro_doc");
    if (co_cli)  whereClauses.push("f.co_cli = @co_cli");
    whereClauses.push("f.fec_emis >= @startDate");
    whereClauses.push("f.fec_emis <= @endDate");

    const query = `
      SELECT 
        f.fact_num, 
        f.co_cli,
        f.campo5, 
        f.campo6,
        f.fec_emis,
        ROUND(
          CASE WHEN f.tasa IS NULL OR f.tasa = 0 
              THEN f.tot_neto 
              ELSE f.tot_neto / f.tasa 
          END, 2
        ) AS tot_neto,
        f.tasa,
        f.tot_bruto,
        c.cli_des,
        c.rif,
        r.reng_num,
        r.num_doc,
        r.co_art,
        r.total_art,
        r.reng_neto,
        r.porc_desc,
        r.prec_vta,
        a.art_des
      FROM factura f
      INNER JOIN reng_fac r ON f.fact_num = r.fact_num
      INNER JOIN clientes c ON f.co_cli = c.co_cli
      LEFT JOIN art a ON r.co_art = a.co_art
      WHERE ${whereClauses.join(" AND ")}
      ORDER BY f.fec_emis DESC, f.fact_num DESC, r.reng_num ASC
    `;

    const request = new sql.Request();
    request.input("cod_prov", sql.VarChar, cod_prov);
    if (nro_doc)      request.input("nro_doc",   sql.VarChar,  nro_doc);
    if (co_cli)       request.input("co_cli",    sql.VarChar,  co_cli);
    if (startDateObj) request.input("startDate", sql.DateTime, startDateObj);
    if (endDateObj)   request.input("endDate",   sql.DateTime, endDateObj);

    const result = await request.query(query);
    let recordset = result.recordset;

    // ─── Fallback: buscar en cotiz_c ──────────────────────────────────────────
    if (recordset.length === 0) {

      const whereClausesCotiz = [
        "(c.campo5 = @cod_prov OR c.campo5 LIKE @cod_prov + ',%')",
        "c.campo6 IS NOT NULL",
        "LTRIM(RTRIM(c.campo6)) <> ''",
        "TRY_CAST(c.campo6 AS FLOAT) IS NOT NULL",
        "r.TIPO_DOC = 'T'"
      ];
      if (nro_doc) whereClausesCotiz.push("c.fact_num = @nro_doc");
      if (co_cli)  whereClausesCotiz.push("c.co_cli = @co_cli");
      whereClausesCotiz.push("c.fec_emis >= @startDate");
      whereClausesCotiz.push("c.fec_emis <= @endDate");

      const queryCotiz = `
        SELECT 
          c.fact_num, 
          c.co_cli,
          c.campo5, 
          c.campo6,
          c.fec_emis,
          ROUND(
            CASE WHEN c.tasa IS NULL OR c.tasa = 0 
                THEN c.tot_neto 
                ELSE c.tot_neto / c.tasa 
            END, 2
          ) AS tot_neto,
          c.tasa,
          c.tot_bruto,
          cl.cli_des,
          cl.rif,
          r.reng_num,
          r.num_doc,
          r.co_art,
          r.total_art,
          r.reng_neto,
          r.porc_desc,
          r.prec_vta,
          a.art_des
        FROM cotiz_c c
        INNER JOIN reng_nde r ON c.fact_num = r.num_doc
        INNER JOIN clientes cl ON c.co_cli = cl.co_cli
        LEFT JOIN art a ON r.co_art = a.co_art
        WHERE ${whereClausesCotiz.join(" AND ")}
        ORDER BY c.fec_emis DESC, c.fact_num DESC, r.reng_num ASC
      `;

      const requestCotiz = new sql.Request();
      requestCotiz.input("cod_prov", sql.VarChar, cod_prov);
      if (nro_doc)      requestCotiz.input("nro_doc",   sql.VarChar,  nro_doc);
      if (co_cli)       requestCotiz.input("co_cli",    sql.VarChar,  co_cli);
      if (startDateObj) requestCotiz.input("startDate", sql.DateTime, startDateObj);
      if (endDateObj)   requestCotiz.input("endDate",   sql.DateTime, endDateObj);

      const resultCotiz = await requestCotiz.query(queryCotiz);
      recordset = resultCotiz.recordset;
    }

    // ─── Cache de nit (sicm) por co_cli ───────────────────────────────────────
    const sicmCache = {};

    const getSicm = async (coCliVal) => {
      const key = safeStr(coCliVal);
      if (!key) return null;
      if (sicmCache[key] !== undefined) return sicmCache[key];

      try {
        const reqCli = new sql.Request();
        reqCli.input("co_cli", sql.VarChar, key);
        const resCli = await reqCli.query(
          "SELECT LTRIM(RTRIM(nit)) AS nit FROM clientes WHERE co_cli = @co_cli"
        );
        const nit = safeStr(resCli.recordset?.[0]?.nit) ?? null;
        sicmCache[key] = nit;
        return nit;
      } catch (e) {
        console.error(`  [sicm] Error buscando nit para co_cli="${key}":`, e.message);
        sicmCache[key] = null;
        return null;
      }
    };

    // ─── Cache de campo4 (cod_bar) por co_art ─────────────────────────────────
    const codBarCache = {};

    const getCodBar = async (coArtVal) => {
      const key = safeStr(coArtVal);
      if (!key) return null;
      if (codBarCache[key] !== undefined) return codBarCache[key];

      try {
        const reqArt = new sql.Request();
        reqArt.input("co_art", sql.VarChar, key);
        const resArt = await reqArt.query(
          "SELECT LTRIM(RTRIM(campo4)) AS campo4 FROM art WHERE co_art = @co_art"
        );
        const campo4 = safeStr(resArt.recordset?.[0]?.campo4) ?? null;
        codBarCache[key] = campo4;
        return campo4;
      } catch (e) {
        console.error(`  [cod_bar] Error buscando campo4 para co_art="${key}":`, e.message);
        codBarCache[key] = null;
        return null;
      }
    };

    // ─── Cache de art_des por co_art (para pedidos_productos) ─────────────────
    const artDesCache = {};

    const getArtDes = async (coArtVal) => {
      const key = safeStr(coArtVal);
      if (!key) return null;
      if (artDesCache[key] !== undefined) return artDesCache[key];

      try {
        const reqArt = new sql.Request();
        reqArt.input("co_art", sql.VarChar, key);
        const resArt = await reqArt.query(
          "SELECT LTRIM(RTRIM(art_des)) AS art_des FROM art WHERE co_art = @co_art"
        );
        const artDes = safeStr(resArt.recordset?.[0]?.art_des) ?? null;
        artDesCache[key] = artDes;
        return artDes;
      } catch (e) {
        console.error(`  [art_des] Error buscando art_des para co_art="${key}":`, e.message);
        artDesCache[key] = null;
        return null;
      }
    };

    // ─── Agrupar por factura ───────────────────────────────────────────────────
    const facturasMap = {};
    recordset.forEach((row) => {
      const cleanRow = cleanStrings(row);
      const { fact_num, co_cli: rowCoCli, cli_des, campo5, campo6, fec_emis, rif, tasa, tot_bruto } = cleanRow;

      const tot_neto_val = cleanRow.tot_neto != null
        ? Number(Number(cleanRow.tot_neto).toFixed(2))
        : null;

      if (!facturasMap[fact_num]) {
        facturasMap[fact_num] = {
          fact_num,
          co_cli:    safeStr(rowCoCli),
          cli_des,
          campo5,
          campo6,
          fec_emis,
          tot_neto:  tot_neto_val,
          rif:       rif ?? null,
          sicm:      null,
          tasa:      tasa != null && tasa !== "" ? Number(Number(tasa).toFixed(2)) : null,
          tot_bruto: tot_bruto != null ? Number(tot_bruto) : tot_bruto,
          co_us_in:  null,
          pedido_num: null,
          db_mysql:  [],
          articulos: [],
        };
      }

      const { reng_num, num_doc, co_art, total_art, prec_vta, porc_desc, reng_neto } = cleanRow;

      const parseNumber = (v) => {
        if (v === null || v === undefined || v === "") return null;
        const n = parseFloat(String(v).replace(/\s+/g, "").replace(/,/g, ""));
        return isNaN(n) ? null : n;
      };

      const applyRate = (value, rate) => {
        if (value === null) return null;
        if (!rate || rate === 0) return Number(Number(value).toFixed(2));
        return Number((value / rate).toFixed(2));
      };

      const tasaNum       = parseNumber(cleanRow.tasa);
      const precVtaFinal  = applyRate(parseNumber(prec_vta),  tasaNum);
      const rengNetoFinal = applyRate(parseNumber(reng_neto), tasaNum);

      facturasMap[fact_num].articulos.push({
        reng_num,
        num_doc,
        co_art,
        total_art: total_art != null ? Number(total_art) : total_art,
        prec_vta:  precVtaFinal,
        porc_desc,
        reng_neto: rengNetoFinal,
        art_des:   (cleanRow.art_des ?? "").trim(),
        cod_bar:   null,
      });
    });

    const facturasArray = Object.values(facturasMap);

    // ─── Rellenar sicm y cod_bar ───────────────────────────────────────────────
    for (const f of facturasArray) {
      f.sicm = await getSicm(f.co_cli);

      for (const art of f.articulos) {
        art.cod_bar = await getCodBar(art.co_art);
      }
    }

    // ─── Rastrear co_us_in, pedido_num, db_mysql para cada factura ────────────
    for (const f of facturasArray) {
      try {
        const factNumLimpio = safeStr(f.fact_num);

        // ── PASO 1 ─────────────────────────────────────────────────────────────
        const req1 = new sql.Request();
        req1.input("p_fact_num", sql.VarChar, factNumLimpio);

        const res1 = await req1.query(`
          SELECT TOP 1 LTRIM(RTRIM(num_doc)) AS num_doc
          FROM reng_fac
          WHERE LTRIM(RTRIM(fact_num)) = @p_fact_num
        `);

        const numDoc1 = safeStr(res1.recordset?.[0]?.num_doc);
        if (!numDoc1) {
          continue;
        }

        // ── PASO 2 ─────────────────────────────────────────────────────────────
        const req2 = new sql.Request();
        req2.input("p_nde_fact_num", sql.VarChar, numDoc1);

        const res2 = await req2.query(`
          SELECT TOP 1 LTRIM(RTRIM(num_doc)) AS num_doc
          FROM reng_nde
          WHERE LTRIM(RTRIM(fact_num)) = @p_nde_fact_num
        `);

        const numDoc2 = safeStr(res2.recordset?.[0]?.num_doc);
        if (!numDoc2) {
          continue;
        }

        f.pedido_num = numDoc2;

        // ── PASO 3 ─────────────────────────────────────────────────────────────
        try {
          const [rows] = await getTransferenciasPool().execute(
            `SELECT co_us_in, fact_num FROM pedidos WHERE TRIM(fact_num) = ? LIMIT 1`,
            [numDoc2]
          );

          if (rows?.length > 0) {
            f.co_us_in = safeStr(rows[0].co_us_in);

            const pedidoFactNum = safeStr(rows[0].fact_num);

            // ── Buscar productos en pedidos_productos ──────────────────────────
            try {
              const [productos] = await getTransferenciasPool().execute(
                `SELECT fact_num, co_art, cantidad, precio, subtotal, created_at
                 FROM pedido_productos
                 WHERE TRIM(fact_num) = ?`,
                [pedidoFactNum]
              );

              if (productos?.length > 0) {
                const db_mysql = [];
                for (const prod of productos) {
                  const art_des = await getArtDes(prod.co_art);
                  db_mysql.push({
                    fact_num:   safeStr(prod.fact_num),
                    co_art:     safeStr(prod.co_art),
                    art_des:    art_des,
                    cantidad:   prod.cantidad,
                    precio:     prod.precio,
                    subtotal:   prod.subtotal,
                    created_at: prod.created_at,
                  });
                }
                f.db_mysql = db_mysql;
              } else {
                f.db_mysql = [];
              }
            } catch (errProductos) {
              console.error(`  [PASO 3] Error consultando pedidos_productos:`, errProductos.message || errProductos);
              f.db_mysql = [];
            }

          } else {
            f.co_us_in = null;
            f.db_mysql  = [];
          }
        } catch (errMysql) {
          console.error(`  [PASO 3] Error MySQL:`, errMysql.message || errMysql);
          f.co_us_in = null;
          f.db_mysql  = [];
        }

        // ── PASO 4 (info_profit adicional, opcional) ───────────────────────────
        try {
          const requestTrace = new sql.Request();
          requestTrace.input("pedidoFactNum", sql.VarChar, factNumLimpio);
          requestTrace.input("codCliente",    sql.VarChar, safeStr(f.co_cli) ?? "");

          const traceResult = await requestTrace.query(traceQuery);
          let clienteDetalle = null;

          if (traceResult.recordsets?.[0]?.length > 0) {
            const encabezado = traceResult.recordsets[0][0];
            const renglones  = traceResult.recordsets[1] || [];

            if (encabezado.co_cli) {
              try {
                const reqCli = new sql.Request();
                reqCli.input("co_cli", sql.VarChar, safeStr(encabezado.co_cli));
                const resCli = await reqCli.query(
                  "SELECT cli_des, rif, nit, direc1, tipo FROM clientes WHERE co_cli = @co_cli"
                );
                if (resCli.recordset?.length > 0) {
                  const c = resCli.recordset[0];
                  clienteDetalle = {
                    cli_des: safeStr(c.cli_des),
                    rif:     safeStr(c.rif),
                    nit:     safeStr(c.nit),
                    direc1:  safeStr(c.direc1),
                    tipo:    safeStr(c.tipo)
                  };
                }
              } catch (eCli) {
                console.error("  [PASO 4] Error buscando cliente:", eCli.message);
              }
            }

            f.info_profit = {
              encontrado: true,
              factura: {
                fact_num:  safeStr(encabezado.fact_num),
                co_cli:    safeStr(encabezado.co_cli)  ?? "",
                tot_neto:  encabezado.tot_neto,
                tot_brut:  encabezado.tot_bruto,
                fec_emis:  encabezado.fec_emis,
                co_ven:    safeStr(encabezado.co_ven)  ?? "",
                glob_desc: encabezado.glob_desc,
                iva:       encabezado.iva,
                status:    safeStr(encabezado.status)  ?? "",
                anulada:   encabezado.anulada,
                tasa:      encabezado.tasa
              },
              cliente: clienteDetalle ?? null,
              renglones_factura: renglones.map(r => {
                const limpio = {};
                for (const key in r) {
                  limpio[key] = typeof r[key] === 'string' ? r[key].trim() : r[key];
                }
                return limpio;
              })
            };

          } else {
            f.info_profit = {
              encontrado:     false,
              mensaje:        "No se encontró la cadena Pedido -> NDE -> Factura",
              debug_fact_num: f.fact_num
            };
          }
        } catch (errTrace) {
          console.error(`  [PASO 4] Error info_profit:`, errTrace.message || errTrace);
        }

      } catch (errGlobal) {
        console.error(`ERROR rastreando fact_num="${f.fact_num}":`, errGlobal.message || errGlobal);
        f.co_us_in   = null;
        f.pedido_num = null;
        f.db_mysql   = [];
      }
    }

    // ─── Consulta adicional a Profit: facturas con status = 2 ─────────────────
    try {
      let profitQuery = `
        SELECT fact_num, saldo, fec_emis, co_cli, tot_neto, tasa
        FROM factura
        WHERE (campo5 = @cod_prov OR campo5 LIKE @cod_prov + ',%') AND status = 2
      `;
      profitQuery += " AND fec_emis >= @startDate";
      profitQuery += " AND fec_emis <= @endDate";
      profitQuery += " ORDER BY fec_emis DESC, fact_num DESC";

      const profitReq = new sql.Request();
      profitReq.input("cod_prov", sql.VarChar, cod_prov);
      if (startDateObj) profitReq.input("startDate", sql.DateTime, startDateObj);
      if (endDateObj)   profitReq.input("endDate",   sql.DateTime, endDateObj);

      const profitResult   = await profitReq.query(profitQuery);
      const facturasProfit = (profitResult.recordset || []).map(cleanStrings);

      res.json({ facturas: facturasArray, facturas_profit: facturasProfit });

    } catch (profitErr) {
      console.error("Error consultando facturas en Profit:", profitErr);
      res.json({
        facturas:        facturasArray,
        facturas_profit: [],
        error_profit:    profitErr.message
      });
    }

  } catch (err) {
    console.error("Error en getRenglonesFactura:", err);
    res.status(500).json({
      error:   "Error al obtener los renglones de factura",
      detalle: err.message
    });
  }
};

export const getPedidosPorUsuario = async (req, res) => {
    try {
        const { user, fecha_desde, fecha_hasta } = req.body;

        if (!user) {
            return res.status(400).json({ error: "El campo 'user' es requerido." });
        }

        // ─────────────────────────────────────────────
        // 1. Rango de fechas: por defecto el mes actual
        // ─────────────────────────────────────────────
        const ahora = new Date();
        const inicioPorDefecto = new Date(ahora.getFullYear(), ahora.getMonth(), 1, 0, 0, 0, 0);
        const finPorDefecto = new Date(ahora.getFullYear(), ahora.getMonth() + 1, 0, 23, 59, 59, 999);

        const parseFecha = (str) => {
            if (!str || typeof str !== 'string' || str.trim() === '') return null;
            const d = new Date(str.trim());
            return isNaN(d.getTime()) ? null : d;
        };

        const inicio = parseFecha(fecha_desde) ?? inicioPorDefecto;
        let fin = parseFecha(fecha_hasta) ?? finPorDefecto;

        if (fecha_hasta && fin.getHours() === 0 && fin.getMinutes() === 0) {
            fin.setHours(23, 59, 59, 999);
        }

        if (inicio > fin) {
            return res.status(400).json({ error: "fecha_desde no puede ser mayor que fecha_hasta." });
        }

        const toMySQLDate = (d) => d.toISOString().slice(0, 19).replace('T', ' ');

        // ─────────────────────────────────────────────
        // 2. Obtener Pedidos del usuario (MySQL)
        // ─────────────────────────────────────────────
        const [pedidos] = await getTransferenciasPool().execute(
            `SELECT id, fact_num, cod_cliente, cod_prov, tot_bruto, tot_neto, saldo, iva,
                    codigo_pedido, porc_gdesc, descrip, co_us_in, fecha
             FROM pedidos
             WHERE co_us_in = ?
               AND fecha BETWEEN ? AND ?
             ORDER BY fecha DESC`,
            [user.trim(), toMySQLDate(inicio), toMySQLDate(fin)]
        );

        if (!pedidos.length) {
            return res.json({
                filtro: { fecha_desde: toMySQLDate(inicio), fecha_hasta: toMySQLDate(fin) },
                pedidos: []
            });
        }

        const pedidoIds = pedidos.map((p) => p.id);

        // ─────────────────────────────────────────────
        // 3. Obtener Productos de los pedidos (MySQL)
        // ─────────────────────────────────────────────
        const placeholders = pedidoIds.map(() => '?').join(',');
        const [todosProductos] = await getTransferenciasPool().execute(
            `SELECT pp.pedido_id, pp.co_art, pp.cantidad, pp.precio, pp.subtotal,
                    pp.co_alma, pp.reng_num
             FROM pedido_productos pp
             WHERE pp.pedido_id IN (${placeholders})
             ORDER BY pp.pedido_id, pp.reng_num ASC`,
            pedidoIds
        );

        // ─────────────────────────────────────────────
        // 4. Obtener Descripciones (art_des) desde SQL Server
        // ─────────────────────────────────────────────
        const coArtsUnicos = [...new Set(todosProductos.map((p) => p.co_art).filter(Boolean))];
        const artDesMap = {};

        if (coArtsUnicos.length > 0) {
            const reqArts = new sql.Request();
            const params = coArtsUnicos.map((co, i) => {
                const pName = `a${i}`;
                reqArts.input(pName, sql.VarChar, co.trim());
                return `@${pName}`;
            }).join(',');

            try {
                const { recordset: arts } = await reqArts.query(
                    `SELECT co_art, art_des FROM art WHERE co_art IN (${params})`
                );
                arts.forEach((a) => {
                    artDesMap[a.co_art.trim()] = a.art_des?.trim() ?? '';
                });
            } catch (errArt) {
                console.error("Error consultando descripciones en SQL Server:", errArt.message);
            }
        }

        // ─────────────────────────────────────────────
        // 5. Agrupar productos e inyectar art_des
        // ─────────────────────────────────────────────
        const productosPorPedido = {};
        for (const prod of todosProductos) {
            const pid = prod.pedido_id;
            if (!productosPorPedido[pid]) productosPorPedido[pid] = [];
            
            productosPorPedido[pid].push({
                ...prod,
                art_des: artDesMap[prod.co_art?.trim()] ?? 'Descripción no encontrada'
            });
        }

        // ─────────────────────────────────────────────
        // 6. Rastrear cada pedido en Profit (SQL Server)
        // ─────────────────────────────────────────────
        await Promise.all(
            pedidos.map(async (pedido) => {
                // Asignar productos ya procesados
                pedido.productos = productosPorPedido[pedido.id] ?? [];

                if (!pedido.fact_num) {
                    pedido.info_profit = { encontrado: false, mensaje: 'Pedido sin fact_num' };
                    return;
                }

                try {
                    const traceQuery = `
                        DECLARE @NumDocPedido   VARCHAR(50) = @pedidoFactNum;
                        DECLARE @NumNotaEntrega INT;
                        DECLARE @NumFactura     INT;

                        -- Paso 1: Pedido -> Nota de Entrega
                        SELECT TOP 1 @NumNotaEntrega = fact_num
                        FROM reng_nde
                        WHERE num_doc = @NumDocPedido AND tipo_doc = 'T';

                        -- Paso 2: NDE -> Factura
                        IF @NumNotaEntrega IS NOT NULL
                            SELECT TOP 1 @NumFactura = fact_num
                            FROM reng_fac
                            WHERE num_doc = CAST(@NumNotaEntrega AS VARCHAR);

                        -- Paso 3: Resultados
                        IF @NumFactura IS NOT NULL
                        BEGIN
                            -- Encabezado Factura
                            SELECT fact_num, co_cli, tot_neto, tot_bruto, fec_emis,
                                   co_ven, glob_desc, iva, status, anulada, tasa
                            FROM factura WHERE fact_num = @NumFactura;

                            -- Datos Cliente
                            SELECT cli_des, rif, nit, direc1, tipo
                            FROM clientes
                            WHERE co_cli = (SELECT co_cli FROM factura WHERE fact_num = @NumFactura);

                            -- Renglones NDE (Detalle físico)
                            SELECT r.fact_num, r.num_doc, a.art_des AS art_des_profit,
                                   r.co_art, r.co_alma, r.total_art, r.uni_venta,
                                   r.prec_vta AS prec_venta, r.porc_desc, r.reng_neto
                            FROM reng_nde r
                            LEFT JOIN art a ON a.co_art = r.co_art
                            WHERE r.num_doc = @NumDocPedido AND r.tipo_doc = 'T'
                            ORDER BY r.reng_num ASC;
                        END
                    `;

                    const reqTrace = new sql.Request();
                    reqTrace.input('pedidoFactNum', sql.VarChar, String(pedido.fact_num).trim());
                    
                    const traceResult = await reqTrace.query(traceQuery);

                    if (traceResult.recordsets.length > 0 && traceResult.recordsets[0].length > 0) {
                        pedido.info_profit = {
                            encontrado: true,
                            factura: traceResult.recordsets[0][0],
                            cliente: traceResult.recordsets[1]?.[0] ?? null,
                            renglones_factura: traceResult.recordsets[2] ?? []
                        };
                    } else {
                        pedido.info_profit = {
                            encontrado: false,
                            mensaje: 'No se encontró el flujo en Profit (Pedido -> NDE -> Factura)'
                        };
                    }
                } catch (errTrace) {
                    pedido.info_profit = { error: 'Error en Profit', detalle: errTrace.message };
                }
            })
        );

        // ─────────────────────────────────────────────
        // 7. Respuesta Final
        // ─────────────────────────────────────────────
        res.json({
            filtro: {
                fecha_desde: toMySQLDate(inicio),
                fecha_hasta: toMySQLDate(fin),
            },
            pedidos: pedidos.map(trimObj),
        });

    } catch (err) {
        console.error('Error en getPedidosPorUsuario:', err);
        res.status(500).json({ error: err.message });
    }
};

/**
 * Función auxiliar para limpiar espacios en blanco de strings en objetos
 */
function trimObj(obj) {
    if (!obj || typeof obj !== 'object') return obj;
    
    // Si es un array, procesar cada elemento
    if (Array.isArray(obj)) return obj.map(trimObj);

    return Object.keys(obj).reduce((acc, key) => {
        let val = obj[key];
        if (val instanceof Date) {
            acc[key] = val;
        } else if (typeof val === 'string') {
            acc[key] = val.trim();
        } else if (typeof val === 'object') {
            acc[key] = trimObj(val);
        } else {
            acc[key] = val;
        }
        return acc;
    }, {});
}

export const getProveedoresProfit = async (req, res) => {
  try {
    const request = new sql.Request();
    const result = await request.query("SELECT * FROM prov");
    // Limpiar los campos string de cada proveedor
    const cleaned = result.recordset.map((item) => {
      const limpio = {};
      for (const key in item) {
        limpio[key] =
          typeof item[key] === "string" ? item[key].trim() : item[key];
      }
      return limpio;
    });
    res.json(cleaned);
  } catch (err) {
    console.error("Error al consultar la tabla prov en Profit:", err);
    res.status(500).json({ error: "Error al consultar la tabla prov en Profit" });
  }
};

export const getFacturasConCampo5 = async (req, res) => {
  try {
    // Soporta startDate / endDate en body JSON o en query. Formato esperado: 'YYYY-MM-DD'
    const startRaw = req.body?.startDate ?? req.query?.startDate;
    const endRaw = req.body?.endDate ?? req.query?.endDate;

    const parseDate = (v) => {
      if (v === null || v === undefined) return null;
      const d = new Date(String(v));
      if (isNaN(d)) return null;
      return d;
    };

    const startDateObj = parseDate(startRaw);
    const endDateObj = parseDate(endRaw);

    // Si no se pasan fechas, devolver sólo los 40 registros más recientes.
    // Si se pasan fechas (start o end), ampliar el TOP a 1000.
    const topCount = (startDateObj === null && endDateObj === null) ? 40 : 1000;

    // Construir query dinámicamente: agregar filtro de fecha solo si hay fechas válidas
    let query = `
      SELECT TOP ${topCount} fact_num, campo5, campo6, fec_emis, tot_neto, tasa, co_cli
      FROM factura
      WHERE campo5 IS NOT NULL AND campo5 <> '' AND anulada = 0
    `;

    if (startDateObj && endDateObj) {
      query += ` AND fec_emis >= @startDate AND fec_emis <= @endDate`;
    } else if (startDateObj) {
      query += ` AND fec_emis >= @startDate`;
    } else if (endDateObj) {
      query += ` AND fec_emis <= @endDate`;
    }

    query += ` ORDER BY fec_emis DESC, fact_num DESC`;

    const request = new sql.Request();
    if (startDateObj) request.input("startDate", sql.DateTime, startDateObj);
    if (endDateObj) request.input("endDate", sql.DateTime, endDateObj);

    const result = await request.query(query);

    // Buscar cli_des para los co_cli devueltos (consulta en lote)
    const recordset = result.recordset || [];
    const uniqueCoCli = Array.from(
      new Set(recordset
        .map(r => (r.co_cli === null || r.co_cli === undefined) ? "" : String(r.co_cli).trim())
        .filter(Boolean))
    );

    let clienteMap = {};
    if (uniqueCoCli.length > 0) {
      const reqClientes = new sql.Request();
      const inParams = uniqueCoCli.map((_, i) => `@c${i}`);
      uniqueCoCli.forEach((c, i) => reqClientes.input(`c${i}`, sql.VarChar, c));
      const clientesQuery = `SELECT co_cli, cli_des FROM clientes WHERE co_cli IN (${inParams.join(",")})`;
      const clientesRes = await reqClientes.query(clientesQuery);
      (clientesRes.recordset || []).forEach((c) => {
        const co = (c.co_cli === null || c.co_cli === undefined) ? "" : String(c.co_cli).trim();
        clienteMap[co] = typeof c.cli_des === "string" ? c.cli_des.trim() : c.cli_des;
      });
    }

    // Buscar prov_des para los campo5 devueltos (consulta en lote)
    const uniqueCampo5 = Array.from(
      new Set(recordset
        .map(r => (r.campo5 === null || r.campo5 === undefined) ? "" : String(r.campo5).trim())
        .filter(Boolean))
    );

    let provMap = {};
    if (uniqueCampo5.length > 0) {
      const reqProv = new sql.Request();
      const inParamsProv = uniqueCampo5.map((_, i) => `@p${i}`);
      uniqueCampo5.forEach((p, i) => reqProv.input(`p${i}`, sql.VarChar, p));
      const provQuery = `SELECT co_prov, prov_des FROM prov WHERE co_prov IN (${inParamsProv.join(",")})`;
      const provRes = await reqProv.query(provQuery);
      (provRes.recordset || []).forEach((p) => {
        const key = (p.co_prov === null || p.co_prov === undefined) ? "" : String(p.co_prov).trim();
        provMap[key] = typeof p.prov_des === "string" ? p.prov_des.trim() : p.prov_des;
      });
    }

    // Limpiar y transformar resultados
    const cleaned = recordset.map((item) => {
      const limpio = {};
      for (const key in item) {
        limpio[key] =
          typeof item[key] === "string" ? item[key].trim() : item[key];
      }

      // Dividir tot_neto entre la tasa y formatear a 2 decimales
      const totNetoRaw = limpio.tot_neto;
      const tasaRaw = limpio.tasa;
      let totNetoDiv = null;
      if (totNetoRaw === null || totNetoRaw === undefined) {
        totNetoDiv = null;
      } else {
        const totNum = Number(totNetoRaw);
        const tasaNumRaw = tasaRaw === null || tasaRaw === undefined ? null : Number(tasaRaw);
        if (tasaNumRaw === null || tasaNumRaw === 0 || isNaN(tasaNumRaw)) {
          totNetoDiv = Number(totNum.toFixed(2));
        } else {
          totNetoDiv = Number((totNum / tasaNumRaw).toFixed(2));
        }
      }

      // Formatear fecha
      if (limpio.fec_emis) {
        const d = new Date(limpio.fec_emis);
        if (!isNaN(d)) {
          limpio.fec_emis = d.toISOString().slice(0, 10);
        }
      }

      // Formatear tasa a 2 decimales (si existe)
      if (limpio.tasa === null || limpio.tasa === undefined || limpio.tasa === "") {
        limpio.tasa = null;
      } else {
        const tasaNum = Number(limpio.tasa);
        limpio.tasa = isNaN(tasaNum) ? null : Number(tasaNum.toFixed(2));
      }

      // Reemplazar tot_neto por el valor dividido entre tasa
      limpio.tot_neto = totNetoDiv;

      // Obtener cli_des desde clienteMap y mostrarlo en lugar de co_cli
      const coCliKey = (limpio.co_cli === null || limpio.co_cli === undefined) ? "" : String(limpio.co_cli).trim();
      const cliDes = clienteMap[coCliKey] ?? limpio.co_cli ?? null;
      limpio.cli_des = cliDes;
      delete limpio.co_cli;

      // Reemplazar campo5 por prov_des (nombre del proveedor) si existe en provMap
      const campo5Key = (limpio.campo5 === null || limpio.campo5 === undefined) ? "" : String(limpio.campo5).trim();
      const provDes = provMap[campo5Key];
      if (provDes) {
        limpio.campo5 = provDes;
      } else {
        limpio.campo5 = campo5Key || limpio.campo5;
      }

      return limpio;
    });

    res.json({ startDate: startDateObj, endDate: endDateObj, count: cleaned.length, rows: cleaned });
  } catch (err) {
    res.status(500).json({ error: "Error al consultar facturas con campo5" });
  }
};
export const getVentasPorUsuariosProveedor = async (req, res) => {
  try {
    const proveedor_codigo = req.body?.proveedor_codigo ?? req.body?.cod_prov;
    if (!proveedor_codigo) {
      return res.status(400).json({ error: "proveedor_codigo requerido" });
    }

    const now = new Date();
    const mes  = req.body?.mes  ? Number(req.body.mes)  : now.getMonth() + 1;
    const anio = req.body?.anio ? Number(req.body.anio) : now.getFullYear();

    const pool = getTransferenciasPool();

    // 1) Obtener usuarios del proveedor
    let usuarios = [];
    try {
      const [rows] = await pool.execute(
        "SELECT id, user, proveedor FROM usuarios WHERE FIND_IN_SET(?, proveedor)",
        [proveedor_codigo]
      );
      usuarios = rows || [];
    } catch (err) {
      console.warn("FALLBACK: error FIND_IN_SET, obtengo todos los usuarios y filtro en JS:", err.message);
      const [allUsers] = await pool.execute("SELECT id, user, proveedor FROM usuarios");
      usuarios = (allUsers || []).filter((u) => {
        if (!u.proveedor) return false;
        const partes = String(u.proveedor).split(",").map((s) => s.trim());
        return partes.includes(String(proveedor_codigo));
      });
    }

    if (!usuarios || usuarios.length === 0) {
      return res.json([]);
    }

    // 2) Agregar ventas filtrando por mes/año
    try {
      const userNames = usuarios.map((u) => u.user);
      const placeholders = userNames.map(() => "?").join(",");

      const sql = `
        SELECT
          p.co_us_in                    AS usuario,
          COALESCE(SUM(pp.cantidad), 0) AS total_unidades
        FROM pedidos p
        JOIN pedido_productos pp ON pp.pedido_id = p.id
        WHERE p.co_us_in IN (${placeholders})
          AND MONTH(p.created_at) = ?
          AND YEAR(p.created_at)  = ?
        GROUP BY p.co_us_in
      `;

      const [rows] = await pool.execute(sql, [...userNames, mes, anio]);
      const mapa = new Map((rows || []).map((r) => [r.usuario, Number(r.total_unidades || 0)]));

      const resultado = usuarios
        .map((u) => ({
          id: u.id,
          usuario: typeof u.user === "string" ? u.user.trim() : u.user,
          total_unidades: mapa.get(u.user) || 0,
          mes,
          anio,
        }))
        .filter((u) => u.total_unidades > 0);

      return res.json(resultado);

    } catch (errAgg) {
      console.warn("FALLBACK: agregación por SQL falló, agrego en JS:", errAgg.message);

      const [pedidos] = await pool.execute(
        "SELECT id, co_us_in, created_at FROM pedidos WHERE MONTH(created_at) = ? AND YEAR(created_at) = ?",
        [mes, anio]
      );

      if (!pedidos || pedidos.length === 0) {
        return res.json([]);
      }

      const pedidoIds = pedidos.map((p) => p.id);
      const placeholders2 = pedidoIds.map(() => "?").join(",");
      const [productos] = await pool.execute(
        `SELECT pedido_id, cantidad FROM pedido_productos WHERE pedido_id IN (${placeholders2})`,
        pedidoIds
      );

      const pedidoToUser = new Map(pedidos.map((p) => [p.id, p.co_us_in]));
      const agg = {};
      for (const prod of productos) {
        const user = pedidoToUser.get(prod.pedido_id) ?? "";
        agg[user] = (agg[user] || 0) + Number(prod.cantidad || 0);
      }

      const resultado = usuarios
        .map((u) => ({
          id: u.id,
          usuario: typeof u.user === "string" ? u.user.trim() : u.user,
          total_unidades: agg[u.user] || 0,
          mes,
          anio,
        }))
        .filter((u) => u.total_unidades > 0);

      return res.json(resultado);
    }
  } catch (err) {
    console.error("Error en getVentasPorUsuariosProveedor:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getProductosMasVendidosPorProveedor = async (req, res) => {
  try {
    const cod_prov =
      req.body?.cod_prov ?? req.body?.proveedor_codigo ?? req.query?.cod_prov;
    if (!cod_prov) {
      return res.status(400).json({ error: "cod_prov requerido" });
    }

    // 1) Obtener fact_num desde Profit (SQL Server) donde campo5 = cod_prov
    const reqFact = new sql.Request();
    reqFact.input("cod_prov", sql.VarChar, cod_prov);

    // 1.a intento estricto (trim + status = 2)
    let factResult = await reqFact.query(
      "SELECT fact_num FROM factura WHERE LTRIM(RTRIM(campo5)) = @cod_prov AND status = 2"
    );
    let factNums = (factResult.recordset || []).map(r => r.fact_num).filter(Boolean);

    // 1.b si no encontró, probar sin el filtro status
    if (factNums.length === 0) {
      const reqCheck = new sql.Request();
      reqCheck.input("cod_prov", sql.VarChar, cod_prov);
      const checkRes = await reqCheck.query(
        "SELECT TOP 50 fact_num, campo5, status, fec_emis FROM factura WHERE LTRIM(RTRIM(campo5)) = @cod_prov ORDER BY fec_emis DESC"
      );

      if (!checkRes.recordset || checkRes.recordset.length === 0) {
        const reqLike = new sql.Request();
        reqLike.input("pattern", sql.VarChar, `%${cod_prov}%`);
        const likeRes = await reqLike.query(
          "SELECT TOP 50 fact_num, campo5, status, fec_emis FROM factura WHERE campo5 LIKE @pattern ORDER BY fec_emis DESC"
        );
        factNums = (likeRes.recordset || []).map(r => r.fact_num).filter(Boolean);
      } else {
        factNums = (checkRes.recordset || []).map(r => r.fact_num).filter(Boolean);
      }
    }

    if (factNums.length === 0) {
      return res.json([]);
    }

    // 2) Consultar reng_fac agrupando por co_art y sumando total_art, trayendo art_des de art
    const inParams = factNums.map((_, i) => `@f${i}`);
    const reqReng = new sql.Request();
    factNums.forEach((fn, i) => {
      reqReng.input(`f${i}`, sql.VarChar, String(fn));
    });

    const query = `
      SELECT 
        r.co_art,
        ISNULL(a.art_des, '') AS art_des,
        SUM(CAST(r.total_art AS FLOAT)) AS total_vendido
      FROM reng_fac r
      LEFT JOIN art a ON r.co_art = a.co_art
      WHERE r.fact_num IN (${inParams.join(",")})
      GROUP BY r.co_art, a.art_des
      ORDER BY total_vendido DESC
    `;

    const rengResult = await reqReng.query(query);

    const resultado = (rengResult.recordset || []).map((row) => {
      const clean = cleanStrings(row);
      return {
        co_art: clean.co_art,
        art_des: clean.art_des ?? "",
        total_vendido: Number(clean.total_vendido ?? 0),
      };
    });

    res.json(resultado);
  } catch (err) {
    res.status(500).json({ error: "Error al obtener productos más vendidos", detalle: err.message });
  }
};

export const getFacturaDetalle = async (req, res) => {
  try {
    const fact_num_raw = req.body?.fact_num ?? req.query?.fact_num;
    const fact_num = fact_num_raw === undefined || fact_num_raw === null ? null : String(fact_num_raw).trim();
    if (!fact_num) {
      return res.status(400).json({ error: "fact_num requerido" });
    }

    const reqFact = new sql.Request();
    reqFact.input("fact_num", sql.VarChar(50), fact_num);
    const facturaQuery = `
      SELECT TOP 1 f.*, c.cli_des
      FROM factura f
      LEFT JOIN clientes c ON f.co_cli = c.co_cli
      WHERE f.fact_num = @fact_num AND f.anulada = 0
    `;
    const facturaRes = await reqFact.query(facturaQuery);

    if (!facturaRes.recordset || facturaRes.recordset.length === 0) {
      return res.status(404).json({ error: "Factura no encontrada" });
    }

    const rawFactura = cleanStrings(facturaRes.recordset[0]);

    // Construir objeto factura con solo los campos requeridos
    const facturaOut = {
      cli_des: rawFactura.cli_des ?? null,
      comentario: rawFactura.comentario ?? null,
      fact_num: rawFactura.fact_num ?? null,
      fec_emis: rawFactura.fec_emis ? (new Date(rawFactura.fec_emis)).toISOString().slice(0, 10) : null,
      moneda: rawFactura.moneda ?? null,
      iva: rawFactura.iva !== undefined && rawFactura.iva !== null ? Number(rawFactura.iva) : null,
      porc_gdesc: rawFactura.porc_gdesc !== undefined && rawFactura.porc_gdesc !== null ? (isNaN(Number(rawFactura.porc_gdesc)) ? rawFactura.porc_gdesc : Number(rawFactura.porc_gdesc)) : null,
      saldo: rawFactura.saldo !== undefined && rawFactura.saldo !== null ? Number(rawFactura.saldo) : null,
      tasa: rawFactura.tasa !== undefined && rawFactura.tasa !== null ? Number(Number(rawFactura.tasa).toFixed(2)) : null,
      tot_neto: rawFactura.tot_neto !== undefined && rawFactura.tot_neto !== null ? Number(rawFactura.tot_neto) : null
    };

    // Obtener renglones de la factura
    const reqReng = new sql.Request();
    reqReng.input("fact_num", sql.VarChar(50), fact_num);
    const rengQuery = `
      SELECT r.*, ISNULL(a.art_des, '') AS art_des
      FROM reng_fac r
      LEFT JOIN art a ON r.co_art = a.co_art
      WHERE r.fact_num = @fact_num
      ORDER BY r.reng_num ASC
    `;
    const rengRes = await reqReng.query(rengQuery);
    const rawRenglones = rengRes.recordset || [];

    const renglones = rawRenglones.map((r) => {
      const item = cleanStrings(r);
      const co_alma_raw = item.co_alma ?? "";
      let co_alma_mapped = co_alma_raw;
      if (String(co_alma_raw).trim() === "01") co_alma_mapped = "S/C";
      else if (String(co_alma_raw).trim() === "04") co_alma_mapped = "BQMTO";

      return {
        art_des: (item.art_des ?? "").trim(),
        co_alma: co_alma_mapped,
        cantidad: item.cantidad !== undefined && item.cantidad !== null ? Number(item.cantidad) : null,
        total_art: item.total_art !== undefined && item.total_art !== null ? Number(item.total_art) : null,
        reng_neto: item.reng_neto !== undefined && item.reng_neto !== null ? Number(item.reng_neto) : null
      };
    });

    return res.json({ factura: facturaOut, renglones });
  } catch (err) {
    console.error("Error en getFacturaDetalle:", err);
    return res.status(500).json({ error: "Error al obtener detalle de factura", detalle: err.message });
  }
};

export const totalizarDeudaPorProveedor = async (req, res) => {
  try {
    // aceptar startDate/endDate desde body o query (formato 'YYYY-MM-DD' o datetime)
    const startRaw = req.body?.startDate ?? req.query?.startDate;
    const endRaw = req.body?.endDate ?? req.query?.endDate;

    const parseDate = (v, endOfDay = false) => {
      if (v === undefined || v === null || v === "") return null;
      const d = new Date(String(v));
      if (isNaN(d)) return null;
      if (endOfDay) d.setHours(23, 59, 59, 999);
      else d.setHours(0, 0, 0, 0);
      return d;
    };

    const sinceDate = parseDate(startRaw, false);
    const endDate = parseDate(endRaw, true);

    const request = new sql.Request();
    const dateClauses = [];
    if (sinceDate) {
      request.input("sinceDate", sql.DateTime, sinceDate);
      dateClauses.push("f.fec_emis >= @sinceDate");
    }
    if (endDate) {
      request.input("endDate", sql.DateTime, endDate);
      dateClauses.push("f.fec_emis <= @endDate");
    }

    // consulta original (sin transformaciones en JS)
    let query = `
      -- Suma del monto de descuento por proveedor (rango aplicable)
      SELECT
        LTRIM(RTRIM(f.campo5)) AS co_prov,
        ISNULL(p.prov_des, '') AS prov_des,
        CAST(ROUND(SUM(
          CASE
            WHEN TRY_CAST(f.campo6 AS FLOAT) IS NULL THEN 0
            ELSE
              (
                CASE
                  WHEN TRY_CAST(f.tasa AS FLOAT) IS NULL OR TRY_CAST(f.tasa AS FLOAT) = 0
                    THEN COALESCE(TRY_CAST(f.tot_bruto AS FLOAT), 0)
                  ELSE COALESCE(TRY_CAST(f.tot_bruto AS FLOAT), 0) / NULLIF(TRY_CAST(f.tasa AS FLOAT), 0)
                END
              ) * (TRY_CAST(f.campo6 AS FLOAT) / 100.0)
          END
        ), 2) AS DECIMAL(18,2)) AS total_desc
      FROM factura f
      INNER JOIN prov p
        ON LTRIM(RTRIM(f.campo5)) = LTRIM(RTRIM(p.co_prov))
      WHERE
        TRY_CAST(LTRIM(RTRIM(f.campo5)) AS BIGINT) IS NOT NULL
        AND LTRIM(RTRIM(f.campo5)) <> ''
        AND f.campo6 IS NOT NULL
        AND LTRIM(RTRIM(f.campo6)) <> ''
        AND TRY_CAST(f.campo6 AS FLOAT) IS NOT NULL
        AND f.anulada = 0
    `;

    if (dateClauses.length > 0) {
      query += " AND " + dateClauses.join(" AND ");
    }

    query += `
      GROUP BY LTRIM(RTRIM(f.campo5)), ISNULL(p.prov_des, '')
      ORDER BY total_desc DESC;
    `;

    // log compacto para depuración

    const result = await request.query(query);
    return res.json(result.recordset || []);
  } catch (err) {
    console.error("totalizarDeudaPorProveedor - error:", err);
    return res.status(500).json({ error: err.message });
  }
};

export const editTiemposPagoTransferencias = async (req, res) => {
  try {
    // Extraer parámetros del body
    const { id, tiempo, porcentaje, columna } = req.body;

    // Validar que se envió el ID
    if (!id) {
      return res.status(400).json({ error: "id requerido" });
    }

    const idNum = Number(id);
    if (isNaN(idNum)) {
      return res.status(400).json({ error: "id debe ser numérico" });
    }

    // Acepta id como string o número y campos como en el JSON de ejemplo:
    const updates = [];
    const params = [];

    if (tiempo !== undefined) {
      updates.push("tiempo = ?");
      params.push(String(tiempo).trim());
    }
    if (porcentaje !== undefined) {
      const pct = Number(porcentaje);
      if (isNaN(pct)) return res.status(400).json({ error: "porcentaje debe ser numérico" });
      updates.push("porcentaje = ?");
      params.push(pct);
    }
    if (columna !== undefined) {
      updates.push("columna = ?");
      params.push(String(columna).trim());
    }

    if (updates.length === 0) {
      return res.status(400).json({ error: "No hay campos para actualizar" });
    }

    // Actualizar fecha de modificación
    updates.push("fecha = NOW()");

    // id como último parámetro para el WHERE
    params.push(idNum);

    const sqlUpdate = `UPDATE tiempos_pago SET ${updates.join(", ")} WHERE id = ?`;
    const [result] = await getTransferenciasPool().execute(sqlUpdate, params);

    if (!result || result.affectedRows === 0) {
      return res.status(404).json({ error: "Registro no encontrado o sin cambios" });
    }

    // Devolver el registro actualizado
    const [rows] = await getTransferenciasPool().execute(
      "SELECT id, tiempo, porcentaje, columna, fecha FROM tiempos_pago WHERE id = ? LIMIT 1",
      [idNum]
    );
    const updated = (rows && rows.length > 0) ? cleanStrings(rows[0]) : null;

    res.json({ success: true, affectedRows: result.affectedRows, updated });
  } catch (err) {
    console.error("Error en editTiemposPagoTransferencias:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getNotasCreditoTransferencias = async (req, res) => {
  try {
    // Debug mínimo para body/query

    const includeMonthsFlag =
      (req.body?.includeMonths ?? req.query?.includeMonths) === true ||
      (req.body?.includeMonths ?? req.query?.includeMonths) === "true" ||
      (req.body?.includeMonths ?? req.query?.includeMonths) === "1";

    const startMonthRaw = req.body?.startMonth ?? req.query?.startMonth; // "YYYY-MM"
    const endMonthRaw = req.body?.endMonth ?? req.query?.endMonth;       // "YYYY-MM"

    let sqlQuery = "SELECT id, proveedor, observacion, factura, monto, fecha FROM notas_credito";
    const params = [];

    if (includeMonthsFlag) {
      if (!startMonthRaw) {
        return res.status(400).json({ error: "startMonth requerido cuando includeMonths=true" });
      }

      const parseMonth = (v) => {
        const parts = String(v).split("-");
        if (parts.length !== 2) return null;
        const y = Number(parts[0]);
        const m = Number(parts[1]);
        if (isNaN(y) || isNaN(m) || m < 1 || m > 12) return null;
        return { year: y, month: m };
      };

      const startParts = parseMonth(startMonthRaw);
      if (!startParts) return res.status(400).json({ error: "startMonth inválido. Formato esperado: YYYY-MM" });

      // fechas en formato YYYY-MM-DD (strings) para pasar como parámetros
      const startDate = new Date(startParts.year, startParts.month - 1, 1, 0, 0, 0, 0);
      let endDateObj;
      if (endMonthRaw) {
        const endParts = parseMonth(endMonthRaw);
        if (!endParts) return res.status(400).json({ error: "endMonth inválido. Formato esperado: YYYY-MM" });
        endDateObj = new Date(endParts.year, endParts.month, 0, 23, 59, 59, 999); // último día del mes
      } else {
        endDateObj = new Date(startParts.year, startParts.month, 0, 23, 59, 59, 999);
      }

      const fmt = (d) => {
        const yyyy = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, "0");
        const dd = String(d.getDate()).padStart(2, "0");
        return `${yyyy}-${mm}-${dd}`;
      };

      const startStr = fmt(startDate);
      const endStr = fmt(endDateObj);

      // Usar comparación directa con parámetros (más simple y fiable que STR_TO_DATE en prepared statements)
      sqlQuery += " WHERE DATE(fecha) >= ? AND DATE(fecha) <= ?";
      params.push(startStr, endStr);
    }

    sqlQuery += " ORDER BY fecha DESC, id DESC";

    // DEBUG: mostrar la query y parámetros antes de ejecutar

    const [rows] = await getTransferenciasPool().execute(sqlQuery, params);


    const cleaned = (rows || []).map((r) => {
      const c = cleanStrings(r);
      if (c.fecha) {
        try {
          const d = new Date(c.fecha);
          if (!isNaN(d)) c.fecha = d.toISOString().slice(0, 10);
        } catch (e) { /* ignore */ }
      }
      return {
        id: c.id,
        proveedor: c.proveedor ?? null,
        observacion: c.observacion ?? null,
        factura: c.factura ?? null,
        monto: c.monto !== undefined && c.monto !== null ? Number(c.monto) : c.monto,
        fecha: c.fecha ?? null
      };
    });

    res.json(cleaned);
  } catch (err) {
    console.error("Error en getNotasCreditoTransferencias:", err);
    res.status(500).json({ error: "Error al obtener notas de crédito", detalle: err.message });
  }
};

export const crearNotaCreditoTransferencias = async (req, res) => {
  try {
    const {
      factura,
      proveedor,
      proveedor_descripcion,
      observacion,
      monto,
      fecha
    } = req.body ?? {};

    if (!factura) return res.status(400).json({ error: "factura requerida" });
    if (!proveedor) return res.status(400).json({ error: "proveedor requerido" });
    if (monto === undefined || monto === null) return res.status(400).json({ error: "monto requerido" });

    const montoNum = Number(monto);
    if (isNaN(montoNum)) return res.status(400).json({ error: "monto debe ser numérico" });

    const fechaVal = fecha ? new Date(fecha) : new Date();
    if (isNaN(fechaVal.getTime())) return res.status(400).json({ error: "fecha inválida" });

    // Obtener columnas reales de la tabla para armar INSERT dinámico
    const [colsRows] = await getTransferenciasPool().execute(
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'notas_credito'",
      [transferenciasConfig.database]
    );
    const existingCols = new Set((colsRows || []).map(r => String(r.COLUMN_NAME).toLowerCase()));

    // Mapa de campos posibles -> valor
    const payload = {
      factura: String(factura).trim(),
      proveedor: String(proveedor).trim(),
      proveedor_descripcion: proveedor_descripcion ? String(proveedor_descripcion).trim() : null,
      observacion: observacion ? String(observacion).trim() : null,
      monto: montoNum,
      fecha: fechaVal
    };

    // Filtrar solo columnas existentes en la tabla

    const colsToInsert = [];
    const params = [];
    for (const [k, v] of Object.entries(payload)) {
      if (existingCols.has(k.toLowerCase())) {
        colsToInsert.push(k);
        params.push(v);
      }
    }

    if (colsToInsert.length === 0) {
      return res.status(400).json({ error: "Ninguna columna válida para insertar en notas_credito" });
    }

    const placeholders = colsToInsert.map(() => "?").join(", ");
    const sqlInsert = `INSERT INTO notas_credito (${colsToInsert.join(", ")}) VALUES (${placeholders})`;

    // Ejecutar INSERT
    const [result] = await getTransferenciasPool().execute(sqlInsert, params);

    let insertId = result.insertId ?? null;
    let created = null;

    if (insertId) {
      const [rows] = await getTransferenciasPool().execute(
        "SELECT * FROM notas_credito WHERE id = ? LIMIT 1",
        [insertId]
      );
      if (rows && rows.length > 0) created = cleanStrings(rows[0]);
    } else {
      // Fallback: buscar por factura+proveedor+monto reciente
      const [rows] = await getTransferenciasPool().execute(
        `SELECT * FROM notas_credito WHERE factura = ? AND proveedor = ? AND monto = ? ORDER BY id DESC LIMIT 1`,
        [String(factura).trim(), String(proveedor).trim(), montoNum]
      );
      if (rows && rows.length > 0) {
        created = cleanStrings(rows[0]);
        insertId = created.id ?? insertId;
      }
    }

    return res.status(201).json({ success: true, insertId, created, rawResult: result });
  } catch (err) {
    console.error("Error en crearNotaCreditoTransferencias:", err);
    return res.status(500).json({ error: "Error al crear nota de crédito", detalle: err.message });
  }
};

export const getNotasCreditoPorProveedor = async (req, res) => {
  try {

    const proveedor = req.body?.proveedor ?? req.query?.proveedor;
    if (!proveedor) {
      return res.status(400).json({ error: "proveedor requerido" });
    }

    const includeMonthsFlag =
      (req.body?.includeMonths ?? req.query?.includeMonths) === true ||
      (req.body?.includeMonths ?? req.query?.includeMonths) === "true" ||
      (req.body?.includeMonths ?? req.query?.includeMonths) === "1";

    const startMonthRaw = req.body?.startMonth ?? req.query?.startMonth; // "YYYY-MM"
    const endMonthRaw = req.body?.endMonth ?? req.query?.endMonth;       // "YYYY-MM"

    let sqlQuery = "SELECT id, proveedor, observacion, factura, monto, fecha FROM notas_credito WHERE proveedor = ?";
    const params = [String(proveedor).trim()];

    if (includeMonthsFlag) {
      if (!startMonthRaw) {
        return res.status(400).json({ error: "startMonth requerido cuando includeMonths=true" });
      }
      const parseMonth = (v) => {
        const parts = String(v).split("-");
        if (parts.length !== 2) return null;
        const y = Number(parts[0]);
        const m = Number(parts[1]);
        if (isNaN(y) || isNaN(m) || m < 1 || m > 12) return null;
        return { year: y, month: m };
      };
      const startParts = parseMonth(startMonthRaw);
      if (!startParts) return res.status(400).json({ error: "startMonth inválido. Formato esperado: YYYY-MM" });

      const startDate = new Date(startParts.year, startParts.month - 1, 1, 0, 0, 0, 0);
      let endDateObj;
      if (endMonthRaw) {
        const endParts = parseMonth(endMonthRaw);
        if (!endParts) return res.status(400).json({ error: "endMonth inválido. Formato esperado: YYYY-MM" });
        endDateObj = new Date(endParts.year, endParts.month, 0, 23, 59, 59, 999);
      } else {
        endDateObj = new Date(startParts.year, startParts.month, 0, 23, 59, 59, 999);
      }

      const fmt = (d) => {
        const yyyy = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, "0");
        const dd = String(d.getDate()).padStart(2, "0");
        return `${yyyy}-${mm}-${dd}`;
      };

      params.push(fmt(startDate), fmt(endDateObj));
      sqlQuery += " AND DATE(fecha) >= ? AND DATE(fecha) <= ?";
    }

    sqlQuery += " ORDER BY fecha DESC, id DESC";

    const [rows] = await getTransferenciasPool().execute(sqlQuery, params);


    const cleaned = (rows || []).map((r) => {
      const c = cleanStrings(r);
      if (c.fecha) {
        try {
          const d = new Date(c.fecha);
          if (!isNaN(d)) c.fecha = d.toISOString().slice(0, 10);
        } catch (e) { }
      }
      return {
        id: c.id,
        proveedor: c.proveedor ?? null,
        observacion: c.observacion ?? null,
        factura: c.factura ?? null,
        monto: c.monto !== undefined && c.monto !== null ? Number(c.monto) : c.monto,
        fecha: c.fecha ?? null
      };
    });

    res.json(cleaned);
  } catch (err) {
    console.error("Error en getNotasCreditoPorProveedor:", err);
    res.status(500).json({ error: "Error al obtener notas de crédito por proveedor", detalle: err.message });
  }
};

export const getProductosMasVendidosPorProveedoresCampo5 = async (req, res) => {
  try {
    // Opciones: startDate/endDate (YYYY-MM-DD), status (number), topPerProvider (number)
    const startRaw = req.body?.startDate ?? req.query?.startDate;
    const endRaw = req.body?.endDate ?? req.query?.endDate;
    const status = req.body?.status ?? req.query?.status; // opcional
    const topPerProvider = Number(req.body?.topPerProvider ?? req.query?.topPerProvider ?? 10);

    const parseDateOrNull = (v, endOfDay = false) => {
      if (v === undefined || v === null || v === "") return null;
      const d = new Date(String(v));
      if (isNaN(d)) return null;
      if (endOfDay) d.setHours(23, 59, 59, 999);
      else d.setHours(0, 0, 0, 0);
      return d;
    };

    const startDateProvided = parseDateOrNull(startRaw, false);
    const endDateProvided = parseDateOrNull(endRaw, true);

    // helper para ejecutar la consulta con un par de fechas (puede ser null para omitir)
    const runQueryWithDates = async (sd, ed) => {
      const req = new sql.Request();
      const where = [
        "f.campo5 IS NOT NULL",
        "LTRIM(RTRIM(f.campo5)) <> ''",
        "f.anulada = 0"
      ];

      if (sd) {
        req.input("startDate", sql.DateTime, sd);
        where.push("f.fec_emis >= @startDate");
      }
      if (ed) {
        req.input("endDate", sql.DateTime, ed);
        where.push("f.fec_emis <= @endDate");
      }
      if (status !== undefined && status !== null && String(status).trim() !== "") {
        req.input("status", sql.Int, Number(status));
        where.push("f.status = @status");
      }

      const query = `
        SELECT
          LTRIM(RTRIM(f.campo5)) AS co_prov,
          ISNULL(p.prov_des, '') AS prov_des,
          r.co_art,
          ISNULL(a.art_des, '') AS art_des,
          SUM(CAST(r.total_art AS FLOAT)) AS total_vendido
        FROM factura f
        INNER JOIN reng_fac r ON f.fact_num = r.fact_num
        LEFT JOIN art a ON r.co_art = a.co_art
        LEFT JOIN prov p ON LTRIM(RTRIM(f.campo5)) = LTRIM(RTRIM(p.co_prov))
        WHERE ${where.join(" AND ")}
        GROUP BY LTRIM(RTRIM(f.campo5)), ISNULL(p.prov_des, ''), r.co_art, ISNULL(a.art_des, '')
        ORDER BY LTRIM(RTRIM(f.campo5)), SUM(CAST(r.total_art AS FLOAT)) DESC
      `;
      const result = await req.query(query);
      return result.recordset || [];
    };

    // Si el cliente envió fechas, usarlas directamente (sin expandir)
    let rows = [];
    if (startDateProvided || endDateProvided) {
      rows = await runQueryWithDates(startDateProvided, endDateProvided);
    } else {
      // Construir intentos: mes actual -> últimos 3 meses -> últimos 12 meses -> sin fecha
      const now = new Date();
      const firstDayCurrentMonth = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0);
      const lastDayCurrentMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59, 999);

      const threeMonthsAgo = new Date(now);
      threeMonthsAgo.setMonth(now.getMonth() - 3);
      threeMonthsAgo.setHours(0, 0, 0, 0);

      const oneYearAgo = new Date(now);
      oneYearAgo.setFullYear(now.getFullYear() - 1);
      oneYearAgo.setHours(0, 0, 0, 0);

      const attempts = [
        { sd: firstDayCurrentMonth, ed: lastDayCurrentMonth },
        { sd: threeMonthsAgo, ed: lastDayCurrentMonth },
        { sd: oneYearAgo, ed: lastDayCurrentMonth },
        { sd: null, ed: null } // último recurso: sin filtro de fecha
      ];

      for (const a of attempts) {
        rows = await runQueryWithDates(a.sd, a.ed);
        if (rows.length > 0) break; // si encontramos datos, detener la expansión
      }
    }

    // Agrupar por proveedor y tomar los top N por proveedor
    const map = new Map();
    for (const r of rows) {
      const clean = cleanStrings(r);
      const co = (clean.co_prov ?? "").toString().trim();
      if (!co) continue;
      const item = {
        co_art: clean.co_art ?? null,
        art_des: (clean.art_des ?? "").toString().trim(),
        total_vendido: Number(clean.total_vendido ?? 0)
      };
      if (!map.has(co)) {
        map.set(co, { co_prov: co, prov_des: (clean.prov_des ?? "").toString().trim(), productos: [item] });
      } else {
        map.get(co).productos.push(item);
      }
    }

    const output = Array.from(map.values()).map((entry) => {
      entry.productos.sort((a, b) => b.total_vendido - a.total_vendido);
      if (Number.isInteger(topPerProvider) && topPerProvider > 0) {
        entry.productos = entry.productos.slice(0, topPerProvider);
      }
      return entry;
    });

    // Siempre devolver array (puede estar vacío si no hay registros en ningún intento)
    res.json(output);
  } catch (err) {
    console.error("Error en getProductosMasVendidosPorProveedoresCampo5:", err);
    res.status(500).json({ error: "Error al obtener productos más vendidos por proveedores (campo5)", detalle: err.message });
  }
};

export const buscarNotasCreditoPorProveedorExacto = async (req, res) => {
  try {
    // Mostrar en consola todo lo que llega al endpoint para depuración

    const proveedorRaw = req.body?.proveedor ?? req.query?.proveedor;

    if (!proveedorRaw) {
      return res.status(400).json({ error: "proveedor requerido en body (JSON) o en query" });
    }

    const proveedor = String(proveedorRaw).trim();

    // Si el valor es sólo dígitos buscamos coincidencias al inicio del campo proveedor
    // para cubrir formatos como "43. - CRIST MEDICALS - ..." o "375 - CASA ..."
    let sqlQuery;
    let params;
    if (/^\d+$/.test(proveedor)) {
      const code = proveedor;
      // REGEXP que busca el código al inicio (posibles espacios) seguido de espacio, punto, guion o fin de string
      const pattern = `^[[:space:]]*${code}([[:space:].-]|$)`;
      sqlQuery = "SELECT id, proveedor, observacion, factura, monto, fecha FROM notas_credito WHERE proveedor REGEXP ? ORDER BY fecha DESC";
      params = [pattern];
    } else {
      // Si se pasa texto, hacemos LIKE para buscar coincidencias parciales (case-insensitive depende de collation)
      sqlQuery = "SELECT id, proveedor, observacion, factura, monto, fecha FROM notas_credito WHERE proveedor LIKE ? ORDER BY fecha DESC";
      params = [`%${proveedor}%`];
    }

    const [rows] = await getTransferenciasPool().execute(sqlQuery, params);

    const cleaned = (rows || []).map((r) => {
      const c = cleanStrings(r);
      if (c.fecha) {
        try {
          const d = new Date(c.fecha);
          if (!isNaN(d)) c.fecha = d.toISOString().slice(0, 10);
        } catch (e) { /* ignore */ }
      }
      return {
        id: c.id,
        proveedor: c.proveedor ?? null,
        observacion: c.observacion ?? null,
        factura: c.factura ?? null,
        monto: c.monto !== undefined && c.monto !== null ? Number(c.monto) : c.monto,
        fecha: c.fecha ?? null,
      };
    });

    res.json(cleaned);
  } catch (err) {
    console.error("Error en buscarNotasCreditoPorProveedorExacto:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getStAlmacPorProveedor = async (req, res) => {
  try {
    const co_prov = req.body?.co_prov ?? req.query?.co_prov;
    if (!co_prov) {
      return res.status(400).json({ error: "co_prov requerido" });
    }

    const request = new sql.Request();
    request.input("co_prov", sql.VarChar, String(co_prov).trim());

    const query = `
      SELECT 
        a.co_art,
        a.art_des AS des_art,
        s.co_alma,
        s.stock_act
      FROM st_almac s
      INNER JOIN art a ON s.co_art = a.co_art
      WHERE a.co_prov = @co_prov
        AND s.co_alma IN ('01','02','04','05')
      ORDER BY a.art_des, s.co_alma
    `;

    const result = await request.query(query);
    const rows = result.recordset || [];

    // Agrupar por artículo y devolver des_art + lista de almacenes con co_alma y stock_act
    const map = new Map();
    for (const r of rows) {
      const c = cleanStrings(r);
      const key = String(c.co_art ?? "").trim() || String(c.des_art ?? "").trim();
      if (!map.has(key)) {
        map.set(key, {
          des_art: (c.des_art ?? "").toString().trim(),
          almacenes: []
        });
      }
      map.get(key).almacenes.push({
        co_alma: c.co_alma ?? null,
        stock_act: c.stock_act !== undefined && c.stock_act !== null ? Number(c.stock_act) : c.stock_act
      });
    }

    const output = Array.from(map.values());
    res.json(output);
  } catch (err) {
    console.error("Error en getStAlmacPorProveedor:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getUsuarios = async (req, res) => {
  try {
    const { id, user, rol } = req.query ?? {};
    let sqlQuery = "SELECT id, user, rol, telefono, estado, segmento, session_id, proveedor, status, fecha, catalogo FROM usuarios";
    const params = [];
    const where = [];

    if (id) {
      where.push("id = ?");
      params.push(Number(id));
    }
    if (user) {
      where.push("user LIKE ?");
      params.push(`%${String(user).trim()}%`);
    }
    if (rol) {
      where.push("rol = ?");
      params.push(String(rol).trim());
    }

    if (where.length) sqlQuery += " WHERE " + where.join(" AND ");
    sqlQuery += " ORDER BY id DESC";

    const [rows] = await getTransferenciasPool().execute(sqlQuery, params);
    res.json((rows || []).map(cleanStrings));
  } catch (err) {
    console.error("getUsuarios:", err);
    res.status(500).json({ error: err.message });
  }
};

export const editUsuario = async (req, res) => {
  try {
    const payload = req.body ?? {};
    const idRaw = payload.id ?? payload.userId;
    if (!idRaw) return res.status(400).json({ error: "id requerido" });

    const id = Number(idRaw);
    if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: "id inválido" });

    // Campos permitidos para actualizar
    const allowed = ["user", "password", "rol", "telefono", "estado", "segmento", "session_id", "proveedor", "status", "catalogo"];
    const updates = [];
    const params = [];

    for (const key of allowed) {
      let val = payload[key];

      // Soporte para alias 'usuario' -> 'user'
      if (key === "user" && val === undefined && payload.usuario !== undefined) {
        val = payload.usuario;
      }

      if (val === undefined) continue;

      if (key === "password") {
        const passwordStr = String(val).trim();
        if (!passwordStr) continue; // omitir si vacío
        const salt = await bcrypt.genSalt(10);
        const hashed = await bcrypt.hash(passwordStr, salt);
        updates.push("password = ?");
        params.push(hashed);
      } else if (["segmento", "proveedor", "catalogo"].includes(key)) {
        // Manejo de arrays para estos campos (consistencia con registrarUsuario)
        const strVal = Array.isArray(val) ? val.join(",") : (val === null ? null : String(val).trim());
        updates.push(`${key} = ?`);
        params.push(strVal);
      } else {
        updates.push(`${key} = ?`);
        params.push(val === null ? null : String(val).trim());
      }
    }

    if (updates.length === 0) return res.status(400).json({ error: "No hay campos para actualizar" });

    params.push(id);
    const updateSql = `UPDATE usuarios SET ${updates.join(", ")} WHERE id = ?`;
    const [result] = await getTransferenciasPool().execute(updateSql, params);

    // Traer el registro actualizado
    const [rows] = await getTransferenciasPool().execute(
      "SELECT id, user, rol, telefono, estado, segmento, session_id, proveedor, status, fecha, catalogo FROM usuarios WHERE id = ? LIMIT 1",
      [id]
    );
    const updated = (rows && rows.length > 0) ? cleanStrings(rows[0]) : null;

    res.json({ success: true, affectedRows: result.affectedRows ?? result.changedRows ?? 0, updated });
  } catch (err) {
    console.error("editUsuario:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getTodosPedidos = async (req, res) => {
  try {
    // Acepta desde body o query
    const startRaw = req.body?.startDate ?? req.query?.startDate;
    const endRaw = req.body?.endDate ?? req.query?.endDate;

    const hasStart = typeof startRaw === "string" && startRaw.trim().length > 0;
    const hasEnd = typeof endRaw === "string" && endRaw.trim().length > 0;

    // Construir SQL dinámico según rango de fecha (DATETIME)
    let sqlBase = `
      SELECT id, fact_num, cod_cliente, cod_prov, tot_bruto, tot_neto, saldo, iva, descrip, co_us_in, fecha, tasa
      FROM pedidos
    `;
    const params = [];
    const where = [];

    if (hasStart) {
      where.push("fecha >= ?");
      params.push(startRaw.trim()); // ej: "2025-11-24 00:00:00"
    }
    if (hasEnd) {
      where.push("fecha <= ?");
      params.push(endRaw.trim()); // ej: "2025-11-24 23:59:59"
    }

    if (where.length > 0) {
      sqlBase += " WHERE " + where.join(" AND ");
    }

    sqlBase += " ORDER BY fecha DESC, id DESC";

    const [rows] = await getTransferenciasPool().execute(sqlBase, params);

    // Mapear cod_cliente -> cli_des y cod_prov -> prov_des usando SQL Server (Profit)
    if (rows && rows.length > 0) {
      const clientesSet = new Set();
      const provSet = new Set();
      const pedidosConFactNum = []; // para rastreo de factura en Profit

      for (const r of rows) {
        const coCli = r.cod_cliente == null ? "" : String(r.cod_cliente).trim();
        const coProv = r.cod_prov == null ? "" : String(r.cod_prov).trim();
        if (coCli) clientesSet.add(coCli);
        if (coProv) provSet.add(coProv);

        // Guardar fact_num del pedido si existe para rastreo en Profit
        const pedidoFactNum = r.fact_num == null ? "" : String(r.fact_num).trim();
        if (pedidoFactNum) pedidosConFactNum.push(pedidoFactNum);
      }

      // Clientes
      let clientesMap = {};
      if (clientesSet.size > 0) {
        const reqCli = new sql.Request();
        const inParams = Array.from(clientesSet).map((_, i) => `@cli${i}`);
        Array.from(clientesSet).forEach((c, i) => reqCli.input(`cli${i}`, sql.VarChar, c));
        const qCli = `SELECT co_cli, cli_des FROM clientes WHERE co_cli IN (${inParams.join(",")})`;
        const rCli = await reqCli.query(qCli);
        (rCli.recordset || []).forEach((row) => {
          const key = row.co_cli == null ? "" : String(row.co_cli).trim();
          clientesMap[key] = typeof row.cli_des === "string" ? row.cli_des.trim() : row.cli_des;
        });
      }

      // Proveedores
      let provMap = {};
      if (provSet.size > 0) {
        const reqProv = new sql.Request();
        const inParams = Array.from(provSet).map((_, i) => `@prov${i}`);
        Array.from(provSet).forEach((p, i) => reqProv.input(`prov${i}`, sql.VarChar, p));
        const qProv = `SELECT co_prov, prov_des FROM prov WHERE co_prov IN (${inParams.join(",")})`;
        const rProv = await reqProv.query(qProv);
        (rProv.recordset || []).forEach((row) => {
          const key = row.co_prov == null ? "" : String(row.co_prov).trim();
          provMap[key] = typeof row.prov_des === "string" ? row.prov_des.trim() : row.prov_des;
        });
      }

      // Reemplazar nombres en salida
      for (const r of rows) {
        const coCli = r.cod_cliente == null ? "" : String(r.cod_cliente).trim();
        const coProv = r.cod_prov == null ? "" : String(r.cod_prov).trim();
        const cliDes = clientesMap[coCli];
        const provDes = provMap[coProv];
        if (cliDes) r.cod_cliente = cliDes;
        if (provDes) r.cod_prov = provDes;
      }

      // Rastreo del número de factura en Profit por cada pedido (cadena: reng_nde -> reng_fac)
      // Nota: se hace por pedido para mantener la lógica exacta de getPedidosPorUsuario.
      for (const r of rows) {
        const pedidoFactNum = r.fact_num == null ? "" : String(r.fact_num).trim();
        if (!pedidoFactNum) {
          r.profit_fact_num = null;
          continue;
        }

        try {
          const requestTrace = new sql.Request();
          requestTrace.input("pedidoFactNum", sql.VarChar, pedidoFactNum);

          const traceQuery = `
            DECLARE @NumDocPedido VARCHAR(50) = @pedidoFactNum;
            DECLARE @NumNotaEntrega INT; 
            DECLARE @NumFactura INT;

            -- Paso A: Buscar NDE usando el número de pedido (tipo 'T' para transferencias)
            SELECT TOP 1 @NumNotaEntrega = fact_num 
            FROM reng_nde 
            WHERE num_doc = @NumDocPedido
              AND tipo_doc = 'T';

            -- Paso B: Buscar Factura usando el número de NDE
            IF @NumNotaEntrega IS NOT NULL
            BEGIN
                SELECT TOP 1 @NumFactura = fact_num 
                FROM reng_fac 
                WHERE num_doc = CAST(@NumNotaEntrega AS VARCHAR);
            END

            -- Paso C: Fallback - Buscar Factura vinculada DIRECTAMENTE al pedido (tipo 'T')
            IF @NumFactura IS NULL
            BEGIN
                SELECT TOP 1 @NumFactura = fact_num 
                FROM reng_fac 
                WHERE num_doc = @NumDocPedido AND tipo_doc = 'T';
            END

            -- Devolver número de factura encontrado
            SELECT @NumFactura AS fact_num;
          `;

          const traceRes = await requestTrace.query(traceQuery);
          const factRow = traceRes.recordset && traceRes.recordset[0] ? traceRes.recordset[0] : null;
          const foundFactNum = factRow?.fact_num ?? null;
          r.profit_fact_num = foundFactNum ? String(foundFactNum).trim() : null;
        } catch (e) {
          r.profit_fact_num = null;
        }
      }
    }

    res.json((rows || []).map(cleanStrings));
  } catch (err) {
    console.error("Error en getTodosPedidos:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getPedidosConInconsistencias = async (req, res) => {
  try {
    const { startDate, endDate } = req.body;

    // Obtener pedidos de MySQL junto con el conteo de productos en una sola consulta
    let pedidosSql = `
      SELECT 
        p.id, 
        p.fact_num, 
        p.cod_cliente, 
        p.cod_prov, 
        p.co_us_in, 
        p.fecha, 
        p.tot_neto,
        COUNT(pp.id) as cant_articulos_mysql
      FROM pedidos p
      LEFT JOIN pedido_productos pp ON p.id = pp.pedido_id
      WHERE p.fact_num IS NOT NULL AND p.fact_num != ''
    `;
    const params = [];

    if (startDate) {
      pedidosSql += " AND p.fecha >= ?";
      params.push(startDate);
    }
    if (endDate) {
      pedidosSql += " AND p.fecha <= ?";
      params.push(endDate);
    }
    pedidosSql += " GROUP BY p.id, p.fact_num, p.cod_cliente, p.cod_prov, p.co_us_in, p.fecha, p.tot_neto";
    pedidosSql += " ORDER BY p.fecha DESC";
    if (!startDate && !endDate) {
      pedidosSql += " LIMIT 200";
    }
    const [pedidos] = await getTransferenciasPool().execute(pedidosSql, params);
    // Función auxiliar para consultar Profit unitariamente (lógica encapsulada)
    const checkProfitInconsistency = async (pedido) => {
      const cantMySQL = pedido.cant_articulos_mysql;
      if (cantMySQL === 0) return null;

      const traceQuery = `
        DECLARE @NumDocPedido VARCHAR(50) = @pedidoFactNum;
        DECLARE @NumNotaEntrega INT; 
        DECLARE @NumFactura INT;
        DECLARE @CoVendedor VARCHAR(20);
        DECLARE @VenDes VARCHAR(100);
        -- Obtener Vendedor del Cliente
        SELECT TOP 1 
            @CoVendedor = c.co_ven,
            @VenDes = v.ven_des
        FROM clientes c
        LEFT JOIN vendedor v ON c.co_ven = v.co_ven
        WHERE c.co_cli = @codCliente;

        -- Paso A: Buscar NDE usando el número de pedido (tipo 'T' para transferencias)
        SELECT TOP 1 @NumNotaEntrega = fact_num 
        FROM reng_nde 
        WHERE num_doc = @NumDocPedido AND tipo_doc = 'T';

        -- Paso B: Buscar Factura usando el número de NDE
        IF @NumNotaEntrega IS NOT NULL
        BEGIN
            SELECT TOP 1 @NumFactura = fact_num 
            FROM reng_fac 
            WHERE num_doc = CAST(@NumNotaEntrega AS VARCHAR);
        END

        -- Paso C: Fallback - Buscar Factura vinculada DIRECTAMENTE al pedido (tipo 'T')
        IF @NumFactura IS NULL
        BEGIN
            SELECT TOP 1 @NumFactura = fact_num 
            FROM reng_fac 
            WHERE num_doc = @NumDocPedido AND tipo_doc = 'T';
        END

        -- RESULTSET 0: Metadata (Vendedor y Factura Header)
        SELECT @CoVendedor AS co_ven, @VenDes AS ven_des, @NumFactura AS fact_num_profit;

        -- RESULTSET 1: Renglones para comparar detalle (desde NDE tipo 'T')
        SELECT co_art
        FROM reng_nde 
        WHERE num_doc = @NumDocPedido AND tipo_doc = 'T';
      `;

      try {
        const requestTrace = new sql.Request();
        const pedidoFactNumStr = String(pedido.fact_num).trim();
        requestTrace.input("pedidoFactNum", sql.VarChar, pedidoFactNumStr);
        requestTrace.input("codCliente", sql.VarChar, String(pedido.cod_cliente || "").trim());
        const result = await requestTrace.query(traceQuery);
        // result.recordsets[0] -> Metadata (Vendedor)
        // result.recordsets[1] -> Items
        if (result.recordsets.length > 0) {
          const metaProfit = result.recordsets[0];
          const rowsProfit = result.recordsets[1] || [];
          const cantProfit = rowsProfit.length;
          const coVendedor = metaProfit.length > 0 ? metaProfit[0].co_ven : null;
          const venDes = metaProfit.length > 0 ? metaProfit[0].ven_des : null;
          const factNumProfit = metaProfit.length > 0 ? metaProfit[0].fact_num_profit : null;
          if (cantMySQL !== cantProfit) {
            return {
              pedido_id_mysql: pedido.id,
              pedido_numero_mysql: pedidoFactNumStr,
              usuario: pedido.co_us_in,
              co_ven: venDes || coVendedor,
              fecha_mysql: pedido.fecha,
              cant_articulos_mysql: cantMySQL,
              cant_articulos_profit: cantProfit,
              factura_profit_num: factNumProfit,
              diferencia: cantMySQL - cantProfit,
              cliente: pedido.cod_cliente,
              proveedor: pedido.cod_prov,
              articulos_profit: rowsProfit.map(r => String(r.co_art).trim())
            };
          }
        }
      } catch (errSQL) {
        console.error(`Error consultando profit para pedido ${pedido.fact_num}:`, errSQL.message);
      }
      return null;
    };

    // Procesar en paralelo con lotes para no saturar conexiones
    const BATCH_SIZE = 20;
    const allResults = [];

    for (let i = 0; i < pedidos.length; i += BATCH_SIZE) {
      const batch = pedidos.slice(i, i + BATCH_SIZE);
      const results = await Promise.all(batch.map(p => checkProfitInconsistency(p)));
      results.forEach(r => {
        if (r) allResults.push(r);
      });
    }

    // Si hay inconsistencias, obtener datos adicionales de MySQL para esos pedidos
    if (allResults.length > 0) {
      // Obtener IDs de los pedidos con inconsistencias
      const inconsistentIds = allResults.map(i => i.pedido_id_mysql);

      if (inconsistentIds.length > 0) {
        // Consultar productos de los pedidos inconsistentes
        const placeholders = inconsistentIds.map(() => '?').join(',');
        const [productosRows] = await getTransferenciasPool().execute(
          `SELECT pedido_id, co_art
             FROM pedido_productos 
             WHERE pedido_id IN (${placeholders})`,
          inconsistentIds
        );

        // Agrupar productos por pedido_id
        const productosPorPedido = {};
        for (const row of productosRows) {
          productosPorPedido[row.pedido_id] = productosPorPedido[row.pedido_id] || [];
          productosPorPedido[row.pedido_id].push({
            co_art: row.co_art
          });
        }

        // Asignar los productos a cada resultado de inconsistencia
        for (const item of allResults) {
          const rawMysql = productosPorPedido[item.pedido_id_mysql] || [];
          item.productos = rawMysql;

          // Análisis detallado de inconsistencias
          const listMysql = rawMysql.map(p => String(p.co_art).trim());
          const listProfit = item.articulos_profit || [];

          const mysqlCounts = {};
          for (const p of listMysql) mysqlCounts[p] = (mysqlCounts[p] || 0) + 1;

          const profitCounts = {};
          for (const p of listProfit) profitCounts[p] = (profitCounts[p] || 0) + 1;

          const enMysqlNoProfit = [];
          const enProfitNoMysql = [];

          const allKeys = new Set([...Object.keys(mysqlCounts), ...Object.keys(profitCounts)]);

          for (const k of allKeys) {
            const cM = mysqlCounts[k] || 0;
            const cP = profitCounts[k] || 0;
            if (cM > cP) {
              // Faltan en profit (o sobran en mysql)
              for (let i = 0; i < cM - cP; i++) enMysqlNoProfit.push(k);
            } else if (cP > cM) {
              // Sobran en profit (o faltan en mysql)
              for (let i = 0; i < cP - cM; i++) enProfitNoMysql.push(k);
            }
          }

          item.inconsistencia_detalle = {
            faltan_en_profit: enMysqlNoProfit,
            sobran_en_profit: enProfitNoMysql,
            resumen: `Faltan en Profit: ${enMysqlNoProfit.length > 0 ? enMysqlNoProfit.join(", ") : "Ninguno"}. Sobran en Profit: ${enProfitNoMysql.length > 0 ? enProfitNoMysql.join(", ") : "Ninguno"}.`
          };

          // Limpiar la lista temporal de profit
          delete item.articulos_profit;
        }
      }
    }
    res.json(allResults);
  } catch (err) {
    console.error("Error en getPedidosConInconsistencias:", err);
    res.status(500).json({ error: err.message });
  }
};