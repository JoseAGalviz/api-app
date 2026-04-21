import 'dotenv/config';
import { sql } from "../config/database.js";
import { v4 as uuidv4 } from "uuid";
import mysql from "mysql2/promise";
import { ejecutarConsulta, limpiarValor, parseNumberFromString, obtenerFechaVenezuelaISO } from "../utils/helpers.js";

const transferenciasPool = mysql.createPool({
  host: process.env.DB_TRANS_HOST,
  user: process.env.DB_TRANS_USER,
  password: process.env.DB_TRANS_PASSWORD,
  database: "app",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

/**
 * Obtiene el siguiente número de factura desde la tabla Sucursales (cotc_num).
 * Incrementa el valor y actualiza la base de datos.
 * @returns {Promise<number|null>} - El nuevo fact_num o null si hay error.
 */
async function obtenerNuevoFactNum() {
  const co_alma = "01";
  try {
    const selectReq = new sql.Request();
    selectReq.input("co_alma", sql.VarChar, co_alma);
    const select = await selectReq.query(
      "SELECT cotc_num FROM Sucursales WHERE RTRIM(co_alma) = @co_alma",
    );
    if (!select.recordset || !select.recordset[0]) {
      console.error("Error: No se encontró Sucursal con co_alma = 01");
      return null;
    }
    const nuevoFactNum = parseInt(select.recordset[0].cotc_num, 10) + 1;

    const updateReq = new sql.Request();
    updateReq.input("nuevoFactNum", sql.Int, nuevoFactNum);
    updateReq.input("co_alma", sql.VarChar, co_alma);
    await updateReq.query(
      "UPDATE Sucursales SET cotc_num = @nuevoFactNum WHERE RTRIM(co_alma) = @co_alma",
    );

    return nuevoFactNum;
  } catch (err) {
    console.error("Error en obtenerNuevoFactNum:", err.message);
    return null;
  }
}

/**
 * Función de respaldo para obtener el siguiente número de factura.
 * @returns {Promise<number|null>} - El siguiente fact_num.
 */
async function obtenerSiguienteFactNum() {
  try {
    const query =
      "SELECT ISNULL(MAX(CAST(fact_num AS INT)), 0) + 1 AS nextFactNum FROM pedidos";
    const result = await ejecutarConsulta(query);
    if (result && result.length > 0) {
      return result[0].nextFactNum;
    } else {
      return 1;
    }
  } catch (error) {
    console.error("Error al obtener el siguiente fact_num:", error);
    return null;
  }
}

/**
 * Obtiene el código de vendedor (co_ven) asociado a un cliente.
 * @param {string} co_cli - Código del cliente.
 * @returns {Promise<string>} - Código del vendedor o "01" por defecto.
 */
async function obtenerCoVenPorCliente(co_cli) {
  const result = await ejecutarConsulta(
    `SELECT co_ven FROM clientes WHERE co_cli = @co_cli`,
    { co_cli },
  );
  return result && result.length > 0 ? result[0].co_ven : "01";
}

/**
 * Obtiene datos completos de un artículo desde la tabla art.
 * @param {string} co_art - Código del artículo.
 * @returns {Promise<Object|null>} - Objeto con datos del artículo o null.
 */
async function obtenerDatosArticulo(co_art) {
  const result = await ejecutarConsulta(
    `SELECT stock_act, prec_vta1, prec_vta2, prec_vta3, prec_vta4, prec_vta5, tipo_imp, co_sucu, co_cat, 
                cos_pro_un, ult_cos_un, ult_cos_om, art_des
         FROM art WHERE co_art = @co_art`,
    { co_art: String(co_art) },
  );
  if (result && result.length > 0) {
    return {
      stock_act: result[0].stock_act,
      prec_vta1: result[0].prec_vta1,
      prec_vta2: result[0].prec_vta2,
      prec_vta3: result[0].prec_vta3,
      prec_vta4: result[0].prec_vta4,
      prec_vta5: result[0].prec_vta5,
      tipo_imp: result[0].tipo_imp,
      co_sucu: result[0].co_sucu,
      co_cat: result[0].co_cat,
      cos_pro_un: result[0].cos_pro_un,
      ult_cos_un: result[0].ult_cos_un,
      ult_cos_om: result[0].ult_cos_om,
      art_des: result[0].art_des,
    };
  }
  return null;
}

/**
 * Detecta dinámicamente las columnas de nombre y RIF en la tabla clientes.
 * @param {string} co_cli - Código del cliente.
 * @returns {Promise<Object>} - {nombre, rif}.
 */
async function obtenerNombreYRifCliente(co_cli) {
  try {
    const cols = await ejecutarConsulta(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'clientes'`,
    );
    const colNames = (cols || []).map((r) =>
      String(r.COLUMN_NAME).toLowerCase(),
    );

    const nameCandidates = [
      "nombre",
      "nombre_cli",
      "cli_des",
      "razon_social",
      "raz_soc",
      "nom_cli",
      "razon",
      "nombrecliente",
    ];
    const rifCandidates = ["rif", "nit", "rfc", "nit_cli", "rif_cli"];

    const nameCol = nameCandidates.find((c) =>
      colNames.includes(c.toLowerCase()),
    );
    const rifCol = rifCandidates.find((c) =>
      colNames.includes(c.toLowerCase()),
    );

    if (!nameCol && !rifCol) return { nombre: "", rif: "" };

    const selectCols = [];
    if (nameCol) selectCols.push(`${nameCol} AS nombre`);
    if (rifCol) selectCols.push(`${rifCol} AS rif`);

    const query = `SELECT ${selectCols.join(
      ", ",
    )} FROM clientes WHERE RTRIM(co_cli) = @co_cli`;
    const cli = await ejecutarConsulta(query, { co_cli });

    if (cli && cli.length > 0) {
      return {
        nombre: limpiarValor(cli[0].nombre),
        rif: limpiarValor(cli[0].rif),
      };
    }
    return { nombre: "", rif: "" };
  } catch (e) {
    console.warn(
      `No se pudo obtener nombre/rif dinámicamente para ${co_cli}: ${e.message}`,
    );
    return { nombre: "", rif: "" };
  }
}

/**
 * Obtiene las longitudes máximas de las columnas de cotiz_c para evitar truncamientos.
 * @returns {Promise<Object>} - Mapa {columna: longitud_maxima}.
 */
async function obtenerLongitudesColumnasCotizC() {
  try {
    const cols = await ejecutarConsulta(
      `SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'cotiz_c'`,
    );
    const map = {};
    (cols || []).forEach((r) => {
      const name = String(r.COLUMN_NAME).toLowerCase();
      map[name] =
        r.CHARACTER_MAXIMUM_LENGTH === null
          ? null
          : Number(r.CHARACTER_MAXIMUM_LENGTH);
    });
    return map;
  } catch (e) {
    console.warn(
      "No se pudieron obtener longitudes de columnas de cotiz_c:",
      e.message,
    );
    return {};
  }
}

/**
 * Obtiene el porcentaje de descuento para un artículo o categoría.
 * Busca primero por co_art, luego por co_cat.
 * @param {string} co_art - Código del artículo.
 * @param {string} co_cat - Código de categoría.
 * @returns {Promise<number>} - Porcentaje de descuento (0.0 si no existe).
 */
async function obtenerDescuentoArticulo(co_art, co_cat) {
  const result = await ejecutarConsulta(
    `SELECT TOP 1 porc1 FROM descuen WHERE co_desc = @co_art OR co_desc = @co_cat ORDER BY CASE WHEN co_desc = @co_art THEN 1 ELSE 2 END`,
    { co_art, co_cat },
  );
  return result && result.length > 0 ? Number(result[0].porc1) : 0.0;
}

/**
 * Obtiene el descuento global del cliente.
 * @param {string} co_cli - Código del cliente.
 * @returns {Promise<number>} - Descuento global (0.0 si no existe).
 */
async function obtenerDescGlobCliente(co_cli) {
  const result = await ejecutarConsulta(
    `SELECT desc_glob FROM clientes WHERE RTRIM(co_cli) = @co_cli`,
    { co_cli },
  );
  return result && result.length > 0 && result[0].desc_glob != null
    ? Number(result[0].desc_glob)
    : 0.0;
}

/**
 * Formatea el porcentaje de descuento para insertar en reng_cac.
 * @param {string} co_art - Código del artículo.
 * @param {string} co_cat - Código de categoría.
 * @returns {Promise<string>} - String formateado '0.00+XX.XX+0.00'.
 */
async function formato_porc_desc_para_reng(co_art, co_cat) {
  const porc_desc =
    (await obtenerDescuentoArticulo(String(co_art), String(co_cat))) || 0.0;
  return `0.00+${porc_desc.toFixed(2)}+0.00`;
}

/**
 * Extrae un porcentaje o número desde la cadena descrip (ej: "Pg en $ 4%" -> "4").
 * @param {string} descrip - Descripción del pedido.
 * @returns {string} - Número extraído o string vacío.
 */
function extraePorcentajeDeDescrip(descrip) {
  if (!descrip) return "";
  const s = String(descrip);
  const m = s.match(/([0-9]+(?:\.[0-9]+)?)\s*%/);
  if (m && m[1]) return m[1];
  const m2 = s.match(/([0-9]+(?:\.[0-9]+)?)/);
  return m2 && m2[1] ? m2[1] : "";
}

/**
 * Utilidad para limpiar los espacios en blanco de los campos de cadena (string) en un objeto.
 * Recorre todas las propiedades del objeto y aplica .trim() si el valor es una cadena.
 * @param {Object} obj - El objeto a limpiar.
 * @returns {Object} - Un nuevo objeto con las cadenas limpias.
 */
function cleanStrings(obj) {
  for (const key in obj) {
    if (typeof obj[key] === "string") {
      obj[key] = obj[key].trim();
    }
  }
  return obj;
}

/**
 * Obtiene el precio de venta correspondiente al tipo de cliente.
 * 1. Busca el tipo del cliente en la tabla clientes.
 * 2. Busca el campo precio_a en tipo_cli para ese tipo.
 * 3. Mapea el campo al precio correcto de la tabla art.
 * @param {string} co_cli - Código del cliente.
 * @param {string} co_art - Código del artículo.
 * @returns {Promise<number>} - Precio de venta correspondiente.
 */
async function obtenerPrecioVentaCliente(co_cli, co_art) {
  try {
    // 1. Obtener el tipo de cliente
    const tipoReq = new sql.Request();
    tipoReq.input("co_cli", sql.VarChar, limpiarValor(co_cli));
    const tipoResult = await tipoReq.query(
      "SELECT tipo FROM clientes WHERE RTRIM(co_cli) = @co_cli",
    );
    const tipo = tipoResult.recordset[0]?.tipo?.trim();
    if (!tipo) {
      console.error(`Tipo de cliente no encontrado para co_cli: ${co_cli}`);
      return 0;
    }

    // 2. Obtener el campo de precio que le corresponde
    const precioAReq = new sql.Request();
    precioAReq.input("tipo", sql.VarChar, tipo);
    const precioAResult = await precioAReq.query(
      "SELECT precio_a FROM tipo_cli WHERE RTRIM(tip_cli) = @tipo",
    );
    const precioA = precioAResult.recordset[0]?.precio_a?.trim().toUpperCase();
    if (!precioA) {
      console.error(
        `Campo de precio (precio_a) no encontrado para tipo: ${tipo}`,
      );
      return 0;
    }

    // 3. Seleccionar el campo de precio correcto
    let campoPrecio = "prec_vta3"; // Por defecto
    if (precioA === "PRECIO 1") campoPrecio = "prec_vta1";
    else if (precioA === "PRECIO 2") campoPrecio = "prec_vta2";
    else if (precioA === "PRECIO 3") campoPrecio = "prec_vta3";
    else if (precioA === "PRECIO 4") campoPrecio = "prec_vta4";

    // 4. Obtener el precio de la tabla art
    const precioReq = new sql.Request();
    precioReq.input("co_art", sql.VarChar, limpiarValor(co_art));
    const precioResult = await precioReq.query(
      `SELECT ${campoPrecio} as precio FROM art WHERE RTRIM(co_art) = @co_art`,
    );
    const precio = precioResult.recordset[0]?.precio || 0;
    return precio;
  } catch (err) {
    console.error(
      `Error en obtenerPrecioVentaCliente para co_cli: ${co_cli}, co_art: ${co_art}. Error: ${err.message}`,
    );
    return 0;
  }
}

/**
 * Obtiene el catálogo de productos (artículos) filtrado por proveedor y nivel de precio.
 * Esta función consulta la tabla 'art' en SQL Server (Profit) y calcula el stock real
 * restando el stock comprometido. También incluye descuentos y existencias por almacén.
 *
 * Parámetros esperados (en query o body):
 * - co_prov: Código(s) de proveedor (puede ser un array o string separado por comas).
 * - precio_num: Nivel de precio a consultar (1 al 4, por defecto 1).
 */
export const getCatalogo = async (req, res) => {
  try {
    // Log de auditoría para depuración

    // Soporta tanto parámetros por URL (GET) como por el cuerpo de la petición (POST)
    const co_prov_input = req.query.co_prov ?? req.body?.co_prov;
    const precio_num = parseInt(
      req.query.precio_num ?? req.body?.precio_num,
      10,
    );

    // Mapeo dinámico de campos de precio según la selección del usuario
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

    // Normalización de la entrada de proveedores (maneja string, array o valores separados por coma)
    let proveedores = [];
    if (Array.isArray(co_prov_input)) {
      proveedores = co_prov_input
        .map(String)
        .map((s) => s.trim())
        .filter(Boolean);
    } else if (
      typeof co_prov_input === "string" &&
      co_prov_input.includes(",")
    ) {
      proveedores = co_prov_input
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    } else if (co_prov_input) {
      proveedores = [String(co_prov_input).trim()];
    }

    // Consulta SQL principal: Filtra artículos con stock disponible y calcula valores dinámicos
    // Se usa RTRIM en campos de texto para mejorar el rendimiento del procesamiento en JS
    let query = `
      SELECT 
        RTRIM(a.co_art) AS imagen, 
        RTRIM(a.co_prov) AS co_prov,
        RTRIM(a.art_des) AS descripcion, 
        (a.stock_act - a.stock_com) AS stock,
        a.${precioCampo} AS Precio,
        a.${precioVtaCampo} AS precio_venta,
        RTRIM(a.tipo_imp) AS tipo_imp,
        ISNULL(d_art.porc1, 0) AS descuento_por_art,
        ISNULL(d_cat.porc1, 0) AS descuento_por_categoria,
        ISNULL(d_lin.porc1, 0) AS descuento_por_linea,
        ISNULL(s01.stock_act - s01.stock_com, 0) AS stock_tachira,
        ISNULL(s04.stock_act - s04.stock_com, 0) AS stock_barquisimeto
      FROM art a
      LEFT JOIN (SELECT co_desc, MAX(porc1) as porc1 FROM descuen GROUP BY co_desc) d_art ON d_art.co_desc = a.co_art
      LEFT JOIN (SELECT co_desc, MAX(porc1) as porc1 FROM descuen GROUP BY co_desc) d_cat ON d_cat.co_desc = a.co_cat
      LEFT JOIN (SELECT co_desc, MAX(porc1) as porc1 FROM descuen GROUP BY co_desc) d_lin ON d_lin.co_desc = a.co_lin
      LEFT JOIN st_almac s01 ON s01.co_art = a.co_art AND s01.co_alma = '01'
      LEFT JOIN st_almac s04 ON s04.co_art = a.co_art AND s04.co_alma = '04'
      WHERE (a.stock_act - a.stock_com) > 0
      AND RTRIM(a.co_art) NOT LIKE '%A'
      AND (
        (ISNULL(s01.stock_act, 0) - ISNULL(s01.stock_com, 0)) > 0 
        OR 
        (ISNULL(s04.stock_act, 0) - ISNULL(s04.stock_com, 0)) > 0
      )
    `;

    const request = new sql.Request();

    // Inserción dinámica de proveedores en la cláusula IN para evitar inyección SQL
    if (proveedores.length > 0) {
      const inParams = proveedores.map((_, i) => `@prov${i}`);
      query += ` AND a.co_prov IN (${inParams.join(",")})`;
      proveedores.forEach((prov, i) => {
        request.input(`prov${i}`, sql.VarChar, prov);
      });
    }

    const result = await request.query(query);

    // Limpiar espacios en blanco de todos los resultados antes de enviar la respuesta
    // (Aunque ya se hizo RTRIM en SQL, se mantiene por seguridad y consistencia)
    const cleanedRecordset = result.recordset.map(cleanStrings);

    res.json(cleanedRecordset);
  } catch (err) {
    console.error("Error en getCatalogo (pedidosApp):", err);
    res.status(500).json({ error: err.message });
  }
};

/**
 * Obtiene la lista de clientes filtrada por RIF, Nit o Segmento.
 * Replica la funcionalidad de /transferencias/clientes.
 *
 * Parámetros esperados (en body):
 * - rif: Filtro por RIF (opcional, búsqueda parcial).
 * - nit: Filtro por NIT (opcional, búsqueda parcial).
 * - co_seg: Código de segmento (opcional, puede ser uno o varios separados por coma).
 */
export const getClientes = async (req, res) => {
  try {
    // Ahora recibe los datos desde el body (JSON)
    const { rif, co_seg, nit } = req.body;

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
    console.error("Error en getClientes (pedidosApp):", err);
    res.status(500).json({ error: err.message });
  }
};
// CONTINUATION OF AUXILIARY FUNCTIONS FOR pedidosApp.controller.js
// Add these functions after the existing code

/**
 * Inserta un nuevo pedido en la tabla cotiz_c de Profit Plus.
 * Replica la lógica completa de transfencP.controller.js
 */
async function insertarPedido(
  fact_num,
  cod_cliente,
  cod_prov,
  tot_bruto,
  tot_neto,
  saldo,
  iva,
  fecha_actual,
  codigo_pedido,
  porc_gdesc,
  porc_gdesc_total,
  descrip,
) {
  try {
    const co_ven = await obtenerCoVenPorCliente(cod_cliente);

    // Obtener desc_glob del cliente e ignorar el valor del JSON (req.body)
    const porcGdescFinal = await obtenerDescGlobCliente(cod_cliente);

    // Obtener nombre y rif usando la detección dinámica de columnas
    let { nombre: nombreCliente, rif: rifCliente } =
      await obtenerNombreYRifCliente(cod_cliente);

    // Obtener tasa desde tabla moneda y redondear a 4 decimales
    let tasa = 1;
    try {
      const tasaRes = await ejecutarConsulta(
        `SELECT cambio FROM moneda WHERE RTRIM(co_mone) = 'US$'`,
      );
      if (tasaRes && tasaRes.length > 0)
        tasa = parseNumberFromString(tasaRes[0].cambio);
    } catch (e) {
      console.warn("No se pudo obtener tasa, usando 1", e.message);
    }
    const tasaRounded = Number(Number(tasa).toFixed(4));

    // Coerción segura de montos y conversión multiplicando por la tasa
    const totBrutoNum = parseNumberFromString(tot_bruto);
    const totNetoNum = parseNumberFromString(tot_neto);
    const saldoNum = parseNumberFromString(saldo);
    const ivaNum = parseNumberFromString(iva);

    const tot_bruto_bs = Number((totBrutoNum * tasaRounded).toFixed(4));
    const tot_neto_bs = Number((totNetoNum * tasaRounded).toFixed(4));
    const saldo_bs = Number((saldoNum * tasaRounded).toFixed(4));
    const iva_bs = Number((ivaNum * tasaRounded).toFixed(4));

    // Asegurarse de que fec_emis almacene sólo la fecha con hora 00:00:00.000
    let fecha_sin_hora = fecha_actual;
    try {
      if (typeof fecha_actual === "string" && fecha_actual.includes(" ")) {
        const datePart = fecha_actual.split(" ")[0];
        fecha_sin_hora = `${datePart} 00:00:00.000`;
      } else if (typeof fecha_actual === "string") {
        fecha_sin_hora = `${fecha_actual} 00:00:00.000`;
      } else {
        const gen = obtenerFechaVenezuelaISO();
        fecha_sin_hora = `${gen.split(" ")[0]} 00:00:00.000`;
      }
    } catch (e) {
      fecha_sin_hora = fecha_actual;
    }

    // Determinar valor para campo6
    const valorCampo6Extraido = extraePorcentajeDeDescrip(descrip);
    const valorCampo6 =
      valorCampo6Extraido !== ""
        ? String(valorCampo6Extraido)
        : String(porcGdescFinal);

    // Calcular monto de descuento global y nuevo subtotal para base de impuestos en Bs
    const glob_desc_bs = Number((tot_bruto_bs * (porcGdescFinal / 100)).toFixed(4));

    const valores = {
      fact_num,
      contrib: 1,
      nombre: nombreCliente || "",
      rif: rifCliente || "",
      nit: "",
      status: 0,
      comentario: "",
      descrip: descrip || `Pedido numero ${codigo_pedido} de APP`,
      saldo: saldo_bs,
      fec_emis: fecha_sin_hora,
      fec_venc: fecha_actual,
      co_cli: limpiarValor(cod_cliente),
      co_ven,
      co_tran: "03",
      dir_ent: "",
      forma_pag: "04",
      tot_bruto: tot_bruto_bs,
      tot_neto: tot_neto_bs,
      iva: iva_bs,
      glob_desc: glob_desc_bs,
      tot_reca: 0,
      porc_gdesc: porcGdescFinal,
      porc_reca: 0,
      total_uc: 0,
      total_cp: 0,
      tot_flete: 0,
      monto_dev: 0,
      totklu: 0,
      anulada: 0,
      impresa: 0,
      iva_dev: 0,
      feccom: fecha_actual,
      numcom: 0,
      tasa: tasaRounded,
      moneda: "US$",
      cta_contab: "",
      seriales: 0,
      tasag: 16,
      tasag10: 16,
      tasag20: 16,
      campo1: "",
      campo2: "",
      campo3: "",
      campo4: "",
      campo5: "",
      campo6: "",
      campo7: "",
      campo8: "",
      co_us_in: "APP",
      fe_us_in: fecha_actual,
      co_us_mo: "",
      fe_us_mo: fecha_actual,
      co_us_el: "",
      fe_us_el: fecha_actual,
      revisado: "",
      trasnfe: "",
      co_sucu: "01",
      rowguid: uuidv4(),
      mon_ilc: 0,
      otros1: 0,
      otros2: 0,
      otros3: 0,
      aux01: 0,
      aux02: "",
      salestax: "",
      origen: "",
      origen_d: "",
      sta_prod: "",
      telefono: "",
    };

    // Truncar strings que excedan la longitud máxima
    try {
      const colLongitudes = await obtenerLongitudesColumnasCotizC();
      for (const [k, v] of Object.entries(valores)) {
        if (v === null || v === undefined) continue;
        if (typeof v === "string") {
          const max = colLongitudes[k.toLowerCase()];
          if (typeof max === "number" && max > 0 && v.length > max) {
            console.warn(
              `Truncando columna '${k}' de ${v.length} a ${max} caracteres.`,
            );
            valores[k] = v.slice(0, max);
          }
        }
      }
    } catch (e) {
      console.warn("Error al truncar valores:", e.message);
    }

    // Armado dinámico del INSERT en cotiz_c
    const columnas = Object.keys(valores).join(", ");
    const placeholders = Object.keys(valores)
      .map((k, i) => `@p${i}`)
      .join(", ");

    const request = new sql.Request();
    Object.values(valores).forEach((v, i) => {
      request.input(`p${i}`, v);
    });

    const query = `INSERT INTO cotiz_c (${columnas}) VALUES (${placeholders})`;
    await request.query(query);

    return true;
  } catch (ex) {
    console.error(`Error al insertar pedido ${fact_num}: ${ex.message}`);
    return false;
  }
}

/**
 * Inserta los renglones del pedido en reng_cac y actualiza stocks.
 */
async function insertarRenglonesPedido(fact_num, items, fecha_actual, cod_cliente) {
  try {
    // Obtener la tasa del dólar
    let tasa = 1;
    try {
      const tasaRes = await ejecutarConsulta(
        `SELECT cambio FROM moneda WHERE RTRIM(co_mone) = 'US$'`,
      );
      if (tasaRes && tasaRes.length > 0)
        tasa = parseNumberFromString(tasaRes[0].cambio);
    } catch (e) {
      console.warn(
        "No se pudo obtener tasa para renglones, usando 1",
        e.message,
      );
    }
    const tasaRounded = Number(Number(tasa).toFixed(4));

    let reng_num = 1;
    for (const item of items) {
      // Asegurar que co_art esté siempre limpio (sin espacios) para búsquedas exactas
      item.co_art = String(item.co_art).trim();

      const cant_bq = parseNumberFromString(item.cant_bq ?? item.quantityBQ);
      const cant_sc = parseNumberFromString(item.cant_sc ?? item.quantitySC);
      const cant_producto =
        item.cant_producto !== undefined && item.cant_producto !== null
          ? parseNumberFromString(item.cant_producto)
          : cant_bq + cant_sc;

      const datos_articulo = await obtenerDatosArticulo(item.co_art);
      if (!datos_articulo) {
        console.warn(`⚠️ Artículo '${item.co_art}' no encontrado`);
        return false;
      }
      const co_cat = datos_articulo.co_cat || "";
      const porc_desc = await obtenerDescuentoArticulo(item.co_art, co_cat);

      const des_art = datos_articulo.art_des || "";
      const uni_venta_db =
        datos_articulo.uni_venta || datos_articulo.uni_venta_db || "UND";
      const tipo_imp = datos_articulo.tipo_imp || 1;

      // Obtener precio de venta según tipo de cliente
      const prec_vta_cliente = await obtenerPrecioVentaCliente(cod_cliente, item.co_art);
      const prec_vta = Number((parseNumberFromString(prec_vta_cliente) * tasaRounded).toFixed(4));
      const prec_vta2 = prec_vta_cliente;
      const cos_pro_un = Number(
        ((datos_articulo.cos_pro_un || 0) * tasaRounded).toFixed(4),
      );
      const ult_cos_un = Number(
        ((datos_articulo.ult_cos_un || 0) * tasaRounded).toFixed(4),
      );
      const ult_cos_om = Number(
        ((datos_articulo.ult_cos_om || 0) * tasaRounded).toFixed(4),
      );

      // Insertar para almacén 01 si existe cant_sc
      if (cant_sc && Number(cant_sc) > 0) {
        const cantidad = Number(cant_sc);
        const rengValores = {
          fact_num,
          reng_num,
          tipo_doc: "",
          reng_doc: 0,
          num_doc: 0,
          co_art: item.co_art,
          co_alma: "01",
          total_art: cantidad,
          stotal_art: 0,
          pendiente: cantidad,
          uni_venta: uni_venta_db,
          prec_vta,
          porc_desc: `0.00+${porc_desc}+0.00`,
          tipo_imp,
          reng_neto: Number((cantidad * prec_vta).toFixed(4)),
          cos_pro_un,
          ult_cos_un,
          ult_cos_om,
          cos_pro_om: 0,
          total_dev: 0,
          monto_dev: 0,
          prec_vta2: prec_vta2,
          anulado: 0,
          des_art,
          seleccion: 0,
          cant_imp: 0,
          comentario: "",
          rowguid: uuidv4(),
          total_uni: 1,
          mon_ilc: 0,
          otros: 0,
          nro_lote: "",
          fec_lote: fecha_actual,
          pendiente2: 0,
          tipo_doc2: "",
          reng_doc2: 0,
          num_doc2: 0,
          co_alma2: "",
          aux01: 0,
          aux02: "",
          cant_prod: 0,
          imp_prod: 0,
        };

        const columnas = Object.keys(rengValores).join(", ");
        const placeholders = Object.keys(rengValores)
          .map((_, i) => `@p${i}`)
          .join(", ");
        const request = new sql.Request();
        Object.values(rengValores).forEach((v, i) => request.input(`p${i}`, v));
        await request.query(
          `INSERT INTO reng_cac (${columnas}) VALUES (${placeholders})`,
        );
        reng_num++;
      }

      // Insertar para almacén 04 si existe cant_bq
      if (cant_bq && Number(cant_bq) > 0) {
        const cantidad = Number(cant_bq);
        const rengValores = {
          fact_num,
          reng_num,
          tipo_doc: "",
          reng_doc: 0,
          num_doc: 0,
          co_art: item.co_art,
          co_alma: "04",
          total_art: cantidad,
          stotal_art: 0,
          pendiente: cantidad,
          uni_venta: uni_venta_db,
          prec_vta,
          porc_desc: `0.00+0.00+0.00`,
          tipo_imp,
          reng_neto: Number((cantidad * prec_vta).toFixed(4)),
          cos_pro_un,
          ult_cos_un,
          ult_cos_om,
          cos_pro_om: 0,
          total_dev: 0,
          monto_dev: 0,
          prec_vta2: prec_vta,
          anulado: 0,
          des_art,
          seleccion: 0,
          cant_imp: 0,
          comentario: "",
          rowguid: uuidv4(),
          total_uni: 1,
          mon_ilc: 0,
          otros: 0,
          nro_lote: "",
          fec_lote: fecha_actual,
          pendiente2: 0,
          tipo_doc2: "",
          reng_doc2: 0,
          num_doc2: 0,
          co_alma2: "",
          aux01: 0,
          aux02: "",
          cant_prod: 0,
          imp_prod: 0,
        };

        const columnas = Object.keys(rengValores).join(", ");
        const placeholders = Object.keys(rengValores)
          .map((_, i) => `@p${i}`)
          .join(", ");
        const request = new sql.Request();
        Object.values(rengValores).forEach((v, i) => request.input(`p${i}`, v));
        await request.query(
          `INSERT INTO reng_cac (${columnas}) VALUES (${placeholders})`,
        );
        reng_num++;
      }

      // Actualizar stock_com
      const stockResultReq = new sql.Request();
      stockResultReq.input("co_art", sql.VarChar, item.co_art);
      const stockResult = await stockResultReq.query(
        "SELECT stock_com FROM art WHERE RTRIM(co_art) = @co_art",
      );
      if (
        stockResult &&
        stockResult.recordset &&
        stockResult.recordset.length > 0
      ) {
        const stock_actual = stockResult.recordset[0].stock_com || 0;
        const nuevo_stock = stock_actual + cant_producto;
        const upd1 = new sql.Request();
        upd1.input("nuevo_stock", sql.Int, nuevo_stock);
        upd1.input("co_art", sql.VarChar, item.co_art);
        await upd1.query(
          "UPDATE art SET stock_com = @nuevo_stock WHERE RTRIM(co_art) = @co_art",
        );

        const upd2 = new sql.Request();
        upd2.input("nuevo_stock", sql.Int, nuevo_stock);
        upd2.input("co_art", sql.VarChar, item.co_art);
        await upd2.query(
          "UPDATE st_almac SET stock_act = @nuevo_stock WHERE RTRIM(co_art) = @co_art AND co_alma = '01'",
        );
      } else {
        console.warn(
          `Artículo '${item.co_art}' no encontrado en la tabla 'art'.`,
        );
        return false;
      }
    }
    return true;
  } catch (ex) {
    console.error(`Error al insertar renglones: ${ex.message}`);
    return false;
  }
}

/**
 * Registra la operación en la tabla pistas para auditoría.
 */
async function registrarPista(fact_num, codigo_pedido, ip_cliente) {
  try {
    const fechaVenezuela = obtenerFechaVenezuelaISO();
    const valores = {
      usuario_id: "App",
      usuario: "aplicacion",
      fecha: fechaVenezuela,
      empresa: "CRISTM25",
      co_sucu: "01",
      tabla: "PEDIDOS",
      num_doc: fact_num,
      codigo: "",
      tipo_op: "I",
      maquina: ip_cliente || "",
      campos: "Pedido realizado por la app",
      rowguid: uuidv4(),
      trasnfe: "",
      AUX01: 0,
      AUX02: "",
    };

    const columnas = Object.keys(valores).join(", ");
    const placeholders = Object.keys(valores)
      .map((k) => `@${k}`)
      .join(", ");
    const query = `INSERT INTO pistas (${columnas}) VALUES (${placeholders})`;

    const request = new sql.Request();
    for (const key in valores) {
      request.input(key, valores[key]);
    }
    await request.query(query);
    return true;
  } catch (ex) {
    console.error(`Error al registrar en pistas: ${ex.message}`);
    return false;
  }
}

/**
 * Endpoint para crear un pedido desde la APP.
 * Replica completamente la funcionalidad de /transfencP/crear
 */
export const crearPedidoApp = async (req, res) => {
  try {

    try {
      const {
        cod_cliente,
        items,
        ip_cliente,
        codigo_pedido, // <-- ID único proporcionado por el frontend (guid)
        tot_bruto,
        tot_neto,
        saldo,
        iva,
        porc_gdesc,
        descrip,
        usuario, // Objeto { user: "...", ... } o similar
        cod_prov,
      } = req.body;

      // Validar que codigo_pedido venga en el body
      if (!codigo_pedido) {
        return res.status(400).json({
          error: "Falta el identificador único del pedido (codigo_pedido).",
        });
      }

      // Revisar si ya existe un pedido con este codigo_pedido en la tabla pedidos de transferencias
      // Esto evita duplicados al reintentar o por doble click
      const [existing] = await transferenciasPool.execute(
        "SELECT id, fact_num FROM pedidos WHERE codigo_pedido = ? LIMIT 1",
        [codigo_pedido],
      );

      if (existing.length > 0) {
        console.warn(
          `[pedidosApp] Pedido duplicado detectado (codigo_pedido: ${codigo_pedido}). Retornando existente.`,
        );
        // Retornamos éxito con el fact_num que ya se generó previamente
        return res.json({
          success: true,
          fact_num: existing[0].fact_num,
          message: "Pedido ya procesado anteriormente.",
        });
      }

      if (
        !cod_cliente ||
        !codigo_pedido ||
        !items ||
        (Array.isArray(items) && items.length === 0)
      ) {
        console.error("[pedidosApp] Petición inválida:", {
          cod_cliente,
          codigo_pedido,
          itemsLength: Array.isArray(items) ? items.length : 0,
        });
        return res
          .status(400)
          .json({ error: "Datos incompletos para crear el pedido." });
      }

      // Obtener desc_glob del cliente para calcular los totales correctos en el backend
      const descGlobCliente = await obtenerDescGlobCliente(cod_cliente);

      // Calcular totales reales desde el backend para evitar errores de IVA y precios
      let totBrutoBackend = 0;
      let totIvaBackend = 0;

      for (const it of items) {
        const cantidad = it.cant_producto !== undefined && it.cant_producto !== null
          ? parseNumberFromString(it.cant_producto)
          : parseNumberFromString(it.cant_bq) + parseNumberFromString(it.cant_sc);

        // Limpiar co_art antes de cualquier búsqueda para garantizar coincidencia exacta
        it.co_art = String(it.co_art).trim();

        // Se usa obtenerPrecioVentaCliente como se hace en insertarRenglonesPedido
        const precioReal = await obtenerPrecioVentaCliente(cod_cliente, it.co_art);
        it.precioReal = Number(precioReal);

        const subtotalBruto = cantidad * precioReal;
        totBrutoBackend += subtotalBruto;

        const datosArticulo = await obtenerDatosArticulo(it.co_art);
        const tipo_imp = datosArticulo ? String(datosArticulo.tipo_imp).trim() : '1';

        // Calcular IVA sobre el monto neto (después del descuento global)
        const subtotalConDescuento = subtotalBruto - (subtotalBruto * (descGlobCliente / 100));

        // Por defecto, tipo_imp '1' en Profit es 16% IVA
        if (tipo_imp === '1') {
          totIvaBackend += subtotalConDescuento * 0.16;
        }
      }

      const totBrutoCalc = Number(totBrutoBackend.toFixed(2));
      const ivaCalc = Number(totIvaBackend.toFixed(2));
      const totNetoCalc = Number((totBrutoCalc - (totBrutoCalc * (descGlobCliente / 100)) + ivaCalc).toFixed(2));
      const saldoCalc = totNetoCalc;

      // Obtener número de factura
      const fact_num = await obtenerNuevoFactNum();
      if (!fact_num) {
        console.error("No se pudo obtener fact_num");
        return res
          .status(500)
          .json({ error: "No se pudo generar número de pedido" });
      }

      const fecha_actual = obtenerFechaVenezuelaISO();

      const pedidoOk = await insertarPedido(
        fact_num,
        cod_cliente,
        cod_prov,
        totBrutoCalc,
        totNetoCalc,
        saldoCalc,
        ivaCalc,
        fecha_actual,
        codigo_pedido,
        porc_gdesc,
        null,
        descrip,
      );
      if (!pedidoOk) {
        console.error("Error: insertarPedido devolvió false");
        return res.status(500).json({ error: "No se pudo insertar el pedido" });
      }

      const renglonesOk = await insertarRenglonesPedido(
        fact_num,
        items,
        fecha_actual,
        cod_cliente,
      );
      if (!renglonesOk) {
        console.error("Error: insertarRenglonesPedido devolvió false");
        return res
          .status(500)
          .json({ error: "No se pudieron insertar los renglones" });
      }

      const pistaOk = await registrarPista(fact_num, codigo_pedido, ip_cliente);
      if (!pistaOk) {
        console.error("Error: registrarPista devolvió false");
        return res.status(500).json({ error: "No se pudo registrar la pista" });
      }

      // Guardar copia en MySQL (base transferencias)
      try {
        const conn = await transferenciasPool.getConnection();
        try {
          await conn.beginTransaction();

          const coUsIn =
            req.body?.usuario?.user ?? req.body?.co_us_in ?? "prov";

          // Obtener tasa para MySQL
          let tasaForMysql = 1;
          try {
            const tasaRes = await ejecutarConsulta(
              `SELECT cambio FROM moneda WHERE RTRIM(co_mone) = 'US$'`,
            );
            if (tasaRes && tasaRes.length > 0) {
              tasaForMysql = parseNumberFromString(tasaRes[0].cambio);
            }
          } catch (e) {
            console.warn(
              "No se pudo obtener tasa para MySQL, usando 1",
              e.message,
            );
          }

          const [resInsert] = await conn.execute(
            `INSERT INTO pedidos
                (fact_num, cod_cliente, cod_prov, tot_bruto, tot_neto, saldo, iva, codigo_pedido, porc_gdesc, descrip, co_us_in, fecha, tasa)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              fact_num,
              cod_cliente,
              cod_prov ?? "",
              totBrutoCalc,
              totNetoCalc,
              saldoCalc,
              ivaCalc,
              String(codigo_pedido),
              descGlobCliente,
              descrip ?? "",
              coUsIn,
              fecha_actual,
              Number(tasaForMysql),
            ],
          );
          const pedidoId = resInsert.insertId;

          for (const it of items) {
            const cantidad =
              it.cant_producto !== undefined && it.cant_producto !== null
                ? parseNumberFromString(it.cant_producto)
                : parseNumberFromString(it.cant_bq) +
                  parseNumberFromString(it.cant_sc);
            const precio = it.precioReal !== undefined ? it.precioReal : parseNumberFromString(it.precio);
            const subtotal = Number((cantidad * precio).toFixed(2));
            const co_alma =
              it.co_alma ??
              (it.cant_sc && Number(it.cant_sc) > 0
                ? "01"
                : it.cant_bq && Number(it.cant_bq) > 0
                  ? "04"
                  : "01");
            await conn.execute(
              `INSERT INTO pedido_productos
                  (pedido_id, fact_num, co_art, cantidad, precio, subtotal, co_alma, reng_num)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
              [
                pedidoId,
                fact_num,
                it.co_art,
                cantidad,
                precio,
                subtotal,
                co_alma,
                null,
              ],
            );
          }

          await conn.commit();
        } catch (errInsert) {
          await conn.rollback();
          console.error(
            "Error al insertar en MySQL transferencias:",
            errInsert.message,
          );
        } finally {
          conn.release();
        }
      } catch (errPool) {
        console.error(
          "No se pudo obtener conexión MySQL transferencias:",
          errPool.message,
        );
      }

      // Además, guardar copia en la base MySQL 'app' (tablas: pedidos, pedido_productos)
      try {
        const connApp = await transferenciasPool.getConnection();
        try {
          await connApp.beginTransaction();

          const coUsInApp =
            req.body?.usuario?.user ?? req.body?.co_us_in ?? "prov";

          // Usar la misma tasa calculada antes; obtenerla si es necesario
          let tasaApp = 1;
          try {
            const tasaRes = await ejecutarConsulta(
              `SELECT cambio FROM moneda WHERE RTRIM(co_mone) = 'US$'`,
            );
            if (tasaRes && tasaRes.length > 0)
              tasaApp = parseNumberFromString(tasaRes[0].cambio);
          } catch (e) {
            console.warn(
              "No se pudo obtener tasa para app MySQL, usando 1",
              e.message,
            );
          }

          const [resInsertApp] = await connApp.execute(
            `INSERT INTO pedidos
                (fact_num, cod_cliente, cod_prov, tot_bruto, tot_neto, saldo, iva, codigo_pedido, porc_gdesc, descrip, co_us_in, fecha, tasa)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              fact_num,
              cod_cliente,
              cod_prov ?? "",
              totBrutoCalc,
              totNetoCalc,
              saldoCalc,
              ivaCalc,
              String(codigo_pedido),
              descGlobCliente,
              descrip ?? "",
              coUsInApp,
              fecha_actual,
              Number(tasaApp),
            ],
          );
          const pedidoAppId = resInsertApp.insertId;

          for (const it of items) {
            const cantidad =
              it.cant_producto !== undefined && it.cant_producto !== null
                ? parseNumberFromString(it.cant_producto)
                : parseNumberFromString(it.cant_bq) +
                  parseNumberFromString(it.cant_sc);
            const precio = it.precioReal !== undefined ? it.precioReal : parseNumberFromString(it.precio);
            const subtotal = Number((cantidad * precio).toFixed(2));
            const co_alma =
              it.co_alma ??
              (it.cant_sc && Number(it.cant_sc) > 0
                ? "01"
                : it.cant_bq && Number(it.cant_bq) > 0
                  ? "04"
                  : "01");
            await connApp.execute(
              `INSERT INTO pedido_productos
                  (pedido_id, fact_num, co_art, cantidad, precio, subtotal, co_alma, reng_num)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
              [
                pedidoAppId,
                fact_num,
                it.co_art,
                cantidad,
                precio,
                subtotal,
                co_alma,
                null,
              ],
            );
          }

          await connApp.commit();
        } catch (errApp) {
          await connApp.rollback();
          console.error("Error al insertar en MySQL app:", errApp.message);
        } finally {
          connApp.release();
        }
      } catch (errAppPool) {
        console.error(
          "No se pudo obtener conexión MySQL app:",
          errAppPool.message,
        );
      }

      res.json({ success: true, fact_num });
    } catch (error) {
      console.error(
        "Error al crear el pedido:",
        error && error.stack ? error.stack : error,
      );
      console.error(
        "Request body that caused the error:",
        JSON.stringify(req.body, null, 2),
      );
      res.status(500).json({ error: "Error interno al crear el pedido." });
    }
  } catch (error) {
    console.error(
      "Error al crear el pedido:",
      error && error.stack ? error.stack : error,
    );
    console.error(
      "Request body that caused the error:",
      JSON.stringify(req.body, null, 2),
    );
    res.status(500).json({ error: "Error interno al crear el pedido." });
  }
};
