import 'dotenv/config';
import { sql } from "../config/database.js";
import { v4 as uuidv4 } from "uuid";
import mysql from "mysql2/promise";
import { ejecutarConsulta, limpiarValor, parseNumberFromString, obtenerFechaVenezuelaISO } from "../utils/helpers.js";

const transferenciasPool = mysql.createPool({
  host: process.env.DB_TRANS_HOST,
  user: process.env.DB_TRANS_USER,
  password: process.env.DB_TRANS_PASSWORD,
  database: "transferencias",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Genera el texto de descripción del pedido de forma consistente con el descuento real
// FIX: centralizado aquí para que descrip SIEMPRE refleje el porc_gdesc real aplicado
function generarDescripPedido(descTotalParaCotiz) {
  if (descTotalParaCotiz > 0) {
    return `Pg en $ ${descTotalParaCotiz}%`;
  }
  return `Pedido numero del cte % = ${descTotalParaCotiz}`;
}

// Usa cotc_num en Sucursales para obtener el siguiente número de factura
async function obtenerNuevoFactNum() {
  const co_alma = "01";
  try {
    const selectReq = new sql.Request();
    selectReq.input("co_alma", sql.VarChar, co_alma);
    const select = await selectReq.query(
      "SELECT cotc_num FROM Sucursales WHERE RTRIM(co_alma) = @co_alma"
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
      "UPDATE Sucursales SET cotc_num = @nuevoFactNum WHERE RTRIM(co_alma) = @co_alma"
    );

    return nuevoFactNum;
  } catch (err) {
    console.error("Error en obtenerNuevoFactNum:", err.message);
    return null;
  }
}

// Función para obtener el siguiente número de factura (por MAX en pedidos)
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

async function obtenerCoVenPorCliente(co_cli) {
  const result = await ejecutarConsulta(
    `SELECT co_ven FROM clientes WHERE co_cli = @co_cli`,
    { co_cli }
  );
  return result && result.length > 0 ? result[0].co_ven : "01";
}

async function obtenerDatosArticulo(co_art) {
  const result = await ejecutarConsulta(
    `SELECT stock_act, prec_vta1, prec_vta2, prec_vta3, prec_vta4, prec_vta5, tipo_imp, co_sucu, co_cat,
            cos_pro_un, ult_cos_un, ult_cos_om, art_des, uni_venta
     FROM art WHERE co_art = @co_art`,
    { co_art }
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
      uni_venta: result[0].uni_venta,
    };
  }
  return null;
}

async function obtenerPrecioVentaCliente(co_cli, co_art) {
  try {
    const tipoResult = await ejecutarConsulta(
      "SELECT tipo FROM clientes WHERE RTRIM(co_cli) = @co_cli",
      { co_cli: limpiarValor(co_cli) }
    );
    const tipo = tipoResult[0]?.tipo?.trim();
    if (!tipo) return 0;

    const precioAResult = await ejecutarConsulta(
      "SELECT precio_a FROM tipo_cli WHERE RTRIM(tip_cli) = @tipo",
      { tipo }
    );
    const precioA = precioAResult[0]?.precio_a?.trim().toUpperCase();
    if (!precioA) return 0;

    let campoPrecio = "prec_vta3";
    if (precioA === "PRECIO 1") campoPrecio = "prec_vta1";
    else if (precioA === "PRECIO 2") campoPrecio = "prec_vta2";
    else if (precioA === "PRECIO 3") campoPrecio = "prec_vta3";
    else if (precioA === "PRECIO 4") campoPrecio = "prec_vta4";
    else if (precioA === "PRECIO 5") campoPrecio = "prec_vta5";

    const precioResult = await ejecutarConsulta(
      `SELECT ${campoPrecio} as precio FROM art WHERE RTRIM(co_art) = @co_art`,
      { co_art: limpiarValor(co_art) }
    );
    const precio = precioResult[0]?.precio || 0;
    return precio;
  } catch (err) {
    console.error(
      `Error en obtenerPrecioVentaCliente para co_cli: ${co_cli}, co_art: ${co_art}. Error: ${err.message}`
    );
    return 0;
  }
}

// FIX PRINCIPAL: insertarPedido recibe descrip ya generado desde el endpoint.
// Ya NO extrae porcentaje del texto ni genera descrip internamente.
// El campo6 y porc_gdesc siempre reflejan porcGdescFinal (el descuento real).
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
  descrip  // <-- Viene ya generado correctamente desde el endpoint /crear
) {
  try {
    const co_ven = await obtenerCoVenPorCliente(cod_cliente);

    // porcGdescFinal es la fuente de verdad del descuento aplicado
    const porcGdescFinal = parseNumberFromString(porc_gdesc);

    let { nombre: nombreCliente, rif: rifCliente } =
      await obtenerNombreYRifCliente(cod_cliente);

    let tasa = 1;
    try {
      const tasaRes = await ejecutarConsulta(
        `SELECT cambio FROM moneda WHERE RTRIM(co_mone) = 'US$'`
      );
      if (tasaRes && tasaRes.length > 0) tasa = parseNumberFromString(tasaRes[0].cambio);
    } catch (e) {
      console.warn("No se pudo obtener tasa, usando 1", e.message);
    }
    const tasaRounded = Number(Number(tasa).toFixed(4));

    const totBrutoNum = parseNumberFromString(tot_bruto);
    const totNetoNum = parseNumberFromString(tot_neto);
    const saldoNum = parseNumberFromString(saldo);
    const ivaNum = parseNumberFromString(iva);

    const tot_bruto_bs = Number((totBrutoNum * tasaRounded).toFixed(4));
    const tot_neto_bs = Number((totNetoNum * tasaRounded).toFixed(4));
    const saldo_bs = Number((saldoNum * tasaRounded).toFixed(4));
    const iva_bs = Number((ivaNum * tasaRounded).toFixed(4));

    const glob_desc_bs = tot_bruto_bs * (porcGdescFinal / 100);

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

    // FIX: campo6 siempre usa porcGdescFinal directamente, sin re-extraer del texto de descrip.
    // Esto garantiza que campo6 == porc_gdesc == el descuento real aplicado.
    const valorCampo6 = String(porcGdescFinal);

    const valores = {
      fact_num,
      contrib: 1,
      nombre: nombreCliente || "",
      rif: rifCliente || "",
      nit: "",
      status: 0,
      comentario: "",
      // FIX: descrip ya viene correcto desde el endpoint, no se genera ni modifica aquí
      descrip: descrip,
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
      glob_desc: Number(glob_desc_bs.toFixed(4)),
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
      campo5: cod_prov ? `${String(cod_prov)},TRANSFERENCIA` : "TRANSFERENCIA",
      // FIX: campo6 siempre es el descuento real, no extraído del texto
      campo6: valorCampo6,
      campo7: "",
      campo8: "",
      co_us_in: "prov",
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

    // Truncar strings que excedan la longitud máxima definida en la columna
    try {
      const colLongitudes = await obtenerLongitudesColumnasCotizC();
      for (const [k, v] of Object.entries(valores)) {
        if (v === null || v === undefined) continue;
        if (typeof v === "string") {
          const max = colLongitudes[k.toLowerCase()];
          if (typeof max === "number" && max > 0 && v.length > max) {
            console.warn(
              `Truncando columna '${k}' de ${v.length} a ${max} caracteres para evitar error de SQL.`
            );
            valores[k] = v.slice(0, max);
          }
        }
      }
    } catch (e) {
      console.warn(
        "Error al truncar valores antes de INSERT en cotiz_c:",
        e.message
      );
    }

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
    console.error(`Error inesperado al insertar pedido ${fact_num}: ${ex.message}`);
    return false;
  }
}

// Inserta los renglones del pedido y actualiza stock -> inserta en reng_cac
async function insertarRenglonesPedido(fact_num, items, fecha_actual, cod_cliente) {
  try {
    let tasa = 1;
    try {
      const tasaRes = await ejecutarConsulta(
        `SELECT cambio FROM moneda WHERE RTRIM(co_mone) = 'US$'`
      );
      if (tasaRes && tasaRes.length > 0) tasa = parseNumberFromString(tasaRes[0].cambio);
    } catch (e) {
      console.warn("No se pudo obtener tasa para renglones, usando 1", e.message);
    }
    const tasaRounded = Number(Number(tasa).toFixed(4));

    let reng_num = 1;
    for (const item of items) {
      item.co_art = String(item.co_art).trim();

      const cant_bq = parseNumberFromString(item.cant_bq ?? item.quantityBQ);
      const cant_sc = parseNumberFromString(item.cant_sc ?? item.quantitySC);
      const cant_producto =
        item.cant_producto !== undefined && item.cant_producto !== null
          ? parseNumberFromString(item.cant_producto)
          : cant_bq + cant_sc;

      const datos_articulo = await obtenerDatosArticulo(item.co_art);
      if (!datos_articulo) {
        console.warn(`⚠️ Alerta: Artículo '${item.co_art}' no encontrado`);
        return false;
      }
      const co_cat = datos_articulo.co_cat || "";

      const des_art = datos_articulo.art_des || "";
      const uni_venta_db = datos_articulo.uni_venta || "UND";
      const tipo_imp = datos_articulo.tipo_imp || 1;

      const prec_vta_cliente = await obtenerPrecioVentaCliente(cod_cliente, item.co_art);
      const prec_vta = Number((parseNumberFromString(prec_vta_cliente) * tasaRounded).toFixed(4));
      const prec_vta2 = prec_vta_cliente;
      const cos_pro_un = Number(((datos_articulo.cos_pro_un || 0) * tasaRounded).toFixed(4));
      const ult_cos_un = Number(((datos_articulo.ult_cos_un || 0) * tasaRounded).toFixed(4));
      const ult_cos_om = Number(((datos_articulo.ult_cos_om || 0) * tasaRounded).toFixed(4));

      const porc_desc_str = await formato_porc_desc_para_reng(item.co_art, co_cat);

      // Insertar para almacén 01 (cant_sc)
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
          prec_vta: prec_vta,
          porc_desc: porc_desc_str,
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
        await request.query(`INSERT INTO reng_cac (${columnas}) VALUES (${placeholders})`);
        reng_num++;
      }

      // Insertar para almacén 04 (cant_bq)
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
          prec_vta: prec_vta,
          porc_desc: porc_desc_str,
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
        await request.query(`INSERT INTO reng_cac (${columnas}) VALUES (${placeholders})`);
        reng_num++;
      }

      // Actualizar stock_com en art y st_almac
      const stockResultReq = new sql.Request();
      stockResultReq.input("co_art", sql.VarChar, item.co_art);
      const stockResult = await stockResultReq.query(
        "SELECT stock_com FROM art WHERE RTRIM(co_art) = @co_art"
      );
      if (stockResult && stockResult.recordset && stockResult.recordset.length > 0) {
        const stock_actual = stockResult.recordset[0].stock_com || 0;
        const nuevo_stock = stock_actual + cant_producto;

        const upd1 = new sql.Request();
        upd1.input("nuevo_stock", sql.Int, nuevo_stock);
        upd1.input("co_art", sql.VarChar, item.co_art);
        await upd1.query(
          "UPDATE art SET stock_com = @nuevo_stock WHERE RTRIM(co_art) = @co_art"
        );

        const upd2 = new sql.Request();
        upd2.input("nuevo_stock", sql.Int, nuevo_stock);
        upd2.input("co_art", sql.VarChar, item.co_art);
        await upd2.query(
          "UPDATE st_almac SET stock_act = @nuevo_stock WHERE RTRIM(co_art) = @co_art AND co_alma = '01'"
        );
      } else {
        console.warn(`Artículo '${item.co_art}' no encontrado en la tabla 'art'.`);
        return false;
      }
    }
    return true;
  } catch (ex) {
    console.error(`Error al insertar renglones del pedido ${fact_num}: ${ex.message}`);
    return false;
  }
}

async function registrarPista(fact_num, codigo_pedido, ip_cliente) {
  try {
    const fechaVenezuela = obtenerFechaVenezuelaISO();
    const valores = {
      usuario_id: "prov",
      usuario: "proveedor",
      fecha: fechaVenezuela,
      empresa: "CRISTM25",
      co_sucu: "01",
      tabla: "PEDIDOS",
      num_doc: fact_num,
      codigo: "",
      tipo_op: "I",
      maquina: ip_cliente || "",
      campos: "Pedido realizado por plataforma de proveedores",
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
    console.error(`Error al registrar en 'pistas': ${ex.message}`);
    return false;
  }
}

async function obtenerNombreYRifCliente(co_cli) {
  try {
    const cols = await ejecutarConsulta(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'clientes'`
    );
    const colNames = (cols || []).map((r) => String(r.COLUMN_NAME).toLowerCase());

    const nameCandidates = [
      "nombre", "nombre_cli", "cli_des", "razon_social",
      "raz_soc", "nom_cli", "razon", "nombrecliente",
    ];
    const rifCandidates = ["rif", "nit", "rfc", "nit_cli", "rif_cli"];

    const nameCol = nameCandidates.find((c) => colNames.includes(c.toLowerCase()));
    const rifCol = rifCandidates.find((c) => colNames.includes(c.toLowerCase()));

    if (!nameCol && !rifCol) return { nombre: "", rif: "" };

    const selectCols = [];
    if (nameCol) selectCols.push(`${nameCol} AS nombre`);
    if (rifCol) selectCols.push(`${rifCol} AS rif`);

    const query = `SELECT ${selectCols.join(", ")} FROM clientes WHERE RTRIM(co_cli) = @co_cli`;
    const cli = await ejecutarConsulta(query, { co_cli });

    if (cli && cli.length > 0) {
      return {
        nombre: limpiarValor(cli[0].nombre),
        rif: limpiarValor(cli[0].rif),
      };
    }
    return { nombre: "", rif: "" };
  } catch (e) {
    console.warn(`No se pudo obtener nombre/rif dinámicamente para ${co_cli}: ${e.message}`);
    return { nombre: "", rif: "" };
  }
}

async function obtenerLongitudesColumnasCotizC() {
  try {
    const cols = await ejecutarConsulta(
      `SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'cotiz_c'`
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
    console.warn("No se pudieron obtener longitudes de columnas de cotiz_c:", e.message);
    return {};
  }
}

async function formato_porc_desc_para_reng(co_art, co_cat) {
  const porc_desc = (await obtenerDescuentoArticulo(String(co_art), String(co_cat))) || 0.0;
  return `0.00+${porc_desc.toFixed(2)}+0.00`;
}

// Endpoint para probar obtenerSiguienteFactNum
export const getNextFactNum = async (req, res) => {
  const nextFactNum = await obtenerSiguienteFactNum();
  if (nextFactNum !== null) {
    res.json({ nextFactNum });
  } else {
    res.status(500).json({ error: "No se pudo obtener el siguiente fact_num" });
  }
};

// Endpoint para crear un pedido en Profit
export const crearPedidoTransf = async (req, res) => {
  try {

    const {
      cod_cliente,
      cod_prov,
      tot_bruto,
      tot_neto,
      saldo,
      iva,
      codigo_pedido,
      porc_gdesc_cliente,
      porc_gdesc_proveedor,
      porc_gdesc,
      suma_descuentos_adicionales,
      descuentos_adicionales,
      porc_gdesc_total,
      descrip,         // <-- Se recibe pero NO se usa directamente (ver FIX abajo)
      ip_cliente,
      items,
      usuario,
      co_us_in,
    } = req.body;

    // coerción numérica segura
    const totBrutoNum = parseNumberFromString(tot_bruto);
    const totNetoNum = parseNumberFromString(tot_neto);
    const saldoNum = parseNumberFromString(saldo);
    const ivaNum = parseNumberFromString(iva);

    if (
      !cod_cliente ||
      codigo_pedido === undefined ||
      !Array.isArray(items) ||
      items.length === 0
    ) {
      console.error("[transfencP] Petición inválida - datos incompletos:", {
        cod_cliente,
        codigo_pedido,
        itemsLength: Array.isArray(items) ? items.length : 0,
      });
      return res.status(400).json({ error: "Datos incompletos para crear el pedido." });
    }

    // descTotalParaCotiz: fuente de verdad del descuento aplicado
    const descTotalParaCotiz = parseNumberFromString(porc_gdesc);


    // VALIDACIÓN DE SEGURIDAD:
    // Si por alguna razón descTotalParaCotiz da mucho más de lo esperado (ej. > 100), alertar.
    if (descTotalParaCotiz > 100) {
      console.warn(
        `[transfencP] ADVERTENCIA: descTotalParaCotiz = ${descTotalParaCotiz} supera el 100%. Revisar lógica de descuentos.`
      );
    }

    // FIX CLAVE: descrip se genera SIEMPRE en el backend usando descTotalParaCotiz.
    // Esto garantiza que descrip, porc_gdesc, campo6 sean siempre consistentes
    // sin importar lo que venga en el body (texto libre, vacío, etc.).
    const descripFinal = generarDescripPedido(descTotalParaCotiz);

    let totBrutoBackend = 0;
    let totIvaBackend = 0;

    for (const it of items) {
      const cantidad =
        it.cant_producto !== undefined && it.cant_producto !== null
          ? parseNumberFromString(it.cant_producto)
          : parseNumberFromString(it.cant_bq) + parseNumberFromString(it.cant_sc);

      it.co_art = String(it.co_art).trim();

      const precioReal = await obtenerPrecioVentaCliente(cod_cliente, it.co_art);
      it.precioReal = Number(precioReal);

      const subtotalBruto = cantidad * precioReal;
      totBrutoBackend += subtotalBruto;

      const datosArticulo = await obtenerDatosArticulo(it.co_art);
      const tipo_imp = datosArticulo ? String(datosArticulo.tipo_imp).trim() : "1";

      const subtotalConDescuento =
        subtotalBruto - subtotalBruto * (descTotalParaCotiz / 100);

      if (tipo_imp === "1") {
        totIvaBackend += subtotalConDescuento * 0.16;
      }
    }

    const totBrutoCalc = Number(totBrutoBackend.toFixed(2));
    const ivaCalc = Number(totIvaBackend.toFixed(2));
    const totNetoCalc = Number(
      (totBrutoCalc - totBrutoCalc * (descTotalParaCotiz / 100) + ivaCalc).toFixed(2)
    );
    const saldoCalc = totNetoCalc;

    const fact_num = await obtenerNuevoFactNum();
    if (!fact_num) {
      console.error("No se pudo obtener nuevo fact_num.");
      return res.status(500).json({ error: "No se pudo generar número de pedido" });
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
      descTotalParaCotiz,   // porc_gdesc: el descuento real
      porc_gdesc_total,
      descripFinal          // FIX: descrip generado consistentemente en el backend
    );

    if (!pedidoOk) {
      console.error("Error: insertarPedido devolvió false", { fact_num, body: req.body });
      return res.status(500).json({ error: "No se pudo insertar el pedido" });
    }

    const renglonesOk = await insertarRenglonesPedido(
      fact_num,
      items,
      fecha_actual,
      cod_cliente
    );
    if (!renglonesOk) {
      console.error("Error: insertarRenglonesPedido devolvió false", {
        fact_num,
        items,
        body: req.body,
      });
      return res.status(500).json({ error: "No se pudieron insertar los renglones" });
    }

    const pistaOk = await registrarPista(fact_num, codigo_pedido, ip_cliente);
    if (!pistaOk) {
      console.error("Error: registrarPista devolvió false", {
        fact_num,
        codigo_pedido,
        ip_cliente,
        body: req.body,
      });
      return res.status(500).json({ error: "No se pudo registrar la pista" });
    }

    // Guardar copia en MySQL (transferencias)
    try {
      const conn = await transferenciasPool.getConnection();
      try {
        await conn.beginTransaction();

        const coUsIn = req.body?.usuario?.user ?? req.body?.co_us_in ?? "prov";

        let tasaForMysql = 1;
        try {
          const tasaRes = await ejecutarConsulta(
            `SELECT cambio FROM moneda WHERE RTRIM(co_mone) = 'US$'`
          );
          if (tasaRes && tasaRes.length > 0) {
            tasaForMysql = parseNumberFromString(tasaRes[0].cambio);
          }
        } catch (e) {
          console.warn("No se pudo obtener tasa para MySQL, usando 1", e.message);
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
            descTotalParaCotiz,
            descripFinal,   // FIX: guardar también el descrip correcto en MySQL
            coUsIn,
            fecha_actual,
            Number(tasaForMysql),
          ]
        );
        const pedidoId = resInsert.insertId;

        for (const it of items) {
          const cantidad =
            it.cant_producto !== undefined && it.cant_producto !== null
              ? parseNumberFromString(it.cant_producto)
              : parseNumberFromString(it.cant_bq) + parseNumberFromString(it.cant_sc);
          const precio =
            it.precioReal !== undefined
              ? it.precioReal
              : parseNumberFromString(it.precio);
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
            [pedidoId, fact_num, it.co_art, cantidad, precio, subtotal, co_alma, null]
          );
        }

        await conn.commit();
      } catch (errInsert) {
        await conn.rollback();
        console.error("Error al insertar en MySQL transferencias:", errInsert.message);
      } finally {
        conn.release();
      }
    } catch (errPool) {
      console.error("No se pudo obtener conexión MySQL transferencias:", errPool.message);
    }

    res.json({ success: true, fact_num });
  } catch (error) {
    console.error(
      "Error al crear el pedido:",
      error && error.stack ? error.stack : error
    );
    console.error("Request body that caused the error:", JSON.stringify(req.body, null, 2));
    res.status(500).json({ error: "Error interno al crear el pedido." });
  }
};

async function obtenerDescuentoArticulo(co_art, co_cat) {
  const result = await ejecutarConsulta(
    `SELECT TOP 1 porc1 FROM descuen WHERE co_desc = @co_art OR co_desc = @co_cat ORDER BY CASE WHEN co_desc = @co_art THEN 1 ELSE 2 END`,
    { co_art, co_cat }
  );
  return result && result.length > 0 ? Number(result[0].porc1) : 0.0;
}

async function calcularTotalesPedido(items, cod_cliente) {
  let tot_bruto = 0;
  let tot_neto = 0;
  let glob_desc = 0;
  let iva = 0;
  let tasa = 1;
  try {
    const tasaRes = await ejecutarConsulta(
      `SELECT cambio FROM moneda WHERE RTRIM(co_mone) = 'US$'`
    );
    if (tasaRes && tasaRes.length > 0) tasa = parseNumberFromString(tasaRes[0].cambio);
  } catch (e) {
    tasa = 1;
  }
  const tasaRounded = Number(Number(tasa).toFixed(4));
  for (const item of items) {
    const cant_bq = parseNumberFromString(item.cant_bq ?? item.quantityBQ);
    const cant_sc = parseNumberFromString(item.cant_sc ?? item.quantitySC);
    const cantidad =
      item.cant_producto !== undefined && item.cant_producto !== null
        ? parseNumberFromString(item.cant_producto)
        : cant_bq + cant_sc;
    const datos_articulo = await obtenerDatosArticulo(item.co_art);
    if (!datos_articulo) continue;
    const co_cat = datos_articulo.co_cat || "";
    const prec_vta_cliente = await obtenerPrecioVentaCliente(cod_cliente, item.co_art);
    const prec_vta = Number((parseNumberFromString(prec_vta_cliente) * tasaRounded).toFixed(4));
    const porc_desc = await obtenerDescuentoArticulo(item.co_art, co_cat);
    const subtotal = cantidad * prec_vta;
    const descuento = subtotal * (porc_desc / 100);
    tot_bruto += subtotal;
    tot_neto += subtotal - descuento;
    glob_desc += descuento;
    const tipo_imp = datos_articulo.tipo_imp || 1;
    if (tipo_imp === 1) {
      iva += (subtotal - descuento) * 0.16;
    }
  }
  return {
    tot_bruto: Number(tot_bruto.toFixed(2)),
    tot_neto: Number(tot_neto.toFixed(2)),
    glob_desc: Number(glob_desc.toFixed(2)),
    iva: Number(iva.toFixed(2)),
  };
}

async function obtenerDescGlobCliente(co_cli) {
  const result = await ejecutarConsulta(
    `SELECT desc_glob FROM clientes WHERE RTRIM(co_cli) = @co_cli`,
    { co_cli }
  );
  return result && result.length > 0 && result[0].desc_glob != null
    ? Number(result[0].desc_glob)
    : 0.0;
}

