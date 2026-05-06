// Importa el objeto sql para realizar consultas a la base de datos
import { sql } from "../config/database.js";
import axios from "axios";

// Obtiene la lista de clientes y sus gestiones (tránsito y vencido)
// Recibe opcionalmente el segmento (co_seg) por query string
export const getGestiones = async (req, res) => {
  try {
    // Obtiene la fecha de hoy en formato YYYY-MM-DD
    const hoyStr = new Date().toISOString().slice(0, 10);
    // Construye la consulta SQL para obtener clientes y sus saldos en tránsito y vencidos
    let query = `
            SELECT 
                c.co_cli, c.cli_des, c.tipo, c.co_seg, c.co_ven, c.nit,
                (SELECT SUM(saldo) FROM factura WHERE co_cli = c.co_cli AND CAST(fec_emis AS DATE) < CAST(fec_venc AS DATE) AND CAST(fec_venc AS DATE) >= @hoy) AS transito,
                (SELECT SUM(saldo) FROM factura WHERE co_cli = c.co_cli AND CAST(fec_venc AS DATE) < @hoy) AS vencido
            FROM dbo.clientes c
            WHERE 1=1
        `;
    const params = {};
    // Obtiene el segmento desde la query string
    const { co_seg } = req.query;

    // Si se envía segmento, filtra por ese segmento (puede ser uno o varios)
    if (co_seg) {
      const segmentos = co_seg
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      if (segmentos.length === 1) {
        query += " AND c.co_seg = @co_seg0";
        params.co_seg0 = segmentos[0];
      } else if (segmentos.length > 1) {
        const inParams = segmentos.map((_, i) => `@co_seg${i}`);
        query += ` AND c.co_seg IN (${inParams.join(",")})`;
        segmentos.forEach((seg, i) => {
          params[`co_seg${i}`] = seg;
        });
      }
    } else {
      // Si no se envía segmento, retorna array vacío
      return res.json([]);
    }

    // Prepara la consulta SQL con los parámetros
    const request = new sql.Request();
    request.input("hoy", sql.Date, hoyStr);
    for (const param in params) {
      request.input(param, sql.VarChar, params[param]);
    }

    // Ejecuta la consulta y procesa los resultados
    const result = await request.query(query);
    const clientes = result.recordset.map((row) => ({
      co_cli: row.co_cli?.trim(), // Código de cliente
      cli_des: row.cli_des?.trim(), // Nombre del cliente
      tipo: row.tipo?.trim(), // Tipo de cliente
      co_seg: row.co_seg?.trim(), // Segmento
      co_ven: row.co_ven?.trim(), // Vendedor
      nit: row.nit?.trim(), // NIT
      transito: row.transito ? Number(row.transito).toFixed(2) : "0.00", // Saldo en tránsito
      vencido: row.vencido ? Number(row.vencido).toFixed(2) : "0.00", // Saldo vencido
    }));

    // Devuelve el array de clientes
    res.json(clientes);
  } catch (err) {
    // Manejo de errores
    console.error("Error al consultar clientes:", err);
    res.status(500).json({ error: "Error al consultar clientes" });
  }
};

// Obtiene los detalles de las facturas de un cliente específico
// Recibe el código de cliente (co_cli) en el body
export const getDetallesCliente = async (req, res) => {
  // Extrae el código de cliente del body
  const { co_cli } = req.body;
  // Si no se envía el código de cliente, retorna error
  if (!co_cli) {
    return res
      .status(400)
      .json({ error: "Debe enviar el campo co_cli en el body" });
  }

  try {
    // Consulta las facturas del cliente y sus datos principales
    const query = `
            SELECT f.fact_num, f.descrip, f.co_cli, 
            CAST(f.fec_emis AS DATE) AS emision, 
            CAST(f.fec_venc AS DATE) AS vence, 
            f.co_ven, f.tot_neto, f.saldo,
            c.tipo
            FROM dbo.factura f
            INNER JOIN dbo.clientes c ON f.co_cli = c.co_cli
            WHERE f.saldo > 0 AND (f.co_cli = @co_cli)
            ORDER BY CAST(f.fec_venc AS DATE) DESC 
            OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY
        `;
    const request = new sql.Request();
    request.input("co_cli", sql.VarChar, co_cli);
    const result = await request.query(query);

    // Procesa cada factura para calcular descuentos y días de mora
    const hoy = new Date();
    const facturas = result.recordset.map((factura) => {
      const fechaEmis = new Date(factura.emision);
      const fechaVence = new Date(factura.vence);
      const diasDesdeEmis = Math.floor(
        (hoy - fechaEmis) / (1000 * 60 * 60 * 24)
      );
      const diasHastaVence = Math.floor(
        (fechaVence - hoy) / (1000 * 60 * 60 * 24)
      );
      const dias_mora = Math.floor((hoy - fechaVence) / (1000 * 60 * 60 * 24));

      // Calcula el porcentaje de descuento y el tipo de descuento según el tipo de cliente y días
      let porcen_desc = 0;
      let tipo_desc = "Sin descuento";

      if (factura.tipo === "30E2" || factura.tipo === "30E") {
        if (diasDesdeEmis >= 0 && diasDesdeEmis <= 9) {
          porcen_desc = 0.07;
          tipo_desc = "7% (0-9 días después de emisión)";
        } else if (diasDesdeEmis >= 10 && diasDesdeEmis <= 19) {
          porcen_desc = 0.05;
          tipo_desc = "5% (10-19 días después de emisión)";
        } else if (diasDesdeEmis >= 20 && hoy <= fechaVence) {
          porcen_desc = 0.03;
          tipo_desc = "3% (20 días después de emisión hasta vencimiento)";
        } else if (hoy > fechaVence) {
          porcen_desc = 0;
          tipo_desc = "0% (después de vencimiento)";
        }
      } else {
        if (diasDesdeEmis >= 0 && diasDesdeEmis <= 4) {
          porcen_desc = 0.07;
          tipo_desc = "7% (0-4 días después de emisión)";
        } else if (diasDesdeEmis >= 5 && diasDesdeEmis <= 10) {
          porcen_desc = 0.05;
          tipo_desc = "5% (5-10 días después de emisión)";
        } else if (diasDesdeEmis >= 11 && diasDesdeEmis <= 15) {
          porcen_desc = 0.03;
          tipo_desc = "3% (11-15 días después de emisión)";
        } else if (diasHastaVence >= 0 && diasHastaVence <= 16) {
          porcen_desc = 0.02;
          tipo_desc = "2% (16 días antes de vencimiento)";
        } else if (hoy > fechaVence) {
          porcen_desc = 0;
          tipo_desc = "0% (después de vencimiento)";
        }
      }

      // Calcula el saldo con descuento y días de mora
      const saldo = Number(factura.saldo);
      const saldo_menos_10 = (saldo - saldo * 0.1).toFixed(2);
      const saldo_con_desc = (
        saldo -
        saldo * 0.1 -
        (saldo - saldo * 0.1) * porcen_desc
      ).toFixed(2);

      return {
        ...factura,
        saldo: saldo.toFixed(2), // Saldo original
        saldo_menos_10, // Saldo menos 10%
        porcen_desc: (porcen_desc * 100).toFixed(2) + "%", // Porcentaje de descuento
        tipo_desc, // Descripción del descuento
        saldo_con_desc, // Saldo con descuento aplicado
        dias_mora, // Días de mora
      };
    });

    // Devuelve el array de facturas procesadas
    res.json(facturas);
  } catch (err) {
    // Manejo de errores
    res.status(500).json({ error: err.message });
  }
};

// Obtiene las facturas vencidas de un grupo de clientes
// Recibe los códigos de clientes por query string (clientes)
export const getFacturasVencidas = async (req, res) => {
  // Extrae los códigos de clientes desde la query string
  let codigos = req.query.clientes;
  if (!codigos) {
    // Si no se envía el parámetro, retorna error
    return res.status(400).json({ error: "Debe enviar el parámetro clientes" });
  }
  // Si es un string, lo convierte en array
  if (typeof codigos === "string") {
    codigos = codigos
      .split(",")
      .map((c) => c.trim())
      .filter(Boolean);
  }

  try {
    // Prepara los parámetros para la consulta SQL
    const inParams = codigos.map((_, i) => `@cli${i}`).join(",");
    const paramsObj = {};
    codigos.forEach((c, i) => (paramsObj[`cli${i}`] = c));

    // Consulta las facturas vencidas de los clientes
    const query = `
            SELECT c.co_cli, c.cli_des, 
            SUM(CASE WHEN d.saldo > 0 AND CONVERT(date, d.fec_venc) < CONVERT(date, GETDATE()) THEN d.saldo ELSE 0 END) AS SaldoVencido
            FROM clientes c
            LEFT JOIN docum_cc d ON c.co_cli = d.co_cli
            WHERE c.co_cli IN (${inParams})
            GROUP BY c.co_cli, c.cli_des
            HAVING SUM(CASE WHEN d.saldo > 0 AND CONVERT(date, d.fec_venc) < CONVERT(date, GETDATE()) THEN d.saldo ELSE 0 END) > 0
            ORDER BY c.co_cli
        `;

    const request = new sql.Request();
    for (const key in paramsObj) {
      request.input(key, sql.VarChar, paramsObj[key]);
    }
    const result = await request.query(query);

    // Procesa los resultados para limpiar los datos
    const cleaned = result.recordset.map((row) => ({
      co_cli: row.co_cli?.trim(), // Código de cliente
      cli_des: row.cli_des?.trim(), // Nombre del cliente
      SaldoVencido: row.SaldoVencido ? String(row.SaldoVencido).trim() : "0", // Saldo vencido
    }));

    // Devuelve el array de clientes con saldo vencido
    res.json(cleaned);
  } catch (err) {
    // Manejo de errores
    res.status(500).json({ error: err.message });
  }
};

// Obtiene la lista de segmentos de clientes
// Recibe opcionalmente el segmento (co_seg) por query string
export const getSegmentos = async (req, res) => {
  try {
    // Construye la consulta SQL para obtener segmentos
    let query = "SELECT co_seg, seg_des FROM segmento";
    const params = {};
    // Obtiene el segmento desde la query string
    const { co_seg } = req.query;

    // Si se envía segmento, filtra por ese segmento (puede ser uno o varios)
    if (co_seg) {
      const segmentos = co_seg
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      if (segmentos.length === 1) {
        query += " WHERE co_seg = @co_seg0";
        params.co_seg0 = segmentos[0];
      } else if (segmentos.length > 1) {
        const inParams = segmentos.map((_, i) => `@co_seg${i}`);
        query += ` WHERE co_seg IN (${inParams.join(",")})`;
        segmentos.forEach((seg, i) => {
          params[`co_seg${i}`] = seg;
        });
      }
    }
    query += " ORDER BY co_seg";

    // Prepara la consulta SQL con los parámetros
    const request = new sql.Request();
    for (const param in params) {
      request.input(param, sql.VarChar, params[param]);
    }

    // Ejecuta la consulta y procesa los resultados
    const result = await request.query(query);
    const cleaned = result.recordset.map((row) => ({
      co_seg: row.co_seg?.trim(), // Código de segmento
      seg_des: row.seg_des?.trim(), // Descripción del segmento
    }));
    // Devuelve el array de segmentos
    res.json(cleaned);
  } catch (err) {
    // Manejo de errores
    console.error("Error al consultar la tabla segmentos:", err);
    res.status(500).json({ error: "Error al consultar la tabla Segmentos" });
  }
};

// Obtiene la lista de vendedores
export const getVendedores = async (req, res) => {
  try {
    // Consulta los vendedores y sus datos principales
    const query = "SELECT co_ven, tipo, ven_des FROM vendedor v";
    const request = new sql.Request();
    const result = await request.query(query);
    // Procesa los resultados para limpiar los datos
    const vendedores = result.recordset.map((row) => ({
      co_ven: row.co_ven?.trim(), // Código de vendedor
      tipo: row.tipo, // Tipo de vendedor
      ven_des: row.ven_des?.trim(), // Nombre del vendedor
    }));
    // Devuelve el array de vendedores
    res.json(vendedores);
  } catch (err) {
    // Manejo de errores
    console.error("Error al consultar vendedores:", err);
    res.status(500).json({ error: "Error al consultar vendedores" });
  }
};

// Descarga la lista completa de clientes desde Bitrix24
export const getClientesBitrix = async (req, res) => {
  const url =
    process.env.BITRIX_MAIN_URL;
  const filter = { UF_CRM_1634787828: "-" };
  const select = ["ID", "TITLE", "UF_CRM_1685651349"];
  let start = 0;
  let allCompanies = [];

  try {
    while (true) {
      const response = await axios.post(url, {
        filter,
        select,
        start,
      });
      const { result, next } = response.data;
      if (result) {
        allCompanies = allCompanies.concat(result);
      }
      if (typeof next === "undefined") break;
      start = next;
    }
    if (!res.headersSent) res.json(allCompanies);
  } catch (err) {
    console.error("Error al consultar Bitrix24:", err);
    if (!res.headersSent) res.status(500).json({ error: "Error al consultar Bitrix24" });
  }
};

export const getGestionesYBitrix = async (req, res) => {
  try {
    const { co_seg } = req.query;
    console.log('[gestiones-bitrix] co_seg:', co_seg);

    const [gestionesRes, bitrixRes] = await Promise.all([
      getGestionesPromise(req),
      getClientesBitrixPromise(co_seg),
    ]);
    console.log('[gestiones-bitrix] gestiones:', gestionesRes.length, '| bitrix:', bitrixRes.length);
    res.json({
      gestiones: gestionesRes,
      bitrix: bitrixRes,
    });
  } catch (err) {
    console.error('[gestiones-bitrix] error:', err.message);
    res.status(500).json({ error: "Error al consultar clientes y Bitrix24" });
  }
};

// Helpers para usar las funciones como promesas
const getGestionesPromise = async (req) => {
  try {
    const hoyStr = new Date().toISOString().slice(0, 10);
    let query = `
            SELECT 
                c.co_cli, 
                c.cli_des, 
                c.tipo, 
                c.co_seg, 
                c.co_ven, 
                c.nit, 
                c.desc_glob,
                (SELECT SUM(saldo) FROM factura WHERE co_cli = c.co_cli AND CAST(fec_emis AS DATE) < CAST(fec_venc AS DATE) AND CAST(fec_venc AS DATE) >= @hoy) AS transito,
                (SELECT SUM(saldo) FROM factura WHERE co_cli = c.co_cli AND CAST(fec_venc AS DATE) < @hoy) AS vencido
            FROM dbo.clientes c
            WHERE 1=1
        `;
    const params = {};
    const { co_seg } = req.query;
    if (co_seg) {
      const segmentos = co_seg
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      if (segmentos.length === 1) {
        query += " AND c.co_seg = @co_seg0";
        params.co_seg0 = segmentos[0];
      } else if (segmentos.length > 1) {
        const inParams = segmentos.map((_, i) => `@co_seg${i}`);
        query += ` AND c.co_seg IN (${inParams.join(",")})`;
        segmentos.forEach((seg, i) => {
          params[`co_seg${i}`] = seg;
        });
      }
    } else {
      return [];
    }
    const request = new sql.Request();
    request.input("hoy", sql.Date, hoyStr);
    for (const param in params) {
      request.input(param, sql.VarChar, params[param]);
    }
    const result = await request.query(query);
    return result.recordset.map((row) => ({
      co_cli: row.co_cli?.trim(),
      cli_des: row.cli_des?.trim(),
      tipo: row.tipo?.trim(),
      co_seg: row.co_seg?.trim(),
      co_ven: row.co_ven?.trim(),
      nit: row.nit?.trim(),
      desc_glob: row.desc_glob ? Number(row.desc_glob).toFixed(2) : "0.00",
      transito: row.transito ? Number(row.transito).toFixed(2) : "0.00",
      vencido: row.vencido ? Number(row.vencido).toFixed(2) : "0.00",
    }));
  } catch (err) {
    return [];
  }
};

const getClientesBitrixPromise = async (co_seg) => {
  const url =
    process.env.BITRIX_MAIN_URL;
  const filter = { UF_CRM_1634787828: "-" };
  const select = ["ID", "TITLE", "UF_CRM_1685651349"];
  let start = 0;
  let allCompanies = [];
  let intentos = 0;
  const maxIntentos = 3;

  while (intentos < maxIntentos) {
    try {
      start = 0;
      allCompanies = [];
      while (true) {
        const response = await axios.post(url, {
          filter,
          select,
          start,
        });
        const { result, next } = response.data;
        if (result) {
          allCompanies = allCompanies.concat(result);
        }
        if (typeof next === "undefined") break;
        start = next;
      }
      // Si obtuvo datos, retorna
      if (allCompanies.length > 0) return allCompanies;
    } catch (err) {
      // Si hay error, espera un poco y reintenta
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    intentos++;
  }
  // Si no obtuvo datos después de varios intentos, retorna array vacío con mensaje
  return [{ error: "No se pudo obtener datos de Bitrix24 después de varios intentos." }];
};
