import 'dotenv/config';
import { sql } from '../config/database.js';
import mysql from 'mysql2/promise';

import fs from 'fs';
import path from 'path';

const logFile = path.join(process.cwd(), 'logs', 'escala-ptc.log');

const writeLog = (data) => {
  const linea = `[${new Date().toISOString()}] ${JSON.stringify(data, null, 2)}\n${'─'.repeat(80)}\n`;
  fs.mkdirSync(path.dirname(logFile), { recursive: true });
  fs.appendFileSync(logFile, linea);
};

const mysqlComparadorConfig = {
    host: process.env.DB_COMPARADOR_HOST,
    user: process.env.DB_COMPARADOR_USER,
    password: process.env.DB_COMPARADOR_PASSWORD,
    database: 'comparador',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

export const getFacturasPorSegmento = async (req, res) => {
  try {
    // Si no hay body, usa valores por defecto
    let {
      co_cli = null,
      co_seg = null,
      co_ven = null,
      fecha_min = null,
      fechas_venc = null,
      page = 1,
      perPage = 20,
      todos = false,
    } = req.body && Object.keys(req.body).length ? req.body : {};

    // Puedes definir aquí valores por defecto si lo deseas, por ejemplo:
    // co_cli = co_cli || 'C001';
    // co_seg = co_seg || ['SEG1'];
    // fecha_min = fecha_min || '2024-01-01';

    // Validaciones de tipos y valores
    if (co_cli && typeof co_cli !== "string") {
      return res
        .status(400)
        .json({ message: "El campo co_cli debe ser un string." });
    }
    if (co_ven && typeof co_ven !== "string") {
      return res
        .status(400)
        .json({ message: "El campo co_ven debe ser un string." });
    }
    if (co_seg && !(Array.isArray(co_seg) || typeof co_seg === "string")) {
      return res
        .status(400)
        .json({
          message: "El campo co_seg debe ser un string o un array de strings.",
        });
    }
    if (fecha_min && typeof fecha_min !== "string") {
      return res
        .status(400)
        .json({
          message:
            "El campo fecha_min debe ser un string con formato YYYY-MM-DD.",
        });
    }
    if (
      fechas_venc &&
      !(Array.isArray(fechas_venc) || typeof fechas_venc === "string")
    ) {
      return res
        .status(400)
        .json({
          message:
            "El campo fechas_venc debe ser un string o un array de strings.",
        });
    }
    if (page && (isNaN(page) || parseInt(page) < 1)) {
      return res
        .status(400)
        .json({
          message: "El campo page debe ser un número mayor o igual a 1.",
        });
    }
    if (perPage && (isNaN(perPage) || parseInt(perPage) < 1)) {
      return res
        .status(400)
        .json({
          message: "El campo perPage debe ser un número mayor o igual a 1.",
        });
    }

    // Normalizar arrays si existen
    if (co_seg && !Array.isArray(co_seg)) co_seg = [co_seg];
    if (fechas_venc && !Array.isArray(fechas_venc)) fechas_venc = [fechas_venc];

    const offset = (parseInt(page) - 1) * parseInt(perPage);

    // Construir filtros dinámicamente
    let where = ["C.saldo > 0"];
    const request = new sql.Request();

    if (co_cli) {
      where.push("LTRIM(RTRIM(B.co_cli)) = @co_cli");
      request.input("co_cli", sql.VarChar, co_cli.trim());
    } else if (co_seg && co_seg.length > 0) {
      const segPlaceholders = co_seg.map((_, i) => `@seg${i}`).join(", ");
      where.push(`A.co_seg IN (${segPlaceholders})`);
      co_seg.forEach((seg, i) =>
        request.input(`seg${i}`, sql.VarChar, seg.trim())
      );
    }

    if (co_ven) {
      where.push("LTRIM(RTRIM(C.co_ven)) = @co_ven");
      request.input("co_ven", sql.VarChar, co_ven.trim());
    }
    if (fecha_min) {
      where.push("CAST(C.fec_venc AS DATE) > @fecha_min");
      request.input("fecha_min", sql.Date, fecha_min);
    }
    if (fechas_venc && fechas_venc.length > 0) {
      const fechasPlaceholders = fechas_venc
        .map((_, i) => `@fecha${i}`)
        .join(", ");
      where.push(`CAST(C.fec_venc AS DATE) IN (${fechasPlaceholders})`);
      fechas_venc.forEach((fv, i) => request.input(`fecha${i}`, sql.Date, fv));
    }

    const whereClause = where.length ? "WHERE " + where.join(" AND ") : "";

    const query = `
      SELECT 
        B.co_cli, 
        B.cli_des, 
        B.tipo, 
        B.contribu, 
        C.tasa,
        C.co_ven, 
        A.co_seg, 
        (C.tot_neto / C.tasa) AS tot_neto, 
        C.fact_num, 
        (D.saldo / C.tasa) AS saldo, 
        C.iva, 
        CAST(D.fec_emis AS DATE) AS emision, 
        CAST(D.fec_venc AS DATE) AS vence,
        (SELECT TOP 1 RB.num_doc
                                FROM dbo.reng_fac RA
                                INNER JOIN dbo.reng_nde RB ON RA.num_doc = RB.fact_num AND RB.tipo_doc='T'
                                INNER JOIN dbo.cotiz_c CC ON RB.num_doc = CC.fact_num AND CC.co_us_in<>'311'
                                WHERE RA.fact_num = C.fact_num) AS origen
      FROM dbo.segmento AS A
      JOIN dbo.clientes AS B ON A.co_seg = B.co_seg
      JOIN dbo.factura AS C ON B.co_cli = C.co_cli
      JOIN dbo.docum_cc AS D ON C.fact_num = D.nro_doc AND D.tipo_doc = 'FACT'
      ${whereClause}
        AND C.saldo = D.saldo
      ORDER BY C.co_cli DESC, C.fec_venc ASC
      ${todos ? "" : "OFFSET @offset ROWS FETCH NEXT @perPage ROWS ONLY"}
    `;

    request.input("offset", sql.Int, offset);
    request.input("perPage", sql.Int, parseInt(perPage));

    const result = await request.query(query);

    // Obtener la tasa de cambio actual del día
    const tasaActualResult = await sql.query(`SELECT cambio FROM moneda WHERE co_mone = 'US$'`);
    const tasaHoy = tasaActualResult.recordset[0]?.cambio || 0;

    // Consultar MySQL por cada fact_num y agregar fecha_escaneo si existe
    const { getMysqlPool } = await import('../config/database.js');
    const mysqlPool = getMysqlPool();

    // Si el pool no está inicializado, lanzar error
    if (!mysqlPool) {
      return res.status(500).json({ message: 'No hay conexión a MySQL' });
    }

    // Consultar en paralelo para eficiencia
    const facturas = await Promise.all(result.recordset.map(async (factura) => {
      // fact_num en SQL es int, en MySQL es varchar
      const factNumStr = factura.fact_num != null ? String(factura.fact_num) : null;
      let facturaLimpia = { ...factura };
      // Limpiar todos los valores string (trim)
      for (const key in facturaLimpia) {
        if (typeof facturaLimpia[key] === 'string') {
          facturaLimpia[key] = facturaLimpia[key].trim();
        }
        // Redondear a 2 decimales si es número con decimales
        if (
          typeof facturaLimpia[key] === 'number' &&
          !Number.isInteger(facturaLimpia[key])
        ) {
          facturaLimpia[key] = Number(facturaLimpia[key].toFixed(2));
        }
      }
      if (!factNumStr) return facturaLimpia;
      try {
        const [rows] = await mysqlPool.query(
          'SELECT fecha_escaneo FROM facturas_cargadas WHERE fact_num = ? LIMIT 1',
          [factNumStr]
        );
        if (rows.length && rows[0].fecha_escaneo != null) {
          // Formatear fecha_escaneo a 'YYYY-MM-DD HH:mm:ss'
          const fecha = new Date(rows[0].fecha_escaneo);
          const pad = n => n.toString().padStart(2, '0');
          const fechaFormateada = `${fecha.getFullYear()}-${pad(fecha.getMonth() + 1)}-${pad(fecha.getDate())} ${pad(fecha.getHours())}:${pad(fecha.getMinutes())}:${pad(fecha.getSeconds())}`;
          facturaLimpia.fecha_escaneo = fechaFormateada;
        }

        // Calcular días de mora (usando componentes locales para evitar desfase de zona horaria)
        const hoy = new Date();
        const fechaVence = new Date(factura.vence);

        // Resetear a medianoche local para comparación exacta de días
        const hoyLocal = new Date(hoy.getFullYear(), hoy.getMonth(), hoy.getDate());
        const venceLocal = new Date(fechaVence.getUTCFullYear(), fechaVence.getUTCMonth(), fechaVence.getUTCDate());

        const diffTime = hoyLocal.getTime() - venceLocal.getTime();
        const diffDays = Math.round(diffTime / (1000 * 60 * 60 * 24));

        facturaLimpia.dias_mora = diffDays;
        facturaLimpia.mora = diffDays;

        // Determinar fecha base para el cálculo del descuento (escaneo o emisión)
        const fechaBaseDesc = rows.length && rows[0].fecha_escaneo != null
          ? new Date(rows[0].fecha_escaneo)
          : new Date(factura.emision);

        const hoyCalculo = new Date();
        const hoyCalculoUTC = Date.UTC(hoyCalculo.getFullYear(), hoyCalculo.getMonth(), hoyCalculo.getDate());

        // fechaBaseDesc puede venir de MySQL o MSSQL emision
        const fechaBaseDescUTC = Date.UTC(fechaBaseDesc.getUTCFullYear(), fechaBaseDesc.getUTCMonth(), fechaBaseDesc.getUTCDate());
        const fechaVenceDescUTC = Date.UTC(fechaVence.getUTCFullYear(), fechaVence.getUTCMonth(), fechaVence.getUTCDate());

        const diasDesdeBase = Math.floor((hoyCalculoUTC - fechaBaseDescUTC) / (1000 * 60 * 60 * 24));
        const tipoCliente = factura.tipo;

        let porcenTiered = 0;
        const saldoOriginal = Number(factura.saldo);

        // Lógica de descuentos según tipo de cliente
        if (tipoCliente.includes('E')) {
          // Clientes Especiales
          if (diasDesdeBase >= 0 && diasDesdeBase <= 9) porcenTiered = 0.07;
          else if (diasDesdeBase >= 10 && diasDesdeBase <= 19) porcenTiered = 0.05;
          else if (diasDesdeBase >= 20 && hoyCalculoUTC <= fechaVenceDescUTC) porcenTiered = 0.03;
        } else if (!tipoCliente.includes('CD') && !tipoCliente.includes('C/D')) {
          // Clientes Normales
          const diasGrace = 5;
          const limitNormalUTC = fechaVenceDescUTC + (diasGrace * 1000 * 60 * 60 * 24);

          if (diasDesdeBase >= 0 && diasDesdeBase <= 4) porcenTiered = 0.07;
          else if (diasDesdeBase >= 5 && diasDesdeBase <= 9) porcenTiered = 0.05;
          else if (diasDesdeBase >= 10 && diasDesdeBase <= 14) porcenTiered = 0.03;
          else if (diasDesdeBase >= 15 && hoyCalculoUTC <= limitNormalUTC) porcenTiered = 0.02;
        }

        // Retención (75% del IVA)
        facturaLimpia.retencion = Math.round((factura.iva || 0) * 0.75 * 100) / 100;
        facturaLimpia.retencion_dolar = facturaLimpia.tasa > 0
          ? Math.round((facturaLimpia.retencion / facturaLimpia.tasa) * 100) / 100
          : 0;

        // Lógica de Descuentos
        let baseFactor = 1.0;
        if (facturaLimpia.dias_mora <= 5) {
          // Si no tiene mora o tiene hasta 5 días, aplica descuento base del 10%
          baseFactor = 0.90;
        } else {
          // Si tiene más de 5 días de mora, NO hay descuentos de ningún tipo
          porcenTiered = 0;
        }

        // Corrección: Si tiene mora (incluso 1 día), pierde el descuento por pronto pago (tiered),
        // pero mantiene el descuento base del 10% si está en el rango de 1-5 días.
        if (facturaLimpia.dias_mora > 0) {
          porcenTiered = 0;
        }

        // Calcular descuento total visual (combinando baseFactor + porcenTiered)
        // PrecioFinal = Precio * BaseFactor * (1 - Tiered)
        const factorFinal = baseFactor * (1 - porcenTiered);
        facturaLimpia.descuento = Math.round((1 - factorFinal) * 100);

        const saldoConBase = saldoOriginal * baseFactor;
        const finalDolar = saldoConBase * (1 - porcenTiered);

        facturaLimpia.saldo_con_descuento = Math.round(saldoConBase * 100) / 100;
        facturaLimpia.monto_dolar = Math.round(finalDolar * 100) / 100;
        facturaLimpia.monto_bs = Math.round(finalDolar * (tasaHoy || facturaLimpia.tasa || 0) * 100) / 100;
        facturaLimpia.tasa_hoy = tasaHoy;
        facturaLimpia.query_origen = query; // Mostrar query en cada factura

        return facturaLimpia;
      } catch (err) {
        // Loguear error de MySQL
        if (err.message !== 'No hay conexión a MySQL') {
          console.error('Error consultando MySQL para fact_num', factNumStr, err.message);
        }

        // Recalcular todo sin depender de MySQL (fallback)
        const hoy = new Date();
        const fechaVence = new Date(factura.vence);
        const hoyLocal = new Date(hoy.getFullYear(), hoy.getMonth(), hoy.getDate());
        const venceLocal = new Date(fechaVence.getUTCFullYear(), fechaVence.getUTCMonth(), fechaVence.getUTCDate());

        const diffTime = hoyLocal.getTime() - venceLocal.getTime();
        const diffDays = Math.round(diffTime / (1000 * 60 * 60 * 24));
        facturaLimpia.dias_mora = diffDays;
        facturaLimpia.mora = diffDays;

        const hoyCalculo = new Date();
        const hoyCalculoUTC = Date.UTC(hoyCalculo.getFullYear(), hoyCalculo.getMonth(), hoyCalculo.getDate());
        const fechaBaseDesc = new Date(factura.emision);
        // Emision de MSSQL se trata como UTC componentes
        const fechaBaseDescUTC = Date.UTC(fechaBaseDesc.getUTCFullYear(), fechaBaseDesc.getUTCMonth(), fechaBaseDesc.getUTCDate());
        const fechaVenceDescUTC = Date.UTC(fechaVence.getUTCFullYear(), fechaVence.getUTCMonth(), fechaVence.getUTCDate());

        const diasDesdeBase = Math.floor((hoyCalculoUTC - fechaBaseDescUTC) / (1000 * 60 * 60 * 24));
        const tipoCliente = factura.tipo;
        let porcenTiered = 0;
        const saldoOriginal = Number(factura.saldo);

        if (tipoCliente.includes('E')) {
          if (diasDesdeBase >= 0 && diasDesdeBase <= 9) porcenTiered = 0.07;
          else if (diasDesdeBase >= 10 && diasDesdeBase <= 19) porcenTiered = 0.05;
          else if (diasDesdeBase >= 20 && hoyCalculoUTC <= fechaVenceDescUTC) porcenTiered = 0.03;
        } else if (!tipoCliente.includes('CD') && !tipoCliente.includes('C/D')) {
          const diasGrace = 5;
          const limitNormalUTC = fechaVenceDescUTC + (diasGrace * 1000 * 60 * 60 * 24);
          if (diasDesdeBase >= 0 && diasDesdeBase <= 4) porcenTiered = 0.07;
          else if (diasDesdeBase >= 5 && diasDesdeBase <= 9) porcenTiered = 0.05;
          else if (diasDesdeBase >= 10 && diasDesdeBase <= 14) porcenTiered = 0.03;
          else if (diasDesdeBase >= 15 && hoyCalculoUTC <= limitNormalUTC) porcenTiered = 0.02;
        }

        facturaLimpia.retencion = Math.round((factura.iva || 0) * 0.75 * 100) / 100;
        facturaLimpia.retencion_dolar = facturaLimpia.tasa > 0
          ? Math.round((facturaLimpia.retencion / facturaLimpia.tasa) * 100) / 100
          : 0;

        let baseFactor = 1.0;
        if (facturaLimpia.dias_mora <= 5) {
          baseFactor = 0.90;
        } else {
          porcenTiered = 0;
        }

        // Corrección: Si tiene mora (incluso 1 día), pierde el descuento por pronto pago (tiered),
        // pero mantiene el descuento base del 10% si está en el rango de 1-5 días.
        if (facturaLimpia.dias_mora > 0) {
          porcenTiered = 0;
        }

        // Calcular descuento total visual (combinando baseFactor + porcenTiered)
        const factorFinal = baseFactor * (1 - porcenTiered);
        facturaLimpia.descuento = Math.round((1 - factorFinal) * 100);

        const saldoConBase = saldoOriginal * baseFactor;
        const finalDolar = saldoConBase * (1 - porcenTiered);

        facturaLimpia.saldo_con_descuento = Math.round(saldoConBase * 100) / 100;
        facturaLimpia.monto_dolar = Math.round(finalDolar * 100) / 100;
        facturaLimpia.monto_bs = Math.round(finalDolar * (tasaHoy || facturaLimpia.tasa || 0) * 100) / 100;
        facturaLimpia.tasa_hoy = tasaHoy;
        facturaLimpia.query_origen = query; // Mostrar query en cada factura

        return facturaLimpia;
      }
    }));

    // Mostrar solo lo que devuelve la consulta SQL Server + fecha_escaneo si existe
    res.json(facturas);
  } catch (error) {
    res
      .status(500)
      .json({
        message: "Error al obtener las facturas por segmento",
        error: error.message,
      });
  }
};

export const getFacturasPorSegmentoDpPago = async (req, res) => {
  try {
    // 1. Extracción y saneamiento de parámetros del cuerpo de la petición
    let {
      co_cli = null,
      co_seg = null,
      co_ven = null,
      page = 1,
      perPage = 20,
      todos = false,
    } = req.body && Object.keys(req.body).length ? req.body : {};

    // 2. Configuración de paginación y Request de SQL
    const offset = (parseInt(page) - 1) * parseInt(perPage);
    const request = new sql.Request();
    let where = ["D.saldo > 0"]; // Solo facturas con saldo pendiente

    // 3. Construcción dinámica de los filtros
    if (co_cli) {
      where.push("LTRIM(RTRIM(B.co_cli)) = @co_cli");
      request.input("co_cli", sql.VarChar, co_cli.trim());
    } else if (co_seg && co_seg.length > 0) {
      const segs = Array.isArray(co_seg) ? co_seg : [co_seg];
      const placeholders = segs.map((_, i) => `@seg${i}`).join(", ");
      where.push(`A.co_seg IN (${placeholders})`);
      segs.forEach((seg, i) => request.input(`seg${i}`, sql.VarChar, seg.trim()));
    }

    if (co_ven) {
      where.push("LTRIM(RTRIM(C.co_ven)) = @co_ven");
      request.input("co_ven", sql.VarChar, co_ven.trim());
    }

    const whereClause = "WHERE " + where.join(" AND ");

    // 4. Consulta SQL Principal (Busca facturas y sus escalas de pronto pago)
    const query = `
      SELECT
        B.co_cli, B.cli_des, B.tipo, B.contribu, C.tasa,
        (C.tot_neto / C.tasa) AS tot_neto, C.fact_num,
        (D.saldo / C.tasa) AS saldo_dolar, C.iva,
        CAST(D.fec_emis AS DATE) AS emision,
        CAST(D.fec_venc AS DATE) AS vence,
        ISNULL(P.hasta1, 0) as hasta1, ISNULL(P.hasta2, 0) as hasta2, ISNULL(P.hasta3, 0) as hasta3,
        ISNULL(P.hasta4, 0) as hasta4, ISNULL(P.hasta5, 0) as hasta5,
        ISNULL(P.porc1, 0) as porc1, ISNULL(P.porc2, 0) as porc2, ISNULL(P.porc3, 0) as porc3,
        ISNULL(P.porc4, 0) as porc4, ISNULL(P.porc5, 0) as porc5, ISNULL(P.porc6, 0) as porc6,
        (SELECT TOP 1 RB.num_doc
                        FROM dbo.reng_fac RA
                        INNER JOIN dbo.reng_nde RB ON RA.num_doc = RB.fact_num AND RB.tipo_doc='T'
                        INNER JOIN dbo.cotiz_c CC ON RB.num_doc = CC.fact_num AND CC.co_us_in<>'311'
                            WHERE RA.fact_num = C.fact_num) AS origen
      FROM dbo.segmento AS A
      JOIN dbo.clientes AS B ON A.co_seg = B.co_seg
      JOIN dbo.factura AS C ON B.co_cli = C.co_cli
      JOIN dbo.docum_cc AS D ON C.fact_num = D.nro_doc
      LEFT JOIN dbo.dppago AS P ON LTRIM(RTRIM(B.tipo)) = LTRIM(RTRIM(P.tipo_cli))
      ${whereClause} AND C.saldo = D.saldo
      ORDER BY C.co_cli DESC, C.fec_venc ASC
      ${todos ? "" : "OFFSET @offset ROWS FETCH NEXT @perPage ROWS ONLY"}
    `;

    request.input("offset", sql.Int, offset);
    request.input("perPage", sql.Int, parseInt(perPage));

    const result = await request.query(query);
    const tasaActualResult = await sql.query(`SELECT cambio FROM moneda WHERE co_mone = 'US$'`);
    const tasaHoy = tasaActualResult.recordset[0]?.cambio || 0;

    // 6. Procesamiento de facturas (Cálculo de Descuentos)
    const facturas = result.recordset.map((factura) => {
      let f = { ...factura };
      for (const key in f) { if (typeof f[key] === 'string') f[key] = f[key].trim(); }

      const hoy = new Date();
      const tHoy = Date.UTC(hoy.getFullYear(), hoy.getMonth(), hoy.getDate());
      const fVence = new Date(f.vence);
      const tVence = Date.UTC(fVence.getUTCFullYear(), fVence.getUTCMonth(), fVence.getUTCDate());
      
      const diffDays = Math.round((tHoy - tVence) / (1000 * 60 * 60 * 24));

      // Determinar escala según días restantes
      let porcEscalaRaw = 0;
      let escalaNombre = "Sin Escala";
      const d = diffDays * -1;

      if (d <= f.hasta1) { porcEscalaRaw = f.porc1; escalaNombre = "Escala 1"; }
      else if (d <= f.hasta2) { porcEscalaRaw = f.porc2; escalaNombre = "Escala 2"; }
      else if (d <= f.hasta3) { porcEscalaRaw = f.porc3; escalaNombre = "Escala 3"; }
      else if (d <= f.hasta4) { porcEscalaRaw = f.porc4; escalaNombre = "Escala 4"; }
      else if (d <= f.hasta5) { porcEscalaRaw = f.porc5; escalaNombre = "Escala 5"; }
      else { porcEscalaRaw = f.porc6; escalaNombre = "Escala 6"; }

      const saldoOriginal = Number(f.saldo_dolar || 0);
      let montoDesc10 = 0;
      let montoDescPP = 0;
      let porcPPFinal = 0;

      const valorEscala = Number(parseFloat(porcEscalaRaw || 0).toFixed(2));

      // --- VALIDACIÓN DE EXCEPCIÓN (ORIGEN + RANGO FECHA) ---
      const fEmis = new Date(f.emision);
      const tEmision = Date.UTC(fEmis.getUTCFullYear(), fEmis.getUTCMonth(), fEmis.getUTCDate());
      const tInicio = Date.UTC(2026, 0, 30); // 30-Ene
      const tFin = Date.UTC(2026, 2, 18);    // 18-Mar

      const cumpleExcepcion = f.origen && (tEmision >= tInicio && tEmision <= tFin);

      if (valorEscala > 0) {
        // CASO 1: NO CUMPLE EXCEPCIÓN -> SE APLICA EL 10% BASE OBLIGATORIO
        if (!cumpleExcepcion) {
          // Primero el 10% del total
          montoDesc10 = Number((saldoOriginal * 0.10).toFixed(2));
          const saldoRestante = Number((saldoOriginal - montoDesc10).toFixed(2));
          
          // Luego la escala (PP) sobre lo que queda
          porcPPFinal = Math.ceil(valorEscala); 
          montoDescPP = Number((saldoRestante * (porcPPFinal / 100)).toFixed(2));
        } 
        // CASO 2: CUMPLE EXCEPCIÓN -> SOLO ESCALA DIRECTA (SIN EL 10%)
        else {
          porcPPFinal = Math.ceil(valorEscala); 
          montoDescPP = Number((saldoOriginal * (porcPPFinal / 100)).toFixed(2));
          montoDesc10 = 0;
        }
      } else {
        escalaNombre = "Vencida";
        porcPPFinal = 0;
      }

      // 8. Cálculos Finales
      const totalAhorroDolar = Number((montoDesc10 + montoDescPP).toFixed(2));
      const montoFinalDolar = Number((saldoOriginal - totalAhorroDolar).toFixed(2));

      return {
        ...f,
        dias_mora: diffDays,
        escala_aplicada: escalaNombre,
        porcentaje_escala: valorEscala,
        porcentaje_dpp: porcPPFinal,
        descuento_pp_porc: porcPPFinal, 
        monto_descuento_base: montoDesc10,
        monto_descuento_pp: montoDescPP,
        monto_descuento_total_dolar: totalAhorroDolar,
        monto_dolar: montoFinalDolar,
        monto_bs: Number((montoFinalDolar * (tasaHoy || f.tasa)).toFixed(2)),
        tasa_hoy: tasaHoy
      };
    });

    res.json(facturas);
  } catch (error) {
    res.status(500).json({ message: "Error", error: error.message });
  }
};

export const getPaginaClientes = async (req, res) => {
  try {
    const { co_cli } = req.query;
    if (!co_cli) {
      return res
        .status(400)
        .json({ message: "Debe enviar el campo co_cli como parámetro" });
    }

    // Detectar si se quiere patrón (contiene '%' o '*')
    const raw = String(co_cli);
    const containsWildcard = /[%*]/.test(raw);
    // Convertir '*' a '%' si el cliente usa asterisco como wildcard
    const paramValue = containsWildcard ? raw.replace(/\*/g, '%').trim() : raw.trim();

    const query = `
      SELECT
        c.co_cli,
        MAX(CONVERT(VARCHAR(MAX), c.cli_des)) AS cli_des,
        c.tipo,
        MAX(CONVERT(VARCHAR(MAX), c.direc1)) AS direc1,
        MAX(CONVERT(VARCHAR(MAX), c.direc2)) AS direc2,
        MAX(CONVERT(VARCHAR(MAX), c.telefonos)) AS telefonos,
        MAX(CONVERT(VARCHAR(MAX), c.email)) AS email,
        MAX(CONVERT(VARCHAR(MAX), c.desc_glob)) AS desc_glob,
        MAX(CONVERT(VARCHAR(MAX), c.fax)) AS fax,
        c.rif,
        c.nit,
        c.saldo,
        c.saldo_ini,
        c.mont_cre,
        c.plaz_pag,
        c.co_ven,
        c.zip,
        c.fecha_reg,
        SUM(CASE WHEN d.saldo > 0 AND CONVERT(date, d.fec_venc) < CONVERT(date, GETDATE()) THEN d.saldo ELSE 0 END) AS SaldoVencido
      FROM clientes AS c
      LEFT JOIN docum_cc AS d ON c.co_cli = d.co_cli
      WHERE 
        (${containsWildcard ? "UPPER(LTRIM(RTRIM(c.co_cli))) LIKE UPPER(@co_cli)" : "UPPER(LTRIM(RTRIM(c.co_cli))) = UPPER(@co_cli)"})
        AND c.inactivo = 0
      GROUP BY
        c.co_cli,
        c.tipo,
        c.rif,
        c.nit,
        c.saldo,
        c.saldo_ini,
        c.mont_cre,
        c.plaz_pag,
        c.co_ven,
        c.zip,
        c.fecha_reg
    `;

    const request = new sql.Request();
    request.input("co_cli", sql.VarChar, paramValue);

    const result = await request.query(query);

    // Si el resultado está vacío porque el cliente está inactivo o no existe
    if (result.recordset.length === 0) {
      return res.status(404).json({ message: "Cliente no encontrado o inactivo" });
    }

    // Limpiar espacios en blanco de los campos string
    const clientes = result.recordset.map((row) => ({
      co_cli: row.co_cli?.trim(),
      cli_des: row.cli_des?.trim(),
      tipo: row.tipo?.trim(),
      direc1: row.direc1?.trim(),
      direc2: row.direc2?.trim(),
      telefonos: row.telefonos?.trim(),
      email: row.email?.trim(),
      desc_glob: row.desc_glob?.trim(),
      fax: row.fax?.trim(),
      rif: row.rif?.trim(),
      nit: row.nit?.trim(),
      saldo: row.saldo,
      saldo_ini: row.saldo_ini,
      mont_cre: row.mont_cre,
      plaz_pag: row.plaz_pag,
      co_ven: row.co_ven?.trim(),
      zip: row.zip?.trim(),
      fecha_reg: row.fecha_reg,
      SaldoVencido: row.SaldoVencido,
    }));

    res.json(clientes);
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error al obtener los clientes", error: error.message });
  }
};

export const getPedidosPaginados = async (req, res) => {
try {
    const { co_cli, page = 1, perPage = 10 } = req.query;
    if (!co_cli) {
      return res
        .status(400)
        .json({ message: "Debe enviar el campo co_cli como parámetro" });
    }

    // Obtener total de pedidos
    const countQuery = `
      SELECT COUNT(*) AS total
      FROM cotiz_c
      WHERE LTRIM(RTRIM(co_cli)) = @co_cli
    `;
    const countRequest = new sql.Request();
    countRequest.input("co_cli", sql.VarChar, co_cli.trim());
    const countResult = await countRequest.query(countQuery);
    const total = countResult.recordset[0]?.total ?? 0;

    // Paginación
    const offset = (parseInt(page) - 1) * parseInt(perPage);

    // Obtener pedidos paginados
    const pedidosQuery = `
      SELECT
        fact_num,
        fec_emis,
        status,
        tot_neto,
        iva,
        tot_neto + iva AS total,
        comentario,
        anulada
      FROM cotiz_c
      WHERE LTRIM(RTRIM(co_cli)) = @co_cli
      ORDER BY fec_emis DESC
      OFFSET @offset ROWS FETCH NEXT @perPage ROWS ONLY
    `;
    const pedidosRequest = new sql.Request();
    pedidosRequest.input("co_cli", sql.VarChar, co_cli.trim());
    pedidosRequest.input("offset", sql.Int, offset);
    pedidosRequest.input("perPage", sql.Int, parseInt(perPage));

    const pedidosResult = await pedidosRequest.query(pedidosQuery);

    // Limpiar espacios en blanco de los campos string
    const pedidos = await Promise.all(
      pedidosResult.recordset.map(async (row) => {
        // Consulta para el cuerpo del pedido (detalle) cruzado con Notas de Entrega
        const cuerpoQuery = `
          SELECT
            rc.reng_num,
            rc.co_art,
            rc.total_art,
            rc.pendiente,
            rc.porc_desc,
            rc.tipo_imp,
            rc.co_alma,
            rc.prec_vta,
            rc.rowguid,
            -- Buscamos cuánto se ha despachado de este renglón específico en Notas de Entrega
            ISNULL((
              SELECT SUM(rn.total_art) 
              FROM reng_nde rn 
              WHERE rn.num_doc = rc.fact_num 
              AND rn.co_art = rc.co_art 
              AND rn.anulado = 0
            ), 0) AS cant_despachada
          FROM reng_cac rc
          WHERE rc.fact_num = @fact_num
        `;
        const cuerpoRequest = new sql.Request();
        cuerpoRequest.input("fact_num", sql.Int, row.fact_num);
        const cuerpoResult = await cuerpoRequest.query(cuerpoQuery);

        let totalSumaPedido = 0;
        let totalSumaDespachado = 0;

        const cuerpo = cuerpoResult.recordset.map((det) => {
          const t_art = det.total_art || 0;
          const d_art = det.cant_despachada || 0;

          totalSumaPedido += t_art;
          totalSumaDespachado += d_art;

          return {
            reng_num: det.reng_num,
            co_art: det.co_art?.trim(),
            total_art: t_art,
            pendiente: det.pendiente,
            porc_desc: det.porc_desc,
            tipo_imp: det.tipo_imp?.trim(),
            co_alma: det.co_alma?.trim(),
            prec_vta: det.prec_vta,
            rowguid: det.rowguid,
            // Campos de validación por artículo
            cant_despachada: d_art,
            completado: d_art >= t_art,
            falta_despacho: (t_art - d_art) > 0 ? (t_art - d_art) : 0
          };
        });

        // Lógica de estatus de despacho general
        let despacho_status = "No Despachado";
        if (totalSumaDespachado > 0) {
          despacho_status = totalSumaDespachado >= totalSumaPedido ? "Despachado Total" : "Despachado Parcial";
        }
        if (row.anulada) despacho_status = "Pedido Anulado";

        return {
          fact_num: row.fact_num,
          fec_emis: row.fec_emis,
          status: row.status?.trim(),
          tot_neto: row.tot_neto,
          iva: row.iva,
          total: row.total,
          comentario: row.comentario?.trim(),
          anulada: row.anulada,
          // Nuevos campos de validación solicitados
          despacho_status: despacho_status,
          resumen_cantidades: {
            pedidas: totalSumaPedido,
            despachadas: totalSumaDespachado,
            pendientes: (totalSumaPedido - totalSumaDespachado) > 0 ? (totalSumaPedido - totalSumaDespachado) : 0
          },
          sku_total: cuerpo,
        };
      })
    );

    res.json({ total, pedidos });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error al obtener los pedidos", error: error.message });
  }
};

export const getMoneda = async (req, res) => {
  try {
    const query = `
      SELECT co_mone, mone_des, cambio
      FROM moneda
      WHERE co_mone = @co_mone
    `;
    const request = new sql.Request();
    request.input("co_mone", sql.VarChar, "US$");
    const result = await request.query(query);

    // Limpiar espacios en blanco de los campos string
    const monedas = result.recordset.map((row) => ({
      co_mone: row.co_mone?.trim(),
      mone_des: row.mone_des?.trim(),
      cambio: row.cambio,
    }));

    res.json(monedas);
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error al obtener la moneda", error: error.message });
  }
};

export const getFacturaDetalle = async (req, res) => {
  try {
    const { co_cli, page = 1, perPage = 10 } = req.query;
    if (!co_cli) {
      return res
        .status(400)
        .json({ message: "Debe enviar el campo co_cli como parámetro" });
    }

    // Obtener total de facturas
    const countQuery = `
      SELECT COUNT(*) AS total
      FROM factura
      WHERE RTRIM(co_cli) = @co_cli
    `;
    const countRequest = new sql.Request();
    countRequest.input("co_cli", sql.VarChar, co_cli.trim());
    const countResult = await countRequest.query(countQuery);
    const total = countResult.recordset[0]?.total ?? 0;

    // Paginación
    const offset = (parseInt(page) - 1) * parseInt(perPage);

    // Consulta principal: facturas paginadas del cliente
    const facturaQuery = `
      SELECT
        f.status,
        f.fec_emis,
        f.fec_venc,
        f.fact_num,
        f.tot_neto,
        f.tot_bruto,
        f.glob_desc,
        f.saldo,
        f.anulada,
        f.comentario,
        c.cli_des AS razon_social,
        c.direc1 AS domicilio_fiscal,
        c.telefonos AS telefono_cliente,
        c.rif AS rif_cliente
      FROM factura f
      INNER JOIN clientes c ON RTRIM(f.co_cli) = RTRIM(c.co_cli)
      WHERE RTRIM(f.co_cli) = @co_cli
      ORDER BY f.fec_emis DESC
      OFFSET @offset ROWS FETCH NEXT @perPage ROWS ONLY
    `;
    const facturaRequest = new sql.Request();
    facturaRequest.input("co_cli", sql.VarChar, co_cli.trim());
    facturaRequest.input("offset", sql.Int, offset);
    facturaRequest.input("perPage", sql.Int, parseInt(perPage));
    const facturaResult = await facturaRequest.query(facturaQuery);

    if (!facturaResult.recordset.length) {
      return res
        .status(404)
        .json({ message: "No se encontraron facturas para este cliente" });
    }

    // Para cada factura, obtener sus ítems
    const facturas = await Promise.all(
      facturaResult.recordset.map(async (factura) => {
        const itemsQuery = `
          SELECT
            rf.reng_num,
            rf.des_art AS descripcion,
            rf.total_art AS cantidad,
            rf.prec_vta AS precio_unitario,
            rf.reng_neto AS total_renglon_neto,
            rf.stotal_art AS total_renglon_bruto
          FROM reng_fac rf
          WHERE RTRIM(rf.fact_num) = @fact_num
          ORDER BY rf.reng_num ASC
        `;
        const itemsRequest = new sql.Request();
        itemsRequest.input(
          "fact_num",
          sql.VarChar,
          factura.fact_num != null ? String(factura.fact_num).trim() : undefined
        );
        const itemsResult = await itemsRequest.query(itemsQuery);

        const items = itemsResult.recordset.map((item) => ({
          reng_num: item.reng_num,
          descripcion: item.descripcion?.trim(),
          cantidad: item.cantidad,
          precio_unitario: item.precio_unitario,
          total_renglon_neto: item.total_renglon_neto,
          total_renglon_bruto: item.total_renglon_bruto,
        }));

        return {
          status: factura.status?.trim(),
          fec_emis: factura.fec_emis,
          fec_venc: factura.fec_venc,
          fact_num:
            factura.fact_num != null
              ? String(factura.fact_num).trim()
              : undefined,
          tot_neto: factura.tot_neto,
          tot_bruto: factura.tot_bruto,
          glob_desc: factura.glob_desc,
          saldo: factura.saldo,
          anulada: factura.anulada,
          comentario: factura.comentario?.trim(),
          razon_social: factura.razon_social?.trim(),
          domicilio_fiscal: factura.domicilio_fiscal?.trim(),
          telefono_cliente: factura.telefono_cliente?.trim(),
          rif_cliente: factura.rif_cliente?.trim(),
          items,
        };
      })
    );

    res.json({ total, facturas });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error al obtener las facturas", error: error.message });
  }
};

export const getFacturaResumen = async (req, res) => {
  try {
    const { co_cli, page = 1, perPage = 10 } = req.query;

    if (!co_cli) {
      return res.status(400).json({ message: "Debe enviar el campo co_cli" });
    }

    const offset = (parseInt(page) - 1) * parseInt(perPage);

    // 1. Total de registros
    const countQuery = `SELECT COUNT(*) AS total FROM factura WHERE RTRIM(co_cli) = @co_cli`;
    const countRequest = new sql.Request();
    countRequest.input("co_cli", sql.VarChar, co_cli.trim());
    const countResult = await countRequest.query(countQuery);
    const total = countResult.recordset[0]?.total ?? 0;

    // 2. Cabeceras
    const facturaQuery = `
      SELECT
        f.status, f.fec_emis, f.fec_venc, f.fact_num,
        f.tot_neto, f.tot_bruto, f.glob_desc,
        (f.tot_bruto - f.tot_neto) AS monto_iva,
        f.saldo, f.anulada, c.cli_des AS razon_social, c.rif AS rif_cliente
      FROM factura f
      INNER JOIN clientes c ON RTRIM(f.co_cli) = RTRIM(c.co_cli)
      WHERE RTRIM(f.co_cli) = @co_cli
      ORDER BY f.fec_emis DESC
      OFFSET @offset ROWS FETCH NEXT @perPage ROWS ONLY
    `;

    const facturaRequest = new sql.Request();
    facturaRequest.input("co_cli", sql.VarChar, co_cli.trim());
    facturaRequest.input("offset", sql.Int, offset);
    facturaRequest.input("perPage", sql.Int, parseInt(perPage));
    const facturaResult = await facturaRequest.query(facturaQuery);

    // 3. Procesamiento con Ecuaciones Matemáticas Limpias
    const facturas = await Promise.all(
      facturaResult.recordset.map(async (f) => {
        // Ecuación Encabezado: % Descuento Global
        const baseGlobal = (f.tot_neto || 0) + (f.glob_desc || 0);
        const porcGlobal = baseGlobal > 0 ? ((f.glob_desc / baseGlobal) * 100).toFixed(2) : 0;

        const itemsQuery = `
          SELECT
            rf.reng_num, rf.co_art, a.art_des, a.ref AS codigo_barra, 
            rf.total_art AS cantidad, 
            rf.prec_vta AS precio_unitario,
            rf.reng_neto AS total_renglon_neto
          FROM reng_fac rf
          INNER JOIN art a ON RTRIM(rf.co_art) = RTRIM(a.co_art)
          WHERE RTRIM(rf.fact_num) = @fact_num
          ORDER BY rf.reng_num ASC
        `;
        const itemsRequest = new sql.Request();
        itemsRequest.input("fact_num", sql.VarChar, String(f.fact_num).trim());
        const itemsResult = await itemsRequest.query(itemsQuery);

        return {
          fact_num: f.fact_num !== null ? String(f.fact_num).trim() : "",
          fec_emis: f.fec_emis ? f.fec_emis.toISOString().split('T')[0] : null,
          fec_venc: f.fec_venc ? f.fec_venc.toISOString().split('T')[0] : null,
          porcentaje_descuento_lineal: parseFloat(porcGlobal),
          monto_descuento_global: f.glob_desc || 0,
          monto_neto: f.tot_neto,
          impuestos: f.monto_iva || 0,
          total_bruto: f.tot_bruto,
          cliente: f.razon_social !== null ? String(f.razon_social).trim() : "",
          articulos: itemsResult.recordset.map((item) => {
            // --- ECUACIÓN PARA EL RENGLÓN CON LIMPIEZA DE DECIMALES ---
            const subtotalTeorico = item.cantidad * item.precio_unitario;
            
            // Calculamos la diferencia y corregimos el error de coma flotante (el e-11)
            let montoDescRaw = subtotalTeorico - item.total_renglon_neto;
            // Si la diferencia es menor a 0.0001, lo tratamos como cero
            const montoDescuentoRenglon = Math.abs(montoDescRaw) < 0.0001 ? 0 : parseFloat(montoDescRaw.toFixed(5));

            const porcDescRenglon = subtotalTeorico > 0 
              ? ((montoDescuentoRenglon / subtotalTeorico) * 100).toFixed(2) 
              : 0;

            return {
              renglon: item.reng_num,
              codigo_articulo: item.co_art !== null ? String(item.co_art).trim() : "",
              codigo_barra: item.codigo_barra !== null ? String(item.codigo_barra).trim() : "",
              descripcion: item.art_des !== null ? String(item.art_des).trim() : "",
              cantidad: item.cantidad,
              precio_original: item.precio_unitario,
              porc_descuento_articulo: parseFloat(porcDescRenglon),
              monto_descuento_articulo: montoDescuentoRenglon,
              total_neto: item.total_renglon_neto
            };
          })
        };
      })
    );

    res.json({ total, facturas });

  } catch (error) {
    res.status(500).json({ message: "Error en servidor", error: error.message });
  }
};

export const getTodosClientes = async (req, res) => {
  try {
    const query = `
      SELECT co_cli, cli_des, tipo
      FROM clientes
      ORDER BY co_cli
    `;
    const request = new sql.Request();
    const result = await request.query(query);

    // Limpiar espacios en blanco de todos los campos string
    const clientes = result.recordset.map((row) => {
      const cleaned = {};
      for (const key in row) {
        cleaned[key] =
          typeof row[key] === "string" ? row[key].trim() : row[key];
      }
      return cleaned;
    });

    res.json(clientes);
  } catch (error) {
    res
      .status(500)
      .json({
        message: "Error al obtener todos los clientes",
        error: error.message,
      });
  }
};

export const getTopArticulosPorCliente = async (req, res) => {
    try {
        // Obtenemos los parámetros co_cli y limit de la query string.
        const { co_cli, limit = 10 } = req.query; 

        if (!co_cli) {
            return res
                .status(400)
                .json({ message: "Debe enviar el campo co_cli como parámetro" });
        }

        // --- Consulta SQL ACTUALIZADA para obtener el TOP N del mes actual ---
        const query = `
            SELECT TOP (@limit)
                TRIM(r.co_art) AS co_art,
                CAST(a.art_des AS VARCHAR(255)) AS des_art,
                SUM(r.total_art) AS cantidad,
                SUM(r.reng_neto) AS total  -- Incluido para mantener la estructura
            FROM factura p
            JOIN reng_fac r ON p.fact_num = r.fact_num
            JOIN art a ON r.co_art = a.co_art
            WHERE RTRIM(p.co_cli) = @co_cli
              -- FILTRO POR MES Y AÑO ACTUAL (SQL Server)
              AND p.fec_emis >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
              AND p.fec_emis < DATEADD(month, DATEDIFF(month, 0, GETDATE()) + 1, 0)
            GROUP BY TRIM(r.co_art), CAST(a.art_des AS VARCHAR(255))
            ORDER BY cantidad DESC
        `;

        const request = new sql.Request();
        request.input("co_cli", sql.VarChar, co_cli.trim());
        request.input("limit", sql.Int, parseInt(limit));
        
        const result = await request.query(query);

        const articulos = result.recordset.map((row) => ({
            co_art: row.co_art?.trim(),
            // des_art ahora proviene de 'a.art_des'
            des_art: row.des_art?.trim(), 
            cantidad: row.cantidad,
            total: row.total, // Suma de reng_neto de la factura
        }));

        res.json(articulos);
    } catch (error) {
        // En caso de error, respondemos con un código de estado 500
        res
            .status(500)
            .json({
                message: "Error al obtener los artículos",
                error: error.message,
            });
    }
};

export const getTipoClientePrecio = async (req, res) => {
  try {
    const { co_cli } = req.query;
    if (!co_cli) {
      return res
        .status(400)
        .json({ message: "Debe enviar el campo co_cli como parámetro" });
    }

    // Buscar el tipo del cliente
    const clienteQuery = `
      SELECT LTRIM(RTRIM(tipo)) AS tipo
      FROM clientes
      WHERE LTRIM(RTRIM(co_cli)) = @co_cli
    `;
    const clienteRequest = new sql.Request();
    clienteRequest.input("co_cli", sql.VarChar, co_cli.trim());
    const clienteResult = await clienteRequest.query(clienteQuery);

    if (!clienteResult.recordset.length) {
      return res.status(404).json({ message: "No se encontró el cliente" });
    }

    const tipo = clienteResult.recordset[0].tipo?.trim();
    if (!tipo) {
      return res
        .status(404)
        .json({ message: "El cliente no tiene tipo definido" });
    }

    // Buscar el tipo en tipo_cli
    const tipoCliQuery = `
      SELECT 
        LTRIM(RTRIM(tip_cli)) AS tip_cli, 
        LTRIM(RTRIM(precio_a)) AS precio_a 
      FROM tipo_cli 
      WHERE LTRIM(RTRIM(tip_cli)) = @tip_cli
    `;
    const tipoCliRequest = new sql.Request();
    tipoCliRequest.input("tip_cli", sql.VarChar, tipo);
    const tipoCliResult = await tipoCliRequest.query(tipoCliQuery);

    if (!tipoCliResult.recordset.length) {
      return res
        .status(404)
        .json({ message: "No se encontró el tipo de cliente en tipo_cli" });
    }

    res.json(
      tipoCliResult.recordset.map((row) => ({
        tip_cli: row.tip_cli,
        precio_a: row.precio_a,
      }))
    );
  } catch (error) {
    res
      .status(500)
      .json({
        message: "Error al obtener el tipo de cliente",
        error: error.message,
      });
  }
};

export const getTopArticulosVendidosMes = async (req, res) => {
  try {
    const query = `
      SELECT TOP 20
        LTRIM(RTRIM(art.co_art)) AS co_art,
        LTRIM(RTRIM(art.co_cat)) AS co_cat,
        LTRIM(RTRIM(art.co_lin)) AS co_lin,
        art.art_des,
        art.imagen1 AS pict,
        art.tipo_imp,
        ISNULL(art.stock_act, 0) AS stock_act,
        ISNULL(art.stock_com, 0) AS stock_com,
        ISNULL(art.prec_agr1, 0.00) AS prec_agr1,
        ISNULL(art.prec_agr2, 0.00) AS prec_agr2,
        ISNULL(art.prec_agr3, 0.00) AS prec_agr3,
        ISNULL(art.prec_agr4, 0.00) AS prec_agr4,
        ISNULL(art.prec_agr5, 0.00) AS prec_agr5,
        SUM(rf.total_art) AS total_vendido
      FROM reng_fac rf
      INNER JOIN art ON rf.co_art = art.co_art
      WHERE MONTH(rf.fec_lote) = MONTH(GETDATE()) 
        AND YEAR(rf.fec_lote) = YEAR(GETDATE())
        AND art.anulado = 0
        AND ISNULL(art.stock_act, 0) > 0
      GROUP BY
        LTRIM(RTRIM(art.co_art)),
        LTRIM(RTRIM(art.co_cat)),
        LTRIM(RTRIM(art.co_lin)),
        art.art_des,
        art.tipo_imp,
        art.imagen1,
        art.stock_act,
        art.stock_com,
        art.prec_agr1,
        art.prec_agr2,
        art.prec_agr3,
        art.prec_agr4,
        art.prec_agr5
      ORDER BY total_vendido DESC
    `;
    const request = new sql.Request();
    const result = await request.query(query);

    const articulos = result.recordset.map((row) => ({
      co_art: row.co_art?.trim(),
      co_cat: row.co_cat?.trim(),
      co_lin: row.co_lin?.trim(),
      art_des: row.art_des?.trim(),
      pict: row.pict,
      tipo_imp: row.tipo_imp?.trim(),
      stock_act: row.stock_act,
      stock_com: row.stock_com,
      prec_agr1: row.prec_agr1,
      prec_agr2: row.prec_agr2,
      prec_agr3: row.prec_agr3,
      prec_agr4: row.prec_agr4,
      prec_agr5: row.prec_agr5,
      total_vendido: row.total_vendido,
    }));

    res.json(articulos);
  } catch (error) {
    res.status(500).json({
      message: "Error al obtener los artículos más vendidos del mes",
      error: error.message,
    });
  }
};

export const getStockArticuloss = async (req, res) => {
  try {
    const { co_arts } = req.body;
    if (!Array.isArray(co_arts) || co_arts.length === 0) {
      return res
        .status(400)
        .json({ message: "Debe enviar un array no vacío en co_arts" });
    }

    const co_arts_trimmed = co_arts.map((art) => art.trim());
    const placeholders = co_arts_trimmed
      .map((_, idx) => `@co_art${idx}`)
      .join(", ");

    // 🚩 Agregamos JOIN con tabla 'art' y agrupamos por los metadatos
    const query = `
      SELECT 
          LTRIM(RTRIM(sa.co_art)) AS co_art,
          LTRIM(RTRIM(a.co_prov)) AS co_prov,
          LTRIM(RTRIM(a.co_lin)) AS co_lin,
          LTRIM(RTRIM(a.co_cat)) AS co_cat,
          CASE 
              WHEN LTRIM(RTRIM(sa.co_alma)) IN ('01','02') THEN '01'
              WHEN LTRIM(RTRIM(sa.co_alma)) IN ('04','05') THEN '04'
          END AS co_alma,
          SUM(sa.stock_act) AS stock_act,
          SUM(sa.stock_com) AS stock_com
      FROM st_almac sa
      INNER JOIN art a ON LTRIM(RTRIM(sa.co_art)) = LTRIM(RTRIM(a.co_art))
      WHERE LTRIM(RTRIM(sa.co_art)) IN (${placeholders})
        AND LTRIM(RTRIM(sa.co_alma)) IN ('01','02','04','05')
      GROUP BY 
          LTRIM(RTRIM(sa.co_art)),
          LTRIM(RTRIM(a.co_prov)),
          LTRIM(RTRIM(a.co_lin)),
          LTRIM(RTRIM(a.co_cat)),
          CASE 
              WHEN LTRIM(RTRIM(sa.co_alma)) IN ('01','02') THEN '01'
              WHEN LTRIM(RTRIM(sa.co_alma)) IN ('04','05') THEN '04'
          END
      HAVING SUM(sa.stock_act) > 0 
        OR SUM(sa.stock_com) > 0;
    `;

    const request = new sql.Request();
    co_arts_trimmed.forEach((art, idx) => {
      request.input(`co_art${idx}`, sql.VarChar, art);
    });

    const result = await request.query(query);

    const cleanResult = result.recordset.map((row) => ({
      co_art: row.co_art?.trim(),
      co_prov: row.co_prov?.trim(), // 🚩 Ahora viaja el proveedor
      co_lin: row.co_lin?.trim(),
      co_cat: row.co_cat?.trim(),
      co_alma: row.co_alma?.trim(),
      stock_act: row.stock_act,
      stock_com: row.stock_com,
    }));

    res.json(cleanResult);
  } catch (error) {
    res.status(500).json({
      message: "Error al obtener el stock con metadatos",
      error: error.message,
    });
  }
};

export const getArticulosConStock = async (req, res) => {
  try {
    const query = `
      SELECT *
      FROM art
    `;
    const request = new sql.Request();
    const result = await request.query(query);

    // Limpiar espacios en blanco de todos los campos string
    const articulos = result.recordset.map((row) => {
      const cleaned = {};
      for (const key in row) {
        cleaned[key] =
          typeof row[key] === "string" ? row[key].trim() : row[key];
      }
      return cleaned;
    });

    res.json(articulos);
  } catch (error) {
    res
      .status(500)
      .json({
        message: "Error al obtener los artículos con stock",
        error: error.message,
      });
  }
};

// GET /api/factura/:fact_num
export const getDatosFactura = async (req, res) => { // <-- Agregado
  const { fact_num } = req.body;
  if (!/^\d+$/.test(String(fact_num))) {
    return res.status(400).json({ message: "Número de factura inválido" });
  }

  const query = `
    SELECT
      f.fact_num,
      f.co_cli,
      f.forma_pag,
      f.co_tran,
      f.fec_emis,
      f.fec_venc,
      c.cli_des,
      v.co_ven,
      v.ven_des,
      z.zon_des,
      s.seg_des,
      f.co_sucu AS co_alma,
      rf.co_art,
      a.art_des,
      rf.total_art,
      rf.num_doc
    FROM factura f
    JOIN clientes c ON f.co_cli = c.co_cli
    LEFT JOIN zona z ON z.co_zon = c.co_zon
    LEFT JOIN segmento s ON s.co_seg = c.co_seg
    LEFT JOIN vendedor v ON v.co_ven = c.co_ven
    JOIN reng_fac rf ON rf.fact_num = f.fact_num
    JOIN art a ON rf.co_art = a.co_art
    WHERE f.fact_num = @fact_num
  `;

  try {
    const request = new sql.Request();
    request.input("fact_num", sql.Int, fact_num);
    const result = await request.query(query);

    if (!result.recordset.length) {
      return res.status(404).json({ message: "Factura no encontrada" });
    }

    let cabecera = null;
    const productos = [];

    result.recordset.forEach((row) => {
      if (!cabecera) {
        cabecera = {
          fact_num: row.fact_num,
          co_cli: row.co_cli?.trim(),
          forma_pag: row.forma_pag?.trim(),
          co_tran: row.co_tran?.trim() ?? "NO_ENCONTRADO",
          cli_des: row.cli_des?.trim(),
          co_ven: row.co_ven?.trim() ?? "NO_ENCONTRADO",
          ven_des: row.ven_des?.trim(),
          zon_des: row.zon_des?.trim(),
          seg_des: row.seg_des?.trim(),
          co_alma: row.co_alma?.trim(),
          num_doc: row.num_doc?.toString().trim() ?? "NO_ENCONTRADO",
        };
      }
      productos.push({
        co_art: row.co_art?.trim(),
        art_des: row.art_des?.trim(),
        total_art: parseFloat(row.total_art),
      });
    });

    res.json({ cabecera, productos });
  } catch (error) {
    res.status(500).json({ message: "Error al obtener la factura", error: error.message });
  }
};

// POST /api/pagina/facturas/buscar
export const buscarFacturasPorCliente = async (req, res) => { // <-- Agregado
  const { co_cli, cli_des } = req.body;

  const query = `
    SELECT TOP 20
      f.fact_num as numero_factura,
      f.co_cli as codigo_cliente,
      c.cli_des as nombre_cliente,
      CONVERT(varchar, f.fec_emis, 103) as fecha_emision,
      v.ven_des as vendedor
    FROM factura f
    JOIN clientes c ON f.co_cli = c.co_cli
    LEFT JOIN vendedor v ON c.co_ven = v.co_ven
    WHERE (c.co_cli LIKE @co_cli OR @co_cli IS NULL)
      AND (c.cli_des LIKE @cli_des OR @cli_des IS NULL)
      AND f.fec_emis >= DATEADD(month, -2, GETDATE())
    ORDER BY f.fec_emis DESC
  `;

  try {
    const request = new sql.Request();
    request.input("co_cli", sql.VarChar, co_cli ? `%${co_cli}%` : null);
    request.input("cli_des", sql.VarChar, cli_des ? `%${cli_des}%` : null);

    const result = await request.query(query);

    // Puedes agregar aquí lógica para sucursal, tipo_correlativo, validacion si tienes esas funciones en JS
    res.json(result.recordset);
  } catch (error) {
    res.status(500).json({ message: "Error al buscar facturas", error: error.message });
  }
};

// POST /api/pagina/facturas/notas-cobros

export const getFacturasConNotasYCobros = async (req, res) => {
  try {
    const { co_cli } = req.body || {};
    const request = new sql.Request();
    
    // 1. Filtro por cliente
    let whereCli = "";
    if (co_cli) {
      whereCli = "AND f.co_cli = @co_cli";
      request.input("co_cli", sql.VarChar, String(co_cli).trim());
    }

    // 2. Definición de la Query con filtro de 6 meses
    const query = `
      SELECT
        f.fact_num,
        f.co_cli,
        f.tot_neto AS monto_f_bs,
        f.tasa AS tasa_f,
        rc.doc_num AS nota_credito_num,
        ISNULL(rc.neto, 0) AS monto_n_bs,
        c.cob_num,
        c.campo2 AS BsS,      -- Monto total del cobro
        c.campo3 AS tasa_c,   -- Tasa grabada en el cobro
        c.fec_cob,
        t_hist.tasa_v AS tasa_v_hist
      FROM CRISTM25.dbo.factura f
      LEFT JOIN CRISTM25.dbo.reng_cob rc ON rc.dppago_tmp = f.fact_num AND rc.tp_doc_cob = 'N/CR'
      LEFT JOIN CRISTM25.dbo.cobros c ON c.cob_num = rc.cob_num
      OUTER APPLY (
        SELECT TOP 1 tasa_v 
        FROM CRISTM25.dbo.tasas 
        WHERE co_mone = 'US$' 
          AND CAST(fecha AS DATE) <= CAST(c.fec_cob AS DATE) 
        ORDER BY fecha DESC
      ) t_hist
      WHERE c.anulado = 0 
        -- FILTRO DE ÚLTIMOS 6 MESES
        AND c.fec_cob >= DATEADD(month, -6, GETDATE())
        ${whereCli}
      ORDER BY c.fec_cob DESC
    `;

    const result = await request.query(query);

    // 3. Mapeo y limpieza de datos para evitar errores en PHP/Laravel
    const rows = result.recordset.map((r) => {
      const tasaF = parseFloat(r.tasa_f) || 1;
      const montoFBs = parseFloat(r.monto_f_bs) || 0;
      const montoFacturaUSD = parseFloat((montoFBs / tasaF).toFixed(2));
      
      return {
        fact_num: r.fact_num,
        co_cli: r.co_cli ? r.co_cli.trim() : "",
        monto_factura: montoFacturaUSD,
        monto_nota_credito: parseFloat(r.monto_n_bs) || 0,
        cob_num: r.cob_num,
        // Forzamos numéricos para evitar el error de "string given" en number_format de PHP
        BsS: parseFloat(r.BsS) || 0,
        USD: montoFacturaUSD, 
        COP: 0, 
        // Datos para la lógica de tasas en el modal
        tasa: parseFloat(r.tasa_c) || 0,
        tasa_respaldo: parseFloat(r.tasa_v_hist) || 0,
        fec_cob: r.fec_cob
      };
    });

    res.json({ count: rows.length, rows });
  } catch (err) {
    console.error("Error en getFacturasConNotasYCobros:", err);
    res.status(500).json({ error: err.message });
  }
};

export const getTopClientesPorProveedores = async (req, res) => {
  try {
    let {
      co_prov,
      fecha_desde ,
      fecha_hasta ,
      top = 10,
    } = req.body || {};

    if (!co_prov || !Array.isArray(co_prov) || co_prov.length === 0) {
      return res.status(400).json({
        message: "Debe enviar co_prov como array de strings no vacío.",
      });
    }

    const request = new sql.Request();

    request.input("fecha_desde", sql.Date, fecha_desde);
    request.input("fecha_hasta", sql.Date, fecha_hasta);
    request.input("top", sql.Int, parseInt(top, 10));

    // Generamos tabla de proveedores como CTE
    const provTable = co_prov
      .map((p, i) => `SELECT '${p.trim().replace(/\.$/, "")}' AS prov`)
      .join(" UNION ALL ");

    const query = `
      WITH Proveedores AS (
        ${provTable}
      ),
      Datos AS (
        SELECT 
          factura.co_cli,
          rf.reng_neto / NULLIF(factura.tasa, 0) AS monto_usd,
          CASE 
            WHEN RIGHT(art.co_prov, 1) = '.' THEN LEFT(art.co_prov, LEN(art.co_prov) - 1)
            ELSE art.co_prov
          END AS prov_base
        FROM dbo.reng_fac rf
        INNER JOIN dbo.art art ON rf.co_art = art.co_art
        INNER JOIN dbo.factura factura ON factura.fact_num = rf.fact_num
        WHERE 
          CONVERT(DATE, factura.fec_emis) BETWEEN @fecha_desde AND @fecha_hasta
          AND factura.anulada = 0
          AND (
            art.co_prov IN (SELECT prov FROM Proveedores)
            OR art.co_prov IN (SELECT prov + '.' FROM Proveedores)
          )
      ),
      Agrupado AS (
        SELECT 
          co_cli,
          SUM(monto_usd) AS total_neto_usd
        FROM Datos
        GROUP BY co_cli
      ),
      TopClientes AS (
        SELECT 
          a.co_cli,
          c.cli_des,
          a.total_neto_usd,
          ROW_NUMBER() OVER (ORDER BY a.total_neto_usd DESC) AS rn
        FROM Agrupado a
        INNER JOIN dbo.clientes c ON a.co_cli = c.co_cli
      )
      SELECT 
        '${co_prov.join(",")}' AS co_prov,
        (SELECT STUFF((
          SELECT ', ' + prov_des 
          FROM dbo.prov 
          WHERE co_prov IN (
            SELECT prov FROM Proveedores
            UNION 
            SELECT prov + '.' FROM Proveedores
          )
          FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
        ) AS prov_des,
        tc.co_cli,
        tc.cli_des,
        ROUND(tc.total_neto_usd, 2) AS total_neto_usd
      FROM TopClientes tc
      WHERE tc.rn <= @top
      ORDER BY tc.total_neto_usd DESC;
    `;

    const result = await request.query(query);

    const rows = (result.recordset || []).map((r) => ({
      co_prov: typeof r.co_prov === "string" ? r.co_prov.trim() : r.co_prov,
      prov_des: typeof r.prov_des === "string" ? r.prov_des.trim() : r.prov_des,
      co_cli: typeof r.co_cli === "string" ? r.co_cli.trim() : r.co_cli,
      cli_des: typeof r.cli_des === "string" ? r.cli_des.trim() : r.cli_des,
      total_neto_usd:
        r.total_neto_usd !== null ? Number(r.total_neto_usd) : null,
    }));

    res.json({ count: rows.length, rows });
  } catch (error) {
    res.status(500).json({
      message: "Error al obtener top clientes por proveedores",
      error: error.message,
    });
  }
};

export const getTodosVendedores = async (req, res) => {
  try {
    const query = `
      SELECT 
        co_ven, 
        ven_des
      FROM vendedor
      WHERE RTRIM(tipo) = 'A'
      ORDER BY co_ven
    `;

    const request = new sql.Request();
    const result = await request.query(query);

    // Limpiar espacios en blanco de todos los campos string
    const vendedores = result.recordset.map((row) => {
      const cleaned = {};
      for (const key in row) {
        cleaned[key] =
          typeof row[key] === "string" ? row[key].trim() : row[key];
      }
      return cleaned;
    });

    res.json(vendedores);
  } catch (error) {
    res.status(500).json({
      message: "Error al obtener todos los vendedores",
      error: error.message,
    });
  }
};

export const getPsicotropicosCompradosRecientes = async (req, res) => {
  try {
    const query = `
      SELECT 
        TRIM(a.co_art) AS co_art_limpio,
        a.art_des,
        a.co_lin,
        a.prec_agr1,
        a.prec_agr2,
        a.prec_agr3,
        a.prec_agr4,
        a.co_cat,
        a.tipo_imp,
        r.fact_num,
        CAST(c.fec_emis AS date) AS fecha_emision,
        SUM(CASE WHEN s.co_alma = '01' THEN s.stock_act ELSE 0 END) AS stock_alma_01,
        SUM(CASE WHEN s.co_alma = '04' THEN s.stock_act ELSE 0 END) AS stock_alma_04
      FROM art a
      INNER JOIN reng_com r 
          ON TRIM(a.co_art) = TRIM(r.co_art)
      INNER JOIN compras c
          ON r.fact_num = c.fact_num
      INNER JOIN st_almac s
          ON TRIM(a.co_art) = TRIM(s.co_art)
      WHERE a.co_lin = '06'
        AND CAST(c.fec_emis AS date) >= CAST('2025-12-01' AS date)
        AND s.co_alma IN ('01','04')
      GROUP BY 
        TRIM(a.co_art),
        a.art_des,
        a.co_lin,
        a.prec_agr1,
        a.prec_agr2,
        a.prec_agr3,
        a.prec_agr4,
        a.co_cat,
        a.tipo_imp,
        r.fact_num,
        CAST(c.fec_emis AS date)
      HAVING 
        SUM(CASE WHEN s.co_alma = '01' THEN s.stock_act ELSE 0 END) > 0
        OR SUM(CASE WHEN s.co_alma = '04' THEN s.stock_act ELSE 0 END) > 0;
    `;

    const request = new sql.Request();
    const result = await request.query(query);

    const articulos = result.recordset.map((row) => {
      const cleaned = {};
      for (const key in row) {
        cleaned[key] =
          typeof row[key] === "string" ? row[key].trim() : row[key];
      }
      return cleaned;
    });

    res.json(articulos);
  } catch (error) {
    res.status(500).json({
      message: "Error al obtener los psicotrópicos con stock en almacenes 01 y 04",
      error: error.message,
    });
  }
};

export const getDescuentos = async (req, res) => {
  try {
    const query = `SELECT * FROM descuen`;
    const request = new sql.Request();
    const result = await request.query(query);

    const articulos = result.recordset.map((row) => {
      const cleaned = {};
      for (const key in row) {
        cleaned[key] =
          typeof row[key] === "string" ? row[key].trim() : row[key];
      }
      return cleaned;
    });

    res.json(articulos);
  } catch (error) {
    res.status(500).json({
      message: "Error al obtener los descuentos",
      error: error.message,
    });
  }
};

export const getArticulosPreciosPorCliente = async (req, res) => {
    try {
        const { co_cli } = req.query; 

        if (!co_cli) {
            return res.status(400).json({ message: "Debe enviar el campo co_cli como parámetro" });
        }

        const request = new sql.Request();
        request.input("co_cli", sql.VarChar, co_cli.trim());

        // Eliminamos OFFSET y FETCH para traer todo
        const query = `
            SELECT 
                TRIM(a.co_art) AS co_art,
                CAST(a.art_des AS VARCHAR(255)) AS des_art,
                a.uni_venta,
                a.ref AS codigo_barra,
                TRIM(a.co_lin) AS co_lin,
                TRIM(a.co_cat) AS co_cat,
                TRIM(c.tipo) AS tipo_cli_cliente,
                CASE 
                    WHEN TRIM(tc.precio_a) = 'PRECIO 1' THEN a.prec_vta1
                    WHEN TRIM(tc.precio_a) = 'PRECIO 2' THEN a.prec_vta2
                    WHEN TRIM(tc.precio_a) = 'PRECIO 3' THEN a.prec_vta3
                    WHEN TRIM(tc.precio_a) = 'PRECIO 4' THEN a.prec_vta4
                    WHEN TRIM(tc.precio_a) = 'PRECIO 5' THEN a.prec_vta5
                    ELSE a.prec_vta1 
                END AS precio_cliente
            FROM art a
            INNER JOIN clientes c ON RTRIM(c.co_cli) = @co_cli
            INNER JOIN tipo_cli tc ON c.tipo = tc.tip_cli
            WHERE a.anulado = 0
              -- Mantenemos el filtro de stock para no traer basura sin existencia
              AND EXISTS (
                  SELECT 1 FROM st_almac s 
                  WHERE s.co_art = a.co_art 
                  AND s.co_alma IN ('01', '02', '04', '05')
                  AND (s.stock_act - s.stock_com) > 0
              )
            ORDER BY a.art_des ASC
        `;

        const result = await request.query(query);

        const articulosFinales = await Promise.all(
            result.recordset.map(async (row) => {
                
                // --- BUSCAR DESCUENTO ---
                const descReq = new sql.Request();
                descReq.input("tipo_cli", sql.VarChar, row.tipo_cli_cliente);
                descReq.input("co_art", sql.VarChar, row.co_art);
                descReq.input("co_lin", sql.VarChar, row.co_lin);
                descReq.input("co_cat", sql.VarChar, row.co_cat);
                
                const descResult = await descReq.query(`
                    SELECT TOP 1 porc1 FROM descuen
                    WHERE tipo_cli = @tipo_cli
                      AND ((tipo_desc = 'A' AND RTRIM(co_desc) = @co_art) OR
                           (tipo_desc = 'L' AND RTRIM(co_desc) = @co_lin) OR
                           (tipo_desc = 'C' AND RTRIM(co_desc) = @co_cat))
                    ORDER BY CASE WHEN tipo_desc = 'A' THEN 1 WHEN tipo_desc = 'L' THEN 2 ELSE 3 END ASC
                `);
                
                const porcDesc = descResult.recordset[0]?.porc1 ?? 0;

                // --- BUSCAR STOCK DETALLADO (Sincronizado con Almacenes 01, 02, 04, 05) ---
                const stockReq = new sql.Request();
                stockReq.input("co_art", sql.VarChar, row.co_art);
                const stockRes = await stockReq.query(`
                    SELECT RTRIM(co_alma) AS co_alma, (stock_act - stock_com) as disponible
                    FROM st_almac WHERE co_art = @co_art AND co_alma IN ('01', '02', '04', '05')
                `);

                let tachiraDisp = 0;
                let laraDisp = 0;

                stockRes.recordset.forEach(s => {
                    if (['01', '02'].includes(s.co_alma)) tachiraDisp += s.disponible;
                    if (['04', '05'].includes(s.co_alma)) laraDisp += s.disponible;
                });

                let regiones = [];
                if (tachiraDisp > 0) regiones.push({ co_alma: "01", nombre: "Táchira", disponible: parseFloat(tachiraDisp.toFixed(2)) });
                if (laraDisp > 0) regiones.push({ co_alma: "04", nombre: "Lara", disponible: parseFloat(laraDisp.toFixed(2)) });

                return {
                    co_art: row.co_art,
                    des_art: row.des_art, 
                    codigo_barra: row.codigo_barra?.trim() || "",
                    unidad: row.uni_venta?.trim(),
                    precio_base: row.precio_cliente,
                    porc_descuento: porcDesc,
                    precio_con_descuento: parseFloat((row.precio_cliente * (1 - (porcDesc / 100))).toFixed(2)),
                    stock_por_region: regiones
                };
            })
        );

        res.json({
            co_cli: co_cli.trim(),
            total_articulos: articulosFinales.length,
            articulos: articulosFinales
        });

    } catch (error) {
        res.status(500).json({ message: "Error al obtener catálogo completo", error: error.message });
    }
};

export const getFacturaMarcel = async (req, res) => {
  try {
    const { co_cli, fec_desde, fec_hasta } = req.query;

    if (!co_cli) {
      return res.status(400).json({ message: "Debe enviar el campo co_cli" });
    }

    const request = new sql.Request();
    request.input("co_cli", sql.VarChar, co_cli.trim());

    let fechaFiltro = "AND f.fec_emis >= DATEADD(month, -6, GETDATE())";
    if (fec_desde && fec_hasta) {
      fechaFiltro = "AND f.fec_emis BETWEEN @fec_desde AND @fec_hasta";
      request.input("fec_desde", sql.Date, fec_desde);
      request.input("fec_hasta", sql.Date, fec_hasta);
    }

    const facturaQuery = `
      SELECT
        f.fact_num, f.fec_emis, 
        c.cli_des AS cliente, c.rif,
        f.tot_bruto AS sub_total,
        f.glob_desc AS descuento,
        f.tot_neto AS total_neto,
        (f.tot_neto - (f.tot_bruto - f.glob_desc)) AS monto_iva,
        f.tasa
      FROM factura f
      INNER JOIN clientes c ON RTRIM(f.co_cli) = RTRIM(c.co_cli)
      WHERE RTRIM(f.co_cli) = @co_cli
      ${fechaFiltro}
      ORDER BY f.fec_emis DESC
    `;

    const facturaResult = await request.query(facturaQuery);

    let facturasFinalObj = {};

    for (let i = 0; i < facturaResult.recordset.length; i++) {
      const f = facturaResult.recordset[i];
      
      const itemsQuery = `
        SELECT
          a.ref AS sku, a.art_des AS descrip, 
          rf.total_art AS cant, 
          rf.prec_vta AS precio,
          rf.reng_neto AS neto_r
        FROM reng_fac rf
        INNER JOIN art a ON RTRIM(rf.co_art) = RTRIM(a.co_art)
        WHERE RTRIM(rf.fact_num) = @fact_num
        ORDER BY rf.reng_num ASC
      `;
      const itemsRequest = new sql.Request();
      itemsRequest.input("fact_num", sql.VarChar, String(f.fact_num).trim());
      const itemsResult = await itemsRequest.query(itemsQuery);

      let articulosObj = {};
      itemsResult.recordset.forEach((item, index) => {
        const subTeorico = item.cant * item.precio;
        const descTotal = subTeorico > item.neto_r ? (subTeorico - item.neto_r) : 0;
        
        articulosObj[`renglon_${index + 1}`] = {
          // --- CAMBIO DE NOMBRE AQUÍ ---
          sku: item.sku ? String(item.sku).trim() : "",
          descrip: item.descrip ? String(item.descrip).trim() : "",
          cant: parseFloat(item.cant) || 0,
          precio: parseFloat(item.precio) || 0,
          desc_monto: parseFloat(descTotal.toFixed(5)),
          neto_und: item.cant > 0 ? parseFloat((item.neto_r / item.cant).toFixed(5)) : 0,
          total: parseFloat(item.neto_r) || 0
        };
      });

      const mIVA = parseFloat(f.monto_iva) || 0;
      const sTotal = parseFloat(f.sub_total) || 0;
      const desc = parseFloat(f.descuento) || 0;
      
      // Base16 = (IVA * 100) / 16
      const base16 = (mIVA * 100) / 16;
      // Exento = (SubTotal - Descuento) - Base16
      const exento = (sTotal - desc) - base16;

      facturasFinalObj[`factura_${i + 1}`] = {
        fact_num: f.fact_num ? String(f.fact_num).trim() : "",
        fec_emis: f.fec_emis ? f.fec_emis.toISOString().split('T')[0] : null,
        cliente: f.cliente ? String(f.cliente).trim() : "",
        rif: f.rif ? String(f.rif).trim() : "",
        sub_total: sTotal,
        descuento: desc,
        base16: parseFloat(base16.toFixed(2)),
        iva16: parseFloat(mIVA.toFixed(2)),
        exento: parseFloat(exento.toFixed(2)) > 0 ? parseFloat(exento.toFixed(2)) : 0,
        total_bruto: sTotal,
        total_neto: parseFloat(f.total_neto) || 0,
        tasa: parseFloat(f.tasa) || 1,
        articulos: articulosObj,
        fin_factura: true 
      };
    }

    res.json(facturasFinalObj);

  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};  

// POST /api/pagina/negociaciones/profit
// Body JSON: { co_cat: array|string, campo3?: int }
export const ListadorNegociacionesProfit = async (req, res) => {
  const { co_cat, campo3 } = req.body;

  // Validación de parámetros obligatorios
  if (!co_cat) {
    return res.status(400).json({ message: "Falta el parámetro 'co_cat' para Profit." });
  }

  // --- CONSULTA A SQL SERVER (PROFIT) ---
  let resultProfit = [];
  try {
    const request = new sql.Request();

    // Manejo de array para co_cat (cláusula IN)
    let coCatArray = Array.isArray(co_cat) ? co_cat : [co_cat];
    let inClauseParams = [];
    coCatArray.forEach((cat, index) => {
      const paramName = `cat${index}`;
      request.input(paramName, sql.VarChar, String(cat).trim());
      inClauseParams.push(`@${paramName}`);
    });

    // Manejo de campo3 opcional
    let campo3Filter = "";
    if (campo3 !== undefined && campo3 !== null) {
      request.input("campo3", sql.Int, campo3);
      campo3Filter = "AND a.campo3 = @campo3";
    }

    const queryProfit = `
      SELECT
        DISTINCT a.co_art, a.art_des, a.tipo_imp, a.stock_act, a.campo4, 
        ( 
          a.prec_agr3 * ((100 - COALESCE(d_art.porc1, 0)) / 100 ) * ((100 - COALESCE(d_cat.porc1, 0)) / 100) * ((100 - COALESCE(d_lin.porc1, 0)) / 100) 
        ) AS preciof 
      FROM art AS a 
      LEFT JOIN descuen AS d_art ON a.co_art = d_art.co_desc AND d_art.tipo_desc = '1' 
      LEFT JOIN descuen AS d_cat ON a.co_cat = d_cat.co_desc AND d_cat.tipo_desc = '2' 
      LEFT JOIN descuen AS d_lin ON a.co_lin = d_lin.co_desc AND d_lin.tipo_desc = '3' 
      WHERE a.stock_act > 0 
        AND a.co_cat IN (${inClauseParams.join(',')})
        ${campo3Filter}
      ORDER BY a.art_des ASC
    `;

    const result = await request.query(queryProfit);
    
    // Limpieza inicial de datos de Profit
    resultProfit = result.recordset.map(item => ({
       co_art: item.co_art ? String(item.co_art).trim() : "",
       art_des: item.art_des ? String(item.art_des).trim() : "",
       tipo_imp: item.tipo_imp ? String(item.tipo_imp).trim() : "",
       stock_act: item.stock_act,
       campo4: item.campo4 ? String(item.campo4).trim() : "",
       preciof: item.preciof
    }));

    res.json({
      data: resultProfit,
      error: null
    });

  } catch (err) {
    console.error("Error en Profit:", err);
    return res.status(500).json({ message: "Error consultando Profit", error: err.message });
  }
};

// POST /api/pagina/negociaciones/comparador
// Body JSON: { barra: string, cod_comp: string }
export const ListadorNegociacionesComparador = async (req, res) => {
  const { barra, cod_comp } = req.body;

  if (!cod_comp) {
    return res.status(400).json({ message: "Falta el parámetro 'cod_comp'." });
  }
  if (!barra) {
    return res.status(400).json({ message: "Falta el parámetro 'barra'." });
  }

  let connection;
  try {
    connection = await mysql.createConnection(mysqlComparadorConfig);
    
    const [rows] = await connection.execute(
        'SELECT precio FROM productos WHERE barra = ? AND cod_comp = ?',
        [barra, cod_comp]
    );

    let precio = null;
    if (rows.length > 0) {
        precio = rows[0].precio;
    }

    res.json({
        barra: barra,
        precio: precio
    });

  } catch (err) {
    console.error("Error en MySQL:", err);
    res.status(500).json({
        barra: barra,
        precio: null,
        error: err.message
    });
  } finally {
    if (connection) await connection.end();
  }
};

// POST /api/pagina/descuentos/escala-prov
// Body JSON: { co_cli: string, co_prov: string, articulos: array<{ cant: number }> }

export const getDescuentoPorEscalaProv = async (req, res) => {
  try {
    const { co_cli, co_prov, articulos } = req.body;

    if (!co_prov || !articulos || !Array.isArray(articulos)) {
      return res.status(400).json({ 
        message: "Faltan datos: co_prov y articulos (array) son obligatorios." 
      });
    }

    const cantidadTotal = articulos.reduce((acc, art) => acc + parseFloat(art.cant || 0), 0);

    // --- NORMALIZAR co_prov: quitar punto final si existe ---
    const co_prov_clean = co_prov.trim().endsWith('.')
      ? co_prov.trim().slice(0, -1).trim()
      : co_prov.trim();

    const request = new sql.Request();
    request.input("co_cli",  sql.VarChar, co_cli ? co_cli.trim() : '');
    request.input("co_prov", sql.VarChar, co_prov_clean);

    const query = `
      SELECT TOP 1
        co_prov, co_cli,
        ISNULL(hasta1, 0) as hasta1, ISNULL(hasta2, 0) as hasta2, ISNULL(hasta3, 0) as hasta3,
        ISNULL(hasta4, 0) as hasta4, ISNULL(hasta5, 0) as hasta5,
        ISNULL(porc1,  0) as porc1,  ISNULL(porc2,  0) as porc2,  ISNULL(porc3,  0) as porc3,
        ISNULL(porc4,  0) as porc4,  ISNULL(porc5,  0) as porc5
      FROM dbo.desc_prov
      WHERE (LTRIM(RTRIM(co_prov)) = @co_prov OR LTRIM(RTRIM(co_prov)) = @co_prov + '.')
      AND (LTRIM(RTRIM(co_cli)) = @co_cli OR co_cli IS NULL OR LTRIM(RTRIM(co_cli)) = '')
      ORDER BY CASE WHEN LTRIM(RTRIM(co_cli)) = @co_cli THEN 0 ELSE 1 END ASC
    `;

    const result = await request.query(query);

    if (result.recordset.length === 0) {
      return res.json({ 
        co_prov:              co_prov_clean,
        tiene_descuento:      false,
        porcentaje_descuento: 0,
        porcentaje_base:      0,
        cantidad_minima:      0,
        escala_aplicada:      "Sin escala" 
      });
    }

    const raw    = result.recordset[0];
    const hasta1 = Number(raw.hasta1 ?? 0);
    const hasta2 = Number(raw.hasta2 ?? 0);
    const hasta3 = Number(raw.hasta3 ?? 0);
    const hasta4 = Number(raw.hasta4 ?? 0);
    const hasta5 = Number(raw.hasta5 ?? 0);
    const porc1  = Number(raw.porc1  ?? 0);
    const porc2  = Number(raw.porc2  ?? 0);
    const porc3  = Number(raw.porc3  ?? 0);
    const porc4  = Number(raw.porc4  ?? 0);
    const porc5  = Number(raw.porc5  ?? 0);

    const esClienteEspecifico = raw.co_cli && raw.co_cli.trim() === co_cli?.trim();

    let porcentajeFinal = 0;
    let nombreEscala    = "Base";

  if (esClienteEspecifico) {
      porcentajeFinal = porc1;
      nombreEscala    = "Descuento Pactado";

      if (hasta2 > 0 && cantidadTotal >= hasta2) { porcentajeFinal = porc2; nombreEscala = "Escala 2"; }
      if (hasta3 > 0 && cantidadTotal >= hasta3) { porcentajeFinal = porc3; nombreEscala = "Escala 3"; }
      if (hasta4 > 0 && cantidadTotal >= hasta4) { porcentajeFinal = porc4; nombreEscala = "Escala 4"; }
      if (hasta5 > 0 && cantidadTotal >= hasta5) { porcentajeFinal = porc5; nombreEscala = "Escala 5"; }

  } else {
      if (hasta1 > 0 && cantidadTotal >= hasta1) { porcentajeFinal = porc1; nombreEscala = "Escala 1"; }
      if (hasta2 > 0 && cantidadTotal >= hasta2) { porcentajeFinal = porc2; nombreEscala = "Escala 2"; }
      if (hasta3 > 0 && cantidadTotal >= hasta3) { porcentajeFinal = porc3; nombreEscala = "Escala 3"; }
      if (hasta4 > 0 && cantidadTotal >= hasta4) { porcentajeFinal = porc4; nombreEscala = "Escala 4"; }
      if (hasta5 > 0 && cantidadTotal >= hasta5) { porcentajeFinal = porc5; nombreEscala = "Escala 5"; }
  }

    return res.json({
      co_cli:               raw.co_cli ? raw.co_cli.trim() : "GENERAL",
      co_prov:              raw.co_prov.trim(),
      cantidad_total:       cantidadTotal,
      tiene_descuento:      porcentajeFinal > 0,
      porcentaje_descuento: porcentajeFinal,
      porcentaje_base:      porc1,
      cantidad_minima:      hasta1,
      escala_aplicada:      nombreEscala,
      tipo_regla:           esClienteEspecifico ? "Especifica" : "General"
    });

  } catch (error) {
    res.status(500).json({ message: "Error en validación de escala", error: error.message });
  }
};

// POST /api/pagina/descuentos/escala-ptc
// Body JSON: { co_cli: string, articulos: array of { co_art: string, cant: number }, modo: "catalogo" | "carrito" }

export const getDescuentoPorEscalaPTC = async (req, res) => {
  try {
    const { co_cli, articulos, modo = "catalogo" } = req.body;

    if (!articulos || !Array.isArray(articulos)) {
      return res.status(400).json({ message: "El campo 'articulos' (array) es obligatorio." });
    }

    const codigosUnicos = [...new Set(articulos.map(a => a.co_art.trim()))];
    
    if (codigosUnicos.length === 0) {
      return res.status(400).json({ message: "La lista de artículos está vacía." });
    }

    // --- REQUEST 1: obtener co_prov de los artículos ---
    const requestProv = new sql.Request();
    const provRes = await requestProv.query(`
      SELECT RTRIM(co_art) as co_art, RTRIM(co_prov) as co_prov 
      FROM art 
      WHERE RTRIM(co_art) IN (${codigosUnicos.map(c => `'${c}'`).join(",")})
    `);

    writeLog({ evento: 'provRes_recordset', data: provRes.recordset });

    // --- LOG: inspección detallada de co_prov ---
    writeLog({
      evento: 'debug_co_prov_art',
      data: provRes.recordset.map(e => ({
        co_art:       e.co_art,
        co_prov:      e.co_prov,
        tiene_punto:  e.co_prov?.endsWith('.'),
        longitud:     e.co_prov?.length,
        char_codes:   [...(e.co_prov || '')].map(c => c.charCodeAt(0))
      }))
    });
    
    const totalesPorProv = {};
    articulos.forEach(art => {
      const infoArt = provRes.recordset.find(p => p.co_art === art.co_art.trim());
      if (infoArt && infoArt.co_prov) {
        const cpRaw = infoArt.co_prov.trim();
        const cp    = cpRaw.endsWith('.') ? cpRaw.slice(0, -1).trim() : cpRaw;

        writeLog({ evento: 'normalizacion_co_prov', co_art: art.co_art, cpRaw, cp, cambio: cpRaw !== cp });

        totalesPorProv[cp] = (totalesPorProv[cp] || 0) + parseFloat(art.cant || 0);
      }
    });

    writeLog({ evento: 'totalesPorProv', data: totalesPorProv });

    const proveedoresEncontrados = Object.keys(totalesPorProv);

    writeLog({ evento: 'proveedoresEncontrados', data: proveedoresEncontrados });

    if (proveedoresEncontrados.length === 0) {
      return res.json({ 
        co_cli: co_cli ? co_cli.trim() : "ANONIMO",
        message: "No se detectaron proveedores válidos.", 
        descuentos_por_proveedor: [] 
      });
    }

    // --- Loguear la query que se va a ejecutar ---
    const inClause = proveedoresEncontrados.flatMap(p => [`'${p}'`, `'${p}.'`]).join(",");
    writeLog({ evento: 'query_desc_ptc_IN', inClause });

    // --- REQUEST 2: obtener escalas de desc_ptc (request fresco) ---
    const requestEscalas = new sql.Request();
    const escalasRes = await requestEscalas.query(`
      SELECT 
        RTRIM(co_prov) as co_prov,
        ISNULL(hasta1, 0) as hasta1, ISNULL(hasta2, 0) as hasta2, ISNULL(hasta3, 0) as hasta3,
        ISNULL(hasta4, 0) as hasta4, ISNULL(hasta5, 0) as hasta5,
        ISNULL(porc1, 0) as porc1, ISNULL(porc2, 0) as porc2, ISNULL(porc3, 0) as porc3,
        ISNULL(porc4, 0) as porc4, ISNULL(porc5, 0) as porc5
      FROM dbo.desc_ptc
      WHERE RTRIM(co_prov) IN (${inClause})
        AND (tipo_cli IS NULL OR LTRIM(RTRIM(tipo_cli)) = '')
    `);

    writeLog({ evento: 'escalasRes_recordset', data: escalasRes.recordset });

    const resultadoFinal = proveedoresEncontrados.map(cp => {
      const cantidadTotal = totalesPorProv[cp];

      const raw = escalasRes.recordset.find(e => {
        const eProv = e.co_prov.trim().endsWith('.')
          ? e.co_prov.trim().slice(0, -1).trim()
          : e.co_prov.trim();
        return eProv === cp;
      });

      writeLog({ evento: 'busqueda_raw', cp, cantidadTotal, raw_encontrado: raw ? raw.co_prov : 'NO ENCONTRADO' });

      let porcentaje   = 0;
      let nombreEscala = "Sin Escala / Base";

      if (raw) {
        const hasta1 = Number(raw.hasta1 ?? 0);
        const hasta2 = Number(raw.hasta2 ?? 0);
        const hasta3 = Number(raw.hasta3 ?? 0);
        const hasta4 = Number(raw.hasta4 ?? 0);
        const hasta5 = Number(raw.hasta5 ?? 0);
        const porc1  = Number(raw.porc1  ?? 0);
        const porc2  = Number(raw.porc2  ?? 0);
        const porc3  = Number(raw.porc3  ?? 0);
        const porc4  = Number(raw.porc4  ?? 0);
        const porc5  = Number(raw.porc5  ?? 0);

        if (modo === "catalogo") {
          if (porc1 > 0) {
            porcentaje   = porc1;
            nombreEscala = "Base PTC (catálogo)";
          }
        } else {
            if (hasta1 > 0 && cantidadTotal >= hasta1) { porcentaje = porc1; nombreEscala = "Escala 1"; }
            if (hasta2 > 0 && cantidadTotal >= hasta2) { porcentaje = porc2; nombreEscala = "Escala 2"; }
            if (hasta3 > 0 && cantidadTotal >= hasta3) { porcentaje = porc3; nombreEscala = "Escala 3"; }
            if (hasta4 > 0 && cantidadTotal >= hasta4) { porcentaje = porc4; nombreEscala = "Escala 4"; }
            if (hasta5 > 0 && cantidadTotal >= hasta5) { porcentaje = porc5; nombreEscala = "Escala 5"; }
        }

        writeLog({ evento: 'escala_calculada', cp, cantidadTotal, porcentaje, nombreEscala, escalas: { hasta1, hasta2, hasta3, hasta4, hasta5, porc1, porc2, porc3, porc4, porc5 } });
      }

      return {
        co_prov:              cp,
        cantidad_acumulada:   cantidadTotal,
        porcentaje_descuento: porcentaje,
        escala_aplicada:      nombreEscala,
        tiene_descuento:      porcentaje > 0,
        modo
      };
    });

    writeLog({ evento: 'resultadoFinal', data: resultadoFinal });

    res.json({
      co_cli:                   co_cli ? co_cli.trim() : "ANONIMO",
      descuentos_por_proveedor: resultadoFinal
    });

  } catch (error) {
    writeLog({ evento: 'ERROR', mensaje: error.message, stack: error.stack });
    res.status(500).json({ message: "Error en el cálculo de escalas PTC", error: error.message });
  }
};