import 'dotenv/config';
import { sql, getComparadorPool } from '../config/database.js';

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

export const ListadorNegociacionesProfit = async (req, res) => {
  const { co_cat, campo3 } = req.body;

  // Validaci�n de par�metros obligatorios
  if (!co_cat) {
    return res.status(400).json({ message: "Falta el par�metro 'co_cat' para Profit." });
  }

  // --- CONSULTA A SQL SERVER (PROFIT) ---
  let resultProfit = [];
  try {
    const request = new sql.Request();

    // Manejo de array para co_cat (cl�usula IN)
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

export const ListadorNegociacionesComparador = async (req, res) => {
  const { barra, cod_comp } = req.body;

  if (!cod_comp) {
    return res.status(400).json({ message: "Falta el par�metro 'cod_comp'." });
  }
  if (!barra) {
    return res.status(400).json({ message: "Falta el par�metro 'barra'." });
  }

  try {
    const pool = getComparadorPool();
    const [rows] = await pool.execute(
      'SELECT precio FROM productos WHERE barra = ? AND cod_comp = ?',
      [barra, cod_comp]
    );

    const precio = rows.length > 0 ? rows[0].precio : null;

    res.json({ barra, precio });

  } catch (err) {
    console.error("Error en MySQL:", err);
    res.status(500).json({ barra, precio: null, error: err.message });
  }
};