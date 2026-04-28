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

    // ─── Fallback: buscar en cotiz_c ─────────────────────────────────────────
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

    // ─── Agrupar por factura ──────────────────────────────────────────────────
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
          co_cli:     safeStr(rowCoCli),
          cli_des,
          campo5,
          campo6,
          fec_emis,
          tot_neto:   tot_neto_val,
          rif:        rif ?? null,
          sicm:       null,
          tasa:       tasa != null && tasa !== "" ? Number(Number(tasa).toFixed(2)) : null,
          tot_bruto:  tot_bruto != null ? Number(tot_bruto) : tot_bruto,
          co_us_in:   null,
          pedido_num: null,
          db_mysql:   [],
          articulos:  [],
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

    // ─── BATCH: nit (sicm) para todos los co_cli únicos ──────────────────────
    const uniqueCoCliList = [...new Set(facturasArray.map(f => safeStr(f.co_cli)).filter(Boolean))];
    const sicmMap = {};
    if (uniqueCoCliList.length > 0) {
      try {
        const inParams = uniqueCoCliList.map((_, i) => `@c${i}`).join(",");
        const reqSicm = new sql.Request();
        uniqueCoCliList.forEach((v, i) => reqSicm.input(`c${i}`, sql.VarChar, v));
        const resSicm = await reqSicm.query(
          `SELECT LTRIM(RTRIM(co_cli)) AS co_cli, LTRIM(RTRIM(nit)) AS nit FROM clientes WHERE co_cli IN (${inParams})`
        );
        resSicm.recordset.forEach(r => { sicmMap[safeStr(r.co_cli)] = safeStr(r.nit); });
      } catch (e) {
        console.error("[batch sicm] Error:", e.message);
      }
    }
    facturasArray.forEach(f => { f.sicm = sicmMap[safeStr(f.co_cli)] ?? null; });

    // ─── BATCH: cod_bar (campo4) para todos los co_art únicos ────────────────
    const uniqueCoArtList = [...new Set(
      facturasArray.flatMap(f => f.articulos.map(a => safeStr(a.co_art))).filter(Boolean)
    )];
    const codBarMap = {};
    if (uniqueCoArtList.length > 0) {
      try {
        const inParams = uniqueCoArtList.map((_, i) => `@a${i}`).join(",");
        const reqBar = new sql.Request();
        uniqueCoArtList.forEach((v, i) => reqBar.input(`a${i}`, sql.VarChar, v));
        const resBar = await reqBar.query(
          `SELECT LTRIM(RTRIM(co_art)) AS co_art, LTRIM(RTRIM(campo4)) AS campo4 FROM art WHERE co_art IN (${inParams})`
        );
        resBar.recordset.forEach(r => { codBarMap[safeStr(r.co_art)] = safeStr(r.campo4); });
      } catch (e) {
        console.error("[batch cod_bar] Error:", e.message);
      }
    }
    facturasArray.forEach(f => {
      f.articulos.forEach(art => { art.cod_bar = codBarMap[safeStr(art.co_art)] ?? null; });
    });

    // ─── BATCH PASO 1: reng_fac → num_doc para todos los fact_num ────────────
    const allFactNums = facturasArray.map(f => safeStr(f.fact_num)).filter(Boolean);
    const rengFacMap = {};
    if (allFactNums.length > 0) {
      try {
        const inParams = allFactNums.map((_, i) => `@f${i}`).join(",");
        const reqRF = new sql.Request();
        allFactNums.forEach((v, i) => reqRF.input(`f${i}`, sql.VarChar, v));
        const resRF = await reqRF.query(
          `SELECT LTRIM(RTRIM(fact_num)) AS fact_num, LTRIM(RTRIM(num_doc)) AS num_doc
           FROM reng_fac
           WHERE LTRIM(RTRIM(fact_num)) IN (${inParams})`
        );
        resRF.recordset.forEach(r => {
          const k = safeStr(r.fact_num);
          if (k && !rengFacMap[k]) rengFacMap[k] = safeStr(r.num_doc);
        });
      } catch (e) {
        console.error("[batch reng_fac] Error:", e.message);
      }
    }

    // ─── BATCH PASO 2: reng_nde → num_doc ────────────────────────────────────
    const paso1Docs = [...new Set(Object.values(rengFacMap).filter(Boolean))];
    const rengNdeMap = {};
    if (paso1Docs.length > 0) {
      try {
        const inParams = paso1Docs.map((_, i) => `@n${i}`).join(",");
        const reqRN = new sql.Request();
        paso1Docs.forEach((v, i) => reqRN.input(`n${i}`, sql.VarChar, v));
        const resRN = await reqRN.query(
          `SELECT LTRIM(RTRIM(fact_num)) AS fact_num, LTRIM(RTRIM(num_doc)) AS num_doc
           FROM reng_nde
           WHERE LTRIM(RTRIM(fact_num)) IN (${inParams})`
        );
        resRN.recordset.forEach(r => {
          const k = safeStr(r.fact_num);
          if (k && !rengNdeMap[k]) rengNdeMap[k] = safeStr(r.num_doc);
        });
      } catch (e) {
        console.error("[batch reng_nde] Error:", e.message);
      }
    }

    facturasArray.forEach(f => {
      const numDoc1 = rengFacMap[safeStr(f.fact_num)];
      if (numDoc1) {
        const numDoc2 = rengNdeMap[numDoc1];
        if (numDoc2) f.pedido_num = numDoc2;
      }
    });

    // ─── BATCH PASO 3: MySQL pedidos + pedido_productos ───────────────────────
    const allPedidoNums = [...new Set(facturasArray.map(f => f.pedido_num).filter(Boolean))];
    const pedidoMap  = {};
    const productosMap = {};
    if (allPedidoNums.length > 0) {
      try {
        const placeholders = allPedidoNums.map(() => "TRIM(fact_num) = ?").join(" OR ");
        const [pedRows] = await getTransferenciasPool().execute(
          `SELECT co_us_in, fact_num FROM pedidos WHERE ${placeholders} LIMIT ${allPedidoNums.length * 2}`,
          allPedidoNums
        );
        pedRows.forEach(r => {
          const k = safeStr(r.fact_num);
          if (k) pedidoMap[k] = { co_us_in: safeStr(r.co_us_in), fact_num: k };
        });

        const mysqlFactNums = [...new Set(pedRows.map(r => safeStr(r.fact_num)).filter(Boolean))];
        if (mysqlFactNums.length > 0) {
          const ph2 = mysqlFactNums.map(() => "TRIM(fact_num) = ?").join(" OR ");
          const [prodRows] = await getTransferenciasPool().execute(
            `SELECT fact_num, co_art, cantidad, precio, subtotal, created_at FROM pedido_productos WHERE ${ph2}`,
            mysqlFactNums
          );

          // BATCH: art_des para todos los co_art de productos
          const prodCoArts = [...new Set(prodRows.map(p => safeStr(p.co_art)).filter(Boolean))];
          const artDesMap = {};
          if (prodCoArts.length > 0) {
            try {
              const inParams = prodCoArts.map((_, i) => `@d${i}`).join(",");
              const reqAD = new sql.Request();
              prodCoArts.forEach((v, i) => reqAD.input(`d${i}`, sql.VarChar, v));
              const resAD = await reqAD.query(
                `SELECT LTRIM(RTRIM(co_art)) AS co_art, LTRIM(RTRIM(art_des)) AS art_des FROM art WHERE co_art IN (${inParams})`
              );
              resAD.recordset.forEach(r => { artDesMap[safeStr(r.co_art)] = safeStr(r.art_des); });
            } catch (e) {
              console.error("[batch art_des] Error:", e.message);
            }
          }

          prodRows.forEach(prod => {
            const k = safeStr(prod.fact_num);
            if (!productosMap[k]) productosMap[k] = [];
            productosMap[k].push({
              fact_num:   k,
              co_art:     safeStr(prod.co_art),
              art_des:    artDesMap[safeStr(prod.co_art)] ?? null,
              cantidad:   prod.cantidad,
              precio:     prod.precio,
              subtotal:   prod.subtotal,
              created_at: prod.created_at,
            });
          });
        }
      } catch (errMysql) {
        console.error("[batch MySQL] Error:", errMysql.message);
      }
    }

    facturasArray.forEach(f => {
      if (!f.pedido_num) { f.co_us_in = null; f.db_mysql = []; return; }
      const ped = pedidoMap[f.pedido_num];
      if (ped) {
        f.co_us_in = ped.co_us_in;
        f.db_mysql = productosMap[ped.fact_num] ?? [];
      } else {
        f.co_us_in = null;
        f.db_mysql = [];
      }
    });

    // ─── PASO 4: info_profit — paralelizado con Promise.all ──────────────────
    await Promise.all(facturasArray.map(async (f) => {
      try {
        const factNumLimpio = safeStr(f.fact_num);
        const requestTrace = new sql.Request();
        requestTrace.input("pedidoFactNum", sql.VarChar, factNumLimpio);
        requestTrace.input("codCliente",    sql.VarChar, safeStr(f.co_cli) ?? "");

        const traceQuery = `
          SELECT TOP 1
            fact_num, co_cli, tot_neto, tot_bruto, fec_emis,
            co_ven, glob_desc, iva, status, anulada, tasa
          FROM factura
          WHERE LTRIM(RTRIM(fact_num)) = @pedidoFactNum;

          SELECT reng_num, co_art, total_art, prec_vta, porc_desc, reng_neto, art_des
          FROM reng_fac
          WHERE LTRIM(RTRIM(fact_num)) = @pedidoFactNum
          ORDER BY reng_num ASC;
        `;

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
                limpio[key] = typeof r[key] === "string" ? r[key].trim() : r[key];
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
    }));

    // ─── Consulta adicional: facturas Profit con status = 2 ──────────────────
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

