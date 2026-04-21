import { sql, remoteConfig } from './src/config/database.js';

async function verify() {
    try {
        console.log('Connecting to SQL Server...');
        await sql.connect(remoteConfig);
        console.log('Connected.');

        // 1. Find a candidate fact_num and campo5
        console.log('Searching for a candidate in cotiz_c...');
        const candidateQuery = `
      SELECT TOP 1 c.fact_num, c.campo5 
      FROM cotiz_c c
      INNER JOIN reng_nde r ON c.fact_num = r.num_doc
      WHERE r.TIPO_DOC = 'T'
      AND c.campo5 IS NOT NULL
      AND c.campo6 IS NOT NULL
      AND LTRIM(RTRIM(c.campo6)) <> ''
      AND TRY_CAST(c.campo6 AS FLOAT) IS NOT NULL
      ORDER BY c.fec_emis DESC
    `;
        const candidateRes = await new sql.Request().query(candidateQuery);

        if (candidateRes.recordset.length === 0) {
            console.log('No suitable candidate found in cotiz_c with TIPO_DOC = T and campo5 set.');
            process.exit(0);
        }

        const { fact_num, campo5 } = candidateRes.recordset[0];
        console.log(`Found candidate: fact_num=${fact_num}, campo5='${campo5}'`);

        // Convert to string to avoid EPARAM error
        const cod_prov = campo5 ? String(campo5) : '';
        const nro_doc = String(fact_num);

        console.log(`Testing query logic with nro_doc="${nro_doc}" (type: ${typeof nro_doc}), cod_prov="${cod_prov}"`);

        // 2. Run the EXACT logic we added (adapted for script)
        const whereClausesCotiz = [
            "c.campo5 = @cod_prov",
            "c.campo6 IS NOT NULL",
            "LTRIM(RTRIM(c.campo6)) <> ''",
            "TRY_CAST(c.campo6 AS FLOAT) IS NOT NULL",
            "r.TIPO_DOC = 'T'"
        ];
        if (nro_doc) whereClausesCotiz.push("c.fact_num = @nro_doc");

        const queryCotiz = `
        SELECT 
          c.fact_num, 
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
        requestCotiz.input("nro_doc", sql.VarChar, nro_doc);

        const resultCotiz = await requestCotiz.query(queryCotiz);

        console.log(`Query returned ${resultCotiz.recordset.length} rows.`);
        if (resultCotiz.recordset.length > 0) {
            console.log('First row sample:', resultCotiz.recordset[0]);
        } else {
            console.warn("Query returned no rows!");

            // Debug
            const debugQ = `SELECT campo6, campo5 FROM cotiz_c WHERE fact_num = '${nro_doc}'`;
            const debugRes = await new sql.Request().query(debugQ);
            console.log(`Debug for ${nro_doc}:`, debugRes.recordset[0]);

            if (debugRes.recordset[0]) {
                const c6 = debugRes.recordset[0].campo6;
                console.log(`campo6 '${c6}' - isNumeric: ${!isNaN(parseFloat(c6))}`);
            }
        }

    } catch (err) {
        console.error('Error:', err);
    } finally {
        await sql.close();
    }
}

verify();
