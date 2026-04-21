import { sql, remoteConfig, localConfig } from './src/config/database.js';
import mysql from 'mysql2/promise';

async function verify() {
    try {
        console.log('Connecting to SQL Server...');
        await sql.connect(remoteConfig);
        console.log('Connected to SQL Server.');

        console.log('Connecting to MySQL...');
        const pool = await mysql.createPool(localConfig);
        console.log('Connected to MySQL.');

        // 1. Find a candidate in cotiz_c that has TIPO_DOC='T'
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
            console.log('No suitable candidate found in cotiz_c.');
            process.exit(0);
        }

        const { fact_num } = candidateRes.recordset[0];
        console.log(`Found cotiz_c candidate: fact_num=${fact_num}`);

        // 2. Check if this fact_num exists in MySQL pedidos
        console.log(`Checking MySQL pedidos for fact_num = '${fact_num}'...`);

        const [rows] = await pool.execute(
            'SELECT id, fact_num, co_us_in FROM pedidos WHERE fact_num = ? LIMIT 1',
            [String(fact_num)]
        );

        if (rows.length > 0) {
            console.log('✅ Found in pedidos:', rows[0]);
        } else {
            console.log('❌ NOT found in pedidos. This explains why co_us_in is null.');

            // Try to find ANY pedido to see format
            const [anyRows] = await pool.execute('SELECT fact_num FROM pedidos ORDER BY id DESC LIMIT 5');
            console.log('Sample fact_nums in pedidos:', anyRows);
        }

    } catch (err) {
        console.error('Error:', err);
    } finally {
        await sql.close();
        process.exit(0);
    }
}

verify();
