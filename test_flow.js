import mysql from 'mysql2/promise';
import mssql from 'mssql';

// MySQL Config
const mysqlConfig = {
    host: '192.168.4.23',
    user: 'desarrollo',
    password: 'E-xUUctByBsPTe7A',
    database: 'transferencias',
};

// SQL Server Config
const sqlConfig = {
    user: 'cristmedicals',
    password: 'F74Qz1Tc#ZyW3a9Y@VWuN',
    server: '192.168.4.7',
    database: 'CRISTM25',
    options: {
        encrypt: false,
        trustServerCertificate: true
    }
};

async function investigarFlujo() {
    let mysqlPool, sqlPool;

    try {
        // 1. MySQL
        mysqlPool = await mysql.createPool(mysqlConfig);
        console.log('\n=== Consultando pedidos (MySQL) ===');
        const [pedidos] = await mysqlPool.execute(
            'SELECT fact_num, co_us_in FROM pedidos ORDER BY id DESC LIMIT 5'
        );

        if (pedidos.length === 0) {
            console.log('❌ No hay pedidos');
            return;
        }

        console.log('Pedidos:');
        pedidos.forEach(p => console.log(`  fact_num: ${p.fact_num}, co_us_in: ${p.co_us_in}`));

        const pedido = pedidos[0];
        console.log(`\n📌 Rastreando pedido fact_num="${pedido.fact_num}"...\n`);

        // 2. SQL Server
        sqlPool = await mssql.connect(sqlConfig);

        // 2a. Buscar en cotiz_c
        const cotizRes = await sqlPool.request()
            .input('factNum', mssql.VarChar, String(pedido.fact_num))
            .query('SELECT TOP 1 fact_num FROM cotiz_c WHERE fact_num = @factNum');

        if (!cotizRes.recordset || cotizRes.recordset.length === 0) {
            console.log(`❌ No encontrado en cotiz_c`);
            return;
        }
        console.log(`✅ Cotización: ${cotizRes.recordset[0].fact_num}`);

        // 2b. Buscar in reng_nde WHERE num_doc = cotización
        const rengNdeRes = await sqlPool.request()
            .input('numDoc', mssql.VarChar, String(cotizRes.recordset[0].fact_num))
            .query('SELECT TOP 1 fact_num, num_doc FROM reng_nde WHERE num_doc = @numDoc');

        if (!rengNdeRes.recordset || rengNdeRes.recordset.length === 0) {
            console.log(`❌ No encontrado en reng_nde`);
            return;
        }
        console.log(`✅ NDE: fact_num=${rengNdeRes.recordset[0].fact_num}, num_doc=${rengNdeRes.recordset[0].num_doc}`);

        // 2c. Buscar en reng_fac WHERE num_doc = NDE fact_num
        const rengFacRes = await sqlPool.request()
            .input('numDoc', mssql.VarChar, String(rengNdeRes.recordset[0].fact_num))
            .query('SELECT TOP 1 fact_num, num_doc FROM reng_fac WHERE num_doc = @numDoc');

        if (!rengFacRes.recordset || rengFacRes.recordset.length === 0) {
            console.log(`❌ No encontrado en reng_fac`);
            return;
        }
        console.log(`✅ Factura: ${rengFacRes.recordset[0].fact_num}`);

        console.log('\n\n========== FLUJO INVERSO CORRECTO ==========');
        console.log(`Factura ${rengFacRes.recordset[0].fact_num}`);
        console.log(`  ↓ buscar reng_fac WHERE fact_num = ${rengFacRes.recordset[0].fact_num}`);
        console.log(`  → reng_fac.num_doc = ${rengFacRes.recordset[0].num_doc}`);
        console.log(`  ↓ buscar reng_nde WHERE fact_num = ${rengFacRes.recordset[0].num_doc}`);
        console.log(`  → reng_nde.num_doc = ${rengNdeRes.recordset[0].num_doc}`);
        console.log(`  ↓ buscar pedidos WHERE fact_num = ${rengNdeRes.recordset[0].num_doc}`);
        console.log(`  → co_us_in = ${pedido.co_us_in} ✅`);

    } catch (error) {
        console.error('❌ Error:', error.message);
    } finally {
        if (mysqlPool) await mysqlPool.end();
        if (sqlPool) await sqlPool.close();
        process.exit(0);
    }
}

investigarFlujo();
