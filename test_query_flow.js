import mysql from 'mysql2/promise';
import { sql } from './config/database.js';

// Configuración MySQL
const mysqlConfig = {
    host: '192.168.4.23',
    user: 'desarrollo',
    password: 'E-xUUctByBsPTe7A',
    database: 'transferencias',
};

async function investigarFlujo() {
    try {
        // 1. Conectar a MySQL y obtener algunos pedidos
        const mysqlPool = await mysql.createPool(mysqlConfig);
        console.log('\n=== PASO 1: Consultando tabla pedidos (MySQL) ===');
        const [pedidos] = await mysqlPool.execute(
            'SELECT fact_num, co_us_in FROM pedidos ORDER BY id DESC LIMIT 5'
        );

        if (pedidos.length === 0) {
            console.log('❌ No hay pedidos en la tabla pedidos');
            return;
        }

        console.log('Pedidos encontrados:');
        pedidos.forEach(p => {
            console.log(`  - fact_num: ${p.fact_num}, co_us_in: ${p.co_us_in}`);
        });

        // Tomar el primer pedido como ejemplo
        const pedidoEjemplo = pedidos[0];
        console.log(`\n📌 Usando pedido fact_num="${pedidoEjemplo.fact_num}" para rastrear...`);

        // 2. Conectar a SQL Server y buscar en cotiz_c
        await sql.connect();
        console.log('\n=== PASO 2: Buscando en cotiz_c (SQL Server) ===');
        const reqCotiz = new sql.Request();
        reqCotiz.input('factNum', sql.VarChar, String(pedidoEjemplo.fact_num));
        const cotizRes = await reqCotiz.query(
            'SELECT TOP 1 fact_num, co_cli, descrip FROM cotiz_c WHERE fact_num = @factNum'
        );

        if (!cotizRes.recordset || cotizRes.recordset.length === 0) {
            console.log(`❌ No encontrado en cotiz_c con fact_num="${pedidoEjemplo.fact_num}"`);
            return;
        }

        const cotiz = cotizRes.recordset[0];
        console.log(`✅ Cotización encontrada: fact_num=${cotiz.fact_num}, descrip="${cotiz.descrip}"`);

        // 3. Buscar en reng_nde donde num_doc = número de cotización
        console.log('\n=== PASO 3: Buscando en reng_nde ===');
        const reqRengNde = new sql.Request();
        reqRengNde.input('numDoc', sql.VarChar, String(cotiz.fact_num));
        const rengNdeRes = await reqRengNde.query(
            'SELECT TOP 5 fact_num, num_doc, reng_num FROM reng_nde WHERE num_doc = @numDoc'
        );

        if (!rengNdeRes.recordset || rengNdeRes.recordset.length === 0) {
            console.log(`❌ No encontrado en reng_nde con num_doc="${cotiz.fact_num}"`);
            return;
        }

        console.log(`✅ Registros en reng_nde (num_doc="${cotiz.fact_num}"):`);
        rengNdeRes.recordset.forEach(r => {
            console.log(`  - fact_num: ${r.fact_num}, num_doc: ${r.num_doc}, reng_num: ${r.reng_num}`);
        });

        const rengNde = rengNdeRes.recordset[0];

        // 4. Buscar la factura con reng_fac
        console.log('\n=== PASO 4: Buscando en reng_fac ===');
        const reqRengFac = new sql.Request();
        reqRengFac.input('numDoc', sql.VarChar, String(rengNde.fact_num));
        const rengFacRes = await reqRengFac.query(
            'SELECT TOP 5 fact_num, num_doc, reng_num FROM reng_fac WHERE num_doc = @numDoc'
        );

        if (!rengFacRes.recordset || rengFacRes.recordset.length === 0) {
            console.log(`❌ No encontrado en reng_fac con num_doc="${rengNde.fact_num}"`);
            return;
        }

        console.log(`✅ Registros en reng_fac (num_doc="${rengNde.fact_num}"):`);
        rengFacRes.recordset.forEach(r => {
            console.log(`  - fact_num: ${r.fact_num}, num_doc: ${r.num_doc}, reng_num: ${r.reng_num}`);
        });

        const rengFac = rengFacRes.recordset[0];

        console.log('\n\n=== RESUMEN DEL FLUJO COMPLETO (FORWARD) ===');
        console.log(`Pedido MySQL (fact_num)    : ${pedidoEjemplo.fact_num} (co_us_in: ${pedidoEjemplo.co_us_in})`);
        console.log(`   ↓`);
        console.log(`Cotización (cotiz_c)        : ${cotiz.fact_num}`);
        console.log(`   ↓ (cotiz.fact_num se almacena en reng_nde.num_doc)`);
        console.log(`NDE (reng_nde)              : fact_num=${rengNde.fact_num}, num_doc=${rengNde.num_doc}`);
        console.log(`   ↓ (reng_nde.fact_num se almacena en reng_fac.num_doc)`);
        console.log(`Factura (reng_fac)          : fact_num=${rengFac.fact_num}, num_doc=${rengFac.num_doc}`);

        console.log('\n\n=== FLUJO INVERSO (LO QUE NECESITAMOS IMPLEMENTAR) ===');
        console.log(`Factura                         : ${rengFac.fact_num}`);
        console.log(`   ↓ buscar reng_fac WHERE fact_num = ${rengFac.fact_num}`);
        console.log(`   → reng_fac.num_doc          : ${rengFac.num_doc} (este es el NDE fact_num)`);
        console.log(`   ↓ buscar reng_nde WHERE fact_num = ${rengFac.num_doc}`);
        console.log(`   → reng_nde.num_doc          : ${rengNde.num_doc} (este es el cotiz_c fact_num)`);
        console.log(`   ↓ buscar pedidos WHERE fact_num = ${rengNde.num_doc}`);
        console.log(`   → pedidos.co_us_in          : ${pedidoEjemplo.co_us_in} ✅`);

        await mysqlPool.end();
        await sql.close();
        process.exit(0);

    } catch (error) {
        console.error('❌ Error:', error.message);
        process.exit(1);
    }
}

investigarFlujo();
