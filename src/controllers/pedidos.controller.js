import { sql } from '../config/database.js';
import { v4 as uuidv4 } from 'uuid';
import { DateTime } from 'luxon';



// Utilidad para limpiar valores
const limpiarValor = (valor) => (valor !== undefined && valor !== null ? String(valor).trim() : '');

// Obtiene el cotc_num, lo incrementa y lo actualiza en Sucursales
async function obtenerNuevoFactNum(pool) {
  // Operación atómica que incrementa y devuelve el nuevo valor
  try {
    const result = await pool.request()
      .query(`UPDATE Sucursales
              SET cotc_num = ISNULL(TRY_CAST(cotc_num AS INT),0) + 1
              OUTPUT inserted.cotc_num AS nuevo
              WHERE RTRIM(co_alma) = '01'`);
    const nuevo = result.recordset?.[0]?.nuevo;
    if (!nuevo) {
      console.error('No se actualizó cotc_num para co_alma=01');
      return null;
    }
    return Number(nuevo);
  } catch (err) {
    console.error('Error en obtenerNuevoFactNum (atómico):', err.message);
    return null;
  }
}

// Obtiene co_ven del cliente
async function obtenerCoVenPorCliente(pool, co_cli) {
  try {
    const result = await pool.request()
      .input('co_cli', sql.VarChar, limpiarValor(co_cli))
      .query('SELECT co_ven FROM clientes WHERE RTRIM(co_cli) = @co_cli');
    return result.recordset[0]?.co_ven?.trim() || '000008';
  } catch (err) {
    console.error('Error en obtenerCoVenPorCliente:', err.message);
    return '000008'; // Valor por defecto
  }
}

// Obtiene desc_glob del cliente
async function obtenerDescGlobCliente(pool, co_cli) {
  try {
    const result = await pool.request()
      .input('co_cli', sql.VarChar, limpiarValor(co_cli))
      .query('SELECT desc_glob FROM clientes WHERE RTRIM(co_cli) = @co_cli');
    return result.recordset[0]?.desc_glob || 0.0;
  } catch (err) {
    console.error('Error en obtenerDescGlobCliente:', err.message);
    return 0.0; // Valor por defecto
  }
}

/**
 * Formatea el porcentaje de descuento para insertar en reng_cac.
 * @param {string} co_art - Código del artículo.
 * @param {string} co_cat - Código de categoría.
 * @returns {Promise<string>} - String formateado '0.00+XX.XX+0.00'.
 */
async function formato_porc_desc_para_reng(co_art, co_cat) {
    const porc_desc = await obtenerDescuentoArticulo(String(co_art), String(co_cat)) || 0.0;
    return `0.00+${porc_desc.toFixed(2)}+0.00`;
}

/**
 * Función Auxiliar: Busca descuento por artículo o categoría
 * Retorna el string formateado '0.00+X.XX+0.00'
 */
async function obtenerDescuentoArticulo(pool, co_art, co_cat) {
    try {
        const query = `
            SELECT TOP 1 porc1 
            FROM descuen 
            WHERE co_desc = @co_art OR co_desc = @co_cat 
            ORDER BY CASE WHEN co_desc = @co_art THEN 1 ELSE 2 END`;

        const result = await pool.request()
            .input('co_art', sql.VarChar, co_art)
            .input('co_cat', sql.VarChar, co_cat)
            .query(query);

        return result.recordset.length > 0 ? Number(result.recordset[0].porc1) : 0.0;
    } catch (error) {
        console.error(`Error al obtener descuento para ${co_art}:`, error.message);
        return 0.0;
    }
}

/**
 * Obtiene la triada de porcentajes de descuento aplicables a un artículo 
 * según la política comercial de Crist Medicals.
 * * @param {Object} pool - Conexión activa a la base de datos SQL Server.
 * @param {string} cod_cliente - Código del cliente que realiza el pedido.
 * @param {string} co_art - Código del artículo (SKU).
 * @param {string} co_cat - Código de la categoría del artículo.
 * @param {string} co_prov - Código del proveedor/laboratorio del artículo.
 * @param {number} cantidadTotalProv - Cantidad total acumulada de artículos del mismo proveedor en el pedido.
 * @returns {Promise<string>} Cadena formateada para Profit Plus (Ej: "5.00+10.00+0.00").
 */

async function obtenerCadenaDescuentoTriple(pool, cod_cliente, co_art, co_cat, co_prov, cantidadTotalProv) {
    const co_prov_clean = co_prov && co_prov.trim().endsWith('.') 
        ? co_prov.trim().slice(0, -1).trim() 
        : co_prov.trim();

    let desc_cliente_prov = 0; 
    let desc_articulo_cat = 0; 
    let desc_general_ptc = 0;  

    try {
        const request = pool.request();

        // --- 1. BUSCAR DESCUENTO ESPECÍFICO (desc_prov) ---
        const resProv = await request
            .input('cp_1', sql.VarChar, co_prov_clean)
            .input('cc_1', sql.VarChar, cod_cliente)
            .query(`SELECT TOP 1 
                        co_prov, co_cli,
                        ISNULL(hasta1,0) as hasta1, ISNULL(hasta2,0) as hasta2, ISNULL(hasta3,0) as hasta3,
                        ISNULL(hasta4,0) as hasta4, ISNULL(hasta5,0) as hasta5,
                        ISNULL(porc1,0) as porc1, ISNULL(porc2,0) as porc2, ISNULL(porc3,0) as porc3,
                        ISNULL(porc4,0) as porc4, ISNULL(porc5,0) as porc5
                    FROM dbo.desc_prov 
                    WHERE (RTRIM(co_prov) = @cp_1 OR RTRIM(co_prov) = @cp_1 + '.') 
                    AND RTRIM(co_cli) = @cc_1`);
        
        if (resProv.recordset.length > 0) {
            desc_cliente_prov = calcularValorEscala(resProv.recordset[0], cantidadTotalProv);
        }

        // --- 2. DESCUENTO POR ARTÍCULO / CATEGORÍA ---
        const resDesc = await request
            .input('art_2', sql.VarChar, co_art)
            .input('cat_2', sql.VarChar, co_cat)
            .query(`SELECT TOP 1 porc1 FROM descuen 
                    WHERE co_desc = @art_2 OR co_desc = @cat_2 
                    ORDER BY CASE WHEN co_desc = @art_2 THEN 1 ELSE 2 END`);
        
        desc_articulo_cat = resDesc.recordset.length > 0 ? Number(resDesc.recordset[0].porc1) : 0;

        // --- 3. DESCUENTO GENERAL (desc_ptc) ---
        if (desc_cliente_prov === 0) {
            const resPtc = await request
                .input('cp_3', sql.VarChar, co_prov_clean)
                .query(`SELECT TOP 1 
                            co_prov,
                            ISNULL(hasta1,0) as hasta1, ISNULL(hasta2,0) as hasta2, ISNULL(hasta3,0) as hasta3,
                            ISNULL(hasta4,0) as hasta4, ISNULL(hasta5,0) as hasta5,
                            ISNULL(porc1,0) as porc1, ISNULL(porc2,0) as porc2, ISNULL(porc3,0) as porc3,
                            ISNULL(porc4,0) as porc4, ISNULL(porc5,0) as porc5
                        FROM dbo.desc_ptc 
                        WHERE (RTRIM(co_prov) = @cp_3 OR RTRIM(co_prov) = @cp_3 + '.') 
                        AND (tipo_cli IS NULL OR LTRIM(RTRIM(tipo_cli)) = '')`);
            
            if (resPtc.recordset.length > 0) {
                desc_general_ptc = calcularValorEscala(resPtc.recordset[0], cantidadTotalProv);
            }
        }

        return `${desc_cliente_prov.toFixed(2)}+${desc_articulo_cat.toFixed(2)}+${desc_general_ptc.toFixed(2)}`;

    } catch (error) {
        console.error(`Error en descuentos para Art ${co_art}:`, error.message);
        return "0.00+0.00+0.00";
    }
}

/**
 * Lógica de negocio para determinar el porcentaje según los rangos 'Hasta' de la tabla.
 * @param {Object} registroEscala - Fila obtenida de desc_prov o desc_ptc.
 * @param {number} totalCantidad - Cantidad acumulada a evaluar.
 */

function calcularValorEscala(registroEscala, totalCantidad) {
    const hasta1 = Number(registroEscala.hasta1 ?? 0);
    const hasta2 = Number(registroEscala.hasta2 ?? 0);
    const hasta3 = Number(registroEscala.hasta3 ?? 0);
    const hasta4 = Number(registroEscala.hasta4 ?? 0);
    const hasta5 = Number(registroEscala.hasta5 ?? 0);
    const porc1  = Number(registroEscala.porc1  ?? 0);
    const porc2  = Number(registroEscala.porc2  ?? 0);
    const porc3  = Number(registroEscala.porc3  ?? 0);
    const porc4  = Number(registroEscala.porc4  ?? 0);
    const porc5  = Number(registroEscala.porc5  ?? 0);

    let resultado = 0;

    if (hasta1 > 0 && totalCantidad >= hasta1) resultado = porc1;
    if (hasta2 > 0 && totalCantidad >= hasta2) resultado = porc2;
    if (hasta3 > 0 && totalCantidad >= hasta3) resultado = porc3;
    if (hasta4 > 0 && totalCantidad >= hasta4) resultado = porc4;
    if (hasta5 > 0 && totalCantidad >= hasta5) resultado = porc5;

    return resultado;
}

// Obtiene la tasa del dólar
async function obtenerTasaDolar(pool) {
  try {
    const result = await pool.request()
      .input('co_mone', sql.VarChar, 'US$')
      .query('SELECT cambio FROM moneda WHERE RTRIM(co_mone) = @co_mone');
    return result.recordset[0]?.cambio || 1.0;
  } catch (err) {
    console.error('Error en obtenerTasaDolar:', err.message);
    return 1.0; // Valor por defecto
  }
}

// Inserta el pedido en cotiz_c
async function insertarPedidoCotizC(pool, valores) {
  try {
    const columnas = Object.keys(valores).join(', ');
    const placeholders = Object.keys(valores).map((_, i) => `@p${i}`).join(', ');
    const request = pool.request();
    Object.values(valores).forEach((v, i) => request.input(`p${i}`, v));
    await request.query(`INSERT INTO cotiz_c (${columnas}) VALUES (${placeholders})`);
  } catch (err) {
    console.error('Error al insertar en cotiz_c:', err.message);
    throw err; // Propagar el error
  }
}

// Obtiene el precio de venta para un cliente y artículo específicos
async function obtenerPrecioVentaCliente(pool, co_cli, co_art) {
  try {
    // 1. Obtener el tipo de cliente
    const tipoResult = await pool.request()
      .input('co_cli', sql.VarChar, limpiarValor(co_cli))
      .query('SELECT tipo FROM clientes WHERE RTRIM(co_cli) = @co_cli');
    const tipo = tipoResult.recordset[0]?.tipo?.trim();
    if (!tipo) {
      console.error(`Tipo de cliente no encontrado para co_cli: ${co_cli}`);
      return 0;
    }

    // 2. Obtener el campo de precio que le corresponde
    const precioAResult = await pool.request()
      .input('tipo', sql.VarChar, tipo)
      .query('SELECT precio_a FROM tipo_cli WHERE RTRIM(tip_cli) = @tipo');
    const precioA = precioAResult.recordset[0]?.precio_a?.trim().toUpperCase();
    if (!precioA) {
      console.error(`Campo de precio (precio_a) no encontrado para tipo: ${tipo}`);
      return 0;
    }

    // 3. Seleccionar el campo de precio correcto
    let campoPrecio = 'prec_vta3'; // Por defecto
    if (precioA === 'PRECIO 1') campoPrecio = 'prec_vta1';
    else if (precioA === 'PRECIO 2') campoPrecio = 'prec_vta2';
    else if (precioA === 'PRECIO 3') campoPrecio = 'prec_vta3';
    else if (precioA === 'PRECIO 4') campoPrecio = 'prec_vta4';

    // 4. Obtener el precio de la tabla art
    const precioResult = await pool.request()
      .input('co_art', sql.VarChar, limpiarValor(co_art))
      .query(`SELECT ${campoPrecio} as precio FROM art WHERE RTRIM(co_art) = @co_art`);
    const precio = precioResult.recordset[0]?.precio || 0;
    return precio;
  } catch (err) {
    console.error(`Error en obtenerPrecioVentaCliente para co_cli: ${co_cli}, co_art: ${co_art}. Error: ${err.message}`);
    return 0;
  }
}

// Inserta registro en pistas
async function registrarPista(pool, fact_num, codigo_pedido, fecha_caracas) {
  try {
    // fecha_caracas se recibe desde el llamador para garantizar consistencia
    const valores = {
      usuario_id: 'API',
      usuario: 'PAGINA CRISTMEDICALS',
      fecha: fecha_caracas,
      empresa: 'CRISTM25',
      co_sucu: '01',
      tabla: 'cotiz_c',
      num_doc: fact_num,
      codigo: '',
      tipo_op: 'I',
      maquina: '198.12.221.39',
      campos: '',
      rowguid: uuidv4(),
      trasnfe: '',
      AUX01: 0,
      AUX02: ''
    };
    const columnas = Object.keys(valores).join(', ');
    const placeholders = Object.keys(valores).map((_, i) => `@p${i}`).join(', ');
    const request = pool.request();
    Object.values(valores).forEach((v, i) => request.input(`p${i}`, v));
    await request.query(`INSERT INTO pistas (${columnas}) VALUES (${placeholders})`);
  } catch (err) {
    console.error('Error al registrar en pistas:', err.message);
    throw err; // Propagar el error
  }
}

// --- Endpoint principal para insertar pedido ---

export const insertarPedidoProfit = async (req, res) => {

  const resultado = {
    pedido_montado: false,
    fact_nums: [],
    errores: [],
    detalles: []
  };

  try {
    const { cod_pedido, cod_cliente, items, porc_gdesc, campo3 } = req.body;

    if (!cod_cliente || !Array.isArray(items) || !cod_pedido || items.length === 0) {
      resultado.errores.push('Faltan datos requeridos (cod_cliente, items(no vacío), cod_pedido)');
      return res.status(400).json({ exito: false, mensaje: 'Datos inválidos', resultado });
    }

    const pedidoKey = String(cod_pedido);

    // ══════════════════════════════════════════════════════
    // BARRERA 1: Set en memoria (instantánea, mismo proceso)
    // ══════════════════════════════════════════════════════
    if (pedidosEnProceso.has(pedidoKey)) {
      return res.status(409).json({
        exito: false,
        mensaje: `El pedido ${cod_pedido} ya está siendo procesado`,
        resultado: { ...resultado, duplicado: true }
      });
    }
    pedidosEnProceso.add(pedidoKey);

    try {
      const pool = await sql.connect();
      const transaction = new sql.Transaction(pool);
      await transaction.begin();

      try {
        // ══════════════════════════════════════════════════════
        // BARRERA 2: sp_getapplock (protege múltiples instancias)
        // ══════════════════════════════════════════════════════
        const lockResult = await transaction.request()
          .input('Resource',    sql.VarChar, `pedido_${pedidoKey}`)
          .input('LockMode',    sql.VarChar, 'Exclusive')
          .input('LockOwner',   sql.VarChar, 'Transaction')
          .input('LockTimeout', sql.Int,     5000)
          .execute('sp_getapplock');

        // lockResult.returnValue: 0=ok, -1=timeout, -2=cancelado, -3=deadlock
        if (lockResult.returnValue < 0) {
          await transaction.rollback();
          return res.status(409).json({
            exito: false,
            mensaje: `No se pudo adquirir el lock para el pedido ${cod_pedido}`,
            resultado: { ...resultado, duplicado: true }
          });
        }

        // ══════════════════════════════════════════════════════
        // BARRERA 3: SELECT dentro de la transacción (red de seguridad BD)
        // ══════════════════════════════════════════════════════
        const pedidoExistente = await transaction.request()
          .input('cod_pedido', sql.VarChar, pedidoKey)
          .query(`
            SELECT fact_num
            FROM cotiz_c
            WHERE descrip LIKE 'Pedido numero ' + @cod_pedido + ' parte%'
          `);

        if (pedidoExistente.recordset.length > 0) {
          await transaction.rollback();
          const factNumsExistentes = pedidoExistente.recordset.map(r => r.fact_num);
          return res.status(409).json({
            exito: false,
            mensaje: `El pedido ${cod_pedido} ya fue procesado anteriormente`,
            resultado: {
              ...resultado,
              pedido_montado: true,
              fact_nums: factNumsExistentes,
              duplicado: true
            }
          });
        }

        // ══════════════════════════════════════════════════════
        // Lógica principal — igual que antes pero usando `transaction`
        // en lugar de `pool` en los inserts críticos
        // ══════════════════════════════════════════════════════
        const calcularCadenaTripleInterna = async (p, cl, art, cat, prov, tot) => {
          try {
            if (typeof obtenerCadenaDescuentoTriple === 'function') {
              return await obtenerCadenaDescuentoTriple(p, cl, art, cat, prov, tot);
            }
            return "0+0+0";
          } catch (e) { return "0+0+0"; }
        };

        const totalesPorProv = {};
        const codigosArticulos = items.map(i => `'${limpiarValor(i.co_art)}'`).join(",");
        const infoArticulosRes = await pool.request().query(`
          SELECT RTRIM(co_art) as co_art, RTRIM(co_prov) as co_prov
          FROM art WHERE co_art IN (${codigosArticulos})
        `);

        items.forEach(item => {
          const artBase = infoArticulosRes.recordset.find(a => a.co_art === limpiarValor(item.co_art));
          if (artBase && artBase.co_prov) {
            const cant = (Number(item.quantitySC) || 0) + (Number(item.quantityBQ) || 0);
            totalesPorProv[artBase.co_prov] = (totalesPorProv[artBase.co_prov] || 0) + cant;
          }
        });

        const co_ven        = await obtenerCoVenPorCliente(pool, cod_cliente);
        const desc_glob_db  = await obtenerDescGlobCliente(pool, cod_cliente);
        const tasa          = await obtenerTasaDolar(pool);
        const descuento_porc = (porc_gdesc !== undefined && porc_gdesc !== null)
          ? Number(porc_gdesc)
          : (desc_glob_db || 0);

        const aplicarEscalas = co_ven?.trim() !== '00027';
        const fecha_caracas_datetime = DateTime.now().setZone('America/Caracas').toFormat('yyyy-LL-dd HH:mm:ss');
        const fecha_emision = DateTime.now().setZone('America/Caracas').toFormat('yyyy-LL-dd') + ' 00:00:00.000';

        const MAX_ITEMS_POR_FACT = 21;
        const batches = [];
        for (let i = 0; i < items.length; i += MAX_ITEMS_POR_FACT) {
          batches.push(items.slice(i, i + MAX_ITEMS_POR_FACT));
        }

        for (let bIndex = 0; bIndex < batches.length; bIndex++) {
          const batchItems = batches[bIndex];
          const renglonesProcesados = [];
          let tot_bruto_usd      = 0.0;
          let base_imponible_usd = 0.0;
          const provConEscala    = new Set();

          for (const item of batchItems) {
            const cantidad = (Number(item.quantitySC) || 0) + (Number(item.quantityBQ) || 0);
            if (cantidad <= 0) continue;

            const co_art_limpio      = limpiarValor(item.co_art);
            const precio_cliente_vta = await obtenerPrecioVentaCliente(pool, cod_cliente, co_art_limpio);
            const subtotal_linea     = cantidad * precio_cliente_vta;

            const artResult = await pool.request()
              .input('co_art', sql.VarChar, co_art_limpio)
              .query('SELECT art_des, uni_venta, tipo_imp, co_cat, co_prov FROM art WHERE RTRIM(co_art) = @co_art');
            const artRow = artResult.recordset?.[0] || {};

            const des_art      = artRow.art_des?.trim()  || '';
            const uni_venta_db = artRow.uni_venta?.trim() || '';
            const tipo_imp_db  = Number(artRow.tipo_imp ?? item.tipo_imp ?? 1) || 6;
            const co_cat       = artRow.co_cat?.trim()   || '';
            const co_prov      = artRow.co_prov?.trim()  || '';

            const totalAcumuladoProv = totalesPorProv[co_prov] || 0;
            const porc_desc_cadena   = aplicarEscalas
              ? await calcularCadenaTripleInterna(pool, cod_cliente, co_art_limpio, co_cat, co_prov, totalAcumuladoProv)
              : "0.00+0.00+0.00";

            const tieneEscala = porc_desc_cadena.split('+').some(v => Number(v) > 0);
            if (tieneEscala && co_prov) provConEscala.add(co_prov);

            tot_bruto_usd += subtotal_linea;
            if (tipo_imp_db === 1) base_imponible_usd += subtotal_linea;

            renglonesProcesados.push({
              item, cantidad, subtotal_linea, co_art_limpio, des_art,
              uni_venta_db, tipo_imp_db, precio_cliente: precio_cliente_vta,
              porc_desc: porc_desc_cadena
            });
          }

          const campo5 = '';
          const descuento_monto_usd        = tot_bruto_usd * (descuento_porc / 100);
          const base_imponible_neta_usd    = base_imponible_usd * (1 - (descuento_porc / 100));
          const iva_usd                    = base_imponible_neta_usd * 0.16;
          const tot_neto_usd               = tot_bruto_usd - descuento_monto_usd + iva_usd;

          const fact_num = await obtenerNuevoFactNum(pool);
          if (!fact_num) {
            resultado.errores.push('No se pudo obtener el número de pedido correlativo.');
            await transaction.rollback();
            return res.status(500).json({ exito: false, mensaje: 'Error de correlativo', resultado });
          }
          resultado.fact_nums.push(fact_num);

          // ── Insertar cabecera dentro de la transacción ──
          try {
            const valoresCotizC = {
              fact_num,
              contrib: 1, nombre: '', rif: '', nit: '', status: 0,
              descrip: `Pedido numero ${cod_pedido} parte ${bIndex + 1} de ${batches.length}`,
              saldo:      tot_neto_usd * tasa,
              fec_emis:   fecha_emision,
              fec_venc:   fecha_caracas_datetime,
              co_cli:     limpiarValor(cod_cliente),
              co_ven,     co_tran: '03', forma_pag: '04',
              tot_bruto:  tot_bruto_usd * tasa,
              tot_neto:   tot_neto_usd * tasa,
              iva:        iva_usd * tasa,
              glob_desc:  descuento_monto_usd * tasa,
              porc_gdesc: descuento_porc,
              tasa,       moneda: 'US$', tasag: 16,
              campo3:     campo3 || '',
              campo5,
              co_us_in:   'API',
              fe_us_in:   fecha_caracas_datetime,
              co_sucu:    '01',
              rowguid:    uuidv4()
            };
            // Pasar `transaction` para que quede dentro del mismo scope atómico
            await insertarPedidoCotizC(transaction, valoresCotizC);
          } catch (err) {
            resultado.errores.push(`Error cotiz_c: ${err.message}`);
            await transaction.rollback();
            return res.status(500).json({ exito: false, mensaje: 'Error en cabecera', resultado });
          }

          // ── Insertar renglones ──
          let reng_num = 1;
          for (const dr of renglonesProcesados) {
            const insertarRenglon = async (alma, cant) => {
              if (cant <= 0) return;
              const v = {
                fact_num, reng_num,
                co_art:    dr.co_art_limpio,
                co_alma:   alma,
                total_art: cant, pendiente: cant,
                uni_venta: dr.uni_venta_db,
                prec_vta:  (dr.precio_cliente * tasa),
                porc_desc: dr.porc_desc,
                tipo_imp:  dr.tipo_imp_db,
                reng_neto: ((cant * dr.precio_cliente) * tasa),
                prec_vta2: dr.precio_cliente,
                des_art:   dr.des_art,
                rowguid:   uuidv4(),
                fec_lote:  fecha_caracas_datetime
              };
              const cols   = Object.keys(v).join(', ');
              const params = Object.keys(v).map((_, i) => `@p${i}`).join(', ');
              const reqReng = transaction.request();           // ← usa transaction
              Object.values(v).forEach((val, i) => reqReng.input(`p${i}`, val));
              await reqReng.query(`INSERT INTO reng_cac (${cols}) VALUES (${params})`);
              reng_num++;
            };

            try {
              await insertarRenglon('01', Number(dr.item.quantitySC) || 0);
              await insertarRenglon('04', Number(dr.item.quantityBQ) || 0);
            } catch (err) {
              resultado.errores.push(`Error renglón ${dr.co_art_limpio}: ${err.message}`);
            }
          }

          await registrarPista(pool, fact_num, cod_pedido, fecha_caracas_datetime).catch(() => {});
        }

        // ── Todo OK: commit ──
        await transaction.commit();

        resultado.pedido_montado = resultado.fact_nums.length > 0 && resultado.errores.length === 0;
        return res.status(201).json({ exito: resultado.pedido_montado, mensaje: 'Proceso finalizado', resultado });

      } catch (err) {
        // Error inesperado dentro de la transacción
        try { await transaction.rollback(); } catch (_) {}
        throw err;
      }

    } finally {
      // SIEMPRE liberar el Set, pase lo que pase
      pedidosEnProceso.delete(pedidoKey);
    }

  } catch (error) {
    return res.status(500).json({ exito: false, mensaje: error.message, resultado });
  }
};

/**
 * Registra la auditoría en la tabla 'pistas' para cada lote de pedidos insertado.
 * @param {Object} pool - Conexión activa a SQL Server.
 * @param {number} fact_num - Correlativo del pedido generado en Profit (pedidos.fact_num).
 * @param {string} codigo_pedido - ID o número de referencia del sistema origen (Web/App).
 * @param {string} fecha_caracas - Fecha y hora actual formateada.
 */

async function registrarPistaPedido(pool, fact_num, codigo_pedido, fecha_caracas) {
  try {
    const valores = {
      usuario_id: 'API',
      usuario: 'PAGINA CRISTMEDICALS',
      fecha: fecha_caracas,
      empresa: 'CRISTM25',
      co_sucu: '01',
      tabla: 'pedidos',     // Refleja el nombre exacto de la tabla de cabecera
      num_doc: fact_num,
      codigo: '',
      tipo_op: 'I',         // 'I' representa Insertar (operación Profit estándar)
      maquina: '198.12.221.39',
      campos: `Pedido origen: ${codigo_pedido}`, // Referencia cruzada útil
      rowguid: uuidv4(),
      trasnfe: '',
      AUX01: 0,
      AUX02: ''
    };

    const request = pool.request();
    
    // Construcción de la consulta dinámica
    const columnas = Object.keys(valores).join(', ');
    const placeholders = Object.keys(valores).map((key) => `@${key}`).join(', ');

    // Mapeo de parámetros
    Object.entries(valores).forEach(([key, value]) => {
      request.input(key, value);
    });

    await request.query(`INSERT INTO pistas (${columnas}) VALUES (${placeholders})`);
    
  } catch (err) {
    console.error('Error al registrar auditoría en pistas:', err.message);
    // No detenemos el flujo principal si falla la pista, pero lo reportamos
    throw err; 
  }
}

// Psicotropicos - Endpoint específico para pedidos de este tipo, con validaciones y lógica adaptada
export const insertarPedido = async (req, res) => {
  const resultado = {
    pedido_montado: false,
    fact_nums: [],
    errores: [],
    detalles: []
  };

  try {
    const { cod_pedido, cod_cliente, items, porc_gdesc } = req.body;
    
    // 1. Validaciones de entrada
    if (!cod_cliente || !Array.isArray(items) || !cod_pedido || items.length === 0) {
      resultado.errores.push('Faltan datos requeridos (cod_cliente, items, cod_pedido)');
      return res.status(400).json({ exito: false, mensaje: 'Datos inválidos', resultado });
    }

    const pool = await sql.connect();

    // 2. Obtener datos maestros
    const co_ven = await obtenerCoVenPorCliente(pool, cod_cliente);
    const desc_glob_db = await obtenerDescGlobCliente(pool, cod_cliente);
    const tasa = await obtenerTasaDolar(pool);
    
    const descuento_porc = await obtenerDescGlobCliente(pool, cod_cliente);

    const ahora_caracas = DateTime.now().setZone('America/Caracas');
    const fecha_caracas_datetime = ahora_caracas.toFormat('yyyy-MM-dd HH:mm:ss');
    const fecha_emision = ahora_caracas.toFormat('yyyy-MM-dd') + ' 00:00:00.000';

    // 3. Segmentación por lotes
    const MAX_ITEMS_POR_FACT = 21;
    const batches = [];
    for (let i = 0; i < items.length; i += MAX_ITEMS_POR_FACT) {
      batches.push(items.slice(i, i + MAX_ITEMS_POR_FACT));
    }

    // 4. Procesamiento
    for (let bIndex = 0; bIndex < batches.length; bIndex++) {
      const batchItems = batches[bIndex];
      const renglonesProcesados = [];
      let tot_bruto_usd = 0.0;
      let base_imponible_usd = 0.0;

      for (const item of batchItems) {
        const cantidad = (Number(item.quantitySC) || 0) + (Number(item.quantityBQ) || 0);
        if (cantidad <= 0) continue;
        // const precio_cliente_vta = await obtenerPrecioVentaCliente(pool, cod_cliente, co_art_limpio);

        const precio = await obtenerPrecioVentaCliente(pool, cod_cliente, co_art_limpio) || 0;
        
        const subtotal_linea = cantidad * precio;
        const co_art_limpio = item.co_art.trim();

        // Obtenemos datos del artículo incluyendo CATEGORÍA para el descuento
        const artResult = await pool.request()
            .input('co_art', sql.VarChar, co_art_limpio)
            .query('SELECT art_des, uni_venta, tipo_imp, co_cat FROM art WHERE RTRIM(co_art) = @co_art');
        
        const artRow = artResult.recordset?.[0] || {};
        const tipo_imp_db = String(artRow.tipo_imp || '1'); 
        const co_cat = artRow.co_cat?.trim() || '';

        // Buscamos el descuento específico o por categoría
        const porc_desc = await obtenerDescuentoArticulo(item.co_art, co_cat);

        tot_bruto_usd += subtotal_linea;
        if (tipo_imp_db === '1') base_imponible_usd += subtotal_linea;

        renglonesProcesados.push({
          item, cantidad, precio, co_art_limpio,
          des_art: artRow.art_des?.trim() || 'ARTICULO PSICOTROPICO',
          uni_venta: artRow.uni_venta?.trim() || 'UND',
          tipo_imp: tipo_imp_db,
          porc_desc_renglon // Guardamos el 0.00+X.XX+0.00
        });
      }

      // Cálculos Financieros
      const desc_monto_usd = tot_bruto_usd * (descuento_porc / 100);
      const base_neta_usd = base_imponible_usd * (1 - (descuento_porc / 100));
      const iva_usd = base_neta_usd * 0.16;
      const total_neto_usd = tot_bruto_usd - desc_monto_usd + iva_usd;

      const fact_num = await obtenerNuevoFactNum(pool);
      if (!fact_num) throw new Error('Error al generar correlativo de Pedido');

      // A. INSERT CABECERA
      await pool.request()
        .input('fact_num', sql.Int, fact_num)
        .input('fec_emis', sql.SmallDateTime, fecha_emision)
        .input('fec_venc', sql.SmallDateTime, fecha_caracas_datetime)
        .input('co_cli', sql.Char(10), cod_cliente)
        .input('co_ven', sql.Char(6), co_ven)
        .input('tot_bruto', sql.Decimal(18, 2), (tot_bruto_usd * tasa))
        .input('tot_neto', sql.Decimal(18, 2), (total_neto_usd * tasa))
        .input('iva', sql.Decimal(18, 2), (iva_usd * tasa))
        .input('glob_desc', sql.Decimal(18, 2), (desc_monto_usd * tasa))
        .input('porc_gdesc', sql.Char(15), descuento_porc.toString())
        .input('tasa', sql.Decimal(18, 5), tasa)
        .input('moneda', sql.Char(6), 'US$')
        .input('co_us_in', sql.Char(6), 'API_PS')
        .input('fe_us_in', sql.DateTime, fecha_caracas_datetime)
        .input('descrip', sql.VarChar(60), `PSICO WEB ${cod_pedido} LOTE ${bIndex + 1}`)
        .input('co_sucu', sql.Char(6), '01')
        .input('status', sql.Char(1), '0')
        .input('forma_pag', sql.Char(6), '04')
        .input('co_tran', sql.Char(6), '03')
        .input('rowguid', sql.UniqueIdentifier, uuidv4())
        .query(`INSERT INTO CRISTM25.dbo.pedidos 
                (fact_num, fec_emis, fec_venc, co_cli, co_ven, tot_bruto, tot_neto, iva, glob_desc, porc_gdesc, tasa, moneda, co_us_in, fe_us_in, descrip, co_sucu, status, forma_pag, co_tran, rowguid)
                VALUES 
                (@fact_num, @fec_emis, @fec_venc, @co_cli, @co_ven, @tot_bruto, @tot_neto, @iva, @glob_desc, @porc_gdesc, @tasa, @moneda, @co_us_in, @fe_us_in, @descrip, @co_sucu, @status, @forma_pag, @co_tran, @rowguid)`);

      // B. INSERT RENGLONES
      let reng_num = 1;
      for (const rp of renglonesProcesados) {
        const almacenes = [
          { co: '01', cant: Number(rp.item.quantitySC) || 0 },
          { co: '04', cant: Number(rp.item.quantityBQ) || 0 }
        ];

        for (const alm of almacenes) {
          if (alm.cant <= 0) continue;
          await pool.request()
            .input('f_num', sql.Int, fact_num)
            .input('r_num', sql.Int, reng_num++)
            .input('art', sql.Char(30), rp.co_art_limpio)
            .input('alm', sql.Char(6), alm.co)
            .input('cant', sql.Decimal(18, 5), alm.cant)
            .input('prec', sql.Decimal(18, 5), (rp.precio * tasa))
            .input('imp', sql.Char(1), rp.tipo_imp)
            .input('neto', sql.Decimal(18, 2), (alm.cant * rp.precio * tasa))
            .input('des', sql.VarChar(sql.MAX), rp.des_art)
            .input('uni', sql.Char(6), rp.uni_venta)
            .input('guid', sql.UniqueIdentifier, uuidv4())
            .input('p_desc', sql.Char(15), rp.porc_desc_renglon) // Insertamos el formato 0.00+X.XX+0.00
            .query(`INSERT INTO CRISTM25.dbo.reng_ped 
                    (fact_num, reng_num, co_art, co_alma, total_art, pendiente, prec_vta, tipo_imp, reng_neto, des_art, uni_venta, rowguid, porc_desc)
                    VALUES 
                    (@f_num, @r_num, @art, @alm, @cant, @cant, @prec, @imp, @neto, @des, @uni, @guid, @p_desc)`);
        }
      }

      // C. AUDITORÍA
      await registrarPistaPedido(pool, fact_num, `PSICO-${cod_pedido}`, fecha_caracas_datetime);
      
      resultado.fact_nums.push(fact_num);
    }

    resultado.pedido_montado = true;
    return res.status(201).json({ exito: true, mensaje: 'Pedido Psico Procesado', resultado });

  } catch (error) {
    console.error('Error Crítico en Psico:', error.message);
    resultado.errores.push(error.message);
    return res.status(500).json({ exito: false, mensaje: 'Error interno en el servidor', resultado });
  }
};