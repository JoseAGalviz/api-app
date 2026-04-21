import { sql } from '../config/database.js';

// Mapeo de tipo a días de crédito para la lógica de scan_fac.js
const tipoDias = {
    "15": 15,
    "18": 18,
    "18T": 18,
    "21": 21,
    "21T": 21,
    "24": 24,
    "24T": 24,
    "30": 30,
    "30E": 30,
    "30E2": 30,
    "30T": 30,
    "35": 35,
    "40": 40,
    "40T": 40,
    "45T": 45
};

// Función auxiliar para transformar el número de factura
function transformarNumFactura(num_factura) {
    if (/^A\d{7}$/.test(num_factura)) {
        if (num_factura.startsWith('A2')) {
            return String(7) + num_factura.slice(1);
        }
        return String(Number(num_factura.slice(1)));
    }
    if (/^B\d{7}$/.test(num_factura)) {
        const serie = num_factura.slice(1);
        if (serie < '0050000') {
            return '8' + serie;
        }
        return '5' + serie;
    }
    return num_factura;
}

// Función para extraer los días de crédito del tipo de cliente
function obtenerDiasCredito(tipo) {
    const match = (tipo || '').toString().match(/\d+/);
    return match ? parseInt(match[0], 10) : null;
}

// Lógica del antiguo facturas.js
export const getFacturasVencidas = async (req, res) => {
    let codigos = req.query.clientes;
    if (!codigos) {
        return res.status(400).json({ error: 'Debe enviar el parámetro clientes' });
    }
    if (typeof codigos === 'string') {
        codigos = codigos.split(',').map(c => c.trim()).filter(Boolean);
    }

    try {
        const inParams = codigos.map((_, i) => `@cli${i}`).join(',');
        const paramsObj = {};
        codigos.forEach((c, i) => paramsObj[`cli${i}`] = c);

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
        const cleaned = result.recordset.map(row => ({
            co_cli: row.co_cli?.trim(),
            cli_des: row.cli_des?.trim(),
            SaldoVencido: row.SaldoVencido ? String(row.SaldoVencido).trim() : "0"
        }));

        res.json(cleaned);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
};

// Lógica del antiguo scan_fac.js (POST /facturas/scan)
export const scanFactura = async (req, res) => {
    let { num_factura } = req.body;
    if (!num_factura || typeof num_factura !== 'string' || !num_factura.trim()) {
        return res.status(400).json({ error: 'El campo num_factura es obligatorio y debe ser un string no vacío.' });
    }

    try {
        const num_factura_bd = transformarNumFactura(num_factura);
        const queryFactura = `
            SELECT fact_num, co_cli, co_ven, CAST(fec_emis AS DATE) AS fec_emis, fec_venc, campo8
            FROM dbo.factura
            WHERE fact_num = @fact_num
        `;
        const requestFactura = new sql.Request();
        requestFactura.input('fact_num', sql.VarChar, num_factura_bd);
        const resultFactura = await requestFactura.query(queryFactura);

        if (!resultFactura.recordset.length) {
            return res.status(404).json({ error: 'No se encontró la factura correspondiente.' });
        }

        const { fact_num, co_cli, co_ven, fec_emis, fec_venc, campo8 } = resultFactura.recordset[0];

        // Verifica si ya fue escaneada
        if (campo8 && campo8.trim() === 'CHEQUEADO APP') {
            return res.status(400).json({ error: 'Factura ya escaneada previamente.' });
        }

        const fec_venc_antes = fec_venc;

        const queryCliente = `SELECT tipo, cli_des, co_zon, co_seg, fax FROM dbo.clientes WHERE co_cli = @co_cli`;
        const requestCliente = new sql.Request();
        requestCliente.input('co_cli', sql.VarChar, co_cli);
        const resultCliente = await requestCliente.query(queryCliente);

        let tipo = null, cli_des = null, tipo_limpio = null, dias_credito = null, co_zon = null, co_seg = null, fax = null;
        if (resultCliente.recordset.length) {
            tipo = resultCliente.recordset[0].tipo;
            cli_des = resultCliente.recordset[0].cli_des;
            co_zon = resultCliente.recordset[0].co_zon;
            co_seg = resultCliente.recordset[0].co_seg;
            tipo_limpio = (tipo || '').toString().replace(/[^A-Za-z0-9]/g, '').toUpperCase();
            dias_credito = obtenerDiasCredito(tipo_limpio);
            fax = resultCliente.recordset[0].fax;
        }

        let zon_des = null;
        let campo1 = null;
        if (co_zon) {
            const queryZona = `SELECT zon_des, campo1 FROM dbo.zona WHERE co_zon = @co_zon`;
            const requestZona = new sql.Request();
            requestZona.input('co_zon', sql.VarChar, co_zon);
            const resultZona = await requestZona.query(queryZona);
            if (resultZona.recordset.length) {
                zon_des = resultZona.recordset[0].zon_des;
                campo1 = resultZona.recordset[0].campo1;
            }
        }

        let seg_des = null;
        if (co_seg) {
            const querySeg = `SELECT seg_des FROM dbo.segmento WHERE co_seg = @co_seg`;
            const requestSeg = new sql.Request();
            requestSeg.input('co_seg', sql.VarChar, co_seg);
            const resultSeg = await requestSeg.query(querySeg);
            if (resultSeg.recordset.length) {
                seg_des = resultSeg.recordset[0].seg_des;
            }
        }

        const fechaEscaneo = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Caracas" }));
        let fec_venc_despues = null;
        if (dias_credito) {
            const nuevaFecha = new Date(fechaEscaneo);
            nuevaFecha.setDate(nuevaFecha.getDate() + dias_credito);
            const yyyy = nuevaFecha.getFullYear();
            const mm = String(nuevaFecha.getMonth() + 1).padStart(2, '0');
            const dd = String(nuevaFecha.getDate()).padStart(2, '0');
            fec_venc_despues = `${yyyy}-${mm}-${dd}`;
        }

        // Limpieza de espacios en blanco en los valores string
        const limpiar = v => (typeof v === 'string' ? v.trim() : v);

        // Validación de rango de horas entre fec_emis y fecha actual usando campo1 y reglas especiales
        let estado_rango = null;
        let horas_extra_aplicadas = false;
        if (fec_emis && campo1) {
            // campo1 esperado: "48 HORAS", "24 HORAS", etc.
            const matchHoras = (campo1 || '').toString().match(/(\d+)\s*HORAS/i);
            if (matchHoras) {
                let horasLimite = parseInt(matchHoras[1], 10);
                // Ajuste de 48 horas según reglas
                const factNumInt = parseInt(fact_num, 10);
                const faxStr = (fax || '').toString().trim();
                if ((factNumInt > 72000000 && faxStr === '01') || (factNumInt < 72000000 && faxStr === '02')) {
                    horasLimite += 48;
                    horas_extra_aplicadas = true;
                }
                const fechaEmision = new Date(fec_emis);
                const fechaActual = fechaEscaneo;
                // Diferencia en milisegundos
                const diffMs = fechaActual - fechaEmision;
                // Diferencia en horas
                const diffHoras = diffMs / (1000 * 60 * 60);
                if (diffHoras > horasLimite) {
                    estado_rango = "FUERA DE RANGO";
                } else {
                    estado_rango = "DENTRO DEL RANGO";
                }
            }
        }

        res.json({
            fact_num: limpiar(fact_num),
            co_cli: limpiar(co_cli),
            cli_des: limpiar(cli_des),
            tipo: limpiar(tipo_limpio),
            dias_credito,
            fec_emis: limpiar(fec_emis),
            fec_venc_antes: limpiar(fec_venc_antes),
            fec_venc_despues: limpiar(fec_venc_despues),
            fecha_escaneo: limpiar(fechaEscaneo.toISOString().slice(0, 10)),
            co_ven: limpiar(co_ven),
            co_zon: limpiar(co_zon),
            zon_des: limpiar(zon_des),
            co_seg: limpiar(co_seg),
            seg_des: limpiar(seg_des),
            campo1,
            fax: limpiar(fax),
            estado_rango,
            horas_extra_aplicadas
        });
    } catch (err) {
        console.error('Error en consulta:', err);
        res.status(500).json({ error: 'Error al buscar la factura' });
    }
};

// Lógica del antiguo scan_fac/update.js
export const updateFacturaFecha = async (req, res) => {

    let { fact_num, fec_venc_despues } = req.body;

    if (!Array.isArray(fact_num) || !Array.isArray(fec_venc_despues) || fact_num.length !== fec_venc_despues.length || fact_num.length === 0) {
        return res.status(400).json({ error: 'fact_num y fec_venc_despues deben ser arrays del mismo tamaño y no vacíos.' });
    }

    const resultados = [];

    for (let i = 0; i < fact_num.length; i++) {
        let num = String(fact_num[i]);
        let fecha = fec_venc_despues[i];

        if (!num || typeof num !== 'string' || !num.trim() || !fecha) {
            resultados.push({ fact_num: num, exito: false, error: 'Datos inválidos.' });
            continue;
        }

        // Suma 1 día a la fecha recibida respetando zona horaria local
        const fechaBase = new Date(fecha + 'T00:00:00');
        fechaBase.setDate(fechaBase.getDate() + 1);
        const fechaFinal = fechaBase.toISOString().split('T')[0];

        try {
            // Consulta campo8 y fec_emis antes de actualizar
            const selectFactura = `
                SELECT campo8, CAST(fec_emis AS DATE) AS fec_emis FROM dbo.factura WHERE fact_num = @fact_num
            `;
            const selectRequest = new sql.Request();
            selectRequest.input('fact_num', sql.VarChar, num);
            const selectResult = await selectRequest.query(selectFactura);
            const campo8Antes = selectResult.recordset.length ? selectResult.recordset[0].campo8 : null;
            const fec_emis = selectResult.recordset.length ? selectResult.recordset[0].fec_emis : null;

            // Si ya está chequeado, no actualizar ni escanear
            if (campo8Antes && campo8Antes.trim() === 'CHEQUEADO APP') {
                resultados.push({ fact_num: num, exito: false, error: 'Factura ya escaneada previamente.' });
                continue;
            }

            // Validación: si han pasado más de 7 días desde fec_emis, bloquear la actualización
            if (fec_emis) {
                const fechaActual = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Caracas" }));
                const fechaEmision = new Date(fec_emis);
                const diffMs = fechaActual - fechaEmision;
                const diffDias = diffMs / (1000 * 60 * 60 * 24);
                if (diffDias > 7) {
                    console.warn(`[BLOQUEADO] fact_num: ${num} supera 1 semana desde fec_emis (${diffDias.toFixed(1)} días). No se actualizará.`);
                    resultados.push({ fact_num: num, exito: false, error: 'La factura supera 1 semana desde su emisión y no puede ser escaneada.' });
                    continue;
                }
            }

            // Actualiza fec_venc y campo8 en factura
            const updateFactura = `
                UPDATE dbo.factura
                SET fec_venc = @fec_venc_despues,
                    campo8 = @campo8
                WHERE fact_num = @fact_num
            `;
            const updateRequest = new sql.Request();
            updateRequest.input('fec_venc_despues', sql.Date, fechaFinal);
            updateRequest.input('campo8', sql.VarChar, 'CHEQUEADO APP');
            updateRequest.input('fact_num', sql.VarChar, num);
            const updateResult = await updateRequest.query(updateFactura);

            // Verifica si se actualizó alguna fila
            if (!updateResult.rowsAffected || updateResult.rowsAffected[0] === 0) {
                console.error(`[ERROR] No se actualizó ninguna factura con fact_num: ${num}`);
                resultados.push({ fact_num: num, exito: false, error: 'No se encontró la factura para actualizar.' });
                continue;
            }

            // Consulta el valor actualizado de campo8
            const selectResultDespues = await selectRequest.query(selectFactura);
            const campo8Despues = selectResultDespues.recordset.length ? selectResultDespues.recordset[0].campo8 : null;

            if (campo8Antes === campo8Despues) {
                console.warn(`[WARN] campo8 NO CAMBIÓ para fact_num: ${num}`);
            }

            // Solo actualiza fec_venc en docum_cc
            const queryDocumCC = `
                SELECT tipo_doc
                FROM dbo.docum_cc
                WHERE nro_doc = @fact_num AND tipo_doc = 'FACT'
            `;
            const requestDocumCC = new sql.Request();
            requestDocumCC.input('fact_num', sql.VarChar, num);
            const resultDocumCC = await requestDocumCC.query(queryDocumCC);

            if (resultDocumCC.recordset.length) {
                const updateDocumCC = `
                    UPDATE dbo.docum_cc
                    SET fec_venc = @fec_venc_despues
                    WHERE nro_doc = @fact_num AND tipo_doc = 'FACT'
                `;
                const updateRequestDocumCC = new sql.Request();
                updateRequestDocumCC.input('fec_venc_despues', sql.Date, fechaFinal);
                updateRequestDocumCC.input('fact_num', sql.VarChar, num);
                await updateRequestDocumCC.query(updateDocumCC);
            }

            resultados.push({ fact_num: num, exito: true });
        } catch (err) {
            console.error('Error actualizando factura/docum_cc:', err);
            resultados.push({ fact_num: num, exito: false, error: 'Error al actualizar la factura/docum_cc' });
        }
    }

    res.json(resultados);
};

// Lógica del antiguo totales.js
export const getTotales = async (req, res) => {
    try {
        const { segmentos } = req.body;
        if (!segmentos || !Array.isArray(segmentos) || segmentos.length === 0) {
            return res.status(400).json({ error: 'Debe enviar un array de segmentos en el campo "segmentos"' });
        }

        const hoyStr = new Date().toISOString().slice(0, 10);
        const inParams = segmentos.map((_, i) => `@seg${i}`).join(',');
        const paramsObj = {};
        segmentos.forEach((seg, i) => paramsObj[`seg${i}`] = seg);

        const query = `
            SELECT 
                c.co_cli,
                SUM(f.saldo) AS saldo_vencido
            FROM dbo.clientes c
            INNER JOIN dbo.factura f ON c.co_cli = f.co_cli
            WHERE c.co_seg IN (${inParams})
            AND CAST(f.fec_venc AS DATE) < @hoy
            GROUP BY c.co_cli
        `;

        const request = new sql.Request();
        request.input('hoy', sql.Date, hoyStr);
        for (const key in paramsObj) {
            request.input(key, sql.VarChar, paramsObj[key]);
        }

        const result = await request.query(query);
        const clientesOrdenados = result.recordset
            .map(row => ({
                co_cli: row.co_cli,
                saldo_vencido: row.saldo_vencido ? Number(row.saldo_vencido) : 0
            }))
            .sort((a, b) => b.saldo_vencido - a.saldo_vencido);

        const top3 = clientesOrdenados.slice(0, 3).map(c => ({
            co_cli: c.co_cli,
            saldo_vencido: c.saldo_vencido.toFixed(2)
        }));

        const saldo_vencido_total = clientesOrdenados
            .reduce((acc, c) => acc + c.saldo_vencido, 0)
            .toFixed(2);

        const response = {
            total_clientes: result.recordset.length,
            saldo_vencido_total,
            top3
        };

        res.json(response);
    } catch (err) {
        console.error('Error al consultar saldos vencidos:', err);
        res.status(500).json({ error: 'Error al consultar saldos vencidos' });
    }
};