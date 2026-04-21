import { getMysqlPool } from '../config/database.js';
import mysql from 'mysql2/promise';

// Función auxiliar para obtener la fecha de Venezuela (GMT-4)
function getFechaVenezuela() {
    const now = new Date();
    const venezuelaOffset = -4 * 60; // minutos
    const localOffset = now.getTimezoneOffset();
    const venezuelaDate = new Date(now.getTime() + (venezuelaOffset - localOffset) * 60000);
    return venezuelaDate.toISOString().replace('T', ' ').substring(0, 19);
}

const remoteConfig = {
    host: process.env.DB_VISOR_HOST,
    user: process.env.DB_VISOR_USER,
    password: process.env.DB_VISOR_PASSWORD,
    database: process.env.DB_VISOR_DATABASE,
    port: 3306
};

// POST /guias/procesar
export const procesarGuia = async (req, res) => {
    const { num_guia, conductor, ruta, vehiculo, comentario, estatus, coordenada, fecha } = req.body;

    if (!num_guia || !conductor || !ruta || !vehiculo || !comentario || !estatus || !coordenada || !fecha) {
        return res.status(400).json({ error: 'Faltan campos obligatorios' });
    }

    let connection;
    try {
        const pool = getMysqlPool();
        if (!pool) return res.status(500).json({ error: 'MySQL no está inicializado' });
        
        connection = await pool.getConnection();
        await connection.query(
            `INSERT INTO guias_procesadas 
            (num_guia, conductor, ruta, vehiculo, comentario, estatus, coordenada, fecha)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [num_guia, conductor, ruta, vehiculo, comentario, estatus, coordenada, fecha]
        );
        // Actualizar el status de reng_guia cuando id_ca = num_guia
        await connection.query(
            'UPDATE reng_guia SET status = ? WHERE id_ca = ?',
            ['cargado', num_guia]
        );
        // Actualizar el status de guia cuando id_ca = num_guia
        await connection.query(
            'UPDATE guia SET status = ? WHERE id_ca = ?',
            ['cargado', num_guia]
        );
        res.json({ exito: true, mensaje: 'Guía procesada y guardada correctamente' });
    } catch (err) {
        console.error('Error al guardar la guía:', err);
        res.status(500).json({ exito: false, error: 'No se pudo guardar la guía', detalle: err.message });
    } finally {
        if (connection) connection.release();
    }
};
// POST /guias/buscar-carga
export const buscarCarga = async (req, res) => {
    const { numeroCarga } = req.body;
    if (!numeroCarga) {
        return res.status(400).json({ error: 'Falta el número de carga' });
    }

    let remoteConnection;
    try {
        remoteConnection = await mysql.createConnection(remoteConfig);
        const [detalleRows] = await remoteConnection.query(
            'SELECT factura, nota, paquetes, descrip, vendedor, responsable FROM detalle WHERE id_ca = ? AND UPPER(COALESCE(status, \'\')) <> ?',
            [numeroCarga, 'NOO']
        );
        const [cargadoRows] = await remoteConnection.query(
            'SELECT ruta, conductor, vehiculo, realizado, estatus FROM cargado WHERE id = ?',
            [numeroCarga]
        );
        res.json({ detalle: detalleRows, cargado: cargadoRows });
    } catch (err) {
        console.error('Error de conexión remota o consulta:', err);
        res.status(500).json({ error: 'No se pudo conectar al servidor remoto o consultar datos' });
    } finally {
        if (remoteConnection) await remoteConnection.end();
    }
};

// POST /guias/guardar-carga
export const guardarCarga = async (req, res) => {
    const { detalle, cargado, ok, id_ca } = req.body;
    // Mostrar TODO lo que llega al endpoint para diagnóstico

    if (!ok || !Array.isArray(detalle) || !Array.isArray(cargado)) {
        return res.status(400).json({ error: 'JSON inválido. Debe incluir ok, detalle y cargado.' });
    }

    let localConnection;
    try {
        const pool = getMysqlPool();
        if (!pool) return res.status(500).json({ error: 'MySQL no está inicializado' });

        localConnection = await pool.getConnection();
        const fechaRegistro = getFechaVenezuela();
        const status = 'pendiente';
        const status_dos = 'pendiente';

        // Comprobación robusta: registrar datos recibidos y verificar existencia usando TRIM/COALESCE

        if (!Array.isArray(cargado) || cargado.length === 0) {
            return res.status(400).json({ error: 'No hay datos de cargado para insertar' });
        }

        const guia0 = cargado[0];
        const params = [
            String(guia0.ruta ?? '').trim(),
            String(guia0.conductor ?? '').trim(),
            String(guia0.vehiculo ?? '').trim(),
            String(guia0.realizado ?? '').trim()
        ];

        // Si el cliente envía id_ca, usamos para permitir mismo id_ca pero detectar si existe otra guía con mismos datos
        const providedIdCa = id_ca ? String(id_ca) : null;
        let existingRows;

        // Obtener columnas reales de la tabla `guia` para evitar insertar campos inexistentes
        const [colsRows] = await localConnection.query(
          "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'guia'"
        );
        const guiaColumns = colsRows.map(r => r.COLUMN_NAME);

        if (providedIdCa) {
            // Si nos envían id_ca, primero comprobar si ya existe esa id en la tabla
            const [idRows] = await localConnection.query(
                'SELECT id_ca FROM guia WHERE id_ca = ? LIMIT 1',
                [providedIdCa]
            );

            if (idRows.length > 0) {
                // Ya existe la fila con ese id_ca: actualizamos sus campos para mantener consistencia

                // Construir UPDATE dinámico solo con columnas existentes
                const updateParts = [];
                const updateParams = [];
                if (guiaColumns.includes('ruta')) { updateParts.push('ruta = ?'); updateParams.push(params[0]); }
                if (guiaColumns.includes('conductor')) { updateParts.push('conductor = ?'); updateParams.push(params[1]); }
                if (guiaColumns.includes('vehiculo')) { updateParts.push('vehiculo = ?'); updateParams.push(params[2]); }
                if (guiaColumns.includes('realizado')) { updateParts.push('realizado = ?'); updateParams.push(params[3]); }
                if (guiaColumns.includes('estatus') && guia0 && Object.prototype.hasOwnProperty.call(guia0, 'estatus')) { updateParts.push('estatus = ?'); updateParams.push(guia0.estatus); }
                if (guiaColumns.includes('fecha')) { updateParts.push('fecha = ?'); updateParams.push(fechaRegistro); }
                if (guiaColumns.includes('status')) { updateParts.push('status = ?'); updateParams.push(status); }

                if (updateParts.length > 0) {
                  updateParams.push(providedIdCa);
                  await localConnection.query(
                    `UPDATE guia SET ${updateParts.join(', ')} WHERE id_ca = ?`,
                    updateParams
                  );
                }

                // También insertar los renglones (se realizará más abajo). No devolver 409.
                existingRows = [];
            } else {
                // id_ca proporcionado pero no existe en la tabla: permitimos insertar un nuevo registro
                existingRows = [];
            }
        } else {
            // No se proporcionó id_ca: cualquier coincidencia es conflicto
            const query = 'SELECT id_ca, ruta, conductor, vehiculo, realizado FROM guia WHERE TRIM(COALESCE(ruta, "")) = ? AND TRIM(COALESCE(conductor, "")) = ? AND TRIM(COALESCE(vehiculo, "")) = ? AND TRIM(COALESCE(realizado, "")) = ? LIMIT 1';
            const [rows] = await localConnection.query(query, params);
            existingRows = rows;
        }

        if (existingRows.length > 0) {
            // Si la fila encontrada tiene distinto id_ca al proporcionado, informar cuál existe
            const found = existingRows[0];
            return res.status(409).json({ 
                error: 'Ya existe una guía con esos datos en la base local', 
                existing: found,
                received: { id_ca: id_ca ?? null, guia: guia0 }
            });
        }

        if (cargado.length > 0) {
            const guia = guia0;

            // Construir lista de campos válidos para INSERT según columnas reales
            const guiaFields = Object.keys(guia).filter(k => guiaColumns.includes(k));
            // Añadir fecha, status, id_ca solo si existen en la tabla
            if (guiaColumns.includes('fecha')) guiaFields.push('fecha');
            if (guiaColumns.includes('status')) guiaFields.push('status');
            if (guiaColumns.includes('id_ca')) guiaFields.push('id_ca');

            const guiaValues = guiaFields.map(f => {
                if (f === 'fecha') return fechaRegistro;
                if (f === 'status') return status;
                if (f === 'id_ca') return id_ca;
                return guia[f];
            });

            await localConnection.query(
                `INSERT INTO guia (${guiaFields.join(',')}) VALUES (${guiaFields.map(() => '?').join(',')})`,
                guiaValues
            );
        }

        for (const reng of detalle) {
            const rengFields = [...Object.keys(reng), 'fecha', 'status'];
            let rengValues = [...Object.values(reng), fechaRegistro, status_dos];
            if (!reng.id_ca) {
                rengFields.push('id_ca');
                rengValues.push(id_ca);
            }
            await localConnection.query(
                `INSERT INTO reng_guia (${rengFields.join(',')}) VALUES (${rengFields.map(() => '?').join(',')})`,
                rengValues
            );
        }

        res.json({ exito: true, mensaje: 'Datos guardados correctamente en la base local' });
    } catch (err) {
        console.error('Error al guardar en la base local:', err);
        res.status(500).json({ error: 'No se pudo guardar en la base local' });
    } finally {
        if (localConnection) localConnection.release();
    }
};

// POST /guias/recibir-guia
export const recibirGuia = async (req, res) => {
    const pool = getMysqlPool();
    if (!pool) {
        return res.status(500).json({ error: 'MySQL no está inicializado' });
    }

    const { registros } = req.body;
    if (!Array.isArray(registros) || registros.length === 0) {
        return res.status(400).json({ error: 'Debes enviar un array "registros" con al menos un elemento.' });
    }

    for (const reg of registros) {
        // 'nota' ya no es obligatorio
        if (!reg.id_ca || !reg.factura || !reg.descripcion || !reg.status || !reg.vendedor || !reg.fecha || !reg.coordenadas) {
            return res.status(400).json({ error: 'Todos los registros deben tener los campos obligatorios excepto "nota".' });
        }
    }

    let conn;
    try {
        conn = await pool.getConnection();

        // Verificar si el id_ca ya existe en guias_recibidas
        const id_ca = registros[0].id_ca;
        const [rows] = await conn.query(
            'SELECT COUNT(*) as total FROM guias_recibidas WHERE id_ca = ?',
            [id_ca]
        );
        if (rows[0].total > 0) {
            return res.status(409).json({ error: `La guía ${id_ca} ya fue recibida previamente.` });
        }

        const values = registros.map(reg => [
            reg.id_ca,
            reg.factura,
            reg.nota ?? '', // Si no viene 'nota', inserta vacío
            reg.descripcion,
            reg.status,
            reg.vendedor,
            reg.comentario || '',
            reg.fecha,
            reg.coordenadas
        ]);
        const placeholders = values.map(() => '(?,?,?,?,?,?,?,?,?)').join(',');
        const insertQuery = `INSERT INTO guias_recibidas (id_ca, factura, nota, descripcion, status, vendedor, comentario, fecha, coordenadas) VALUES ${placeholders}`;
        const flatValues = values.flat();
        await conn.query(insertQuery, flatValues);
        res.json({ exito: true, mensaje: 'Registros insertados correctamente.', cantidad: registros.length });
    } catch (err) {
        console.error('Error insertando en guias_recibidas:', err);
        res.status(500).json({ error: 'No se pudo insertar los registros.' });
    } finally {
        if (conn) conn.release();
    }
};


// POST /
export const getGuiasAndRenglones = async (req, res) => {
    const pool = getMysqlPool();
    if (!pool) {
        return res.status(500).json({ error: 'MySQL no está inicializado' });
    }

    const { co_ven } = req.body;
    if (!co_ven || typeof co_ven !== 'string' || !co_ven.trim()) {
        return res.status(400).json({ error: 'El campo co_ven es obligatorio.' });
    }

    let conn;
    try {
        conn = await pool.getConnection();
        const [renglones] = await conn.query(
            'SELECT id_ca, factura, nota, descrip, status FROM reng_guia WHERE TRIM(vendedor) = ? AND status = ?',
            [co_ven.trim(), 'cargado']
        );

        const idCas = renglones.map(r => r.id_ca);

        let guias = [];
        if (idCas.length > 0) {
            const [resultGuias] = await conn.query(
                `SELECT id_ca, ruta, conductor, vehiculo, realizado FROM guia WHERE id_ca IN (${idCas.map(() => '?').join(',')})`,
                idCas
            );
            guias = resultGuias;
        }

        res.json({ guias, renglones });
    } catch (err) {
        console.error('Error consultando guias:', err);
        res.status(500).json({ error: 'Error consultando guias y renglones' });
    } finally {
        if (conn) conn.release();
    }
};