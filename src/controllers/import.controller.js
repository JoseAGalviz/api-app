import { getMysqlPool } from '../config/database.js';

export const importExcel = async (req, res) => {
    console.log("📥 Petición de importación de JSON recibida");
    try {
        const data = req.body;

        if (!data || !Array.isArray(data)) {
            console.log("❌ No se recibió un array de datos");
            return res.status(400).json({ message: 'No se recibieron datos válidos (se espera un array)' });
        }

        console.log(`📊 Filas recibidas desde el frontend: ${data.length}`);

        if (data.length === 0) {
            console.log("⚠️ El array de datos está vacío");
            return res.status(400).json({ message: 'El array de datos está vacío' });
        }

        const pool = getMysqlPool();
        const connection = await pool.getConnection();

        // Helper para buscar columnas de forma flexible (ignorando minúsculas/mayúsculas y espacios)
        const getVal = (row, possibleKeys) => {
            const keys = Object.keys(row);
            const lowerPossible = possibleKeys.map(k => k.toLowerCase().trim());

            const foundKey = keys.find(k => lowerPossible.includes(k.toLowerCase().trim()));
            return foundKey ? row[foundKey] : null;
        };

        try {
            await connection.beginTransaction();

            const query = `
                INSERT INTO matriz_excel_datos 
                (sicm, nombre, ciudad, compras_promedio_droguerias, compras_crist, peso_prom, inicio_relacion, nro_pedidos, promedio_pago, estado, segmento_zona_bitrix)
                VALUES ?
            `;

            const values = data.map(row => [
                getVal(row, ['SICM', 'Código', 'Codigo']),
                getVal(row, ['Nombre', 'Cliente']),
                getVal(row, ['G:Ciudad', 'Ciudad', 'G: Ciudad']),
                parseFloat(getVal(row, ['Compras promedio a todas las droguerias', 'Compras promedio'])) || 0,
                parseInt(getVal(row, ['Compras a CRIST', 'Compras CRIST'])) || 0,
                getVal(row, ['Peso Prom', 'Peso promedio']),
                getVal(row, ['Inicio relacion', 'Inicio relación']),
                parseInt(getVal(row, ['Nro pedidos', 'Nro. pedidos', 'Numero pedidos'])) || 0,
                parseFloat(getVal(row, ['Promedio Pago', 'Promedio de pago'])) || 0,
                getVal(row, ['ESTADO', 'Estado']),
                getVal(row, ['segmento zona profit (Bitrix)', 'segemento zona profit (Bitrix)', 'zona bitrix'])
            ]);

            await connection.query(query, [values]);
            await connection.commit();

            res.status(200).json({
                message: 'Datos importados correctamente',
                count: values.length
            });
        } catch (error) {
            await connection.rollback();
            console.error('Error al insertar datos:', error);
            res.status(500).json({ message: 'Error al insertar datos en la base de datos', error: error.message });
        } finally {
            connection.release();
        }

    } catch (error) {
        console.error('Error al procesar el archivo:', error);
        res.status(500).json({ message: 'Error interno al procesar el archivo Excel', error: error.message });
    }
};
