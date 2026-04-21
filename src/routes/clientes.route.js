import express from 'express';
import { 
    getGestiones, 
    getDetallesCliente, 
    getFacturasVencidas, 
    getSegmentos, 
    getVendedores,
    getClientesBitrix,
    getGestionesYBitrix
} from '../controllers/clientes.controller.js';

const router = express.Router();

// Lógica de clientes.js (GET /clientes)
router.get('/', getGestiones);

// Lógica de detalles.js (POST /clientes/detalles)
router.post('/detalles', getDetallesCliente);

// Lógica de facturas.js (GET /clientes/facturas-vencidas)
router.get('/facturas-vencidas', getFacturasVencidas);

// Lógica de segmentos.js (GET /clientes/segmentos)
router.get('/segmentos', getSegmentos);

// Lógica de segmentos.js (GET /clientes/vendedores)
router.get('/vendedores', getVendedores);

// Lógica para obtener clientes de Bitrix (GET /clientes/clientes-bitrix)
router.get('/clientes-bitrix', getClientesBitrix);

// Nuevo endpoint combinado
router.get('/gestiones-bitrix', getGestionesYBitrix);

export default router;