import express from 'express';
import { 
    getFacturasVencidas, 
    scanFactura, 
    updateFacturaFecha, 
    getTotales 
} from '../controllers/facturas.controller.js';

const router = express.Router();

// Lógica del antiguo facturas.js (GET /facturas)
router.get('/', getFacturasVencidas);

// Lógica del antiguo scan_fac.js (POST /facturas/scan)
router.post('/scan', scanFactura);

// Lógica del antiguo scan_fac/update.js (POST /facturas/update)
router.post('/update', updateFacturaFecha);

// Lógica del antiguo totales.js (POST /facturas/totales)
router.post('/totales', getTotales);

export default router;