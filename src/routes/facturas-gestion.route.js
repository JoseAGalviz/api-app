import express from 'express';
import {
  saveFacturasLocales,
  getGestiones,
  saveGestiones,
  loginUser,
  registerUser,
  getFacturasCargadas,
  redirectToIp,
  redirectToFixedIp,
  redirectToVendedorFixedIp
} from '../controllers/facturas-gestion.controller.js';
import { verifyToken } from '../middleware/auth.js';
import { rateLimit } from 'express-rate-limit';

const router = express.Router();

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  message: { error: "Demasiados intentos. Intenta en 15 minutos." },
  standardHeaders: true,
  legacyHeaders: false,
});

// Rutas públicas
router.post('/login', authLimiter, loginUser);
router.post('/register', registerUser);

// Rutas protegidas
router.post('/facturas_locales', saveFacturasLocales);
router.get('/gestiones', getGestiones);
router.post('/gestiones', saveGestiones);
router.get('/facturas_cargadas', getFacturasCargadas);

// Redirects (internos, sin token requerido)
router.post('/redirect-to-ip', redirectToIp);
router.get('/redirect-to-ip', redirectToIp);
router.post('/redirect-fixed-ip', redirectToFixedIp);
router.get('/redirect-fixed-ip', redirectToFixedIp);
router.post('/redirect-vendedor-fixed-ip', redirectToVendedorFixedIp);
router.get('/redirect-vendedor-fixed-ip', redirectToVendedorFixedIp);

export default router;
