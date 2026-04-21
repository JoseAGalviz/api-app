import express from 'express';
import { 
  saveFacturasLocales, 
  getGestiones, 
  saveGestiones, 
  loginUser, 
  registerUser,
  getFacturasCargadas // <-- agrega esto
} from '../controllers/facturas-gestion.controller.js';

const router = express.Router();

// Rutas para facturas_locales.js
router.post('/facturas_locales', saveFacturasLocales);

// Rutas para gestion_list.js y gestion.js
router.get('/gestiones', getGestiones);
router.post('/gestiones', saveGestiones);

// Rutas para login.js y register.js
router.post('/login', loginUser);
router.post('/register', registerUser);

router.get('/facturas_cargadas', getFacturasCargadas); // <-- agrega esta línea

export default router;