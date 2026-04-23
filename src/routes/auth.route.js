import express from 'express';
import { loginUser, redirectToFixedIp, redirectToVendedorFixedIp, loginKpi } from '../controllers/facturas-gestion.controller.js';

const router = express.Router();

// Ruta de ejemplo para login
router.post('/login', loginUser);

// Ruta de ejemplo para registro
router.post('/register', (req, res) => {
  res.json({ message: 'Register endpoint funcionando' });
});

// Ruta para login KPI
router.post('/login-kpi', loginKpi);


// Ruta para redirigir a IP fija (acepta GET con query y POST con JSON body)
router.post('/redirect-fixed-ip', redirectToFixedIp);
router.get('/redirect-fixed-ip', redirectToFixedIp);

// Nueva ruta para redirigir a IP fija del vendedor
router.post('/redirect-vendedor-fixed-ip', redirectToVendedorFixedIp);
router.get('/redirect-vendedor-fixed-ip', redirectToVendedorFixedIp);

export default router;
