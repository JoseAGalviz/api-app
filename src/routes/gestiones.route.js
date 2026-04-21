import express from 'express';
import { 
    gestionVisitas
} from '../controllers/gestiones.controller.js';

const router = express.Router();

// Rutas para guías
router.post('/gestionVisita', gestionVisitas);