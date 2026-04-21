import express from 'express';
import { 
    procesarGuia, 
    buscarCarga, 
    guardarCarga, 
    recibirGuia, 
    getGuiasAndRenglones 
} from '../controllers/guias.controller.js';

const router = express.Router();

// Rutas para guías
router.post('/procesar', procesarGuia);
router.post('/buscar-carga', buscarCarga);
router.post('/guardar-carga', guardarCarga);
router.post('/recibir-guia', recibirGuia);
router.post('/', getGuiasAndRenglones);

export default router;