import express from 'express';
import multer from 'multer';
import { importExcel } from '../controllers/import.controller.js';

const router = express.Router();

// Ruta para cargar los datos del Excel en JSON
router.post('/excel', importExcel);

export default router;
