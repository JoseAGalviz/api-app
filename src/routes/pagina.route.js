import { Router } from 'express';
import {
  getFacturasPorSegmento
} from '../controllers/pagina.controller.js';

const router = Router();

router.post('/facturas-segmento', getFacturasPorSegmento);


export default router;