import { Router } from 'express';
import {
  getFacturasPorSegmento, ListadorNegociacionesComparador, ListadorNegociacionesProfit
} from '../controllers/pagina.controller.js';

const router = Router();

router.post('/facturas-segmento', getFacturasPorSegmento);

router.post('/negociaciones/profit', ListadorNegociacionesProfit); // Consultar SOLO Profit
router.post('/negociaciones/comparador', ListadorNegociacionesComparador); // Consultar SOLO MySQL
export default router;