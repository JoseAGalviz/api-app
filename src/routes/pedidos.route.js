import { Router } from 'express';

import { insertarPedidoProfit } from '../controllers/pedidos.controller.js';
import { insertarPedido } from '../controllers/pedidos.controller.js';

const router = Router();

router.post('/pedido-profit', insertarPedidoProfit);

router.post('/pedido-profit-Psico', insertarPedido);

export default router;