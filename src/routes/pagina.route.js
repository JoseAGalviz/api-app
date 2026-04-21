import { Router } from 'express';
import { 
  getPaginaClientes, 
  getPedidosPaginados, 
  getMoneda, 
  getFacturaDetalle, 
  getTodosClientes,
  getTopArticulosPorCliente,
  getTipoClientePrecio,
  getTopArticulosVendidosMes,
  getStockArticuloss,
  getFacturasPorSegmento,
  getArticulosConStock,
  getDatosFactura,           // <-- Importa el controlador
  buscarFacturasPorCliente,   // <-- Importa el controlador3
  getFacturasConNotasYCobros,
  getTopClientesPorProveedores,
  getTodosVendedores,
  getPsicotropicosCompradosRecientes,
  getDescuentos,
  getArticulosPreciosPorCliente,
  getFacturaResumen,
  getFacturaMarcel,
  ListadorNegociacionesProfit,
  ListadorNegociacionesComparador,
  getFacturasPorSegmentoDpPago,
  getDescuentoPorEscalaProv,
  getDescuentoPorEscalaPTC
} from '../controllers/pagina.controller.js';

const router = Router();

router.get('/', getPaginaClientes);
router.get('/pedidos', getPedidosPaginados);
router.get('/moneda', getMoneda);
router.get('/factura', getFacturaDetalle);
router.get('/factura/resumen', getFacturaResumen);
router.get('/factura/FAR', getFacturaMarcel);
router.get('/clientes/todos', getTodosClientes);
router.get('/top-articulos-cliente', getTopArticulosPorCliente);
router.get('/articulos', getArticulosPreciosPorCliente);
router.get('/tipo-cliente-precio', getTipoClientePrecio);
router.get('/top-articulos-mes', getTopArticulosVendidosMes);
router.post('/stock-articulos', getStockArticuloss);
router.post('/facturas-segmento', getFacturasPorSegmento);
router.post('/facturas-segmento-dp-pago', getFacturasPorSegmentoDpPago);
//router.post('/descuento-escala-prov', getDescuentoPorEscalaProv);
//router.post('/descuento-escala-ptc', getDescuentoPorEscalaPTC);
router.get('/articulos-stock', getArticulosConStock);

// NUEVAS RUTAS:
router.post('/factura', getDatosFactura); // Consulta una factura específica
router.post('/facturas/buscar', buscarFacturasPorCliente); // Busca facturas por cliente
router.post('/facturas-con-notas-cobros', getFacturasConNotasYCobros); // Facturas con notas y cobros
router.post('/top-clientes-proveedores', getTopClientesPorProveedores); // Top clientes por proveedores
router.get('/vendedores/todos', getTodosVendedores); // Todos los vendedores
router.get('/psicotropicos-recientes', getPsicotropicosCompradosRecientes); // Psicotrópicos comprados recientemente
router.get('/descuentos', getDescuentos); // Obtener descuentos para artículos y clientes
router.post('/negociaciones/profit', ListadorNegociacionesProfit); // Consultar SOLO Profit
router.post('/negociaciones/comparador', ListadorNegociacionesComparador); // Consultar SOLO MySQL

export default router;