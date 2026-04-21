import { Router } from "express";
import { getArticulos, getPrecios, getClientes, getProveedores, 
registrarUsuario, loginUsuario, getCatalogo, getTiemposPago, getTipoCli,
getUnidadesPorUsuario, getProductosVendidos, getTransferenciasProveedor, getTotalGeneralProveedor, getRenglonesFactura, getPedidosPorUsuario, getProveedoresProfit, getFacturasConCampo5, getVentasPorUsuariosProveedor, getProductosMasVendidosPorProveedor, getFacturaDetalle, totalizarDeudaPorProveedor, getTiemposPagoTransferencias, editTiemposPagoTransferencias, getNotasCreditoTransferencias, crearNotaCreditoTransferencias, getProductosMasVendidosPorProveedoresCampo5, buscarNotasCreditoPorProveedorExacto, getStAlmacPorProveedor, getUsuarios, editUsuario, getTodosPedidos, getPedidosConInconsistencias } from "../controllers/transferencias.controller.js";
const router = Router();
// Define la ruta GET / para obtener las transferencias
router.get("/", getArticulos);

router.post("/precios", getPrecios);

router.post("/clientes", getClientes);

router.get("/proveedores", getProveedores);

router.post("/registrar-usuario", registrarUsuario);

router.post("/login-usuario", loginUsuario);

router.post("/catalogo", getCatalogo);

router.get("/tiempos-pago", getTiemposPago);

router.post("/tipo-cli", getTipoCli);

// Rutas nuevas (estadísticas y transferencias por proveedor)
// Ejemplo: GET /transferencias/estadisticas/unidades-por-usuario?proveedor_codigo=43
router.get("/estadisticas/unidades-por-usuario", getUnidadesPorUsuario);

// Cambiado a POST: recibe { "proveedor_codigo": "43" } en el body (JSON)
// Ejemplo: POST /transferencias/estadisticas/productos-vendidos
router.post("/estadisticas/productos-vendidos", getProductosVendidos);

// Ejemplo: GET /transferencias/proveedor?proveedor_codigo=43
router.get("/proveedor", getTransferenciasProveedor);

router.get("/proveedor/total-general", getTotalGeneralProveedor);

// Nueva ruta para obtener renglones de factura
// Ejemplo: GET /transferencias/renglones-factura?nro_doc=123&co_cli=ABC&cod_prov=XYZ
router.post("/renglones-factura", getRenglonesFactura);

router.post("/pedidos-por-usuario", getPedidosPorUsuario);

router.get("/proveedores-profit", getProveedoresProfit);

router.post("/facturas-campo5", getFacturasConCampo5);

// Ruta para obtener detalle de una factura por fact_num
// Body JSON: { "fact_num": "12345" }
router.post("/detalle-pedido-admin", getFacturaDetalle);

router.post("/ventas-usuarios-proveedor", getVentasPorUsuariosProveedor);

// Nueva ruta: POST /transferencias/productos-mas-vendidos
// Body JSON: { "cod_prov": "43" }
router.post("/productos-mas-vendidos", getProductosMasVendidosPorProveedor);

router.post("/totalizar-deuda-proveedor", totalizarDeudaPorProveedor);
// Permitir también GET para facilitar pruebas desde navegador
router.get("/totalizar-deuda-proveedor", totalizarDeudaPorProveedor);

router.get("/tiempos-pago-transferencias", getTiemposPagoTransferencias);

router.post("/tiempos-pago-transferencias/editar", editTiemposPagoTransferencias);

// GET para listar notas de crédito
router.post("/notas-credito-transferencias", getNotasCreditoTransferencias);

// POST para crear una nota de crédito (recibe JSON según especificación)
router.post("/notas-credito-transferencias", crearNotaCreditoTransferencias);

router.post("/productos-mas-vendidos-campo5", getProductosMasVendidosPorProveedoresCampo5);

router.post("/buscar-notas-credito-proveedor-exacto", buscarNotasCreditoPorProveedorExacto);

router.get("/st-almac-proveedor", getStAlmacPorProveedor);

// RUTAS DE USUARIOS
// Listar usuarios (acepta filtros por query: id, user, rol)
// Ejemplo: GET /transferencias/usuarios?id=1
router.get("/usuarios", getUsuarios);

// Editar usuario (envía JSON en body; incluye id o userId)
// Ejemplo: POST /transferencias/usuarios/editar  { "id": 1, "telefono": "1234", "rol": "visitador" }
router.post("/usuarios/editar", editUsuario);

router.post("/todos-pedidos", getTodosPedidos);

// Ruta para obtener pedidos con inconsistencias entre MySQL y Profit
router.post("/pedidos-inconsistencias", getPedidosConInconsistencias);

export default router;

