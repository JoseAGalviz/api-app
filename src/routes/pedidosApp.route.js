import express from "express";
import { getCatalogo, getClientes, crearPedidoApp } from "../controllers/pedidosApp.controller.js";

const router = express.Router();

/**
 * Rutas para la aplicación móvil (pedidosApp).
 */

// Obtener catálogo de productos
router.get("/catalogo", getCatalogo);
router.post("/catalogo", getCatalogo);

// Obtener lista de clientes (replicado de transferencias)
router.post("/clientes", getClientes);

// Crear pedido en Profit Plus desde la APP
router.post("/crear", crearPedidoApp);

export default router;
