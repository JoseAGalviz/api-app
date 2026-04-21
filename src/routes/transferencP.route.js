import { Router } from "express";
import transfencPController from "../controllers/transfencP.controller.js";
const router = Router();

// Monta el controlador completo en la raíz de esta ruta.
// Con esto basta exponer un único punto de montaje en la app:
// POST /transferencP/crear  -> se ejecuta la lógica de crear pedido
router.use("/", transfencPController);

export default router;