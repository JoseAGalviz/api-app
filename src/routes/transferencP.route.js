import { Router } from "express";
import { getNextFactNum, crearPedidoTransf } from "../controllers/transfencP.controller.js";

const router = Router();

router.get("/next-fact-num", getNextFactNum);
router.post("/crear", crearPedidoTransf);

export default router;
