import { Router } from "express";
import {
  getVendedoresPagina,
  getGestionesPorDia,
  getSegmentos,
  getZonas,
  insertarKpiVendedor,
  getGestionesConPromedioHoras,
  getKpiVendedores,
  getKpiMetas,
  registrarComisionVendedor,
  getMatrixExcelDatos,
  getExcelDataPotencial,
  getComisionesVendedores,
  getVendedoresMercado,
  getVendedoresRutas,
  getCoberturaVendedores
} from "../controllers/auditoriaVen.controller.js";

const router = Router();

// ===============================
// Ruta POST /
// Devuelve la lista de usuarios con rol 'vendedor' desde la base de datos local MySQL.
// Controlador: getVendedoresPagina
// ===============================
router.post("/", getVendedoresPagina);

// ===============================
// Ruta POST /gestiones-por-dia
// Devuelve la cantidad de gestiones realizadas por usuario y día en un rango de fechas.
// Controlador: getGestionesPorDia
// ===============================
router.post("/gestiones-por-dia", getGestionesPorDia);

// ===============================
// Ruta POST /gestiones-por-dia-mes
// Devuelve la cantidad de gestiones por usuario y día para el mes consultado (anio y mes por query)
// Controlador: getGestionesPorDia
// ===============================
router.post("/gestiones-por-dia-mes", getGestionesPorDia);

router.post("/segmentos", getSegmentos);
// Agregar soporte GET para poder consultar con GET desde el navegador
router.get("/segmentos", getSegmentos);

router.post("/zonas", getZonas);
// Agregar soporte GET para poder consultar con GET desde el navegador
router.get("/zonas", getZonas);

// Nuevo endpoint para insertar/actualizar KPI de vendedores
// POST /kpi-vendedores  -> cuerpo JSON con la estructura indicada
router.post("/kpi-vendedores", insertarKpiVendedor);

// Nuevo: permitir GET para consultar KPIs guardados
router.get("/kpi-vendedores", getKpiVendedores);

router.post("/gestiones-con-promedio-horas", getGestionesConPromedioHoras);

router.get("/kpi-metas", getKpiMetas);

// Permitir POST para enviar startDate/endDate en el body (ISO datetime)
router.post("/kpi-metas", getKpiMetas);

// Nueva ruta para registrar comisiones de vendedores
router.post("/comisiones-vendedor", registrarComisionVendedor);

// Endpoint GET para consultar comisiones almacenadas
router.get("/comisiones-vendedor", getComisionesVendedores);

// Nueva ruta para consultar matrix_excel_datos (GET y POST)
router.get("/matrix-excel-datos", getMatrixExcelDatos);
router.post("/matrix-excel-datos", getMatrixExcelDatos);

// Nuevo endpoint para consultar excel_data_potencial
router.get("/excel-data-potencial", getExcelDataPotencial);
router.post("/excel-data-potencial", getExcelDataPotencial);

router.get("/vendedores-mercado", getVendedoresMercado);
router.post("/vendedores-mercado", getVendedoresMercado);


router.get("/vendedores-rutas", getVendedoresRutas);
router.post("/vendedores-rutas", getVendedoresRutas);

// Nueva ruta para consultar cobertura_vendedores (GET y POST)
router.get("/cobertura-vendedores", getCoberturaVendedores);
router.post("/cobertura-vendedores", getCoberturaVendedores);

// Exporta el router para ser utilizado en la configuración principal de la API
export default router;
