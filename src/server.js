import 'dotenv/config';
import express from "express";
import cors from "cors";
import { rateLimit } from "express-rate-limit";
import { connectDB, checkDBHealth, reconnectSQL, sql } from "./config/database.js";
import { verifyToken } from "./middleware/auth.js";
import { notFound, errorHandler } from "./middleware/errorHandler.js";
import clientesRoutes from "./routes/clientes.route.js";
import facturasRoutes from "./routes/facturas.route.js";
import guiasRoutes from "./routes/guias.route.js";
import facturasGestionRoutes from "./routes/facturas-gestion.route.js";
import authRoutes from "./routes/auth.route.js";
import paginaRoutes from "./routes/pagina.route.js";
import pedidosRoutes from "./routes/pedidos.route.js";
import auditoriaVenRoutes from "./routes/auditoriaVen.route.js";
import transferenciasRoutes from "./routes/transferencias.route.js";
import transfencPController from "./routes/transferencP.route.js";
import pedidosAppRoutes from "./routes/pedidosApp.route.js";

const app = express();
const PORT = process.env.PORT || 8001;

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  message: { error: "Demasiados intentos. Intenta en 15 minutos." },
  standardHeaders: true,
  legacyHeaders: false,
});

app.use(cors());
app.use((req, res, next) => {
  console.log(`[${new Date().toLocaleTimeString()}] ${req.method} ${req.url}`);
  next();
});
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));

connectDB("remote");
connectDB("local");
connectDB("negociaciones");

// Health check (público)
app.get("/health", async (req, res) => {
  const db = await checkDBHealth();
  const allUp = Object.values(db).every(Boolean);
  res.status(allUp ? 200 : 503).json({ status: allUp ? "ok" : "degraded", db });
});

app.get("/", (req, res) => res.send("API is running."));

// Auth routes (públicas + rate limit)
app.use("/api/auth", authLimiter, authRoutes);

// Rutas protegidas
app.use("/api/clientes", verifyToken, clientesRoutes);
app.use("/api/facturas", verifyToken, facturasRoutes);
app.use("/api/guias", verifyToken, guiasRoutes);
app.use("/api/pagina", verifyToken, paginaRoutes);
//app.use("/api/pedidos", verifyToken, pedidosRoutes);
app.use("/api/auditoria", verifyToken, auditoriaVenRoutes);
app.use("/api/pedidosApp", verifyToken, pedidosAppRoutes);

// facturas-gestion: login/register públicos, datos protegidos (en route file)
app.use("/api", facturasGestionRoutes);

// transferencias: auth propio
app.use("/api/transferencias", transferenciasRoutes);
app.use("/api/pedidosTrans", transfencPController);

// 404 y error handler global (SIEMPRE al final)
app.use(notFound);
app.use(errorHandler);

const server = app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

// Reconexión automática SQL Server cada 5 min si cae
setInterval(async () => {
  try {
    await new sql.Request().query('SELECT 1');
  } catch {
    console.error('[DB] SQL Server desconectado — reconectando...');
    await reconnectSQL();
  }
}, 5 * 60 * 1000);

// Graceful shutdown
async function shutdown(signal) {
  console.log(`\n[${signal}] Cerrando servidor...`);
  server.close(async () => {
    try { await sql.close(); } catch { /* ignore */ }
    console.log('Servidor cerrado limpiamente.');
    process.exit(0);
  });
  // Forzar cierre si tarda más de 10s
  setTimeout(() => {
    console.error('Forzando cierre por timeout.');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Prevenir crash por errores no capturados
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err);
  // Solo salir si es un error irrecuperable
  if (err.code === 'EADDRINUSE') process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  console.error('[unhandledRejection]', reason);
});
