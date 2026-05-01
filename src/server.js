import './env.js';
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
app.set('trust proxy', 1);
const PORT = process.env.PORT || 8001;

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 1000,
  message: { error: "Demasiados intentos. Intenta en 15 minutos." },
  standardHeaders: true,
  legacyHeaders: false,
});

app.use(cors());
app.use((req, res, next) => {
  console.log(`[${new Date().toLocaleTimeString()}] ${req.method} ${req.url}`);
  next();
});

app.use((req, res, next) => {
  res.setTimeout(25000, () => {
    if (!res.headersSent) res.status(503).json({ error: 'Request timeout' });
  });
  next();
});
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));

connectDB("remote");
connectDB("local");
connectDB("negociaciones");
connectDB("comparador");

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
app.use("/api/clientes", clientesRoutes);
app.use("/api/facturas", facturasRoutes);
app.use("/api/guias", guiasRoutes);
app.use("/api/pagina", paginaRoutes);
//app.use("/api/pedidos", pedidosRoutes);
app.use("/api/auditoria", auditoriaVenRoutes);
app.use("/api/pedidosApp", pedidosAppRoutes);

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

// Kill hanging requests after 30s
server.timeout = 30000;
server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;

// Reconexión automática SQL Server cada 60s si cae
setInterval(async () => {
  try {
    await new sql.Request().query('SELECT 1');
  } catch {
    console.error('[DB] SQL Server desconectado — reconectando...');
    await reconnectSQL();
  }
}, 60 * 1000);

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
