// Importa el framework Express para crear el servidor web
import express from "express";
// Importa CORS para permitir peticiones desde otros dominios (cross-origin)
import cors from "cors";
// Importa la función para conectar a la base de datos (MySQL y SQL Server)
import { connectDB } from "./config/database.js";
// Importa las rutas de cada módulo de la API
import clientesRoutes from "./routes/clientes.route.js"; // Rutas para clientes
import facturasRoutes from "./routes/facturas.route.js"; // Rutas para facturas
import guiasRoutes from "./routes/guias.route.js"; // Rutas para guías
import facturasGestionRoutes from "./routes/facturas-gestion.route.js"; // Rutas para gestión de facturas
import authRoutes from "./routes/auth.route.js"; // Rutas para autenticación
import paginaRoutes from "./routes/pagina.route.js"; // Rutas para página web
import pedidosRoutes from "./routes/pedidos.route.js"; // Rutas para pedidos
import auditoriaVenRoutes from "./routes/auditoriaVen.route.js"; // Rutas para visitas
import transferenciasRoutes from "./routes/transferencias.route.js"; // Rutas para transferencias
import transfencPController from "./routes/transferencP.route.js"; // Rutas para transferencias P
import pedidosAppRoutes from "./routes/pedidosApp.route.js"; // Rutas para pedidos App  
//import importRoutes from "./routes/import.route.js"; // Rutas para importación de Excel
// import whatsappRoutes from "./routes/whatsapp.route.js"; // Rutas para el Chatbot con IA
// Crea la aplicación Express
const app = express();
// Define el puerto en el que se ejecutará el servidor (por defecto 8001)
const PORT = process.env.PORT || 8001;

// Habilita CORS para permitir peticiones desde otros dominios
app.use(cors());
app.use((req, res, next) => {
  console.log(`[${new Date().toLocaleTimeString()}] ${req.method} ${req.url}`);
  if (req.headers['content-type']?.includes('multipart/form-data')) {
    console.log("📎 Petición detectada como multipart (archivo)");
  }
  next();
});
// Permite recibir y procesar datos en formato JSON en las peticiones
app.use(express.json({ limit: "10mb" })); // <-- Aumenta el límite a 10mb para soportar payloads grandes
app.use(express.urlencoded({ extended: true }));
// Conecta a SQL Server (remoto)
connectDB("remote");
// Conecta a MySQL (local)
connectDB("local");
// Conecta a MySQL (negociaciones)
connectDB("negociaciones");

// Ruta principal (root) para verificar que la API está corriendo
app.get("/", (req, res) => {
  res.send("API is running.");
});

// Rutas de la API
// Asocia cada grupo de rutas con su endpoint correspondiente
app.use("/api/clientes", clientesRoutes); // Endpoint para clientes
app.use("/api/facturas", facturasRoutes); // Endpoint para facturas
app.use("/api/guias", guiasRoutes); // Endpoint para guías
app.use("/api", facturasGestionRoutes); // Endpoint para gestión de facturas (incluye login y registro)
app.use("/api/auth", authRoutes); // Endpoint para autenticación de usuarios
app.use("/api/pagina", paginaRoutes); // Endpoint para operaciones de la página web (clientes, pedidos, artículos, etc.)
app.use("/api/pedidos", pedidosRoutes); // Endpoint para inserción de pedidos en Profit
app.use("/api/auditoria", auditoriaVenRoutes); // Endpoint para gestión de visitas y vendedores
app.use("/api/transferencias", transferenciasRoutes); // Endpoint para transferencias
app.use("/api/pedidosTrans", transfencPController); // Endpoint para transferencias P
app.use("/api/pedidosApp", pedidosAppRoutes); // Endpoint para pedidos App

// app.use("/api/whatsapp", whatsappRoutes); // Endpoint para el Chatbot de WhatsApp



// Inicia el servidor en el puerto definido y muestra un mensaje en consola
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
