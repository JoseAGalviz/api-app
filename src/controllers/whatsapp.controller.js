// import { GoogleGenerativeAI } from "@google/generative-ai";
// import { sql } from "../config/database.js"; 
// import dotenv from 'dotenv';
// import path from 'path';
// import { fileURLToPath } from 'url';

// // --- CONFIGURACIÓN DE ENTORNO ---
// const __filename = fileURLToPath(import.meta.url);
// const __dirname = path.dirname(__filename);
// dotenv.config({ path: path.resolve(__dirname, '../../.env') });

// // --- INICIALIZACIÓN DE IA ---
// const apiKey = process.env.GG_API_KEY || process.env.GEMINI_API_KEY;
// const genAI = new GoogleGenerativeAI(apiKey);

// /**
//  * Procesa el lenguaje natural y genera SQL específico para Profit Plus
//  */
// async function obtenerRespuestaIA(pregunta) {
//     try {
//         const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });

//         const prompt = `
//             Eres un experto en SQL Server y el ERP Profit Plus. 
//             Tu misión es generar un SELECT para buscar existencias de artículos.
            
//             REGLAS ESTRICTAS:
//             1. Tabla: CRISTM25.dbo.art (DEBES usar la ruta completa).
//             2. Columnas: RTRIM(art_des) as art_des, stock_act.
//             3. Filtro: Usa UPPER y reemplaza espacios por % para máxima flexibilidad.
//             4. Limpieza: Ignora saludos, gracias, o frases como "hay existencia". Solo extrae el producto.
            
//             EJEMPLO DE SALIDA:
//             SELECT TOP 5 RTRIM(art_des) as art_des, stock_act FROM CRISTM25.dbo.art WHERE art_des LIKE UPPER('%METRONIDAZOL%500MG%')
            
//             Pregunta del usuario: "${pregunta}"
            
//             Responde ÚNICAMENTE el código SQL, sin explicaciones ni formato markdown.
//         `;

//         const result = await model.generateContent(prompt);
//         const response = await result.response;
//         let sqlText = response.text().trim();

//         sqlText = sqlText.replace(/```sql|```/g, "").trim();

//         if (!sqlText.toUpperCase().startsWith("SELECT")) {
//             throw new Error("La IA no generó un SELECT válido.");
//         }

//         return sqlText;
//     } catch (err) {
//         console.error('❌ Error Motor IA:', err.message);
//         throw err;
//     }
// }

// /**
//  * Lógica principal adaptada para Baileys
//  * @param {Object} sock - Instancia de conexión de Baileys
//  * @param {Object} m - Objeto del mensaje recibido
//  */
// export const recibirMensajeBaileys = async (sock, m) => {
//     // Extraer el texto del mensaje y el remitente
//     const msgUsuario = m.message?.conversation || m.message?.extendedTextMessage?.text;
//     const remoteJid = m.key.remoteJid;

//     // Validaciones iniciales
//     if (!msgUsuario || m.key.fromMe) return;

//     console.log(`\n--- Nueva Consulta (Baileys): ${new Date().toLocaleTimeString()} ---`);
//     console.log(`📩 Mensaje de ${remoteJid}: ${msgUsuario}`);

//     try {
//         // 1. Obtener SQL desde Gemini
//         const querySQL = await obtenerRespuestaIA(msgUsuario);
//         console.log(`🤖 SQL Generado: ${querySQL}`);

//         // 2. Ejecutar consulta en SQL Server
//         const result = await sql.query(querySQL);
//         const totalFilas = result.recordset ? result.recordset.length : 0;
//         console.log(`📊 Filas encontradas en DB: ${totalFilas}`);

//         // 3. Formatear y enviar respuesta vía Baileys
//         let respuestaString = "";

//         if (totalFilas > 0) {
//             respuestaString = "📦 *Inventario Profit Plus*:\n\n";

//             result.recordset.forEach(item => {
//                 const row = {};
//                 Object.keys(item).forEach(key => row[key.toLowerCase()] = item[key]);

//                 const nombre = row.art_des ? row.art_des.trim() : "Sin nombre";
//                 const stock = parseFloat(row.stock_act || 0);

//                 respuestaString += `🔹 *${nombre}*\n   Existencia: *${Math.floor(stock)}*\n\n`;
//             });
//         } else {
//             respuestaString = `🔍 No encontré existencias para: "${msgUsuario}" en la base de datos CRISTM25.`;
//         }

//         // Envío directo de WhatsApp
//         await sock.sendMessage(remoteJid, { text: respuestaString });
//         console.log("✅ Respuesta enviada exitosamente.");

//     } catch (error) {
//         console.error("❌ Error General:", error.message);
        
//         let errorMsg = "⚠️ Hubo un problema al consultar el stock. Intenta de nuevo más tarde.";
//         if (error.message.includes("429")) {
//             errorMsg = "⚠️ El servicio de IA está ocupado. Intenta de nuevo en 30 segundos.";
//         }

//         await sock.sendMessage(remoteJid, { text: errorMsg });
//     }
// };