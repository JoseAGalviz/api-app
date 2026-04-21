import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv from 'dotenv';
dotenv.config();

const genAI = new GoogleGenerativeAI(process.env.GG_API_KEY);

async function listModels() {
  try {
    // Intentamos listar lo que Google te permite ver
    const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models?key=${process.env.GG_API_KEY}`);
    const data = await response.json();
    
    console.log("--- MODELOS DISPONIBLES PARA TU LLAVE ---");
    data.models.forEach(m => {
      if (m.supportedGenerationMethods.includes("generateContent")) {
        console.log(`✅ Modelo: ${m.name.replace('models/', '')}`);
      }
    });
  } catch (e) {
    console.error("Error al listar:", e);
  }
}

listModels();