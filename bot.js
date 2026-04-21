import { makeWASocket, useMultiFileAuthState, DisconnectReason } from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';
import qrcode from 'qrcode-terminal';
import { recibirMensajeBaileys } from './src/controllers/whatsapp.controller.js';

async function conectarWhatsApp() {
    // Usamos 'auth_info' para guardar la sesión
    const { state, saveCreds } = await useMultiFileAuthState('auth_info');

    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: false, // Lo manejamos manual abajo
        browser: ["Profit Plus Bot", "MacOS", "1.0.0"]
    });

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        // GENERAR EL QR EN LA TERMINAL
        if (qr) {
            console.log("\n📢 ESCANEA ESTE QR CON TU WHATSAPP:");
            qrcode.generate(qr, { small: true });
        }

        if (connection === 'close') {
            const debeReconectar = (lastDisconnect.error instanceof Boom)?.output?.statusCode !== DisconnectReason.loggedOut;
            if (debeReconectar) conectarWhatsApp();
        } else if (connection === 'open') {
            console.log('\n✅ ¡BOT CONECTADO Y LISTO PARA PROFIT PLUS!');
        }
    });

    sock.ev.on('messages.upsert', async ({ messages }) => {
        const m = messages[0];
        await recibirMensajeBaileys(sock, m);
    });

    sock.ev.on('creds.update', saveCreds);
}

console.log("⏳ Iniciando...");
conectarWhatsApp().catch(err => console.error("Error fatal:", err));