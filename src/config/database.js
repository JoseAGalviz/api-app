import sql from 'mssql';
import mysql from 'mysql2/promise';

export const remoteConfig = {
  user: 'profit',
  password: 'profit',
  server: '192.168.4.20',
  port: 1433,
  database: 'CRISTM25',

  options: {
    encrypt: false,
    trustServerCertificate: true
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  },
  connectionTimeout: 300000,
  requestTimeout: 300000
};

export const localConfig = {
  host: '192.168.4.23',
  user: 'desarrollo',
  password: 'E-xUUctByBsPTe7A',
  database: 'app',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};

// Configuración MSSQL (tedious / mssql)
export const mssqlConfig = {
  user: process.env.MSSQL_USER || "desarrollo",
  password: process.env.MSSQL_PASSWORD || "E-xUUctByBsPTe7A",
  server: process.env.MSSQL_HOST || "192.168.4.23",
  database: process.env.MSSQL_DATABASE || "app",
  options: {
    encrypt: false,                 // ajustar según entorno
    trustServerCertificate: true,   // necesario en entornos internos sin certificado
  },
  // Timeouts en milisegundos: aumentar desde 15000 a 300000 (5 min)
  connectionTimeout: 300000,
  requestTimeout: 300000,
};

export let mysqlPool = null;
export let negociacionesPool = null;

export const negociacionesConfig = {
  host: '192.168.4.23',
  user: 'desarrollo',
  password: 'E-xUUctByBsPTe7A',
  database: 'negociaciones',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};

export async function connectDB(configType = 'remote') {
  try {
    if (configType === 'local') {
      mysqlPool = await mysql.createPool(localConfig);
      console.log('Conexión exitosa a MySQL (local)');
    } else if (configType === 'negociaciones') {
      negociacionesPool = await mysql.createPool(negociacionesConfig);
      console.log('Conexión exitosa a MySQL (negociaciones)');
    } else {
      await sql.connect(remoteConfig);
      console.log('Conexión exitosa a SQL Server (remoto)');
    }
  } catch (err) {
    console.error('Error de conexión:', err);
  }
}

export function getMysqlPool() {
  return mysqlPool;
}

export function getNegociacionesPool() {
  return negociacionesPool;
}

export { sql };

