import 'dotenv/config';
import sql from 'mssql';
import mysql from 'mysql2/promise';

export const remoteConfig = {
  user: process.env.DB_REMOTE_USER,
  password: process.env.DB_REMOTE_PASSWORD,
  server: process.env.DB_REMOTE_SERVER,
  port: 1433,
  database: process.env.DB_REMOTE_DATABASE,
  options: {
    encrypt: false,
    trustServerCertificate: true
  },
  pool: {
    max: 10,
    min: 2,
    idleTimeoutMillis: 30000,
    acquireTimeoutMillis: 15000,
  },
  connectionTimeout: 15000,
  requestTimeout: 60000
};

export const localConfig = {
  host: process.env.DB_LOCAL_HOST,
  user: process.env.DB_LOCAL_USER,
  password: process.env.DB_LOCAL_PASSWORD,
  database: 'app',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 50,
  connectTimeout: 10000,
  enableKeepAlive: true,
  keepAliveInitialDelay: 10000,
};

export const mssqlConfig = {
  user: process.env.MSSQL_USER || process.env.DB_LOCAL_USER,
  password: process.env.MSSQL_PASSWORD || process.env.DB_LOCAL_PASSWORD,
  server: process.env.MSSQL_HOST || process.env.DB_LOCAL_HOST,
  database: process.env.MSSQL_DATABASE || 'app',
  options: {
    encrypt: false,
    trustServerCertificate: true,
  },
  connectionTimeout: 15000,
  requestTimeout: 60000,
};

export let mysqlPool = null;
export let negociacionesPool = null;
export let comparadorPool = null;

export const negociacionesConfig = {
  host: process.env.DB_NEG_HOST,
  user: process.env.DB_NEG_USER,
  password: process.env.DB_NEG_PASSWORD,
  database: 'negociaciones',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 50,
  connectTimeout: 10000,
  enableKeepAlive: true,
  keepAliveInitialDelay: 10000,
};

export const comparadorConfig = {
  host: process.env.DB_VISOR_HOST,
  user: process.env.DB_VISOR_USER,
  password: process.env.DB_VISOR_PASSWORD,
  database: 'comparador',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 50,
  connectTimeout: 10000,
  enableKeepAlive: true,
  keepAliveInitialDelay: 10000,
};

export async function connectDB(configType = 'remote') {
  try {
    if (configType === 'local') {
      mysqlPool = mysql.createPool(localConfig);
      console.log('✅ Conexión exitosa a MySQL (local)');
    } else if (configType === 'negociaciones') {
      negociacionesPool = mysql.createPool(negociacionesConfig);
      console.log('✅ Conexión exitosa a MySQL (negociaciones)');
    } else if (configType === 'comparador') {
      comparadorPool = mysql.createPool(comparadorConfig);
      console.log('✅ Conexión exitosa a MySQL (comparador)');
    } else {
      console.log('⏳ Intentando conectar a SQL Server (remoto)...');
      await sql.connect(remoteConfig);
      console.log('✅ Conexión exitosa a SQL Server (remoto)');
    }
  } catch (err) {
    console.error(`❌ Error de conexión [${configType}]:`, err.message);
    throw err; // Propagar el error para que el llamador decida qué hacer
  }
}

export async function checkDBHealth() {
  const status = { sqlServer: false, mysql: false, negociaciones: false, comparador: false };
  try {
    await new sql.Request().query('SELECT 1');
    status.sqlServer = true;
  } catch { /* unreachable or down */ }

  try {
    const conn = await mysqlPool?.getConnection();
    if (conn) { conn.release(); status.mysql = true; }
  } catch { /* down */ }

  try {
    const conn = await negociacionesPool?.getConnection();
    if (conn) { conn.release(); status.negociaciones = true; }
  } catch { /* down */ }

  try {
    const conn = await comparadorPool?.getConnection();
    if (conn) { conn.release(); status.comparador = true; }
  } catch { /* down */ }

  return status;
}

export async function reconnectSQL() {
  try {
    console.log('🔄 Intentando reconectar a SQL Server...');
    try {
      await sql.close();
    } catch { /* already closed */ }
    await connectDB('remote');
    console.log('✅ Reconexión a SQL Server exitosa');
  } catch (err) {
    console.error('❌ Falló la reconexión a SQL Server:', err.message);
  }
}

export function getMysqlPool() {
  return mysqlPool;
}

export function getNegociacionesPool() {
  return negociacionesPool;
}

export function getComparadorPool() {
  return comparadorPool;
}

export { sql };
