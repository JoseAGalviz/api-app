const isProd = process.env.NODE_ENV === 'production';

export function notFound(req, res, next) {
  res.status(404).json({ error: `Ruta no encontrada: ${req.method} ${req.originalUrl}` });
}

export function errorHandler(err, req, res, next) {
  // JSON parse errors (malformed request body)
  if (err.type === 'entity.parse.failed') {
    return res.status(400).json({ error: 'JSON inválido en el body de la solicitud' });
  }

  // JWT errors forwarded via next(err)
  if (err.name === 'JsonWebTokenError' || err.name === 'TokenExpiredError') {
    return res.status(401).json({ error: 'Token inválido o expirado' });
  }

  // Validation errors
  if (err.status >= 400 && err.status < 500) {
    return res.status(err.status).json({ error: err.message });
  }

  // Server errors — log full stack, never expose in production
  console.error(`[ERROR] ${req.method} ${req.originalUrl}:`, err);
  res.status(err.status || 500).json({
    error: isProd ? 'Error interno del servidor' : err.message,
  });
}
