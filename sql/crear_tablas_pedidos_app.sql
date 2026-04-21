-- Script SQL para crear las tablas necesarias en MySQL
-- Database: transferencias
-- Descripción: Tablas para almacenar copias de los pedidos creados desde la APP

-- ============================================
-- Tabla: pedidos
-- ============================================
-- Almacena la cabecera de los pedidos creados desde la APP
CREATE TABLE IF NOT EXISTS `pedidos` (
  `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID autoincremental del pedido',
  `fact_num` VARCHAR(20) NOT NULL COMMENT 'Número de factura/pedido en Profit Plus',
  `cod_cliente` VARCHAR(15) NOT NULL COMMENT 'Código del cliente',
  `cod_prov` VARCHAR(15) DEFAULT NULL COMMENT 'Código del proveedor',
  `tot_bruto` DECIMAL(18, 4) DEFAULT 0 COMMENT 'Total bruto del pedido en dólares',
  `tot_neto` DECIMAL(18, 4) DEFAULT 0 COMMENT 'Total neto del pedido en dólares',
  `saldo` DECIMAL(18, 4) DEFAULT 0 COMMENT 'Saldo del pedido en dólares',
  `iva` DECIMAL(18, 4) DEFAULT 0 COMMENT 'IVA del pedido en dólares',
  `codigo_pedido` VARCHAR(50) NOT NULL COMMENT 'Código único del pedido en la APP',
  `porc_gdesc` DECIMAL(10, 2) DEFAULT 0 COMMENT 'Porcentaje de descuento global',
  `descrip` VARCHAR(255) DEFAULT NULL COMMENT 'Descripción del pedido',
  `co_us_in` VARCHAR(15) DEFAULT 'prov' COMMENT 'Usuario que creó el pedido',
  `fecha` DATETIME NOT NULL COMMENT 'Fecha y hora de creación del pedido',
  `tasa` DECIMAL(18, 4) DEFAULT 1 COMMENT 'Tasa de cambio aplicada',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp de creación del registro',
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp de última actualización',
  INDEX `idx_fact_num` (`fact_num`),
  INDEX `idx_cod_cliente` (`cod_cliente`),
  INDEX `idx_codigo_pedido` (`codigo_pedido`),
  INDEX `idx_fecha` (`fecha`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Cabecera de pedidos creados desde la APP';

-- ============================================
-- Tabla: pedido_productos
-- ============================================
-- Almacena los items/renglones de cada pedido
CREATE TABLE IF NOT EXISTS `pedido_productos` (
  `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID autoincremental del producto en el pedido',
  `pedido_id` INT NOT NULL COMMENT 'ID del pedido (FK a pedidos.id)',
  `fact_num` VARCHAR(20) NOT NULL COMMENT 'Número de factura/pedido en Profit Plus',
  `co_art` VARCHAR(20) NOT NULL COMMENT 'Código del artículo',
  `cantidad` DECIMAL(18, 4) NOT NULL DEFAULT 0 COMMENT 'Cantidad del producto',
  `precio` DECIMAL(18, 4) NOT NULL DEFAULT 0 COMMENT 'Precio unitario en dólares',
  `subtotal` DECIMAL(18, 4) NOT NULL DEFAULT 0 COMMENT 'Subtotal (cantidad * precio)',
  `co_alma` VARCHAR(5) DEFAULT '01' COMMENT 'Código del almacén (01=Tachira, 04=Barquisimeto)',
  `reng_num` INT DEFAULT NULL COMMENT 'Número de renglón en Profit Plus',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp de creación del registro',
  INDEX `idx_pedido_id` (`pedido_id`),
  INDEX `idx_fact_num` (`fact_num`),
  INDEX `idx_co_art` (`co_art`),
  CONSTRAINT `fk_pedido_productos_pedido` 
    FOREIGN KEY (`pedido_id`) 
    REFERENCES `pedidos` (`id`) 
    ON DELETE CASCADE 
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Productos/items de los pedidos de la APP';

-- ============================================
-- Comentarios y Notas
-- ============================================
-- 1. La tabla 'pedidos' almacena una copia de cada pedido creado desde la APP
--    con los mismos datos que se insertaron en Profit Plus (SQL Server).
--
-- 2. La tabla 'pedido_productos' almacena cada item/artículo del pedido,
--    permitiendo consultar rápidamente qué productos fueron pedidos.
--
-- 3. Los campos están en dólares (no en bolívares) para mantener coherencia
--    con el JSON recibido. La conversión a bolívares se hace al insertar en Profit Plus.
--
-- 4. El campo 'co_alma' indica el almacén:
--    - '01': Tachira (San Cristóbal)
--    - '04': Barquisimeto
--
-- 5. El campo 'tasa' guarda la tasa de cambio dólar/bolívar usada al momento del pedido.
--
-- 6. Se incluyen índices para optimizar las consultas más comunes por:
--    - Número de factura (fact_num)
--    - Código de cliente (cod_cliente)
--    - Código de pedido (codigo_pedido)
--    - Fecha del pedido
--    - Artículos vendidos (co_art)

-- ============================================
-- Verificación de Tablas
-- ============================================
-- Para verificar que las tablas se crearon correctamente, ejecutar:
-- SHOW TABLES LIKE 'pedidos%';
-- SHOW CREATE TABLE pedidos;
-- SHOW CREATE TABLE pedido_productos;
