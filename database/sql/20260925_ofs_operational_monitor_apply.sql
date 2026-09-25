-- Monitor operacional OFS compartilhado.
-- A tela nunca consulta a API Oracle: usa o read model alimentado pelo worker.

SET @schema_name = DATABASE();

SET @ddl = IF(
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=@schema_name AND table_name='ofs_activity_operational_state' AND column_name='record_type'),
  'DO 0',
  'ALTER TABLE ofs_activity_operational_state ADD COLUMN record_type VARCHAR(32) NULL AFTER activity_type'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @ddl = IF(
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=@schema_name AND table_name='ofs_activity_operational_state' AND column_name='start_time'),
  'DO 0',
  'ALTER TABLE ofs_activity_operational_state ADD COLUMN start_time DATETIME NULL AFTER record_type'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @ddl = IF(
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=@schema_name AND table_name='ofs_activity_operational_state' AND column_name='duration_minutes'),
  'DO 0',
  'ALTER TABLE ofs_activity_operational_state ADD COLUMN duration_minutes INT UNSIGNED NULL AFTER start_time'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @ddl = IF(
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=@schema_name AND table_name='ofs_activity_operational_state' AND column_name='time_slot'),
  'DO 0',
  'ALTER TABLE ofs_activity_operational_state ADD COLUMN time_slot VARCHAR(32) NULL AFTER duration_minutes'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @ddl = IF(
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=@schema_name AND table_name='ofs_activity_operational_state' AND column_name='is_black'),
  'DO 0',
  'ALTER TABLE ofs_activity_operational_state ADD COLUMN is_black TINYINT(1) NULL DEFAULT 0 AFTER time_slot'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Eventos podem omitir a propriedade personalizada. NULL significa "não informado"
-- e evita apagar um valor conhecido durante atualização parcial via Events API.
ALTER TABLE ofs_activity_operational_state
    MODIFY COLUMN is_black TINYINT(1) NULL DEFAULT 0;

SET @ddl = IF(
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=@schema_name AND table_name='ofs_activity_operational_state' AND column_name='customer_name'),
  'DO 0',
  'ALTER TABLE ofs_activity_operational_state ADD COLUMN customer_name VARCHAR(255) NULL AFTER is_black'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS ofs_operational_monitor_snapshot (
    scope_key VARCHAR(32) NOT NULL,
    work_date DATE NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'missing',
    payload_json LONGTEXT NULL,
    refreshed_at DATETIME(6) NULL,
    expires_at DATETIME(6) NULL,
    refresh_started_at DATETIME(6) NULL,
    refresh_finished_at DATETIME(6) NULL,
    requested_by_user_id INT NULL,
    requested_by_username VARCHAR(150) NULL,
    error_text VARCHAR(500) NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (scope_key),
    KEY idx_ofs_monitor_snapshot_expiry (expires_at),
    KEY idx_ofs_monitor_snapshot_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ofs_operational_monitor_refresh_log (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    scope_key VARCHAR(32) NOT NULL,
    status VARCHAR(16) NOT NULL,
    requested_by_user_id INT NULL,
    requested_by_username VARCHAR(150) NULL,
    started_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6) NULL,
    technicians_count INT UNSIGNED NULL,
    activities_count INT UNSIGNED NULL,
    error_text VARCHAR(500) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    KEY idx_ofs_monitor_refresh_scope_started (scope_key,started_at),
    KEY idx_ofs_monitor_refresh_status_started (status,started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO permissoes (recurso, descricao)
SELECT 'ofs.monitor_operacional', 'Acessar e atualizar Monitor Operacional OFS'
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes WHERE recurso='ofs.monitor_operacional'
);

INSERT INTO perfil_permissao (perfil_id, permissao_id)
SELECT pf.id, pm.id
FROM perfis pf
JOIN permissoes pm ON pm.recurso='ofs.monitor_operacional'
WHERE pf.slug='admin'
  AND NOT EXISTS (
      SELECT 1 FROM perfil_permissao pp
      WHERE pp.perfil_id=pf.id AND pp.permissao_id=pm.id
  );
