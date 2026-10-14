-- Dedicated tables: do not change operational/notdone read models.
CREATE TABLE IF NOT EXISTS ofs_repeated_orders_control (
 id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
 last_requested_at DATETIME(6) NULL,
 last_started_at DATETIME(6) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT IGNORE INTO ofs_repeated_orders_control (id) VALUES (1);

CREATE TABLE IF NOT EXISTS ofs_repeated_orders_run (
 id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
 status VARCHAR(16) NOT NULL,
 requested_at DATETIME(6) NOT NULL,
 requested_by_user_id INT NULL,
 requested_by_username VARCHAR(150) NULL,
 started_at DATETIME(6) NULL,
 heartbeat_at DATETIME(6) NULL,
 finished_at DATETIME(6) NULL,
 date_from DATE NULL,
 date_to DATE NULL,
 pages INT UNSIGNED NOT NULL DEFAULT 0,
 received INT UNSIGNED NOT NULL DEFAULT 0,
 error_text VARCHAR(500) NULL,
 KEY idx_repeat_status (status,id),
 KEY idx_repeat_requested (requested_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ofs_repeated_orders_snapshot (
 id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
 run_id BIGINT UNSIGNED NOT NULL,
 payload_json LONGTEXT NOT NULL,
 published_at DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO permissoes (recurso,descricao)
SELECT 'ofs.os_repetidas.visualizar','Visualizar análise de OS repetidas por contrato'
WHERE NOT EXISTS (SELECT 1 FROM permissoes WHERE recurso='ofs.os_repetidas.visualizar');
INSERT INTO permissoes (recurso,descricao)
SELECT 'ofs.os_repetidas.atualizar','Solicitar atualização global de OS repetidas'
WHERE NOT EXISTS (SELECT 1 FROM permissoes WHERE recurso='ofs.os_repetidas.atualizar');
INSERT INTO perfil_permissao (perfil_id,permissao_id)
SELECT pf.id,pm.id FROM perfis pf JOIN permissoes pm
ON pm.recurso IN ('ofs.os_repetidas.visualizar','ofs.os_repetidas.atualizar')
WHERE pf.slug='admin' AND NOT EXISTS
(SELECT 1 FROM perfil_permissao pp WHERE pp.perfil_id=pf.id AND pp.permissao_id=pm.id);
