-- Tratativas ficam fora do payload do snapshot para sobreviver a cada atualização.
CREATE TABLE IF NOT EXISTS ofs_operational_monitor_treatment (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    scope_key VARCHAR(32) NOT NULL,
    work_date DATE NOT NULL,
    indicator VARCHAR(16) NOT NULL,
    item_key VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'open',
    item_label VARCHAR(255) NOT NULL DEFAULT '',
    technician VARCHAR(255) NOT NULL DEFAULT '',
    area VARCHAR(255) NOT NULL DEFAULT '',
    state_uf VARCHAR(64) NOT NULL DEFAULT '',
    actor_user_id INT NULL,
    actor_username VARCHAR(150) NULL,
    lease_token VARCHAR(128) NULL,
    lease_expires_at DATETIME(6) NULL,
    resolved_by_user_id INT NULL,
    resolved_by_username VARCHAR(150) NULL,
    resolved_at DATETIME(6) NULL,
    note VARCHAR(500) NULL,
    revision INT UNSIGNED NOT NULL DEFAULT 0,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_ofs_monitor_treatment_item (scope_key,work_date,indicator,item_key),
    KEY idx_ofs_monitor_treatment_resolved (scope_key,status,resolved_at),
    KEY idx_ofs_monitor_treatment_lease (status,lease_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ofs_operational_monitor_treatment_event (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    treatment_id BIGINT UNSIGNED NOT NULL,
    scope_key VARCHAR(32) NOT NULL,
    work_date DATE NOT NULL,
    indicator VARCHAR(16) NOT NULL,
    item_key VARCHAR(64) NOT NULL,
    action VARCHAR(16) NOT NULL,
    old_status VARCHAR(16) NOT NULL,
    new_status VARCHAR(16) NOT NULL,
    actor_user_id INT NOT NULL,
    actor_username VARCHAR(150) NOT NULL,
    note VARCHAR(500) NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    KEY idx_ofs_monitor_treatment_event_time (scope_key,occurred_at),
    KEY idx_ofs_monitor_treatment_event_item (treatment_id,occurred_at),
    CONSTRAINT fk_ofs_monitor_treatment_event_item FOREIGN KEY (treatment_id)
        REFERENCES ofs_operational_monitor_treatment (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO permissoes (recurso, descricao)
SELECT 'ofs.monitor_operacional.tratar', 'Tratar casos do Monitor Operacional OFS'
WHERE NOT EXISTS (SELECT 1 FROM permissoes WHERE recurso='ofs.monitor_operacional.tratar');

INSERT INTO permissoes (recurso, descricao)
SELECT 'ofs.monitor_operacional.supervisionar', 'Consultar supervisão das tratativas do Monitor Operacional OFS'
WHERE NOT EXISTS (SELECT 1 FROM permissoes WHERE recurso='ofs.monitor_operacional.supervisionar');

INSERT INTO perfil_permissao (perfil_id,permissao_id)
SELECT pf.id,pm.id FROM perfis pf JOIN permissoes pm
  ON pm.recurso IN ('ofs.monitor_operacional.tratar','ofs.monitor_operacional.supervisionar')
WHERE pf.slug='admin' AND NOT EXISTS (
    SELECT 1 FROM perfil_permissao pp WHERE pp.perfil_id=pf.id AND pp.permissao_id=pm.id
);
