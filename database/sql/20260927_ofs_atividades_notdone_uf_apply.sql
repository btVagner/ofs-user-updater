-- UF das atividades improdutivas. Migração idempotente.

SET @schema_name = DATABASE();

SET @ddl = IF(
  EXISTS(
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema=@schema_name
      AND table_name='ofs_atividades_notdone'
      AND column_name='state_province'
  ),
  'DO 0',
  'ALTER TABLE ofs_atividades_notdone ADD COLUMN state_province VARCHAR(64) NULL AFTER city'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @ddl = IF(
  EXISTS(
    SELECT 1
    FROM information_schema.statistics
    WHERE table_schema=@schema_name
      AND table_name='ofs_atividades_notdone'
      AND index_name='idx_notdone_date_state_treated'
  ),
  'DO 0',
  'ALTER TABLE ofs_atividades_notdone ADD INDEX idx_notdone_date_state_treated (`date`, state_province, tratado_em)'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;
