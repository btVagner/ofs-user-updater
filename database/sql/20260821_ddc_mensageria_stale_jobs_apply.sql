-- Demanda 02 - DDC Mensageria: recuperação auditável de jobs travados
-- Aplicar ANTES do deploy do código correspondente.

ALTER TABLE ddc_mensageria_jobs
    ADD COLUMN heartbeat_at DATETIME NULL AFTER started_at,
    ADD COLUMN abandoned_at DATETIME NULL AFTER finished_at,
    ADD COLUMN abandoned_reason VARCHAR(500) NULL AFTER abandoned_at;

-- Dá uma janela inicial de segurança para um job running que eventualmente
-- esteja ativo no momento da migration. O código passará a renovar este campo.
UPDATE ddc_mensageria_jobs
SET heartbeat_at = NOW()
WHERE status = 'running'
  AND heartbeat_at IS NULL;
