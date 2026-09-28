SELECT table_name FROM information_schema.tables
WHERE table_schema=DATABASE()
  AND table_name IN ('ofs_operational_monitor_treatment','ofs_operational_monitor_treatment_event')
ORDER BY table_name;

SELECT recurso,descricao FROM permissoes
WHERE recurso IN ('ofs.monitor_operacional.tratar','ofs.monitor_operacional.supervisionar')
ORDER BY recurso;

SELECT status,COUNT(*) AS total FROM ofs_operational_monitor_treatment GROUP BY status ORDER BY status;
