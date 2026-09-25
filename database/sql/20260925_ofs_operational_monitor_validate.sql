SELECT table_name
FROM information_schema.tables
WHERE table_schema=DATABASE()
  AND table_name IN ('ofs_operational_monitor_snapshot','ofs_operational_monitor_refresh_log')
ORDER BY table_name;

SELECT column_name,column_type,is_nullable,column_default
FROM information_schema.columns
WHERE table_schema=DATABASE()
  AND table_name='ofs_activity_operational_state'
  AND column_name IN ('record_type','start_time','duration_minutes','time_slot','is_black','customer_name')
ORDER BY ordinal_position;

SELECT recurso,descricao
FROM permissoes
WHERE recurso='ofs.monitor_operacional';

SELECT pf.id,pf.nome,pf.slug
FROM perfis pf
JOIN perfil_permissao pp ON pp.perfil_id=pf.id
JOIN permissoes pm ON pm.id=pp.permissao_id
WHERE pm.recurso='ofs.monitor_operacional'
ORDER BY pf.id;

SELECT scope_key,status,work_date,refreshed_at,expires_at,requested_by_username,error_text
FROM ofs_operational_monitor_snapshot
ORDER BY scope_key;

