SELECT table_name,engine FROM information_schema.tables WHERE table_schema=DATABASE()
AND table_name IN ('ofs_repeated_orders_control','ofs_repeated_orders_run','ofs_repeated_orders_snapshot');
SELECT table_name,column_name,column_type,is_nullable FROM information_schema.columns WHERE table_schema=DATABASE()
AND table_name IN ('ofs_repeated_orders_control','ofs_repeated_orders_run','ofs_repeated_orders_snapshot') ORDER BY table_name,ordinal_position;
SELECT * FROM ofs_repeated_orders_control WHERE id=1;
SELECT recurso,descricao FROM permissoes WHERE recurso IN ('ofs.os_repetidas.visualizar','ofs.os_repetidas.atualizar');
SELECT pf.slug,pm.recurso FROM perfil_permissao pp JOIN perfis pf ON pf.id=pp.perfil_id
JOIN permissoes pm ON pm.id=pp.permissao_id WHERE pm.recurso IN ('ofs.os_repetidas.visualizar','ofs.os_repetidas.atualizar');
SELECT status,COUNT(*) AS total FROM ofs_repeated_orders_run GROUP BY status;
