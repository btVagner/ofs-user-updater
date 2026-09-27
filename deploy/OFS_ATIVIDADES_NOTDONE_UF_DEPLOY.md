# Deploy controlado — filtro de UF em Atividades Improdutivas

## Escopo

- adiciona `state_province` à tabela `ofs_atividades_notdone`;
- cria índice para período, UF e situação da tratativa;
- passa a capturar `stateProvince` da API OFS;
- adiciona filtro server-side de UF às abas Pendentes e Tratadas;
- preserva integralmente os campos de tratativa durante a atualização dos dados de origem.

## Pré-deploy

Execute cada comando separadamente e valide a saída antes de avançar.

```bash
git status --short --branch
git rev-parse HEAD | tee /tmp/ofs_notdone_uf_commit_anterior.txt
sudo git fetch origin main
sudo git log --oneline HEAD..origin/main
```

## Atualização

Interrompa apenas o painel web. O worker operacional não precisa ser parado.

```bash
sudo systemctl stop ofs-painel.service
sudo git pull --ff-only origin main
./venv/bin/python tools/ofs_atividades_notdone_uf_schema.py --apply
./venv/bin/python tools/ofs_atividades_notdone_uf_schema.py --validate
```

O `validate` deve confirmar:

- coluna `state_province` do tipo `varchar(64)`;
- índice `idx_notdone_date_state_treated` com `date,state_province,tratado_em`;
- contagem existente preservada. Antes da primeira atualização OFS, `rows_with_state` pode ser zero.

## Validação de código e inicialização

```bash
PYTHONPYCACHEPREFIX=/tmp/ofs_notdone_uf_pycache ./venv/bin/python -m py_compile routes/atividades_notdone_routes.py tools/ofs_atividades_notdone_uf_schema.py
sudo systemctl start ofs-painel.service
sudo systemctl status ofs-painel.service --no-pager -l
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8001/atividades-notdone
```

Sem sessão, o endpoint pode responder `302` para o login. Isso confirma que o processo web está atendendo.

## Preenchimento da UF

Na tela, selecione o período desejado e use **Atualizar da API**. A atualização passa a preencher a UF tanto em registros novos quanto nos registros já existentes retornados pela API.

Valide a cobertura:

```bash
./venv/bin/python -c 'from database.connection import get_connection; c=get_connection(); q=c.cursor(); q.execute("SELECT COUNT(*),SUM(state_province IS NOT NULL AND TRIM(state_province)<>%s),COUNT(DISTINCT NULLIF(TRIM(state_province),%s)) FROM ofs_atividades_notdone",("","")); print({"total_com_uf_ufs_distintas":q.fetchone()}); q.execute("SELECT state_province,COUNT(*) FROM ofs_atividades_notdone WHERE state_province IS NOT NULL AND TRIM(state_province)<>%s GROUP BY state_province ORDER BY state_province",("",)); print(q.fetchall()); q.close(); c.close()'
```

## Rollback

O rollback da interface/código deve usar o commit salvo. A remoção da coluna é opcional e destrói somente os valores de UF, sem apagar atividades ou tratativas:

```bash
./venv/bin/python tools/ofs_atividades_notdone_uf_schema.py --rollback --confirm DROP_NOTDONE_STATE_PROVINCE
```
