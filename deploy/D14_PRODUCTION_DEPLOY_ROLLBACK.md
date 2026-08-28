# Demanda 14 — Deploy, validação e rollback de produção

Este roteiro é a barreira final de GO/NO-GO. Ele não adiciona funcionalidades e preserva as decisões D10–D13: monitor HTTP somente local/MySQL, worker independente, Events incremental, GET Route completo apenas em baseline/recovery e sincronização estrutural independente.

## 0. Premissas

- Executar no repositório Git real de produção/staging, não no ZIP sem `.git` usado para handoff entre agentes.
- Nunca copiar `.env`, `instance/`, `temp_uploads/`, `venv/`, `.git/`, `__pycache__/` ou `*.pyc` para o pacote de distribuição.
- O `hierarchy sync` não deve iniciar baseline operacional nem GET Route em massa.
- `caught_up` continua `null` quando não houver evidência persistida.

Defina antes dos comandos:

```bash
export PROJECT_DIR=/home/bvagner/projeto_update_ofs
export APP_USER="$(stat -c '%U' "$PROJECT_DIR")"
cd "$PROJECT_DIR"
```

## 1. Checkpoint Git / backup pré-deploy

```bash
cd "$PROJECT_DIR"
git status --short
git rev-parse --verify HEAD
git log -5 --oneline
```

O deploy só segue com o estado conhecido. Consolidar D11–D13/D14 em commit rastreável no repositório real e confirmar novamente:

```bash
git status --short
git diff --check
git log -3 --oneline
```

Registrar o SHA do commit liberado:

```bash
export RELEASE_COMMIT="$(git rev-parse HEAD)"
printf '%s\n' "$RELEASE_COMMIT"
```

## 2. Atualização do código

Usar o fluxo Git já adotado pela operação (`git pull`/checkout do commit aprovado). Após atualizar:

```bash
cd "$PROJECT_DIR"
git status --short
git rev-parse HEAD
```

O SHA deve ser o mesmo `RELEASE_COMMIT` aprovado e o working tree deve estar limpo.

## 3. Banco — migrations/validates

D11, D12, D13 e D14 não introduzem schema novo. Não executar migration inventada. Validar o schema já existente e depois o gate D14 read-only:

```bash
mysql -u "$DB_USER" -p "$DB_NAME" < database/sql/20260825_ofs_resource_hierarchy_validate.sql
mysql -u "$DB_USER" -p "$DB_NAME" < database/sql/20260826_ofs_technician_operational_validate.sql
mysql -u "$DB_USER" -p "$DB_NAME" < database/sql/20260826_ofs_d10_hardening_validate.sql
mysql -u "$DB_USER" -p "$DB_NAME" < database/sql/20260828_ofs_d14_release_validate.sql
```

Critérios: zero datas futuras; zero registros anteriores a `CURDATE()-6`; health operacional e estrutural coerentes; hierarquia com `rows_total = distinct_resources` e somente o batch corrente em `last_seen_at`.

## 4. Sincronização inicial da hierarquia

Executar antes de instalar/ativar o timer, comprovando uma coleta real saudável:

```bash
cd "$PROJECT_DIR"
./venv/bin/python tools/sync_ofs_resource_hierarchy.py
```

Registrar `resources_total/active_total`, duração, chamadas OFS e `hierarchy_last_success_at_utc`. A falha deve interromper o deploy; não apagar o snapshot anterior.

## 5. Instalar/atualizar hierarchy service + timer

Gerar as units sem inserir segredo nelas:

```bash
sudo sed \
  -e "s|__OFS_SERVICE_USER__|$APP_USER|g" \
  -e "s|__OFS_PROJECT_DIR__|$PROJECT_DIR|g" \
  deploy/systemd/ofs-resource-hierarchy-sync.service.example \
  | sudo tee /etc/systemd/system/ofs-resource-hierarchy-sync.service >/dev/null

sudo cp deploy/systemd/ofs-resource-hierarchy-sync.timer.example \
  /etc/systemd/system/ofs-resource-hierarchy-sync.timer

sudo systemctl daemon-reload
sudo systemctl enable --now ofs-resource-hierarchy-sync.timer
sudo systemctl start ofs-resource-hierarchy-sync.service

systemctl status ofs-resource-hierarchy-sync.service --no-pager
systemctl status ofs-resource-hierarchy-sync.timer --no-pager
systemctl list-timers --all --no-pager | grep ofs-resource-hierarchy-sync
```

Aceite: timer `enabled/active`; execução oneshot mais recente com `Result=success`; `last_success_at` estrutural recente.

## 6. Instalar/atualizar worker operacional

```bash
sudo sed \
  -e "s|__OFS_SERVICE_USER__|$APP_USER|g" \
  -e "s|__OFS_PROJECT_DIR__|$PROJECT_DIR|g" \
  deploy/systemd/ofs-technician-operational-worker.service.example \
  | sudo tee /etc/systemd/system/ofs-technician-operational-worker.service >/dev/null

sudo systemctl daemon-reload
sudo systemctl enable ofs-technician-operational-worker.service
sudo systemctl restart ofs-technician-operational-worker.service
systemctl status ofs-technician-operational-worker.service --no-pager
systemctl show ofs-technician-operational-worker.service \
  -p ActiveState -p SubState -p UnitFileState -p ExecMainStatus -p TimeoutStopUSec
```

Aceite: `active/running`, `enabled`, `ExecMainStatus=0` e timeout equivalente a 300 s.

## 7. Baseline / estabilização

**Não executar baseline completo por rotina de upgrade se o cursor/read model já estiver saudável.** Isso evitará ~1.333 GET Route desnecessários. Deixar o worker retomar Events/reconciliação e aguardar health estabilizar.

Somente quando cursor/read model estiver ausente ou o próprio mecanismo de recovery exigir baseline, parar o serviço antes do baseline manual:

```bash
sudo systemctl stop ofs-technician-operational-worker.service
cd "$PROJECT_DIR"
./venv/bin/python tools/ofs_technician_operational_worker.py --baseline-once
sudo systemctl start ofs-technician-operational-worker.service
```

## 8. Gunicorn

Reiniciar a unit Gunicorn já existente no ambiente. Não inventar nome de serviço; identificar antes:

```bash
systemctl list-units --type=service --all | grep -i gunicorn
```

Depois reiniciar somente a unit correta da aplicação e confirmar `active/running`.

## 9. Gates automáticos finais

```bash
cd "$PROJECT_DIR"
./venv/bin/python -m pytest -q
./venv/bin/python tools/ofs_d14_release_probe.py \
  --repetitions 10 \
  --systemd \
  --check-lock \
  --output instance/reports/ofs_d14_release/d14_final.json

git diff --check
git status --short
```

Critério automático esperado: `release_gate.status = AUTOMATED_CHECKS_PASS`.

## 10. Network real no navegador

Com DevTools > Network limpo:

1. Abrir `/ofs/` e confirmar **0** requests `/dashboard/technicians/*` antes de clicar em “Técnicos e Rotas”.
2. No primeiro clique, confirmar exatamente o carregamento do `summary` + raiz `tree?mode=children`.
3. Expandir um ramo e confirmar `parent_id=<id>`.
4. Clicar KPIs/filtros e confirmar propagação de `filter=` e/ou `only_problems=1` server-side.
5. Confirmar ausência de `mode=full` em todas as requests.
6. Durante `systemctl start ofs-resource-hierarchy-sync.service`, repetir navegação e confirmar que a home/monitor continuam respondendo.

Registrar screenshots ou HAR sem cookies/Authorization/credenciais.

## 11. Smoke funcional

Validar no prefixo real `/ofs`:

- Dashboard OS;
- Técnicos e Rotas;
- Relatórios;
- BI Activities;
- DDC Mensageria;
- Atualizar leitura do monitor;
- worker operacional ativo;
- sync/timer da hierarquia ativo.

Não executar operação destrutiva ou envio massivo apenas para smoke. Usar telas/consultas seguras já adotadas pela operação.

## 12. ZIP sanitizado + SHA-256

No checkout limpo do commit aprovado:

```bash
cd "$PROJECT_DIR"
./venv/bin/python tools/build_d14_release_package.py \
  --output ../ofs-updater-d14-release.zip \
  --manifest ../ofs-updater-d14-release.manifest.json

cat ../ofs-updater-d14-release.zip.sha256
```

O manifesto deve registrar `forbidden_members=[]` e `secret_scan_issues=[]`.

## 13. Decisão GO/NO-GO

**GO** somente com todos os itens simultaneamente verdadeiros:

- suíte completa verde;
- `release_gate = AUTOMATED_CHECKS_PASS`;
- Events/Activities/Calendars dentro dos thresholds e `overall_integrity=ok`;
- hierarchy status `ok`, `last_success_at` recente e timer saudável;
- worker ativo com lifecycle/lock corretos;
- retenção sem datas futuras/expiradas;
- Network incremental validado em navegador real;
- smoke das rotas principais OK;
- Git limpo e commit registrado;
- ZIP sanitizado e SHA-256 registrado.

Qualquer falha acima = **NO-GO** até correção dentro da D14.

# Rollback

## Código

Voltar ao commit pré-deploy aprovado pelo fluxo Git da operação e reiniciar Gunicorn/worker. Não usar `git reset --hard` se houver trabalho local não preservado.

## Worker operacional

```bash
sudo systemctl disable --now ofs-technician-operational-worker.service
```

Se o código anterior ainda usa o worker validado D10/D11, reinstalar a unit correspondente à versão anterior e reativar após rollback do código.

## Hierarchy timer/service

```bash
sudo systemctl disable --now ofs-resource-hierarchy-sync.timer
sudo systemctl stop ofs-resource-hierarchy-sync.service
```

Desativar o timer não apaga `ofs_resource_hierarchy`; o último snapshot válido permanece disponível para o painel.

## Schema

D14 não cria nem altera schema. Não há rollback SQL específico da D14. Os rollbacks D04/D06 somente devem ser usados se o rollback de arquitetura exigir remover completamente os componentes auxiliares, nunca como primeira ação de incidente.
