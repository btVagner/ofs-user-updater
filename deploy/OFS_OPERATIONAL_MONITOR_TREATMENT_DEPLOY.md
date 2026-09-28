# Tratativas do Monitor Operacional OFS

Esta fase adiciona apenas duas tabelas e duas permissões no MySQL. O snapshot compartilhado
e o worker operacional não são alterados; ler ou tratar um caso não consulta a API do OFS.

## Antes de produção

1. Confirmar checkout limpo, commit/branch e backup do banco. Verificar relatórios em
   `instance/reports` com status `queued`/`running` antes de reiniciar `ofs-painel.service`:
   extrações rodam em threads do Gunicorn e um restart pode interrompê-las.
2. Aplicar e validar o schema na base de teste local:
   `python tools/ofs_operational_monitor_treatment_schema.py --apply` e `--validate`.
3. Testar claims simultâneos, token vencido, permissões e páginas; o teste de concorrência
   MySQL é opt-in (`OFS_MONITOR_TREATMENT_DB_TEST=1`).

## Produção, quando autorizada

1. Fazer `git fetch`, conferir o diff/commit e `git pull --ff-only` de maneira controlada.
2. Com o painel antigo ainda rodando, executar `./venv/bin/python tools/ofs_operational_monitor_treatment_schema.py --apply`
   e depois `--validate`. A migração é aditiva e idempotente; não altera dados existentes.
3. Conferir que não há extrações em andamento antes de reiniciar **somente** `ofs-painel.service`.
   Não reiniciar o worker operacional para esta fase.
4. Conceder `ofs.monitor_operacional.tratar` aos perfis de agentes e
   `ofs.monitor_operacional.supervisionar` aos supervisores. O perfil admin recebe ambas
   na migração. Usuários precisam entrar novamente para atualizar permissões na sessão.
5. Em duas sessões diferentes, confirmar reserva exclusiva da mesma linha, atualização
   automática de estado, filtro por tratativa, decisão, auditoria e supervisão.

## Semântica e recuperação

- Chave do caso: macro + data operacional + visão + ID da OS ou do técnico. Ela persiste
  entre snapshots do mesmo dia; `Clientes Black` nunca é tratável.
- O primeiro clique reserva a linha por cinco minutos; o modal renova a reserva a cada minuto
  enquanto a guia estiver visível. Fechar o modal libera o caso. Uma reserva perdida não
  autoriza salvar; o servidor devolve conflito e o estado corrente.
- `Resolvido` é final para o caso desse dia. `Aguardar` continua disponível para retomada;
  `Manter em aberto` volta ao estado inicial. Todas as decisões são registradas no log local.
- Na supervisão, resolvidas e ranking respeitam o período selecionado; reservas em análise
  representam o instante atual e `Aguardando hoje` usa a data operacional corrente. Os filtros
  de ação e agente consultam o histórico no MySQL antes do limite de 30 movimentações; não
  alteram o ranking nem fazem chamadas ao OFS. O supervisor atualiza os dados pelo botão.
- Rollback de código: retornar ao commit anterior e reiniciar apenas o painel após checar
  extrações. **Não remover as tabelas** em rollback operacional: elas guardam auditoria.
- Se a migração não estiver aplicada, não ativar o novo painel. Se falhar a leitura das
  tratativas, a interface desabilita as ações; o snapshot e o worker permanecem intactos.
