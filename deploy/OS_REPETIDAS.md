# OS repetidas por contrato — implantação e validação

Implementação inicial local de 14/10/2026, validada com fixtures e MySQL
descartável. Naquela etapa não houve push, migração no banco configurado,
consulta à API real ou alteração de serviços de produção. A ativação local
posterior está registrada ao final deste documento.

## Regras e suposições

- Janela fixa: data operacional de hoje até hoje + 6 dias, inclusive, pelo campo
  `date`. O dia é calculado em `OFS_OPERATIONAL_TIMEZONE` (padrão
  `America/Sao_Paulo`) no início da execução. Solicitações que aguardam na fila
  usam o dia em que o worker efetivamente começa.
- Contrato: `customerNumber` textual, preservando zeros iniciais. Somente status
  `pending`, `enroute` e `started` participam. `cancelled`, `notdone`, outros status
  e status ausente são excluídos antes do agrupamento. Não há filtro OFS `q` nem limite
  aos técnicos com tipos específicos: a árvore do recurso **02** é lida.
- Excluem-se os tipos de não-OS já usados pelo monitor (`LUNCH`, `ALM`,
  `MAN_VEIC`, `REUNIAO`, `EQP_DUP`) e `recordType` lunch/break/travel. Também
  são excluídos Retirada (`RET`, `RET_*`, `RET-*`, `RETIRADA*` com separador).
- NG: origem comparada sem distinguir maiúsculas/minúsculas, após remover espaços
  externos. OS com o mesmo prefixo antes do primeiro hífen contam uma vez por
  contrato. Sem hífen, o número inteiro é o prefixo. Prefixos diferentes contam
  separadamente, mesmo com o mesmo sufixo. Fora de NG, usa-se o número inteiro
  por origem. Não se fundem números de sistemas de origem diferentes.
- `activityId` repetido e idêntico é eliminado. Números de OS repetidos são
  consolidados, mantendo as atividades diferentes nos detalhes. Para um mesmo
  `activityId` com dados diferentes, o worker consulta GET activities/{id}
  uma vez e usa a versão atual. Aplica novamente data, Retirada e hierarquia
  sobre essa versão. Há limite de 100 IDs a reconciliar por execução; consulta
  inválida, falha ou limite excedido impedem publicar qualquer resultado parcial.
- Duas OS distintas: vermelho somente se todos os tipos envolvidos forem iguais;
  caso contrário amarelo. Assim, grupos NG com múltiplos tipos nunca produzem
  um vermelho indevido. Três ou mais OS distintas: roxo.
- Registros sem contrato, número de OS, prefixo NG válido, tipo ou data válida
  são excluídos, com contadores no snapshot e até 20 amostras por motivo em
  `diagnostic_samples` (activityId, contrato, número e data). Uma atividade pode
  incrementar mais de um motivo. OS válida sem `activityId` invalida a coleta.

Essas decisões ficam explícitas na tela. Ajustar a inclusão de status ou o
tratamento de tipos ausentes exige uma decisão funcional, não uma alteração
silenciosa de filtros.

## Persistência e concorrência

Não é possível aproveitar `ofs_activity_operational_state` para esta análise:
faltam contrato/origem e o worker operacional reconcilia o dia corrente. A tabela
`ofs_atividades_notdone` cobre apenas um subconjunto. As duas permanecem intactas.

As tabelas dedicadas são `ofs_repeated_orders_control` (controle singleton),
`ofs_repeated_orders_run` (auditoria/fila) e `ofs_repeated_orders_snapshot`
(último resultado completo). As rotas GET e os filtros não consultam OFS. POST
retorna 202 depois de enfileirar no MySQL; nenhum thread é criado no Gunicorn.

`SELECT ... FOR UPDATE` sobre o singleton serializa solicitações entre processos.
Uma solicitação pendente/ativa bloqueia outra. O intervalo de 600 segundos usa
o maior timestamp entre solicitação e início efetivo, obtidos com relógio UTC
do MySQL. O worker também verifica 600 segundos entre inícios, cobrindo filas
atrasadas. Falhas e interrupções não dispensam o intervalo.

O lock nomeado `ofs_repeated_orders_worker` é mantido pela conexão do worker
durante a coleta inteira. Antes de progresso/publicação, verifica-se que a mesma
conexão ainda possui o lock. Outro processo não pode assumir trabalho por idade
do heartbeat. Quando o processo/conexão anterior termina, o próximo dono registra
`running` abandonado como `interrupted`, preserva o resultado anterior e permite
uma nova solicitação após o intervalo. Solicitações `queued` sobrevivem ao restart.
Não há retry automático de uma execução interrompida, evitando nova coleta pesada
sem solicitação. O cliente existente mantém seus retries limitados por requisição.

O worker só instancia o cliente OFS quando existe trabalho. Faz GET activities
sequencial com 11 campos, `limit=1000`, `includeChildren=all` e datas inclusivas.
Avança `offset` pela quantidade recebida e continua enquanto `hasMore=true`,
mesmo que a página seja curta. Ausência de `hasMore` encerra, conforme a
[documentação Oracle](https://docs.oracle.com/en/cloud/saas/field-service/cxfsc/op-rest-ofsccore-v1-activities-get.html).
Antes da coleta, lê todos os descendentes atuais de `02`, com campo `resourceId`,
100 itens por página, `totalResults` e offset pela quantidade recebida. Não usa
a hierarquia local antiga como whitelist. Inclui o próprio recurso `02` e
exclui atividades com recurso ausente ou fora desse conjunto. Inconsistência
ou coleta incompleta da hierarquia impedem publicar o resultado.
Recursos sem `resourceId` externo, que a Oracle pode retornar apenas com
`resourceInternalId`, são contabilizados em `scope_metrics.missing_external_id`
e não entram na whitelist. O offset inclui todos os itens recebidos, inclusive
esses registros, para esgotar a paginação corretamente.

Página vazia com `hasMore`, formato inválido, erro HTTP ou limite de 10.000 páginas
impedem publicação. Não há truncamento bem-sucedido. A API paginada não garante
uma visão transacional das atividades mutáveis; duplicatas diferentes recebem
reconciliação pontual. A origem da divergência (campos que mudaram) e o total
reconciliado ficam no snapshot em `reconciliation_samples` e
`diagnostics.reconciled_activity`. Mudanças sem repetição de ID entre páginas
continuam sujeitas à limitação de consistência da API.

Progresso (`pages`, `received`, `heartbeat_at`), solicitante, datas, início, fim,
status e erro sanitizado ficam em MySQL. O snapshot só muda junto com
`succeeded` na mesma transação InnoDB. Falhas não apagam o resultado anterior.
O painel sinaliza quando a janela do snapshot pertence a outro dia. A auditoria
de execuções não tem exclusão automática; planejar retenção conforme uso.

## Implantação futura (não executada)

1. Confirmar o checkout/commit e o alvo MySQL antes de qualquer comando. Fazer
   backup das tabelas/permissões afetadas. `CREATE TABLE` MySQL tem commit implícito;
   o apply é idempotente, mas não é uma transação DDL reversível.
2. A raiz desta análise é fixa em `02`: `OFS_REPEATED_ORDERS_ROOT_ID` e
   `OFS_MONITOR_CASA_CLIENTE_ROOT_ID` não podem ampliar/trocar esse escopo.
   Confirmar `OFS_OPERATIONAL_TIMEZONE`. O cliente reutiliza `OFS_BASE_URL`,
   `OFS_USERNAME`, `OFS_PASSWORD` e os timeouts/retries operacionais existentes.
   Não é necessário ampliar subscriptions nem modificar workers existentes.
3. Aplicar somente a migração nova, depois validar:

   ```bash
   venv/bin/python tools/ofs_repeated_orders_schema.py --apply
   venv/bin/python tools/ofs_repeated_orders_schema.py --validate
   ```

   `validate` retorna código de falha para tabelas/colunas/chaves/permissões
   ausentes e engine diferente de InnoDB. A saída lista estrutura e concessões.
   O apply não tenta reparar uma estrutura incompatível existente.
4. Instalar uma unit separada a partir de
   `deploy/systemd/ofs-repeated-orders-worker.service.example`, substituindo os
   dois placeholders. O worker carrega o `.env` do projeto. Não colocar segredos
   na unit. Reiniciar o painel somente após a migração, dentro de uma janela
   autorizada; iniciar apenas o novo worker. Nenhum serviço existente precisa
   de alteração por esta implementação.
5. As permissões `ofs.os_repetidas.visualizar` e `ofs.os_repetidas.atualizar` são
   independentes. O apply concede ambas ao perfil `admin`; conceder as demais
   pelo gerenciamento de perfis. Atualizar a sessão/login após mudar permissões.
   POST exige ambas e token CSRF da página.
6. Abrir `/ofs/os-repetidas`: deve ler apenas MySQL e mostrar estado sem snapshot.
   Com autorização para uma coleta real controlada, clicar uma vez em atualizar.
   Conferir progresso, sucesso e contadores com uma amostra real; comparar NG com
   prefixos iguais/diferentes. Segunda solicitação deve receber 409. Sessão sem
   permissão de atualizar deve ler, mas não disparar coleta.
7. Diagnóstico sem consulta OFS:

   ```bash
   venv/bin/python tools/ofs_repeated_orders_worker.py --status
   ```

   Para consumir uma única solicitação já enfileirada, em ambiente autorizado:

   ```bash
   venv/bin/python tools/ofs_repeated_orders_worker.py --once
   ```

   Uma fila parada indica worker ausente/erro de infraestrutura. `running` sem
   progresso exige verificar o processo/conexão e logs; não liberar locks nem
   editar status enquanto o dono estiver ativo. Após reiniciar o novo worker,
   verificar `interrupted` e solicitar novamente quando liberado.

## Rollback

Interromper o novo worker e o acesso web à funcionalidade, salvar backup do
snapshot/auditoria/concessões e reverter o código da funcionalidade. O rollback
remove exclusivamente as três tabelas e as duas permissões novas, incluindo
concessões; perde o histórico. O executor exige confirmação textual e adquire
o lock do worker durante a remoção, recusando um worker ativo.

```bash
venv/bin/python tools/ofs_repeated_orders_schema.py --rollback --confirm DROP_OFS_REPEATED_ORDERS
```

Não executar o SQL bruto com a aplicação ativa. A confirmação e o lock estão
no executor Python; consultas SQL isoladas não oferecem essas proteções.

## Reprodução local segura

O runner abaixo substitui o módulo de conexão por um bloqueio. Rotas antigas
importam o pool antecipadamente; a substituição evita abrir o banco do `.env`.

```powershell
.\venv\Scripts\python.exe tools/test_ofs_repeated_orders_local.py tests -q
```

Integração: contêiner descartável, nome `ofs-repeated-orders-test-20261014`,
MySQL 8.4, porta **127.0.0.1:33317**, banco `ofs_repeat_test`, senha de fixture
`local-repeated-orders-test`, sem volumes. O teste usa factory com destino fixo
e recria apenas esse banco de fixtures, independentemente das variáveis DB_*.

```powershell
docker run --detach --name ofs-repeated-orders-test-20261014 --publish 127.0.0.1:33317:3306 --env MYSQL_ROOT_PASSWORD=local-repeated-orders-test --env MYSQL_DATABASE=ofs_repeat_test mysql:8.4
$env:OFS_REPEAT_DISPOSABLE_MYSQL='1'
.\venv\Scripts\python.exe tools/test_ofs_repeated_orders_local.py tests/test_ofs_repeated_orders_mysql.py -q
docker rm --force ofs-repeated-orders-test-20261014
```

Aguardar readiness MySQL antes dos testes. Nunca apontar essa factory para
banco com dados operacionais. A senha acima serve exclusivamente às fixtures.

Preview sem banco/OFS, somente em loopback:

```powershell
.\venv\Scripts\python.exe tools/preview_ofs_repeated_orders_local.py
```

Abrir `http://127.0.0.1:5097/fixture-session`. O preview é uma aplicação de teste
separada, não faz parte das rotas do app real e não deve ser implantado como
serviço. Em outro terminal, com Playwright disponível via NODE_PATH:

```powershell
node tests/ofs_repeated_orders_frontend.cjs
```

O teste verifica filtros, paginação, detalhes preservados após polling, dados
escapados, erros e estado pendente; captura notebook e mobile (sidebar recolhida).
Saídas ficam em `temp_uploads/repeated-orders-qa/`, ignoradas pelo Git. Encerrar
o preview com Ctrl+C e remover somente o contêiner de teste ao terminar.

## Evidência da implementação inicial

Resultados finais locais em 14/10/2026:

- Suíte offline completa: **342 passed, 10 skipped, 9 subtests passed**. Oito
  skips correspondem à integração opt-in, executada separadamente; dois são
  testes condicionais já existentes.
- Integração real em MySQL 8.4 descartável: **8 passed**. Inclui apply repetido,
  validate, rollback/reapply, solicitações entre quatro processos, lock entre
  conexões, perda de propriedade do lock, recuperação de interrupção, falha de
  paginação, fila atrasada e rollback de transação durante publicação.
- Playwright/Chrome: filtros, paginação de 30 contratos, detalhes após polling,
  texto potencialmente malicioso sem execução, erros e estado pendente passaram.
  Sem erros de JavaScript; todas as requisições foram ao preview loopback.
  Layout conferido em 1366×768, 1280×720 e 1024×768, e em 390×844 com sidebar
  recolhida. Capturas e relatório em `temp_uploads/repeated-orders-qa/`.
- `node --check` e `git diff --check` passaram; compilação Python sem erros.
- Preview e contêiner descartável encerrados ao terminar. Não foi criada unit
  real, alterado serviço, aplicado SQL ao banco configurado ou feito push.

Evidência de fixtures e MySQL descartável não representa aceite com dados reais
nem comprovação de publicação em produção. A validação do schema e o aceite
no ambiente de produção permanecem etapas de implantação autorizada futura.

## Ativação no ambiente local — 14/10/2026

Após o usuário iniciar `app.py` na porta 5000, foi confirmado o banco
`localhost:3306/ofs`, servidor `PC-BRUNO`, MySQL 8.0.45. As três tabelas e as
duas permissões ainda não existiam. O apply foi executado nesse banco local,
seguido de validate bem-sucedido; ambas as permissões foram concedidas a `admin`.
Sessões anteriores à migração precisam de logout/login para carregar as concessões.

GETs na aplicação já em execução confirmaram HTTP 200, menu/card/sidebar,
permissões em `/perfis`, tela `/ofs/os-repetidas` e endpoint de snapshot. Não
foi necessário reiniciar o painel. A validação usou uma sessão local de teste,
com cache de tipos e presença previamente preenchidos para evitar consultas OFS
e escritas de presença durante esses GETs.

Uma solicitação feita no painel por `bvagner` às 10:16:18 entrou na fila. Foi
iniciado o worker local separado em segundo plano para consumi-la, com logs
em `instance/repeated-orders-local/worker.stdout.log` e `worker.stderr.log`.
O worker permanece local e ativo, aguardando solicitações; não foi instalada unit.

A execução 1 ocorreu entre 10:18:17 e 10:18:36 (America/Sao_Paulo), janela
14/10/2026–20/10/2026, 16 páginas e 15.230 registros. Terminou como `failed`
porque o mesmo `activityId` apareceu com dados diferentes. A causa exata das
diferenças não está identificada: os registros dessa coleta não foram salvos
como um snapshot válido. A proteção de consistência impediu publicação parcial;
naquela etapa o primeiro snapshot ainda não existia. O intervalo global continuou valendo.

Não houve push, alteração de serviços de produção ou escrita na API OFS.
Naquela etapa, a única coleta real foi a solicitação registrada pelo usuário no painel local.

## Ajuste de interface — 14/10/2026

Os cards de severidade passaram a filtrar por clique, com seleção combinada:
roxo mostra `3+`, vermelho usa apenas a borda e amarelo mostra `≠` com borda.
Os mesmos sinais são usados nos contratos. O dropdown de severidade foi
removido; tipos de OS são botões com seleção combinada e opção Todos. A busca
textual por contrato foi mantida.

O card Atividades na malha conta `activityId` únicos dos contratos em alerta,
incluindo cada atividade NG ligada, sem confundir esse total com a contagem
de OS consolidadas ou com todos os registros recebidos da API. O card mantém
o total global e limpa os filtros ao clicar; a linha junto aos filtros mostra
contratos e atividades do resultado filtrado. Clicar novamente em um filtro
selecionado o remove.

Verificação: 10 testes de rotas passaram; Playwright verificou seleção múltipla,
remoção de filtros, interação por teclado, total de 81 atividades únicas na
fixture (com atividade NG ligada e duplicata exata), contagem filtrada, ausência
de dropdowns, paginação e polling. Layout conferido em 1366×768, 1280×720,
1024×768 e 390×844 com sidebar recolhida, sem erros JavaScript. Não houve
coleta real nem mudança de schema neste ajuste. O painel local na porta 5000
foi reiniciado para renovar o cache de templates; o worker foi preservado.

## Retirada, escopo 02 e duplicatas — 14/10/2026

O usuário acrescentou a exclusão de Retirada e confirmou a restrição à hierarquia
02, após nova falha por ID repetido. O catálogo local confirmou `RET` como código
com descrição Retirada. O worker passou a fixar a raiz em 02, buscar seus
descendentes atuais uma vez por execução e validar o recurso de cada atividade.
A exclusão de Retirada e a validação de recurso ocorrem antes do agrupamento.

Para IDs repetidos com diferenças, o worker usa a versão atual de
[GET activity](https://docs.oracle.com/en/cloud/saas/field-service/cxfsc/op-rest-ofsccore-v1-activities-activityid-get.html),
com no máximo 100 consultas pontuais, reaplicando os critérios. Duplicatas
idênticas não fazem GET adicional. Falha de reconciliação preserva a publicação
atômica e impede qualquer snapshot parcial.

A execução 3 parou antes de coletar atividades porque a resposta da hierarquia
continha recursos sem ID externo. Duas consultas mínimas de diagnóstico
confirmaram o formato: `resourceInternalId` pode existir sem `resourceId`.
A leitura foi ajustada para registrar/excluir esses recursos da whitelist,
incluindo-os na contagem efetiva do offset. O intervalo de 10 minutos não foi
alterado; a execução seguinte aguardou a liberação normal.

Execução 4, solicitante `codex-local-validation`, **succeeded**: iniciou às
10:55:56 e terminou às 10:57:11, janela 14/10–20/10, snapshot publicado no
MySQL local e confirmado via HTTP 200 no endpoint da porta 5000.

- Hierarquia: 4.471 registros em 45 páginas; 144 sem ID externo. Whitelist
  de 4.328 IDs, incluindo o próprio recurso 02.
- Coleta: 15.676 registros em 16 páginas, esgotadas por hasMore/offset.
- Excluídas: 1.192 atividades de Retirada e 6.315 registros que não são OS.
- Duplicatas de atividade removidas: 3; números de OS consolidados: 13.
- Resultado: **262 contratos / 553 atividades únicas** na malha. Severidades:
  **9 roxo, 164 vermelho, 89 amarelo**.
- Nenhuma Retirada ou atividade fora da whitelist permaneceu na malha. A
  validação do payload também confirmou ausência de IDs de atividade duplicados.
- Essa coleta não precisou reconciliar IDs diferentes: as três duplicatas
  eram idênticas. Portanto, os campos divergentes das falhas anteriores não
  puderam ser identificados retrospectivamente. A nova reconciliação foi
  validada com testes de mudança de status/recurso/data e consulta malsucedida.

Verificação final: **361 testes offline e 9 subtestes**, com 10 skips (8 opt-in
MySQL e 2 condicionais). Os **8 testes MySQL** foram executados separadamente
e passaram. Código e critérios carregados no worker e no painel local; nenhum
push, alteração de produção, migração adicional ou escrita na API OFS ocorreu.

## Status ativos e campos visíveis — 14/10/2026

O usuário restringiu os status a `pending`, `enroute` e `started`. O worker
aplica essa whitelist antes de formar as OS/grupos NG e as severidades, inclusive
após reconciliar a versão atual de um ID. `notdone`, `cancelled`, outros status
e valores ausentes não participam. `diagnostics.excluded_status` registra a
exclusão nas novas coletas.

A leitura de snapshots antigos reaplica localmente a política aos detalhes
persistidos e recompõe tipos, OS, contratos e resumo. Como essa mudança remove
atividades, um contrato que não estava em alerta não pode ganhar OS adicionais;
os detalhes anteriores são suficientes para obter a nova malha desse mesmo
instante de coleta. A projeção mantém a data de publicação e os indicadores
da coleta original e identifica o motivo em `status_policy_source=previous_mesh`
e `diagnostics.excluded_status_from_previous_mesh`. Não dispara OFS nem altera
o histórico persistido. A próxima coleta salva diretamente a política nova.

No snapshot da execução 4, a projeção resultou em **64 contratos / 132 atividades**,
somente pending/enroute/started. Foram retiradas 286 atividades por status da
malha antiga; outras atividades ativas deixaram a malha porque seus contratos
passaram a ter menos de duas OS. A API local na porta 5000 confirmou esses totais
e a ausência de status não permitidos.

`date` e `resourceId` já eram coletados e persistidos. A interface agora exibe
**Date** e **ResourceId** no resumo de cada contrato e como colunas explícitas
nos detalhes. O resumo reúne datas e recursos, com até três IDs visíveis e
indicação de adicionais; o tooltip e os detalhes contêm todos os valores.

Validação: **373 testes e 9 subtestes passaram**, com os 10 skips condicionais
já descritos. Playwright verificou campos no resumo/detalhes, filtros, paginação
e quatro resoluções, sem erros JavaScript. Worker e painel locais recarregados;
nenhuma nova coleta real, alteração de schema ou produção foi feita neste ajuste.

## Regional e timeSlot por OS — 14/10/2026

A coleta passa a solicitar também `city`, `XA_ORIGIN_BUCKET` e `timeSlot`
(12 campos no total). Cada atividade guarda seu próprio `regional` e `timeSlot`;
as colunas ficam somente nos detalhes expandidos, mantendo as linhas recolhidas compactas.

Regional usa o cadastro existente `projToquio.td_bucket.Regional_PBI`, somente
linhas ativas. O vínculo prefere bucket de origem + cidade da OS; na ausência
deste par, aceita bucket com regional única. Quando a OS não informa bucket,
aceita cidade com regional única. Maiúsculas/minúsculas, espaços e acentos são
normalizados para comparar; variações de grafia da mesma regional não criam
botões duplicados. Nenhum vínculo ambíguo escolhe arbitrariamente uma regional:
o resultado fica como Sem regional. Falha ao consultar o cadastro interrompe
a atualização e preserva o snapshot anterior.

O worker consulta o cadastro uma vez por atualização e persiste o resultado
no JSON compartilhado. Abrir a página, filtrar e fazer polling continuam lendo
somente o snapshot, sem consultar OFS ou o cadastro de regionais.
Snapshot antigo sem estes campos mostra Sem regional e um traço no timeSlot
até a próxima coleta; não há inferência pelo recurso técnico da atividade.

Os botões de regional permitem seleção múltipla, combinada com contrato,
tipos e cores. O filtro mantém contratos com pelo menos uma OS nas regionais
selecionadas; detalhes e contagem preservam todas as OS destes contratos,
inclusive as de outras regionais, sem alterar a classificação da malha.
Todas limpa apenas regional; Atividades na malha limpa todos os filtros.

Validação automatizada: 377 testes passaram, 10 foram ignorados por dependerem
de infraestrutura opt-in e 9 subtestes passaram. Playwright validou regional
e timeSlot por atividade, seleção múltipla, Sem regional, combinação com tipo
e severidade, limpeza dos filtros, ausência de requisições durante filtragem,
preservação das OS de outras regionais e quatro resoluções sem overflow ou
erros JavaScript. Painel e worker locais foram atualizados com o worker ocioso.

Coleta local de leitura OFS run 5 concluída e publicada às 12:09:46 de
14/10/2026 (15:09:46 UTC), confirmada pelo endpoint da porta 5000:
16.455 registros / 17 páginas, 62 contratos / 128 atividades na malha.
Todas as 128 atividades informam timeSlot (67 manhã, 61 tarde).
71 atividades possuem regional identificada; 48 não encontram vínculo
no cadastro ativo e 9 possuem vínculo ambíguo (bucket BK_VIAMAO / cidade VIAMAO).
Estas 57 ficam acessíveis pelo botão Sem regional. Não foram modificados
cadastros, schema, GitHub ou serviços de produção.

## Simplificação para estados — 14/10/2026

Por solicitação do usuário, o agrupamento Regional_PBI foi substituído por UF.
Esta seção substitui a regra de regionais detalhada acima. A coleta solicita
`stateProvince` da própria OS, mantendo `timeSlot`, e deixa de solicitar `city`
e `XA_ORIGIN_BUCKET` nesta análise (11 campos). O worker não consulta mais
`projToquio.td_bucket`. Não há dependência do recurso técnico para determinar estado.

Siglas e nomes completos dos 26 estados e DF são normalizados para a mesma UF,
com comparação sem acentos e sem distinguir maiúsculas/minúsculas. Ausência ou
valor desconhecido fica como Sem estado. Regional antiga não é apresentada como
UF: snapshots anteriores precisam de nova coleta. `regional_source` no snapshot
passa a ser `activity.stateProvince` e o campo derivado `regional` guarda a UF.

Filtro e coluna são intitulados Estado. Mantêm botões com seleção múltipla,
combinação com tipo/cor/contrato e todas as OS do contrato nos detalhes.
timeSlot, Date e ResourceId continuam vinculados a cada atividade.

Validação de backend: 99 testes passaram, incluindo todas as UFs, nomes
completos, acentos, campos ausentes, rejeição de nomes de regional como estado,
duplicatas e projeção de status preservando estado e timeSlot por OS.

Playwright passou em quatro resoluções, sem erros JavaScript ou overflow,
validando filtros por UF combinados, multisseleção, Sem estado e limpeza.
Nova coleta local run 6 publicada em 14/10/2026 às 12:45:08 (15:45:08 UTC),
confirmada no endpoint da porta 5000: 16.711 registros, 63 contratos e
130 atividades na malha. Todas as 130 possuem UF e timeSlot.
Distribuição: GO 10, MG 40, MS 2, RS 24, SC 14 e SP 40.
Nenhuma mudança de schema, cadastro, GitHub ou produção.

## Limpeza de estados e escopo das bordas — 14/10/2026

Todos substitui Todas no filtro de estados. Limpar seleção desmarca todas as
UFs e seleciona Todos, preservando contrato, tipo e severidade. O teste de tela
confirmou multisseleção, limpeza e ausência de requisições ao filtrar/limpar.

A regra de bordas retas foi corrigida para atingir exclusivamente os elementos
dentro de `#repeated-orders`. O seletor do sidebar foi removido, substituindo
o tratamento descrito anteriormente. Chrome confirmou ícones do sidebar com
raio original de 6 px e controles do conteúdo com raio zero. CSS e JavaScript
novos confirmados na porta 5000, sem reinício ou nova coleta.

## Padrão visual do Monitor operacional — 14/10/2026

OS repetidas passou a carregar `ofs_operational_monitor.css` antes do seu CSS
específico, reutilizando a paleta e as classes de cabeçalho, painéis, cards,
botões, opções de filtros e tabelas do Monitor. A página usa o mesmo tamanho
máximo, margens e regras responsivas da referência. O CSS de OS repetidas mantém
somente adaptações da malha: cores/símbolos dos alertas, linhas recolhidas
compactas, carregamento e bordas retas exclusivamente dentro do seu conteúdo.
O stylesheet original do Monitor e o sidebar não foram alterados.

Filtros por contrato/tipo, estados e resultados foram organizados nos painéis
do mesmo padrão, com botão de atualização azul e cabeçalhos de tabela neutros.
Todos e Limpar seleção, Date/ResourceId/timeSlot por atividade, estados e
preservação das OS de outros estados continuam com o mesmo comportamento.

Validação: 10 testes de rotas e sintaxe JavaScript passaram. Playwright passou
em 1366, 1280, 1024 e 390 px, sem overflow ou erros JavaScript, conferindo filtros,
limpeza, paginação, expansão, sidebar arredondado, bordas retas do conteúdo,
campos por OS e estados da barra de carregamento. Capturas de notebook e celular
foram revisadas. Apenas o painel local foi recarregado para atualizar o template;
não houve nova coleta OFS, alteração de worker, schema, GitHub ou produção.

## Componentes completos do Monitor, com arredondamentos — 14/10/2026

O usuário revogou explicitamente a preferência por bordas retas e pediu os
mesmos modelos do Monitor operacional. Esta seção substitui as restrições
anteriores de arredondamento. A regra que zerava os raios foi removida.

Os cards usam diretamente `om-kpis`: rótulo acima do número, borda fina, raio,
sombra, padding, altura, grade responsiva e destaque `is-active` do Monitor.
O total também reflete visualmente se toda a malha está selecionada.
Permanecem os símbolos 3+ e diferente, com bordas de severidade na seleção
neutra. Filtros sincronizam `is-active` com `aria-pressed` e usam o estilo
original de `om-state-options`; limpeza continua exclusiva dos estados.

Painéis e linhas expansíveis usam `om-card`, resultados usam `om-table-card`,
campo de contrato usa `om-table-tools` e paginação usa `om-actions`/`om-secondary`.
Foram removidos estilos próprios que substituíam os modelos de cards, inputs,
botões selecionados e paginação. Linhas recolhidas continuam compactas; o CSS
específico contém somente ajustes de layout, severidade e carregamento.

Validação: 10 testes de rotas passaram. Chrome comparou o estilo calculado
do card de total com um card original `om-kpis` após a transição de seleção,
confirmando igualdade de disposição, padding, altura mínima, raio, largura
da borda, fundo, sombra, tamanho da fonte e alinhamento. Os testes de tela
passaram em quatro resoluções, mantendo filtros, limpeza, paginação, campos
por OS, expansão e carregamento, sem erros JavaScript ou overflow. Captura
do notebook revisada. Apenas o painel local foi recarregado; sem nova coleta.

## Linhas compactas e carregamento — 14/10/2026

O usuário esclareceu que Date e ResourceId pertencem a cada OS/atividade.
Os valores foram mantidos exclusivamente nas colunas dos detalhes expandidos,
usando `activity.date` e `activity.resourceId`. O resumo agregado de datas e
recursos foi removido. Linhas recolhidas mostram apenas contrato, quantidade
de OS e indicador, com altura de 36 px no notebook. Tipos individuais continuam
nos detalhes e nos filtros por botões.

Todos os elementos da análise têm bordas retas; o mesmo tratamento foi aplicado
ao sidebar somente nesta página. Não altera a aparência dos demais módulos.

Atualizar análise mostra imediatamente uma barra animada indeterminada, sem
percentual inventado. O estado acompanha a solicitação POST, fila e execução,
exibindo páginas/atividades recebidas e último heartbeat. A barra termina ao
confirmar sucesso ou falha. O botão permanece bloqueado durante a solicitação
e o processamento; o restante da malha anterior permanece disponível. O polling
consulta somente o snapshot local a cada 2 segundos enquanto há atualização e
a cada 10 segundos quando ocioso. Erros de leitura ficam visíveis durante novas
tentativas de recuperar o estado; nenhum novo job é criado pelo polling.

Verificação: 10 testes de rotas e checks de sintaxe/diff passaram. Playwright
testou alturas e bordas, datas e recursos diferentes por OS, preservação da
expansão, filtros/paginação e barra em solicitação, fila, execução, sucesso,
falha e rejeição de cooldown. Quatro resoluções passaram, sem erros JavaScript.
Capturas atualizadas em `temp_uploads/repeated-orders-qa/`, incluindo
`loading-1366.png`. A porta 5000 confirmou os novos arquivos após reiniciar
somente o painel local. Não houve coleta real, alteração de schema ou worker.
