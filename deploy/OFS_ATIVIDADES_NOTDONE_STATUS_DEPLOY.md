# Status da OS em atividades improdutivas/canceladas

Escopo: somente a rota `/ofs/atividades-notdone` (pendentes, tratadas e detalhes). Não altera a tratativa nem o worker operacional. A importação de canceladas Adapter deve estar presente no mesmo código antes de publicar esta tela. São elegíveis os códigos `998`, `999`, `99999` e `52482` em `XA_SER_CLO_IMP_ADA`, respeitando a lista atual de tipos desconsiderados.

## Antes de publicar em produção

1. Confirmar que o checkout de produção está limpo e registrar o commit anterior.
2. Fazer o pull por fast-forward. Não reiniciar o painel ainda: o código novo consulta `activity_status`.
3. Na base usada pelo painel, aplicar e validar a coluna:

   ```bash
   ./venv/bin/python tools/ofs_atividades_notdone_status_schema.py --apply
   ./venv/bin/python tools/ofs_atividades_notdone_status_schema.py --validate
   ```

   A aplicação é idempotente. Registros antigos recebem `notdone`, pois antes só esse status era importado. A migração preserva todos os campos de tratativa. O `ALTER TABLE` pode adquirir bloqueio breve; fazer em janela de baixo uso e verificar a duração no ambiente de produção.

4. Conferir que não há relatórios ativos antes de reiniciar **somente** `ofs-painel.service`.
5. Validar as abas pendentes e tratadas, o modal, a rota de detalhes e uma importação controlada de um dia/recurso conhecido. A nova consulta faz chamadas separadas para `notdone` e `cancelled`.

## Reversão

Se necessário, primeiro voltar o painel ao commit anterior. Só então remover a coluna, se realmente desejado, com:

```bash
./venv/bin/python tools/ofs_atividades_notdone_status_schema.py --rollback --confirm DROP_NOTDONE_ACTIVITY_STATUS
```

Remover a coluna apaga a distinção já importada entre canceladas e improdutivas. É mais seguro mantê-la durante um rollback do código.
