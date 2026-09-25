# Monitor Operacional OFS — implantação controlada

Esta entrega adiciona uma tela restrita que lê snapshots compartilhados no MySQL. A abertura da tela, os filtros e a exportação não consultam a API Oracle.

## Ordem segura

1. Registrar o commit atual para rollback e confirmar árvore Git limpa.
2. Atualizar o código sem reiniciar serviços.
3. Aplicar e validar o schema:

   ```bash
   ./venv/bin/python tools/ofs_operational_monitor_schema.py --apply
   ./venv/bin/python tools/ofs_operational_monitor_schema.py --validate
   ```

   A migração é idempotente. Em instalações que já possuem o monitor, ela acrescenta
   somente `customer_state`, usado pelo filtro geográfico de UF.

4. Sincronizar a hierarquia Casa e Cliente:

   ```bash
   ./venv/bin/python tools/sync_ofs_resource_hierarchy.py --root 02
   ```

5. Confirmar no `.env` o timezone operacional da aplicação:

   ```text
   OFS_OPERATIONAL_TIMEZONE=America/Sao_Paulo
   ```

6. Reiniciar primeiro o worker operacional. O baseline inicial faz uma leitura de rota por técnico; executar em janela controlada e aguardar `Baseline operacional concluído`.
7. Reiniciar o serviço web e validar acesso com um usuário do perfil administrador.
8. Distribuir a permissão `ofs.monitor_operacional` aos demais perfis pelo menu Gerenciar Perfis.

## Comportamento de carga

- O worker continua sendo o único componente que consulta OFS.
- Atividades são reconciliadas em lote para Casa e Cliente; rotas continuam atualizadas por Events após o baseline.
- A tela gera o snapshot somente com dados MySQL.
- `GET_LOCK` impede dois usuários de atualizarem ao mesmo tempo.
- Um snapshot válido bloqueia nova atualização por 10 minutos para todos os usuários.
- Se uma fonte estiver desatualizada, regras baseadas em ausência são suprimidas para evitar falsos alertas.

## Rollback

O rollback de código deve voltar ao commit anterior. O rollback de schema remove tabelas e colunas e é destrutivo; só executar com serviços interrompidos e confirmação explícita:

```bash
./venv/bin/python tools/ofs_operational_monitor_schema.py --rollback --confirm DROP_OFS_OPERATIONAL_MONITOR
```
