# Integração com o Alphora

A primeira integração deve substituir aquisições de leitura duplicadas, sem mover regras de trading para a Coalestra.

## Escopo inicial

Use a Coalestra para obter:

- preço atual;
- mark price;
- posição por símbolo;
- ordens normais abertas;
- ordens algorítmicas abertas;
- informações da conta;
- regras da exchange.

Não mova para a biblioteca:

- score da estratégia;
- decisão de entrada ou saída;
- portfolio risk;
- pre-execution guard;
- criação e envio de ordens;
- decisão de reconciliação de estado.

## Vocabulário sugerido

```python
PRICE = lambda symbol: ResourceKey("market", "price", symbol)
MARK_PRICE = lambda symbol: ResourceKey("market", "mark_price", symbol)
POSITION = lambda symbol: ResourceKey("account", "position", symbol)
OPEN_ORDERS = lambda symbol: ResourceKey("orders", "open", symbol)
OPEN_ALGO_ORDERS = lambda symbol: ResourceKey("orders", "open_algo", symbol)
EXCHANGE_RULES = lambda symbol: ResourceKey("exchange", "rules", symbol)
ACCOUNT = ResourceKey("account", "summary")
```

## Prioridade das fontes

```text
100: MarketDataHub / UserDataStream auditado
 50: cache operacional local validado
 10: Binance REST
```

Fontes de stream devem informar o horário real do evento em `observed_at`. Se o valor estiver fora do TTL, a Coalestra continua automaticamente para a próxima fonte.

## Uso no ciclo do Governor

No início do ciclo:

1. Determine símbolos vencidos, em fast lane e com posição aberta.
2. Monte o conjunto de recursos necessários.
3. Construa um único snapshot.
4. Coloque esse snapshot no `CycleContext`.
5. Faça light e heavy evaluation lerem do mesmo snapshot.
6. Mantenha portfolio risk final, pre-execution guard e execução serializados no Alphora.

```python
snapshot = provider.build(
    cycle_resource_keys,
    strict=False,
    deadline_seconds=3.0,
    metadata={"cycle_id": cycle_id},
)

cycle_context.operational_snapshot = snapshot
```

Crie o `SyncSnapshotBuilder` uma vez no startup e feche-o no shutdown. Não crie uma nova fachada a cada ciclo, pois isso descartaria o estado de cache e resiliência.

## Migração segura

1. **Shadow:** construa o snapshot e compare com o caminho atual.
2. **Adoção read-only:** use o snapshot para preços e regras da exchange.
3. **Dados privados:** adote posição e ordens do User Data Stream auditado, mantendo fallback REST.
4. **Consolidação:** remova caches duplicados do Governor, Router e ExecutionEngine.
5. **Medição:** acompanhe chamadas REST por ciclo, cache hit, stale fallback e latência p50/p95.

## Regra de segurança

A presença de um valor no snapshot não significa que ele esteja autorizado para execução. O Alphora deve continuar validando freshness, estado reconciliado, risco e payload final antes de qualquer mutação.
