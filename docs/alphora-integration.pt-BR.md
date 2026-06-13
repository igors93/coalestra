# Integração com o Alphora

A Coalestra deve substituir somente aquisição e composição de leituras. Estratégia, risco, reconciliação decisória e envio de ordens permanecem no Alphora.

## Modelo recomendado

Use uma `SnapshotSession` por ciclo do Governor:

```text
ciclo do Governor
    |
    +-- estágio base
    |     conta, posições e mercado leve
    |
    +-- seleção
    |     posições abertas, scheduler, dirty e fast lane
    |
    +-- estágio pesado
          mark price, ordens e regras somente para símbolos selecionados
```

A sessão garante o mesmo `snapshot_id`, deadline e valores já adquiridos nos dois estágios.

## Vocabulário sugerido

```python
from coalestra import ResourceKey

ACCOUNT = ResourceKey("account", "summary")
ALL_POSITIONS = ResourceKey("account", "positions")
EXCHANGE_INFO = ResourceKey("exchange", "info")


def market_state(symbol: str) -> ResourceKey:
    return ResourceKey("market", "state", symbol)


def mark_price(symbol: str) -> ResourceKey:
    return ResourceKey("market", "mark-price", symbol)


def position(symbol: str) -> ResourceKey:
    return ResourceKey("account", "position", symbol)


def open_orders(symbol: str) -> ResourceKey:
    return ResourceKey("orders", "open", symbol)


def open_algo_orders(symbol: str) -> ResourceKey:
    return ResourceKey("orders", "open-algo", symbol)


def exchange_rules(symbol: str) -> ResourceKey:
    return ResourceKey("exchange", "rules", symbol)
```

## Fontes em lote

Use `BatchSnapshotSource` quando uma única leitura consegue atender várias chaves:

- leitura de vários preços mantidos pelo `MarketDataHub`;
- endpoint que devolve mark prices de todos os símbolos;
- consulta de todas as posições;
- consulta de ordens para vários símbolos;
- leitura de um cache compartilhado com várias entradas.

Uma fonte em lote pode retornar apenas parte das chaves. As ausentes seguem automaticamente para a próxima fonte, normalmente REST.

## Recursos derivados

Dois recursos importantes não devem gerar chamadas externas por símbolo:

```text
ALL_POSITIONS
    +-- POSITION(BTCUSDT)
    +-- POSITION(ETHUSDT)
    +-- POSITION(SOLUSDT)

EXCHANGE_INFO
    +-- EXCHANGE_RULES(BTCUSDT)
    +-- EXCHANGE_RULES(ETHUSDT)
    +-- EXCHANGE_RULES(SOLUSDT)
```

Exemplo de regras derivadas:

```python
from coalestra import CallableDerivedSource

rules_source = CallableDerivedSource(
    name="alphora-symbol-rules",
    priority=100,
    supports=lambda key: key.namespace == "exchange" and key.name == "rules",
    dependencies=lambda _key: (EXCHANGE_INFO,),
    deriver=lambda key, snapshot, _context: extract_symbol_rules(
        snapshot.value(EXCHANGE_INFO, dict),
        key.subject,
    ),
)
```

Exemplo de posição derivada:

```python
position_source = CallableDerivedSource(
    name="alphora-position-view",
    priority=100,
    supports=lambda key: key.namespace == "account" and key.name == "position",
    dependencies=lambda _key: (ALL_POSITIONS,),
    deriver=lambda key, snapshot, _context: extract_position(
        snapshot.value(ALL_POSITIONS, list),
        key.subject,
    ),
)
```

## Ciclo incremental

```python
with coalestra_provider.session(
    snapshot_id=cycle_id,
    deadline_seconds=3.0,
    metadata={"cycle_id": cycle_id},
) as session:
    baseline_keys = [ACCOUNT, ALL_POSITIONS]
    baseline_keys.extend(market_state(symbol) for symbol in configured_symbols)
    baseline = session.resolve(baseline_keys, strict=False)

    open_symbols = find_open_symbols(baseline.value(ALL_POSITIONS, list))
    selected_symbols = rank_due_symbols(open_symbols, dirty_symbols, fast_lane_symbols)

    heavy_keys = [EXCHANGE_INFO]
    for symbol in selected_symbols:
        heavy_keys.extend(
            [
                mark_price(symbol),
                position(symbol),
                open_orders(symbol),
                open_algo_orders(symbol),
                exchange_rules(symbol),
            ]
        )

    operational_snapshot = session.resolve(heavy_keys, strict=False)
    cycle_context.operational_snapshot = operational_snapshot
```

## Pontos de integração

### Startup

Crie um único `SyncSnapshotBuilder` junto com `MarketDataHub`, `UserDataStreamHub` e o cliente Binance. Não recrie o provider a cada ciclo.

### `CycleContext`

Adicione:

```python
operational_snapshot: Snapshot | None = None
```

### Avaliação leve

Leia `market_state(symbol)` da sessão em vez de pedir novo snapshot ao `MarketDataRouter`.

### Avaliação pesada

Mapeie as chaves da Coalestra para o `HeavyMarketSnapshot`. Não faça novas leituras dentro do mapper.

### Consolidação

Depois da comparação shadow:

1. remover cache de conta do `MarketDataRouter`;
2. remover cache de exchange info do `MarketDataRouter`;
3. remover cache duplicado de exchange info do `ExecutionEngine`;
4. remover consultas individuais de posição usadas somente para montar a visão do ciclo;
5. manter escrita, risco e execução serializados no Alphora.

## Migração

1. **Shadow:** construir a sessão e comparar com o caminho atual.
2. **Mercado e regras:** adotar market state e exchange rules derivadas.
3. **Posições:** adotar `ALL_POSITIONS` e posições derivadas.
4. **Ordens e conta:** adotar fontes de stream/cache com REST como fallback.
5. **Remoção:** retirar caches e aquisições duplicadas do Alphora.

## Métricas mínimas

- chamadas individuais por ciclo;
- chamadas em lote por ciclo;
- tamanho médio dos lotes;
- recursos derivados;
- dependências compartilhadas;
- cache hits;
- single-flight joins;
- stale fallbacks;
- duração do estágio base;
- duração do estágio pesado;
- duração total da sessão.
