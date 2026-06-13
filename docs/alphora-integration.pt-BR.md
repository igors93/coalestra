# Integração com o Alphora

A Coalestra deve substituir aquisição e composição de leituras. Estratégia, risco, reconciliação decisória e envio de ordens permanecem no Alphora.

## Modelo recomendado

Use um único `SyncSnapshotBuilder` durante toda a execução e uma `SnapshotSession` por ciclo do Governor:

```text
startup
    +-- cria fontes
    +-- cria SnapshotBuilder
    +-- cria SyncSnapshotBuilder

ciclo do Governor
    +-- abre sessão
    +-- estágio base: conta, posições e mercado leve
    +-- seleção: posições abertas, scheduler, dirty e fast lane
    +-- estágio pesado: mark price, ordens e regras dos selecionados
    +-- fecha sessão
```

O limite global pertence ao builder e é compartilhado por todos os ciclos, callbacks e sessões. Isso impede que ciclos concorrentes multipliquem a pressão sobre a Binance ou sobre pools de threads.

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

## Configuração de capacidade

Uma configuração inicial adequada para medir o Alphora:

```python
builder = SnapshotBuilder(
    sources=sources,
    max_concurrency=8,
    source_concurrency={
        "binance-rest": 4,
        "market-stream": 16,
        "user-data-stream": 16,
        "alphora-symbol-rules": 4,
        "alphora-position-view": 4,
    },
)
```

Os limites de stream podem ser maiores porque a leitura é local e rápida. O limite REST deve ser pequeno e medido com latência p95 e peso real dos endpoints.

`source_concurrency` deve ser usado para configuração operacional. O `max_concurrency` declarado no adaptador pode servir como padrão reutilizável.

## Políticas de resiliência sugeridas

### MarketDataHub

```python
market_stream_policy = SourceResiliencePolicy(
    retry=RetryPolicy(max_attempts=1),
    circuit=CircuitBreakerPolicy(
        scope=CircuitScope.SUBJECT,
        failure_threshold=2,
        recovery_timeout_seconds=2.0,
    ),
)
```

Use `SUBJECT` para que dados stale ou ausentes de `BTCUSDT` não desativem o stream para `ETHUSDT`.

### User Data Stream

Use `SUBJECT` para posições e ordens por símbolo. Para o resumo global da conta, use `RESOURCE` ou uma fonte separada com circuito próprio.

### Binance REST

```python
rest_policy = SourceResiliencePolicy(
    retry=RetryPolicy(
        max_attempts=2,
        base_delay_seconds=0.05,
        max_delay_seconds=0.2,
    ),
    circuit=CircuitBreakerPolicy(
        scope=CircuitScope.NAMESPACE,
        failure_threshold=3,
        recovery_timeout_seconds=5.0,
    ),
)
```

`NAMESPACE` permite separar falhas de mercado, conta e ordens sem criar um circuito para cada chave. Endpoints com comportamento muito diferente podem ser representados por fontes REST separadas.

## Publicação do MarketDataHub

O callback do WebSocket pode publicar o estado de mercado sem bloquear a thread do produtor:

```python
def on_market_snapshot(snapshot) -> None:
    coalestra_provider.publisher.submit_publish(
        market_state(snapshot.symbol),
        snapshot,
        source="market-stream",
        observed_at=snapshot.received_at,
        metadata={
            "event_time": snapshot.event_time,
            "bid": str(snapshot.bid_price),
            "ask": str(snapshot.ask_price),
        },
    )
```

O `observed_at` deve representar o instante real do dado, não o momento em que o Governor o leu.

## Publicação do User Data Stream

Após classificar e converter o evento:

```python
def on_position_update(symbol: str, position_value, event_time: float) -> None:
    coalestra_provider.publisher.submit_publish(
        position(symbol),
        position_value,
        source="user-data-stream",
        observed_at=event_time,
    )


def on_orders_update(symbol: str, orders, event_time: float) -> None:
    coalestra_provider.publisher.submit_publish(
        open_orders(symbol),
        orders,
        source="user-data-stream",
        observed_at=event_time,
    )
```

Eventos atrasados são ignorados automaticamente. Eventos duplicados com o mesmo timestamp também são ignorados. Durante reconciliação REST, use `force=True` somente quando a resposta autoritativa deve substituir o cache independentemente da ordem temporal recebida.

Quando houver gap de stream ou estado incompleto:

```python
coalestra_provider.publisher.invalidate(
    position(symbol),
    reason="user-stream-gap",
)
```

A próxima resolução usará a cadeia normal de fontes e poderá cair para REST.

## Fontes em lote

Use `BatchSnapshotSource` quando uma operação atende várias chaves:

- leitura local de vários símbolos do `MarketDataHub`;
- endpoint ou cache com vários mark prices;
- consulta de todas as posições;
- consulta de ordens para vários símbolos;
- leitura de várias entradas de um cache operacional.

Uma fonte em lote pode retornar parte das chaves. As ausentes seguem para a próxima fonte. Com circuito `SUBJECT`, apenas símbolos cujo grupo falhou deixam de entrar nos próximos lotes.

## Recursos derivados

Não gere chamadas externas por símbolo para dados que já existem em uma resposta global:

```text
ALL_POSITIONS
    +-- POSITION(BTCUSDT)
    +-- POSITION(ETHUSDT)

EXCHANGE_INFO
    +-- EXCHANGE_RULES(BTCUSDT)
    +-- EXCHANGE_RULES(ETHUSDT)
```

```python
rules_source = CallableDerivedSource(
    name="alphora-symbol-rules",
    priority=100,
    supports=lambda key: key.namespace == "exchange" and key.name == "rules",
    dependencies=lambda _key: (EXCHANGE_INFO,),
    deriver=lambda key, snapshot, _context: extract_symbol_rules(
        snapshot.value(EXCHANGE_INFO, dict),
        key.subject,
    ),
    max_concurrency=4,
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

Uma publicação recebida depois que uma chave foi resolvida não altera o valor já fixado na sessão atual. Ela será observada no próximo ciclo. Isso evita que partes diferentes de uma decisão usem versões diferentes do mesmo recurso.

## Pontos de integração

### Startup

Crie o provider junto com `MarketDataHub`, `UserDataStreamHub` e o cliente Binance. Feche o `SyncSnapshotBuilder` no shutdown.

### `CycleContext`

Adicione:

```python
operational_snapshot: Snapshot | None = None
```

### Avaliação leve

Leia `market_state(symbol)` da sessão em vez de pedir um novo snapshot ao `MarketDataRouter`.

### Avaliação pesada

Mapeie as chaves da Coalestra para `HeavyMarketSnapshot`. O mapper não deve realizar I/O.

### Consolidação

Após comparação shadow:

1. remover cache de conta do `MarketDataRouter`;
2. remover cache de exchange info do `MarketDataRouter`;
3. remover cache duplicado de exchange info do `ExecutionEngine`;
4. remover consultas individuais de posição usadas somente para montar a visão do ciclo;
5. manter escrita, risco e execução serializados no Alphora.

## Migração

1. **Shadow:** construir a sessão e comparar com o caminho atual.
2. **Publicação de mercado:** alimentar a Coalestra pelo MarketDataHub.
3. **Mercado e regras:** adotar market state e regras derivadas.
4. **Posições:** publicar User Data Stream e adotar `ALL_POSITIONS`/posições derivadas.
5. **Ordens e conta:** publicar cache privado com REST como fallback.
6. **Remoção:** retirar caches e aquisições duplicadas.

## Métricas mínimas

- concorrência global em uso e em espera;
- concorrência por fonte em uso e em espera;
- tempo esperando capacidade por fonte;
- circuitos abertos por escopo;
- tentativas por fonte;
- publicações aceitas, antigas e duplicadas;
- invalidações;
- chamadas individuais e em lote;
- tamanho médio dos lotes;
- cache hits e single-flight joins;
- stale fallbacks;
- duração do estágio base, estágio pesado e sessão completa.
