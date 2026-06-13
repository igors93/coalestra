# Revisão final para integração

## Contrato recomendado

O sistema consumidor deve manter um `SnapshotBuilder` e um `SyncSnapshotBuilder` durante todo o processo. Cada ciclo abre uma sessão e utiliza `SnapshotRequest` para separar dados obrigatórios de dados auxiliares.

```python
baseline_request = SnapshotRequest(
    required=[ACCOUNT, ALL_POSITIONS],
    optional=[market_state(symbol) for symbol in configured_symbols],
)

with provider.session(snapshot_id=cycle_id, deadline_seconds=3.0) as session:
    baseline = session.resolve_request(baseline_request)
    selected = select_symbols(baseline)
    operational = session.resolve_request(heavy_request(selected))
```

## Configuração inicial para o Alphora

- `max_concurrency=8`;
- `source_concurrency={"binance-rest": 4}`;
- `max_batch_size` ajustado ao endpoint real;
- `run_sync_in_thread=False` somente para MarketDataHub e caches locais garantidamente não bloqueantes;
- `ObservationPolicy(future_tolerance_seconds=1.0)`;
- `manage_lifecycle=True` quando as fontes e sinks pertencem exclusivamente ao provider.

## Recursos obrigatórios

Para decisão e execução, normalmente são obrigatórios:

- posição do símbolo;
- preço ou estado de mercado atual;
- regras da exchange;
- conta quando o cálculo depende de saldo ou exposição;
- ordens abertas quando há reconciliação ou proteção.

Dados de learning, relatórios, contexto auxiliar e observabilidade podem ser opcionais enquanto a integração é estabilizada.

## Shutdown

1. impedir novos ciclos;
2. fechar sessões em andamento;
3. fechar `SyncSnapshotBuilder`;
4. fechar hubs WebSocket que não sejam gerenciados pelo provider;
5. persistir métricas finais.

## Métricas mínimas

Registre por ciclo:

- `duration_ms`;
- `cache_hits` e `cache_misses`;
- `source_calls_by_source`;
- `retries`;
- `batch_chunks`;
- `support_cache_hits`;
- `future_timestamp_rejections`;
- `observation_skew_ms`;
- recursos opcionais e obrigatórios que falharam.

## Sequência de adoção

1. shadow, comparando os valores com o caminho atual;
2. exchange info e regras derivadas;
3. market state local com execução inline;
4. posições e ordens publicadas por eventos;
5. conta compartilhada por ciclo;
6. remoção de caches duplicados do Alphora.
