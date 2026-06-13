# Implementação completa das melhorias 7, 8 e 9

## 7. Identidade genérica de recursos

A identidade deixou de impor lowercase em `namespace`/`name` e uppercase em `subject`. O comportamento padrão agora preserva case, removendo uma premissa específica de símbolos de mercado.

Foram adicionados:

- `KeyNormalizer` configurável;
- `PRESERVE_KEY_NORMALIZER` como padrão;
- `LEGACY_KEY_NORMALIZER` para compatibilidade 0.1-0.3;
- `CASE_INSENSITIVE_KEY_NORMALIZER`;
- `ResourceKey.legacy(...)`;
- qualifiers imutáveis e ordenados;
- `qualifier()`, `with_qualifiers()` e `without_qualifiers()`.

Os qualifiers participam de hash, igualdade, ordenação, cache, single-flight e circuit breaker por recurso.

## 8. Cache em lote e refresh

### Cache

`AsyncMemoryCache` agora possui:

- `get_many()`;
- `set_many()`;
- `invalidate_many()`;
- `invalidate_matching()`;
- `invalidate_namespace()`;
- `prune()`;
- `stats()`;
- limite LRU padrão de 10.000 entradas;
- remoção automática de entradas além de `max_stale_seconds`.

O protocolo opcional `BatchAsyncCache` permite que caches externos implementem operações em lote. Caches antigos continuam funcionando por fallback.

O builder consulta todo o conjunto de chaves no cache de uma vez e agrupa writes frescos por fonte. A publicação em lote também usa `get_many`/`set_many` sob locks ordenados.

### Refresh

`FreshnessPolicy` ganhou:

- `RefreshMode.BLOCKING`;
- `RefreshMode.STALE_WHILE_REVALIDATE`;
- `RefreshMode.REFRESH_AHEAD`;
- `refresh_ahead_seconds`.

Refreshes são deduplicados por chave, usam o single-flight existente, respeitam capacidade e resiliência, podem ser aguardados por `wait_for_refreshes()` e não substituem o cache por um resultado stale.

## 9. Observabilidade desacoplada e diagnósticos

### Diagnósticos por snapshot

Cada `Snapshot` contém `SnapshotDiagnostics` com:

- duração;
- recursos pedidos, resolvidos e com erro;
- hits e misses de cache;
- operações batch de cache;
- valores stale;
- joins single-flight;
- chamadas de fonte, batch e derivação;
- refreshes agendados, concluídos e com falha;
- chamadas e latência acumulada por fonte;
- skew entre timestamps de observação.

Os diagnósticos são acumulados por sessão. Dependências internas contam como aquisição, mas não como recursos explicitamente pedidos.

### Sinks bufferizados

Foram adicionados:

- `BufferedEventSink`;
- `BufferedMetricsSink`;
- `BufferOverflowPolicy`;
- `BufferedSinkStats`.

Os sinks colocam registros em uma fila limitada e entregam em thread dedicada. Assim, log em arquivo, serialização ou exportação de métricas não bloqueiam o caminho de aquisição.

Políticas de overflow:

- `DROP_OLDEST`;
- `DROP_NEWEST`;
- `RAISE`.

Falhas do downstream são contabilizadas e não retornam para o pipeline de snapshot.
