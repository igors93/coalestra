# Implementação das melhorias 4, 5 e 6

Base utilizada: commit `61deb30279fbcb0fda41bd7655b72dae8f20942d`.

## 4. Concorrência global e por fonte

Implementado em `coalestra.concurrency` e integrado ao `SnapshotBuilder`.

- `max_concurrency` agora limita o builder inteiro, não cada sessão isoladamente.
- Builds e sessões simultâneas compartilham o mesmo limite.
- `source_concurrency` permite limites operacionais por nome de fonte.
- `max_concurrency` nos adapters define um padrão por fonte.
- Configuração central substitui a declaração do adapter.
- A capacidade da fonte é adquirida antes da capacidade global.
- Espera por capacidade participa do timeout e do deadline.
- Batch consome uma unidade por chamada, não por chave.
- Retry não mantém capacidade durante o backoff.
- `capacity_snapshot()` e `CapacityController.snapshot()` expõem uso e espera.

## 5. Circuit breaker com escopo configurável

Implementado em `coalestra.resilience`.

- `CircuitScope.SOURCE`
- `CircuitScope.NAMESPACE`
- `CircuitScope.SUBJECT`
- `CircuitScope.RESOURCE`
- `CircuitBreakerPolicy`
- `SourceResiliencePolicy`
- `ResiliencePolicyResolver`
- Retry configurável por fonte.
- Circuitos independentes por grupo em fontes batch.
- Grupos com valor fresh registram sucesso.
- Grupos somente stale ou omitidos registram falha.
- Cancelamento de probe half-open retorna o circuito para open.
- API source-only anterior permanece compatível.

## 6. Publicação direta no cache

Implementado em `coalestra.cache.publisher` e nas fachadas síncronas.

- `ResourcePublisher`
- `SyncResourcePublisher`
- `ResourceUpdate`
- `PublishResult`
- `PublishStatus`
- publicação individual e em lote;
- rejeição de eventos antigos;
- rejeição de duplicatas;
- reconciliação com `force=True`;
- substituição no mesmo timestamp com `replace_equal=True`;
- invalidação individual e em lote;
- publicação não bloqueante para callbacks síncronos com `submit_publish()`;
- valores já fixados em uma sessão não são alterados por eventos posteriores.

## Correções adicionais

- Deadline expirado não cria mais coroutine sem await.
- Contabilidade de capacidade é segura contra cancelamento.
- Single-flight continua protegendo trabalho compartilhado contra cancelamento de um waiter.
- Circuitos stale usam o escopo configurado, evitando desativação global acidental quando `SUBJECT` ou `RESOURCE` é usado.

## Validação

- Ruff format.
- Ruff lint.
- mypy strict.
- 56 testes automatizados.
- build de wheel e source distribution.
- instalação e smoke test em ambiente virtual limpo.
