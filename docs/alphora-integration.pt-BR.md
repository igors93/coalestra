# Integração do Alphora com o Coalestra 0.6

Este documento descreve o contrato recomendado para a próxima atualização do Alphora.

## Dependência

```toml
coalestra = ">=0.6.0,<0.7.0"
```

## Verificação na inicialização

```python
from coalestra import require_capabilities

require_capabilities(
    features=(
        "snapshot_acceptance",
        "snapshot_consistency",
        "transactional_revalidation",
        "automatic_observability_buffering",
        "blocking_source_timeout_guarantees",
        "builder_health_serialization",
    ),
    schemas={
        "builder_health": 1,
        "error_diagnostics": 1,
    },
)
```

## Declarações das fontes do Alphora

Fontes locais, derivadas e leituras de cache devem declarar:

```python
blocking_io=False
run_sync_in_thread=False
```

Fontes REST devem declarar o timeout realmente aplicado pelo cliente:

```python
CallableSource(
    ...,
    timeout_seconds=settings.rest_timeout_seconds,
    blocking_io=True,
    transport_timeout_seconds=max(
        settings.rest_connect_timeout_seconds,
        settings.rest_read_timeout_seconds,
    ),
)
```

O valor declarado deve representar o limite máximo real da chamada e precisa ser menor
que `timeout_seconds`.

## Snapshots usados para execução

Use `SnapshotAcceptancePolicy` na revalidação de posição, ordens e conta. A política deve
proibir stale, limitar idade e exigir autoridade mínima adequada para a operação.

## Saúde operacional

Substitua listas manuais de campos por:

```python
health = provider.health_snapshot()
payload = health.to_dict(previous=previous_health)
assessment = health.assess(previous=previous_health)
```

Gere alerta imediato quando ocorrer:

- `unsafe_blocking_source`;
- aumento de `source_transport_timeout_violation_count`;
- shutdown incompleto;
- saturação persistente de capacidade;
- severidade `critical`.

## Encerramento

O Alphora deve continuar fechando primeiro suas submissões e depois o
`SyncSnapshotBuilder`. O timeout de shutdown precisa ser maior que os budgets de cópia e
de observabilidade configurados no builder.
