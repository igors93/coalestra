# Migration from 0.3 to 0.4

## Resource identity

`ResourceKey` now trims whitespace but preserves case by default. This removes the trading-specific assumption that every subject should be uppercased.

Before 0.4:

```python
ResourceKey(" Market ", " Price ", " btcusdt ")
# market:price:BTCUSDT
```

Version 0.4 default:

```python
ResourceKey(" Market ", " Price ", " btcusdt ")
# Market:Price:btcusdt
```

Applications that depend on the previous behavior can migrate without changing key factories:

```python
from coalestra import LEGACY_KEY_NORMALIZER, ResourceKey

key = ResourceKey(
    " Market ",
    " Price ",
    " btcusdt ",
    normalizer=LEGACY_KEY_NORMALIZER,
)
```

Or use:

```python
key = ResourceKey.legacy(" Market ", " Price ", " btcusdt ")
```

For Alphora, keep symbols explicitly normalized in its key factory:

```python
def position(symbol: str) -> ResourceKey:
    return ResourceKey("account", "position", symbol.upper().strip())
```

## Qualifiers

Parameterized resources no longer need to encode parameters in the subject:

```python
ResourceKey(
    "market",
    "candles",
    "BTCUSDT",
    {"interval": "1m", "limit": 500},
)
```

Qualifiers participate in equality, ordering, hashing, cache identity, single-flight identity, and resource-scoped circuits.

## Cache compatibility

`AsyncCache` retains the original single-key contract. A cache may additionally implement `BatchAsyncCache`:

```python
async def get_many(keys, *, now, policies): ...
async def set_many(values): ...
async def invalidate_many(keys): ...
```

`SnapshotBuilder` detects this capability automatically and falls back to single-key operations for older custom caches.

`AsyncMemoryCache` now defaults to `max_entries=10_000`. Pass `max_entries=None` to preserve an unbounded cache.

Fully expired entries are removed when read. A lookup past `max_stale_seconds` therefore returns no value instead of returning an unusable value.

## Refresh policies

Existing `FreshnessPolicy(ttl, max_stale)` behavior remains blocking by default.

Opt in to background behavior:

```python
FreshnessPolicy(
    ttl_seconds=2.0,
    max_stale_seconds=15.0,
    refresh_mode=RefreshMode.STALE_WHILE_REVALIDATE,
)
```

or:

```python
FreshnessPolicy(
    ttl_seconds=60.0,
    max_stale_seconds=300.0,
    refresh_mode=RefreshMode.REFRESH_AHEAD,
    refresh_ahead_seconds=10.0,
)
```

Background refresh requires a running event loop. Short `asyncio.run(...)` programs should call `await builder.wait_for_refreshes()` before the loop exits when they need the refresh to complete.

## Snapshot diagnostics

`Snapshot` gained a `diagnostics` field with a default value, so direct construction remains source-compatible.

## Observability

Existing synchronous event and metrics sinks are unchanged. Wrap slow sinks explicitly with `BufferedEventSink` or `BufferedMetricsSink` and close them during application shutdown.
