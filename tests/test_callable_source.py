from __future__ import annotations

import asyncio
import threading

from coalestra import CallableSource, FetchContext, ResourceKey


def test_synchronous_fetcher_runs_outside_event_loop_thread() -> None:
    caller_thread = threading.get_ident()
    worker_thread = caller_thread

    def fetch(_key, _context):
        nonlocal worker_thread
        worker_thread = threading.get_ident()
        return 7

    async def scenario():
        source = CallableSource(
            name="sync",
            priority=1,
            supports=lambda _key: True,
            fetcher=fetch,
        )
        return await source.fetch(
            ResourceKey("test", "value"),
            FetchContext(requested_at=0.0),
        )

    payload = asyncio.run(scenario())

    assert payload.value == 7
    assert worker_thread != caller_thread
