from __future__ import annotations

import logging
from typing import Any


class NullEventSink:
    coalestra_non_blocking = True

    def emit(self, event_type: str, **payload: Any) -> None:
        return None


class LoggingEventSink:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger("coalestra")

    def emit(self, event_type: str, **payload: Any) -> None:
        self.logger.info("coalestra event=%s payload=%s", event_type, payload)
