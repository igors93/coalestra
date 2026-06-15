from __future__ import annotations

from typing import TypedDict

from coalestra.core.keys import ResourceKey


class ResourceMetricLabels(TypedDict):
    """Bounded metric labels describing one resource type."""

    resource_namespace: str
    resource_name: str


def resource_metric_labels(key: ResourceKey) -> ResourceMetricLabels:
    """Return low-cardinality labels describing one resource type.

    Resource subjects and qualifier values identify individual instances such
    as symbols, accounts, or tenants. They are intentionally excluded from
    metric labels so the default metric series remain bounded by the set of
    resource types configured by the application.
    """

    return {
        "resource_namespace": key.namespace,
        "resource_name": key.name,
    }
