from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

StringTransform = Callable[[str], str]
QualifierInput = Mapping[str, Any] | Iterable[tuple[str, Any]]


def _strip(value: str) -> str:
    return value.strip()


def _lower(value: str) -> str:
    return value.strip().lower()


def _upper(value: str) -> str:
    return value.strip().upper()


def _casefold(value: str) -> str:
    return value.strip().casefold()


@dataclass(frozen=True)
class KeyNormalizer:
    """Configurable identity normalization for :class:`ResourceKey`.

    The default normalizer only strips surrounding whitespace and preserves case. Applications
    that use case-insensitive identifiers can opt in to another normalizer without imposing those
    semantics on every Coalestra consumer.
    """

    namespace: StringTransform = _strip
    name: StringTransform = _strip
    subject: StringTransform = _strip
    qualifier_name: StringTransform = _strip
    qualifier_value: StringTransform = _strip

    def normalize_namespace(self, value: str) -> str:
        return self.namespace(str(value))

    def normalize_name(self, value: str) -> str:
        return self.name(str(value))

    def normalize_subject(self, value: str) -> str:
        return self.subject(str(value))

    def normalize_qualifier(self, name: str, value: Any) -> tuple[str, str]:
        return (
            self.qualifier_name(str(name)),
            self.qualifier_value(str(value)),
        )


PRESERVE_KEY_NORMALIZER = KeyNormalizer()
"""Default normalizer: trim whitespace while preserving case-sensitive identity."""

LEGACY_KEY_NORMALIZER = KeyNormalizer(
    namespace=_lower,
    name=_lower,
    subject=_upper,
    qualifier_name=_lower,
    qualifier_value=_strip,
)
"""Compatibility normalizer matching Coalestra 0.1-0.3 key behavior."""

CASE_INSENSITIVE_KEY_NORMALIZER = KeyNormalizer(
    namespace=_casefold,
    name=_casefold,
    subject=_casefold,
    qualifier_name=_casefold,
    qualifier_value=_casefold,
)
"""Normalizer for systems whose complete resource identity is case-insensitive."""


@dataclass(frozen=True, order=True, init=False)
class ResourceKey:
    """Stable, hashable and optionally parameterized resource identity.

    ``qualifiers`` make parameterized resources first-class without forcing consumers to encode
    query parameters into ``subject``. Qualifiers are normalized, sorted and stored as an immutable
    tuple, so mapping insertion order never affects equality or hashing.
    """

    namespace: str
    name: str
    subject: str
    qualifiers: tuple[tuple[str, str], ...]

    def __init__(
        self,
        namespace: str,
        name: str,
        subject: str = "",
        qualifiers: QualifierInput | None = None,
        *,
        normalizer: KeyNormalizer | None = None,
    ) -> None:
        selected = normalizer or PRESERVE_KEY_NORMALIZER
        normalized_namespace = selected.normalize_namespace(namespace)
        normalized_name = selected.normalize_name(name)
        normalized_subject = selected.normalize_subject(subject)
        if not normalized_namespace:
            raise ValueError("namespace cannot be empty")
        if not normalized_name:
            raise ValueError("name cannot be empty")

        raw_qualifiers = qualifiers.items() if isinstance(qualifiers, Mapping) else qualifiers or ()
        normalized_qualifiers: list[tuple[str, str]] = []
        seen: set[str] = set()
        for raw_name, raw_value in raw_qualifiers:
            qualifier_name, qualifier_value = selected.normalize_qualifier(raw_name, raw_value)
            if not qualifier_name:
                raise ValueError("qualifier name cannot be empty")
            if qualifier_name in seen:
                raise ValueError(f"duplicate qualifier name: {qualifier_name}")
            seen.add(qualifier_name)
            normalized_qualifiers.append((qualifier_name, qualifier_value))

        object.__setattr__(self, "namespace", normalized_namespace)
        object.__setattr__(self, "name", normalized_name)
        object.__setattr__(self, "subject", normalized_subject)
        object.__setattr__(self, "qualifiers", tuple(sorted(normalized_qualifiers)))

    @classmethod
    def legacy(
        cls,
        namespace: str,
        name: str,
        subject: str = "",
        qualifiers: QualifierInput | None = None,
    ) -> ResourceKey:
        """Build a key using the normalization behavior from Coalestra 0.1-0.3."""

        return cls(
            namespace,
            name,
            subject,
            qualifiers,
            normalizer=LEGACY_KEY_NORMALIZER,
        )

    def normalized(self, normalizer: KeyNormalizer) -> ResourceKey:
        """Return the same logical fields transformed by ``normalizer``."""

        return ResourceKey(
            self.namespace,
            self.name,
            self.subject,
            self.qualifiers,
            normalizer=normalizer,
        )

    def qualifier(self, name: str, default: str | None = None) -> str | None:
        """Return one qualifier value using exact qualifier-name identity."""

        for qualifier_name, value in self.qualifiers:
            if qualifier_name == name:
                return value
        return default

    def with_qualifiers(
        self,
        qualifiers: QualifierInput | None = None,
        **additional: Any,
    ) -> ResourceKey:
        """Return a new key with merged qualifiers.

        Explicit ``additional`` values replace qualifiers with the same exact name.
        """

        merged = dict(self.qualifiers)
        if qualifiers is not None:
            items = qualifiers.items() if isinstance(qualifiers, Mapping) else qualifiers
            merged.update((str(name), str(value)) for name, value in items)
        merged.update((str(name), str(value)) for name, value in additional.items())
        return ResourceKey(self.namespace, self.name, self.subject, merged)

    def without_qualifiers(self, *names: str) -> ResourceKey:
        """Return a new key without the selected qualifier names."""

        removed = set(names)
        return ResourceKey(
            self.namespace,
            self.name,
            self.subject,
            ((name, value) for name, value in self.qualifiers if name not in removed),
        )

    def __str__(self) -> str:
        base = f"{self.namespace}:{self.name}"
        if self.subject:
            base = f"{base}:{self.subject}"
        if self.qualifiers:
            return f"{base}?{urlencode(self.qualifiers)}"
        return base
