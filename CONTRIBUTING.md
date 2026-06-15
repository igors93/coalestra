# Contributing

1. Create a virtual environment.
2. Install the project with `python -m pip install -e ".[dev]"`.
3. Run `make quality` before opening a pull request.
4. Keep the core independent from HTTP clients, exchanges, databases, and application models.
5. Add deterministic tests for concurrency, fallback, freshness, and failure behavior.

Public APIs must remain protocol-oriented. Application-specific integrations belong in adapters or in the consuming application.

## Release checklist

A release is accepted only when all of the following succeed:

```bash
make release-check
```

The release gate verifies formatting, lint, strict mypy, the test-count floor, deterministic concurrency regressions, the complete test suite, version consistency, package build, clean wheel installation, and the exact public API manifest.

Before creating a tag:

1. Update `pyproject.toml`, `src/coalestra/__init__.py`, and `CHANGELOG.md` to the same version.
2. Update `scripts/public_api.txt` when a public export is intentionally added or removed.
3. Run `make release-check` from a clean checkout.
4. Create a tag in the form `vX.Y.Z`. The release workflow rejects a tag that does not match the project version.
5. Use only the wheel and source distribution produced by the verified release workflow.

The public API manifest is intentionally strict. Any addition or removal must be reviewed as an explicit compatibility decision.
