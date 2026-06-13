# Contributing

1. Create a virtual environment.
2. Install the project with `python -m pip install -e ".[dev]"`.
3. Run `make quality` before opening a pull request.
4. Keep the core independent from HTTP clients, exchanges, databases, and application models.
5. Add tests for concurrency, fallback, freshness, and failure behavior.

Public APIs must remain protocol-oriented. Application-specific integrations belong in adapters or in the consuming application.
