.PHONY: install format lint typecheck test test-count benchmark build quality

install:
	python -m pip install -e ".[dev]"

format:
	python -m ruff format .

lint:
	python -m ruff check .

typecheck:
	python -m mypy

test:
	python -m pytest

test-count:
	python scripts/check_test_count.py --minimum 500

benchmark:
	PYTHONPATH=src python benchmarks/benchmark_snapshot.py
	PYTHONPATH=src python benchmarks/benchmark_local_sources.py

build:
	python -m build

quality: format lint typecheck test-count test build
