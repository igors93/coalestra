.PHONY: install format format-check lint typecheck test test-count concurrency-test version-check public-api-check release-contract-check benchmark clean-dist build distribution-check quality release-check

install:
	python -m pip install -e ".[dev]"

format:
	python -m ruff format .

format-check:
	python -m ruff format --check .

lint:
	python -m ruff check .

typecheck:
	python -m mypy

test:
	python -m pytest

test-count:
	python scripts/check_test_count.py --minimum 700

concurrency-test:
	python -m pytest -m release_concurrency --tb=short

version-check:
	python scripts/check_version_consistency.py

public-api-check:
	python scripts/check_public_api.py

release-contract-check:
	python scripts/check_release_contract.py

benchmark:
	PYTHONPATH=src python benchmarks/benchmark_snapshot.py
	PYTHONPATH=src python benchmarks/benchmark_local_sources.py

clean-dist:
	python -c "import shutil; shutil.rmtree('dist', ignore_errors=True)"

build: clean-dist version-check
	python -m build

distribution-check: build
	python scripts/verify_distribution.py

quality: format-check lint typecheck version-check public-api-check release-contract-check test-count concurrency-test test distribution-check

release-check: quality
