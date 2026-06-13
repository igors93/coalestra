.PHONY: install format lint typecheck test build quality

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

build:
	python -m build

quality: format lint typecheck test build
