PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin

.PHONY: setup pipeline demo test lint clean

setup:
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements.txt
	@test -f .env || cp .env.example .env
	@echo "Setup listo. Ejecute: make pipeline && make demo"

pipeline:
	$(BIN)/python -m src.main run-pipeline

demo:
	LLM_PROVIDER=mock $(BIN)/python -m src.main demo

test:
	$(BIN)/python -m pytest

lint:
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .
	$(BIN)/mypy

clean:
	rm -rf data/lakehouse output .pytest_cache .mypy_cache .ruff_cache
