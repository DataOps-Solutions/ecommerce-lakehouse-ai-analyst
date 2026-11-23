PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin

.PHONY: setup pipeline demo ingest-sample ui test lint clean

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

# Sample run on fictitious store exports. The salt below is for these fictitious files only;
# for real exports set PSEUDONYMIZATION_SALT in .env or in the environment.
ingest-sample:
	PSEUDONYMIZATION_SALT=$${PSEUDONYMIZATION_SALT:-sample-data-only-not-secret} \
	$(BIN)/python -m src.main ingest-csv --input-dir data/samples_real \
		--mapping mappings/generic_store_orders.yaml \
		--mapping mappings/costs_and_stock.yaml \
		--output-dir output/sample_csv

ui:
	$(BIN)/streamlit run app/dashboard.py

test:
	$(BIN)/python -m pytest

lint:
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .
	$(BIN)/mypy

clean:
	rm -rf data/lakehouse data/lakehouse_csv output .pytest_cache .mypy_cache .ruff_cache
