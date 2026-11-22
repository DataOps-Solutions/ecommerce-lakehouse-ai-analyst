"""Guardrail tests: write/DDL blocking, Bronze/Silver blocking and valid Gold execution."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.analyst.bi_agent import execute_readonly
from src.analyst.semantic import SemanticLayer, load_semantic_layer, parse_semantic_layer
from src.analyst.sql_guardrails import validate_sql
from src.medallion.gold import build_gold
from src.medallion.silver import run_silver
from src.models import GuardrailViolationError
from tests.helpers import RUN_TS, fresh_bronze, paid_batch2

ROOT = Path(__file__).resolve().parent.parent
LAYER = load_semantic_layer(ROOT / "semantic_layer" / "metrics.yaml")
LIMIT = 1000


@pytest.mark.parametrize(
    "statement",
    [
        "DROP TABLE mart_daily_unit_economics",
        "DELETE FROM mart_sku_health WHERE sku = 'REM01-NEG-M'",
        "INSERT INTO mart_sku_health SELECT * FROM mart_sku_health",
        "UPDATE mart_sku_health SET stock_on_hand = 0",
        "ALTER TABLE mart_sku_health ADD COLUMN x INTEGER",
        "CREATE TABLE evil AS SELECT * FROM mart_sku_health",
        "TRUNCATE mart_sku_health",
        "COPY mart_sku_health TO '/tmp/leak.csv'",
        "ATTACH '/tmp/other.duckdb' AS other",
        "WITH x AS (SELECT sku FROM mart_sku_health) DELETE FROM x",
        "SELECT 1; DROP TABLE mart_sku_health",
        "PRAGMA database_list",
        "SET enable_external_access = true",
    ],
)
def test_write_and_ddl_statements_are_blocked(statement: str) -> None:
    with pytest.raises(GuardrailViolationError):
        validate_sql(statement, LAYER, LIMIT)


@pytest.mark.parametrize(
    "statement, code",
    [
        ("SELECT * FROM silver_orders LIMIT 5", "restricted_layer"),
        ("SELECT * FROM bronze_orders LIMIT 5", "restricted_layer"),
        ("SELECT * FROM quarantine_orders LIMIT 5", "restricted_layer"),
        ("SELECT sku FROM mart_sku_health JOIN silver_products USING (sku)", "restricted_layer"),
        ("SELECT * FROM some_random_table", "unauthorized_table"),
        ("SELECT * FROM read_parquet('/etc/passwd')", "blocked_function"),
        ("SELECT * FROM mart_sku_health -- hidden", "comment"),
        ("SELECT order_id FROM mart_daily_unit_economics", "unauthorized_column"),
    ],
)
def test_non_gold_access_is_blocked(statement: str, code: str) -> None:
    with pytest.raises(GuardrailViolationError) as info:
        validate_sql(statement, LAYER, LIMIT)
    assert info.value.code == code


def test_valid_gold_select_is_allowed_and_limited() -> None:
    result = validate_sql(
        "SELECT channel, net_margin_amount FROM mart_daily_unit_economics", LAYER, LIMIT
    )
    assert result.tables == ("mart_daily_unit_economics",)
    assert result.sql.endswith(f"LIMIT {LIMIT}")
    assert "read_only_keywords" in result.checks


def test_cte_and_aliases_over_gold_are_allowed() -> None:
    sql = (
        "WITH t AS (SELECT sku, units_sold, units_returned FROM mart_sku_health) "
        "SELECT sku, units_returned * 1.0 / NULLIF(units_sold, 0) AS ratio "
        "FROM t WHERE units_sold > 0 ORDER BY ratio DESC LIMIT 5"
    )
    result = validate_sql(sql, LAYER, LIMIT)
    assert result.tables == ("mart_sku_health",)


def test_excessive_limit_is_clamped() -> None:
    result = validate_sql("SELECT sku FROM mart_sku_health LIMIT 999999", LAYER, LIMIT)
    assert result.sql.endswith(f"LIMIT {LIMIT}")


def test_union_is_wrapped_with_row_limit() -> None:
    sql = "SELECT sku FROM mart_sku_health UNION ALL SELECT sku FROM mart_sku_health"
    result = validate_sql(sql, LAYER, LIMIT)
    assert result.sql.endswith(f"LIMIT {LIMIT}")


def test_validated_gold_query_executes_over_gold_tables() -> None:
    silver = run_silver(fresh_bronze(batch2=paid_batch2()), RUN_TS).tables
    gold = build_gold(silver)
    guarded = validate_sql(
        "SELECT channel, SUM(net_margin_amount) AS margen_neto "
        "FROM mart_daily_unit_economics GROUP BY channel ORDER BY margen_neto DESC",
        LAYER,
        LIMIT,
    )
    table = execute_readonly(guarded.sql, gold)
    assert table.column_names == ["channel", "margen_neto"]
    assert table.num_rows == 1


def test_semantic_layer_rejects_metric_on_unknown_column() -> None:
    raw = {
        "version": 1,
        "domain": "test",
        "currency": "ARS",
        "description": "x",
        "restricted_layers": ["bronze"],
        "authorized_tables": [
            {
                "name": "mart_x",
                "layer": "gold",
                "description": "x",
                "grain": "x",
                "columns": [
                    {"name": "a", "type": "INTEGER", "role": "measure", "description": "x"}
                ],
            }
        ],
        "metrics": [
            {
                "name": "bad",
                "display_name": "bad",
                "table": "mart_x",
                "sql": "SUM(not_a_column)",
                "unit": "count",
                "definition": "x",
            }
        ],
    }
    with pytest.raises(ValueError, match="unknown column"):
        parse_semantic_layer(raw)


def test_semantic_layer_loads_and_lists_only_gold() -> None:
    assert isinstance(LAYER, SemanticLayer)
    assert LAYER.table_names == {
        "mart_daily_unit_economics",
        "mart_checkout_funnel",
        "mart_sku_health",
    }
    assert set(LAYER.restricted_layers) == {"bronze", "silver", "quarantine"}
