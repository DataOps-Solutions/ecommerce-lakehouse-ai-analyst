"""BI analyst tests: mock planner, Bedrock provider with a stubbed client, grounding checks."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.analyst.bi_agent import (
    BedrockProvider,
    BiAnalyst,
    MockProvider,
    PlanResult,
    is_grounded,
)
from src.analyst.semantic import load_semantic_layer
from src.config import Settings
from src.medallion.gold import build_gold
from src.medallion.silver import run_silver
from tests.helpers import RUN_TS, fresh_bronze, paid_batch2

ROOT = Path(__file__).resolve().parent.parent
LAYER = load_semantic_layer(ROOT / "semantic_layer" / "metrics.yaml")
SETTINGS = Settings(lakehouse_storage="local", lakehouse_local_dir=ROOT / "data" / "unused")


@pytest.fixture(scope="module")
def gold() -> dict[str, Any]:
    silver = run_silver(fresh_bronze(batch2=paid_batch2()), RUN_TS).tables
    return build_gold(silver)


class _FakeBedrock:
    """Minimal stand-in for the boto3 bedrock-runtime client (Converse API)."""

    def __init__(self, replies: list[str]) -> None:
        self._replies = list(replies)
        self.calls: list[dict[str, Any]] = []

    def converse(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        text = self._replies.pop(0)
        return {"output": {"message": {"role": "assistant", "content": [{"text": text}]}}}


def test_mock_plans_demo_questions_over_gold_only() -> None:
    planner = MockProvider(row_limit=1000)
    margin = planner.plan("¿Cuál fue el margen neto por canal?", LAYER)
    assert margin is not None
    assert margin.metrics == ("margen_neto",)
    assert "FROM mart_daily_unit_economics" in margin.sql
    assert "GROUP BY channel" in margin.sql

    returns = planner.plan("¿Qué SKUs tienen la mayor tasa de devolución? top 10", LAYER)
    assert returns is not None
    assert returns.sql.endswith("LIMIT 10")

    daily = planner.plan("¿Cuál fue la venta bruta y los descuentos por día?", LAYER)
    assert daily is not None
    assert "ORDER BY order_date ASC" in daily.sql
    assert "LIMIT 1000" in daily.sql

    assert planner.plan("¿Cuál es el clima de mañana?", LAYER) is None


def test_mock_analyst_returns_ok_response_with_sql_rows_and_reading(gold: dict[str, Any]) -> None:
    analyst = BiAnalyst(SETTINGS, MockProvider(), LAYER, gold)
    response = analyst.answer("¿Cuál fue el margen neto por canal?")
    assert response.status == "ok"
    assert response.sql is not None and response.sql.startswith("SELECT")
    assert response.tables == ["mart_daily_unit_economics"]
    assert response.row_count == len(response.rows) == 1
    assert response.interpretation_source == "mock"
    assert response.interpretation is not None
    assert "$ 6.894" in response.interpretation
    assert "margen_neto (channel = web)" in response.interpretation


def test_bedrock_plan_is_guarded_and_executed(gold: dict[str, Any]) -> None:
    client = _FakeBedrock(
        [
            "```sql\nSELECT channel, SUM(net_margin_amount) AS m "
            "FROM mart_daily_unit_economics GROUP BY channel\n```",
            "El margen neto del canal web es 6.893,80.",
        ]
    )
    provider = BedrockProvider(SETTINGS, client=client)
    analyst = BiAnalyst(SETTINGS, provider, LAYER, gold)
    response = analyst.answer("margen por canal")
    assert response.status == "ok"
    assert response.provider == "bedrock"
    assert response.interpretation_source == "llm"
    assert len(client.calls) == 2


def test_bedrock_sql_that_breaks_policy_is_blocked(gold: dict[str, Any]) -> None:
    client = _FakeBedrock(["DROP TABLE mart_daily_unit_economics"])
    analyst = BiAnalyst(SETTINGS, BedrockProvider(SETTINGS, client=client), LAYER, gold)
    response = analyst.answer("borra todo")
    assert response.status == "blocked"
    assert response.error is not None and "not_read_only" in response.error


def test_bedrock_reading_with_invented_numbers_falls_back_to_mock(gold: dict[str, Any]) -> None:
    client = _FakeBedrock(
        [
            "```sql\nSELECT channel, SUM(net_margin_amount) AS m "
            "FROM mart_daily_unit_economics GROUP BY channel\n```",
            "El margen creció un 999 % frente al trimestre anterior.",
        ]
    )
    analyst = BiAnalyst(SETTINGS, BedrockProvider(SETTINGS, client=client), LAYER, gold)
    response = analyst.answer("margen por canal")
    assert response.status == "ok"
    assert response.interpretation_source == "mock_fallback"


def test_bedrock_unanswerable_marker_is_respected(gold: dict[str, Any]) -> None:
    client = _FakeBedrock(["UNANSWERABLE"])
    analyst = BiAnalyst(SETTINGS, BedrockProvider(SETTINGS, client=client), LAYER, gold)
    assert analyst.answer("¿qué tiempo hará?").status == "unanswerable"


def test_grounding_accepts_only_numbers_present_in_table() -> None:
    columns = ["channel", "net_margin_amount", "return_rate"]
    rows: list[dict[str, Any]] = [
        {"channel": "web", "net_margin_amount": 6893.8, "return_rate": 0.5},
    ]
    assert is_grounded("Margen de 6.893,80 en web.", columns, rows)
    assert is_grounded("La tasa fue 50,0 %.", columns, rows)
    assert not is_grounded("Crecimiento de 12,5 % vs. mes previo.", columns, rows)


def test_plan_result_is_frozen_dataclass() -> None:
    plan = PlanResult(sql="SELECT 1", metrics=())
    with pytest.raises(AttributeError):
        plan.sql = "SELECT 2"  # type: ignore[misc]
