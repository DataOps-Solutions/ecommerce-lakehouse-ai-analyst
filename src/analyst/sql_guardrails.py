"""Read-only SQL guardrails for the BI analyst.

Layers of defence, applied in order to every candidate statement:

1. Comments and multiple statements are rejected (checked outside string literals).
2. The statement must start with ``SELECT`` or ``WITH``.
3. Write/DDL/admin keywords are rejected by token search (outside string literals).
4. The statement is parsed with sqlglot (DuckDB dialect). It must be a single query, with no
   DDL/DML/command nodes, no table functions (``read_parquet``...), no qualified names.
5. Every table must be an authorized Gold table from ``semantic_layer/metrics.yaml``.
   Bronze, Silver and quarantine names are rejected explicitly.
6. Every column must belong to an authorized table, a CTE output or a select alias.
7. A row limit is enforced.

The DuckDB connection is also opened with ``enable_external_access=False`` as a last line of
defence.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from src.analyst.semantic import SemanticLayer
from src.models import GuardrailViolationError

FORBIDDEN_KEYWORDS: tuple[str, ...] = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "COPY",
    "ATTACH",
    "CREATE",
    "TRUNCATE",
    "DETACH",
    "PRAGMA",
    "CALL",
    "EXPORT",
    "INSTALL",
    "LOAD",
    "SET",
    "GRANT",
    "REVOKE",
    "MERGE",
    "VACUUM",
    "CHECKPOINT",
)
BLOCKED_FUNCTION_PREFIXES: tuple[str, ...] = (
    "read_",
    "duckdb_",
    "pragma_",
    "sqlite_",
    "glob",
    "query",
    "parquet_",
    "csv_",
)
RESTRICTED_PREFIXES: tuple[str, ...] = ("bronze", "silver", "quarantine")
_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")


@dataclass(frozen=True)
class GuardrailResult:
    """A validated statement, rewritten to its canonical DuckDB form."""

    sql: str
    tables: tuple[str, ...]
    checks: tuple[str, ...]


def _strip_literals(sql: str) -> str:
    return _STRING_LITERAL.sub("''", sql)


def _violation(code: str, message: str) -> GuardrailViolationError:
    return GuardrailViolationError(code, message)


def validate_sql(sql: str, layer: SemanticLayer, row_limit: int) -> GuardrailResult:
    """Validate ``sql`` against the read-only policy and the semantic layer whitelist.

    Raises :class:`GuardrailViolationError` with a machine-readable ``code`` on any violation.
    """
    text = sql.strip()
    if not text:
        raise _violation("empty", "la consulta está vacía")

    sanitized = _strip_literals(text).strip()
    if "--" in sanitized or "/*" in sanitized:
        raise _violation("comment", "los comentarios SQL no están permitidos")
    if sanitized.endswith(";"):
        text = text[:-1].rstrip()
        sanitized = sanitized[:-1].rstrip()
    if ";" in sanitized:
        raise _violation("multiple_statements", "solo se permite una sentencia")

    first = re.match(r"\s*([A-Za-z_]+)", sanitized)
    if first is None or first.group(1).upper() not in ("SELECT", "WITH"):
        raise _violation("not_read_only", "solo se permiten sentencias SELECT o WITH")

    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", sanitized, flags=re.IGNORECASE):
            raise _violation("forbidden_keyword", f"palabra clave prohibida: {keyword}")

    try:
        statements = sqlglot.parse(text, read="duckdb")
    except SqlglotError as exc:
        raise _violation("parse_error", f"SQL inválido: {exc}") from exc
    if len(statements) != 1 or statements[0] is None:
        raise _violation("multiple_statements", "solo se permite una sentencia")
    root = statements[0]
    if not isinstance(root, (exp.Select, exp.Union)):
        raise _violation("not_select", "el nodo raíz debe ser una consulta SELECT")

    if root.find(exp.DDL, exp.DML, exp.Command, exp.Set, exp.Pragma, exp.Use, exp.Attach, exp.Copy):
        raise _violation("forbidden_node", "se detectó un nodo DDL/DML/comando")

    for func in root.find_all(exp.Func):
        fn_name = func.name.lower() if isinstance(func, exp.Anonymous) else func.sql_name().lower()
        if fn_name.startswith(BLOCKED_FUNCTION_PREFIXES):
            raise _violation("blocked_function", f"función no permitida: {fn_name}")

    cte_nodes = list(root.find_all(exp.CTE))
    cte_names = {cte.alias_or_name.lower() for cte in cte_nodes}
    cte_columns: set[str] = set()
    for cte in cte_nodes:
        if isinstance(cte.this, exp.Select):
            cte_columns.update(name.lower() for name in cte.this.named_selects)

    used_tables: set[str] = set()
    qualifiers: set[str] = set(cte_names)
    for table in root.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            raise _violation("table_function", "funciones de tabla no permitidas en FROM/JOIN")
        if table.args.get("db") or table.args.get("catalog"):
            raise _violation("qualified_name", "nombres de tabla calificados no permitidos")
        name = table.name.lower()
        qualifiers.add(name)
        if table.alias:
            qualifiers.add(table.alias.lower())
        if name in cte_names:
            continue
        if name not in layer.table_names:
            if name.startswith(RESTRICTED_PREFIXES) or name in layer.restricted_layers:
                raise _violation(
                    "restricted_layer", f"acceso a capa no autorizada: {name} (solo Gold)"
                )
            raise _violation("unauthorized_table", f"tabla no autorizada: {name}")
        used_tables.add(name)

    aliases = {a.alias.lower() for a in root.find_all(exp.Alias) if a.alias}
    for subquery in root.find_all(exp.Subquery):
        if subquery.alias:
            qualifiers.add(subquery.alias.lower())
    allowed_columns = layer.all_column_names | aliases | cte_columns
    for column in root.find_all(exp.Column):
        if isinstance(column.this, exp.Star):
            continue
        col_name = column.name.lower()
        if col_name not in allowed_columns:
            raise _violation("unauthorized_column", f"columna no autorizada: {col_name}")
        if column.table and column.table.lower() not in qualifiers:
            raise _violation("unknown_qualifier", f"calificador desconocido: {column.table}")

    if isinstance(root, exp.Select):
        limit = root.args.get("limit")
        if limit is None:
            root = root.limit(row_limit)
        else:
            value = limit.expression
            if not isinstance(value, exp.Literal) or not value.is_int:
                raise _violation("limit", "LIMIT debe ser un entero literal")
            if int(value.this) > row_limit:
                root = root.limit(row_limit)
    else:
        root = exp.select("*").from_(root.subquery("_guarded")).limit(row_limit)

    checks = (
        "single_statement",
        "read_only_keywords",
        "ast_statement_type",
        "authorized_tables",
        "authorized_columns",
        f"row_limit<={row_limit}",
    )
    return GuardrailResult(
        sql=root.sql(dialect="duckdb"),
        tables=tuple(sorted(used_tables)),
        checks=checks,
    )
