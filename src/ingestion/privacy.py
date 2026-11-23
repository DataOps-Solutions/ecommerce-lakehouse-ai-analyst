"""Personal-data hygiene applied before anything is persisted in the lakehouse.

Principles (Ley 25.326 minimization and purpose limitation):

* Fields that analytics does not need are **dropped** and never extracted from the file.
* Identifiers needed to count repeat customers are **pseudonymized** with a keyed,
  deterministic hash (SHA-256 over ``salt`` and the normalized value). The same customer
  maps to the same token within one salt; changing the salt unlinks the tokens.
* The number of cells dropped or pseudonymized per field is reported, so the client can see
  what was filtered without ever seeing the values.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from typing import Literal

from pydantic import BaseModel

from src.ingestion.mapping import PERSONAL_FIELDS, SENSITIVE_HINTS, MappingError, normalize_header

Action = Literal["drop", "pseudonymize"]

# Defaults applied to canonical fields unless a mapping overrides them.
DEFAULT_SENSITIVE: dict[str, Action] = {
    **{field: "drop" for field in PERSONAL_FIELDS},
    "customer_id": "pseudonymize",
}


def looks_personal(name: str) -> bool:
    """Heuristic on a field name (accent and separator insensitive) for personal data."""
    normalized = normalize_header(name)
    return any(hint in normalized for hint in SENSITIVE_HINTS)


class FieldPrivacyStats(BaseModel):
    """What happened to one personal-data field during ingestion."""

    source: str
    field: str
    action: Action
    cells_affected: int


def pseudonymize(value: str, salt: str) -> str:
    """Return a deterministic token ``CUS-<12 hex>`` for ``value`` under ``salt``."""
    digest = hashlib.sha256(f"{salt}:{value.strip().lower()}".encode()).hexdigest()
    return f"CUS-{digest[:12].upper()}"


class PrivacyPolicy:
    """Decides, per source field, whether a value is dropped, pseudonymized or kept."""

    def __init__(self, actions: dict[tuple[str, str], Action], salt: str | None) -> None:
        self._actions = actions
        self._salt = salt
        self._counts: Counter[tuple[str, str]] = Counter()
        if "pseudonymize" in actions.values() and not salt:
            raise MappingError(
                "pseudonymization requires PSEUDONYMIZATION_SALT (a secret, non-empty value)"
            )

    @classmethod
    def from_mappings(
        cls,
        sensitive_by_source: dict[str, dict[str, Action]],
        mapped_fields: dict[str, set[str]],
        salt: str | None,
        custom_by_source: dict[str, set[str]] | None = None,
    ) -> PrivacyPolicy:
        """Build the policy from canonical defaults, custom-field heuristics and overrides.

        Custom attributes whose names look personal (email, phone, dni, name...) are dropped
        by default. Renaming the attribute in the mapping is the way to keep a non-personal
        field whose name happens to contain one of those words.
        """
        customs = custom_by_source or {}
        actions: dict[tuple[str, str], Action] = {}
        for source, fields in mapped_fields.items():
            for field in fields:
                if field in DEFAULT_SENSITIVE:
                    actions[(source, field)] = DEFAULT_SENSITIVE[field]
        for source, names in customs.items():
            for name in names:
                if looks_personal(name):
                    actions[(source, name)] = "drop"
        for source, overrides in sensitive_by_source.items():
            allowed = mapped_fields.get(source, set()) | customs.get(source, set())
            for field, action in overrides.items():
                if field in allowed:
                    actions[(source, field)] = action
        return cls(actions, salt)

    def is_sensitive(self, source: str, field: str) -> bool:
        return (source, field) in self._actions

    def action_for(self, source: str, field: str) -> Action | None:
        return self._actions.get((source, field))

    def apply(self, source: str, field: str, value: str) -> str | None:
        """Return the value to persist, or None when the field must be dropped.

        Blank cells are not counted as affected.
        """
        action = self._actions.get((source, field))
        if action is None:
            return value
        if not value.strip():
            return None
        self._counts[(source, field)] += 1
        if action == "drop":
            return None
        assert self._salt is not None  # guaranteed by __init__ when action is pseudonymize
        return pseudonymize(value, self._salt)

    def stats(self) -> list[FieldPrivacyStats]:
        """Counts per sensitive field, sorted for stable reports."""
        rows = [
            FieldPrivacyStats(
                source=source,
                field=field,
                action=self._actions[(source, field)],
                cells_affected=self._counts.get((source, field), 0),
            )
            for (source, field) in sorted(self._actions)
        ]
        return rows
