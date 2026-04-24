"""Map Hotglue ``selected-filters.json`` stream entries to Coupa Core API query parameters.

``clause_1``, ``clause_2``, … are applied in order and merged into one query param dict.
``EQ`` → ``field=value``; ``IN`` → ``field[in]=a,b,c``. Other keys (e.g. ``operator_*``) are ignored.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Tuple

_LOG = logging.getLogger(__name__)

_CLAUSE_KEY_RE = re.compile(r"^clause_(\d+)$")

def _ordered_clauses(stream_filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    clauses: List[Tuple[int, Dict[str, Any]]] = []
    for key, val in stream_filters.items():
        m = _CLAUSE_KEY_RE.match(key)
        if not m:
            continue
        if not isinstance(val, dict):
            raise ValueError(f"Invalid {key}: expected an object, got {type(val).__name__}")
        clauses.append((int(m.group(1)), val))
    clauses.sort(key=lambda x: x[0])
    return [c[1] for c in clauses]


def _clause_to_params(clause: Dict[str, Any]) -> Dict[str, Any]:
    try:
        field = clause["field"]
        op = str(clause["operator"]).strip().upper()
        value = clause["value"]
    except KeyError as e:
        raise ValueError(f"Clause missing required key: {e}") from e

    if not isinstance(field, str) or not field.strip():
        raise ValueError("Clause 'field' must be a non-empty string.")

    if op == "IN":
        if isinstance(value, list):
            if len(value) == 0:
                return {}
            joined = ",".join(str(v) for v in value)
        else:
            joined = str(value)
        return {f"{field}[in]": joined}

    if op == "EQ":
        return {field: value}

    raise ValueError(f"Unsupported clause operator {op!r}; supported: EQ, IN.")


def parse_coupa_selected_filters(stream_filters: Dict[str, Any]) -> Dict[str, Any]:
    """Build Coupa GET query parameters from one stream's selected-filters object."""
    clauses = _ordered_clauses(stream_filters)
    if not clauses:
        return {}
    merged: Dict[str, Any] = {}
    for clause in clauses:
        merged.update(_clause_to_params(clause))
    return merged
