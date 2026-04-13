"""Map Hotglue ``selected-filters.json`` stream entries to Coupa Core API query parameters.

Only the **lowest-numbered** ``clause_*`` key is used (e.g. ``clause_1`` before ``clause_2``);
additional clauses are ignored. Keys other than ``clause_*`` are ignored.

Supported operators per clause: ``EQ`` (equality) and ``IN`` (comma-separated list in the
query value, via ``field[in]=a,b,c``). A non-list ``IN`` value is converted with ``str(value)``.

If there are no ``clause_*`` entries, or the chosen clause has an empty ``IN`` list, logs a
warning and returns an empty dict. Malformed clauses or unsupported operators raise
``ValueError``.
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
    """Build Coupa GET query parameters from one stream's selected-filters object.

    Uses the first clause after ordering by ``clause_N`` suffix. Returns ``{}`` (and logs)
    when there is no clause or the clause yields no params (e.g. empty ``IN`` list).

    Raises:
        ValueError: Invalid clause shape, empty field, or operator other than ``EQ`` / ``IN``.
    """
    clauses = _ordered_clauses(stream_filters)
    if not clauses:
        _LOG.warning(
            "No clause_* entries found in selected filters."
        )
        return {}
    
    params = _clause_to_params(clauses[0])
    if not params:
        _LOG.warning(
            "Selected filter clause produced no query parameters (empty IN list?)."
        )
        return {}
    return params
