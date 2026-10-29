"""Per-source cursors for saved extract runs (INF-1006).

Scheduled reads and ``POST /extract-sources/{slug}/run`` share one cursor
map on the saved source. Three catalog templates window the next read.
Every other template stays a full pull. ``POST /ingest/dlt`` does not read
or write cursors.

SQL (``postgres``, ``mysql``) uses dlt incremental on a user-chosen
``cursor_column``. Stripe sends ``created[gte]``. Details and the exact
list: ``docs/extract-incremental.md``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Literal, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict

from infona_client.ingestion.models import DltExtractSource, DltSourceSpec

SQL_INCREMENTAL_CONNECTORS = frozenset({"postgres", "mysql"})
STRIPE_CONNECTOR_ID = "stripe"
STRIPE_CURSOR_COLUMN = "created"
STRIPE_FILTER_PARAM = "created[gte]"
STRIPE_HOST = "api.stripe.com"
#: Stripe list endpoints cap ``limit`` at 100. One page, matching single_page.
STRIPE_PAGE_LIMIT = 100

IncrementalMode = Literal["sql", "stripe"]

_SQL_NOTE = (
    "Incremental only when the saved source sets cursor_column. "
    "Later runs use dlt incremental with an exclusive lower bound "
    "(range_start open) ordered ascending on that column: rows greater than "
    "the stored per-table cursor. No cursor_column means a full pull."
)
_STRIPE_NOTE = (
    "Each resource is requested with created[gte] set to the stored cursor "
    "(greatest created value already ingested, unix seconds) and limit at most "
    "100. Rows with created less than or equal to the cursor are not ingested. "
    "The paginator stays single_page. The cursor is saved only when the page "
    "is shorter than the limit sent; a full page is requested again next run. "
    "The first run sends no created[gte]."
)


class ConnectorIncremental(BaseModel):
    """How stripe, postgres, or mysql windows a saved extract."""

    model_config = ConfigDict(extra="forbid")

    mode: IncrementalMode
    #: Fixed field for Stripe (``created``). Null for SQL — the user picks.
    column: Optional[str] = None
    #: REST query parameter. Stripe only.
    filter: Optional[str] = None
    note: str


class IncrementalWindow(BaseModel):
    """Per-run window attached to a spec copy. Not stored on the source."""

    model_config = ConfigDict(extra="forbid")

    mode: IncrementalMode
    column: str
    param: Optional[str] = None
    #: Resource name → stored cursor. Empty on the first run and on full refresh.
    starts: dict[str, str] = {}
    full_refresh: bool = False


def template_incremental(connector_id: str) -> Optional[ConnectorIncremental]:
    """Incremental description for one catalog id, or None for a full pull."""
    cid = (connector_id or "").strip().lower()
    if cid == STRIPE_CONNECTOR_ID:
        return ConnectorIncremental(
            mode="stripe",
            column=STRIPE_CURSOR_COLUMN,
            filter=STRIPE_FILTER_PARAM,
            note=_STRIPE_NOTE,
        )
    if cid in SQL_INCREMENTAL_CONNECTORS:
        return ConnectorIncremental(mode="sql", note=_SQL_NOTE)
    return None


def validate_saved_incremental(
    source: DltSourceSpec, connector_id: Optional[str]
) -> None:
    """Reject a cursor column on a template that cannot use one."""
    cid = (connector_id or "").strip().lower() or None
    if source.cursor_column and cid not in SQL_INCREMENTAL_CONNECTORS:
        raise ValueError(
            "cursor_column is only used by the postgres and mysql templates"
        )
    if cid in SQL_INCREMENTAL_CONNECTORS and source.kind != "sql":
        raise ValueError("postgres and mysql templates are SQL sources")
    if cid == STRIPE_CONNECTOR_ID and source.kind != "rest_api":
        raise ValueError("the stripe template is a REST source")


def _stripe_host(base_url: Optional[str]) -> bool:
    if not base_url:
        return False
    return (urlparse(base_url).hostname or "").lower() == STRIPE_HOST


def _norm(name: str) -> str:
    return name.strip().strip("/").replace("/", "_").replace("-", "_").lower()


def _canonical(name: str, resources: list[str]) -> Optional[str]:
    if name in resources:
        return name
    target = _norm(name)
    for resource in resources:
        if _norm(resource) == target:
            return resource
    return None


def _lookup(mapping: dict[str, str], name: str) -> Optional[str]:
    if name in mapping and mapping[name]:
        return mapping[name]
    target = _norm(name)
    for key, value in mapping.items():
        if value and _norm(key) == target:
            return value
    return None


def plan_window(
    source: DltExtractSource, *, full_refresh: bool
) -> Optional[IncrementalWindow]:
    """Window for this run, or None when the source is a full pull.

    Postgres and MySQL window only after the user sets ``cursor_column``.
    Stripe windows on ``created``. A full refresh carries the column (so the
    new high-water mark can be stored) and no lower bound.
    """
    cid = (source.connector_id or "").strip().lower() or None
    if cid in SQL_INCREMENTAL_CONNECTORS:
        column = source.source.cursor_column
        if not column:
            return None
        mode: IncrementalMode = "sql"
        param = None
    elif cid == STRIPE_CONNECTOR_ID or (
        cid is None and _stripe_host(source.source.base_url)
    ):
        mode = "stripe"
        column = STRIPE_CURSOR_COLUMN
        param = STRIPE_FILTER_PARAM
    else:
        return None
    starts: dict[str, str] = {}
    if not full_refresh:
        for name in source.source.resources:
            value = _lookup(source.cursors, name)
            if value:
                starts[name] = value
    return IncrementalWindow(
        mode=mode,
        column=column,
        param=param,
        starts=starts,
        full_refresh=full_refresh,
    )


def bind_incremental(
    spec: DltSourceSpec, window: Optional[IncrementalWindow]
) -> DltSourceSpec:
    """Return a spec copy whose ``incremental`` property is ``window``."""
    bound = spec.model_copy(deep=True)
    bound._incremental = window
    return bound


def window_of(spec: DltSourceSpec) -> Optional[IncrementalWindow]:
    window = getattr(spec, "incremental", None)
    if isinstance(window, IncrementalWindow):
        return window
    return None


def _number(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_cursor(value: Any) -> Optional[str]:
    """Stable text form. Numbers stay decimal. UTC datetimes use a Z suffix."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value:  # NaN
            return None
        if value.is_integer():
            return str(int(value))
        return repr(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("+00:00"):
        text = text[:-6] + "Z"
    return text


def is_newer(value: Any, start: str) -> bool:
    """True when ``value`` is strictly after the stored cursor."""
    left = format_cursor(value)
    if left is None:
        return False
    right = format_cursor(start) or start
    left_n, right_n = _number(left), _number(right)
    if left_n is not None and right_n is not None:
        return left_n > right_n
    return left > right


def coerce_cursor_param(start: str) -> int | str:
    """Integer cursors travel as ints (Stripe ``created``, numeric SQL columns)."""
    number = _number(start)
    if number is not None and number.is_integer():
        return int(number)
    return start


def stripe_endpoint_params(
    spec: DltSourceSpec, resource_name: str
) -> Optional[dict[str, Any]]:
    """Query params for one Stripe resource, or None when this run is not Stripe.

    ``created[gte]`` is omitted on the first run and on a full refresh.
    ``limit`` is always sent for a Stripe window so a short page is visible.
    """
    window = window_of(spec)
    if window is None or window.mode != "stripe":
        return None
    params: dict[str, Any] = {"limit": min(int(spec.limit), STRIPE_PAGE_LIMIT)}
    if window.full_refresh:
        return params
    start = _lookup(window.starts, resource_name)
    if start:
        params[STRIPE_FILTER_PARAM] = coerce_cursor_param(start)
    return params


def sql_incremental_plan(spec: DltSourceSpec) -> dict[str, dict[str, Any]]:
    """Per-table kwargs for ``dlt.sources.incremental``. Empty on a full pull.

    ``initial_value`` is the stored cursor. Infona does not keep a dlt
    pipeline, so there is no dlt state between runs — the stored cursor is
    passed again every time. ``range_start=open`` keeps rows strictly greater
    than that cursor.
    """
    window = window_of(spec)
    if window is None or window.mode != "sql" or window.full_refresh:
        return {}
    plan: dict[str, dict[str, Any]] = {}
    for name in spec.resources:
        start = _lookup(window.starts, name)
        if not start:
            continue
        plan[name] = {
            "cursor_path": window.column,
            "initial_value": coerce_cursor_param(start),
            "range_start": "open",
            "row_order": "asc",
            "on_cursor_value_missing": "raise",
        }
    return plan


def _cell(raw: Any, column: str) -> Any:
    if isinstance(raw, dict):
        return raw.get(column)
    return None


def collect_rows(
    raw_iter: Iterable[Any],
    *,
    window: IncrementalWindow,
    resource_name: str,
    limit: int,
) -> tuple[list[Any], Optional[str]]:
    """Rows to ingest and the next cursor (None = keep the previous cursor).

    SQL stops after ``limit`` kept rows and stores the greatest cursor among
    them. Stripe stores a cursor only when the page is shorter than the limit
    sent (at most 100); a full page would skip objects that were not returned.
    """
    start = None if window.full_refresh else _lookup(window.starts, resource_name)
    cap = min(limit, STRIPE_PAGE_LIMIT) if window.mode == "stripe" else None
    kept: list[Any] = []
    seen = 0
    high: Optional[str] = None
    for raw in raw_iter:
        seen += 1
        if cap is not None and seen > cap:
            return kept, None
        value = _cell(raw, window.column)
        if start is not None and not is_newer(value, start):
            if cap is not None and seen >= cap:
                break
            continue
        if len(kept) < limit:
            kept.append(raw)
            formatted = format_cursor(value)
            if formatted is not None and (high is None or is_newer(formatted, high)):
                high = formatted
        if cap is None and len(kept) >= limit:
            break
        if cap is not None and seen >= cap:
            break
    if cap is not None and seen >= cap:
        return kept, None
    return kept, high


def next_cursors(
    existing: dict[str, str],
    observed: dict[str, Optional[str]],
    resources: list[str],
    *,
    full_refresh: bool,
) -> dict[str, str]:
    """Merge this run's high-water marks into the stored map.

    A full refresh drops the previous cursors for ``resources`` and keeps
    only marks this run produced. An incremental run never moves a cursor
    backward. A missing mark leaves the previous cursor in place, except on
    a full refresh, where that resource's cursor is removed.
    """
    out: dict[str, str] = {}
    if not full_refresh:
        for key, value in existing.items():
            canon = _canonical(key, resources)
            if canon and value:
                out[canon] = value
    for name, value in observed.items():
        if not value:
            continue
        canon = _canonical(name, resources)
        if canon is None:
            continue
        prev = out.get(canon)
        if prev is None or is_newer(value, prev):
            out[canon] = value
    return out
