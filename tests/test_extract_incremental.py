"""Saved-extract cursors (INF-1006).

A fake source_factory stands in for dlt. The second run must request and
return only rows newer than the cursor stored on the extract source. That
cursor is part of the saved document (it survives a reload). A full refresh
does not send it and replaces it.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from infona_client.ingestion.catalog import get_connector, list_connectors
from infona_client.ingestion.dlt_source import (
    _build_dlt_source,
    _incremental_hint,
    _with_sql_incremental,
)
from infona_client.ingestion.errors import DltExtractError
from infona_client.ingestion.extract_source_store import (
    InMemoryExtractSourceStore,
    StoredExtractSource,
)
from infona_client.ingestion.incremental import (
    IncrementalWindow,
    bind_incremental,
    collect_rows,
    is_newer,
    next_cursors,
    plan_window,
    sql_incremental_plan,
    stripe_endpoint_params,
)
from infona_client.ingestion.models import (
    DltExtractSource,
    DltResourceMap,
    DltSourceSpec,
)
from infona_client.ingestion.saved_run import run_saved_extract
from infona_client.ingestion.secrets import ResolvedSecrets
from infona_client.resolver.models import IngestResult

REPO = Path(__file__).resolve().parents[1]


class _FakeResource:
    def __init__(self, name: str, rows: list[dict]):
        self.name = name
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _FakeSource:
    def __init__(self, resources: list[_FakeResource]):
        self.resources = {r.name: r for r in resources}


def _sql_source(**kwargs) -> DltExtractSource:
    column = kwargs.pop("cursor_column", "updated_at")
    cursors = kwargs.pop("cursors", {})
    connector_id = kwargs.pop("connector_id", "postgres")
    resources = kwargs.pop("resources", ["contacts"])
    return DltExtractSource(
        slug="warehouse",
        title="Warehouse",
        kind="sql",
        connector_id=connector_id,
        cursors=cursors,
        source=DltSourceSpec(
            kind="sql",
            dsn="postgresql://user@db.example.com/app",
            resources=resources,
            cursor_column=column,
        ),
        map={name: DltResourceMap(type="Contact", id_field="id") for name in resources},
    )


@pytest.mark.asyncio
@patch("infona_client.ingestion.run.refresh_after_write", new_callable=AsyncMock)
@patch("infona_client.ingestion.run.SchemaResolver")
async def test_second_run_uses_stored_cursor_and_survives_restart(
    mock_resolver_cls, _refresh
):
    inst = AsyncMock()
    inst.ingest_structured_rows.return_value = IngestResult(
        rows_in=1, entities_resolved=1, triples_inserted=1, types_created=["Contact"]
    )
    mock_resolver_cls.return_value = inst

    table = [
        {"id": "1", "updated_at": "2024-01-01T00:00:00Z"},
        {"id": "2", "updated_at": "2024-06-01T00:00:00Z"},
    ]
    seen: list[dict | None] = []
    yielded: list[list[str]] = []

    def factory(spec, _secrets):
        window = spec.incremental
        seen.append(None if window is None else window.model_dump())
        start = None
        if window is not None and not window.full_refresh:
            start = window.starts.get("contacts")
        rows = [r for r in table if start is None or r["updated_at"] > start]
        yielded.append([r["id"] for r in rows])
        return _FakeSource([_FakeResource("contacts", rows)])

    store = InMemoryExtractSourceStore()
    await store.upsert(StoredExtractSource(tenant_id="t", source=_sql_source()))
    source = (await store.get("t", "warehouse")).source

    await run_saved_extract(
        tenant_id="t",
        source=source,
        neptune=object(),
        source_factory=factory,
        store=store,
    )
    assert yielded[0] == ["1", "2"]
    assert seen[0]["starts"] == {}
    assert seen[0]["full_refresh"] is False

    saved = await store.get("t", "warehouse")
    assert saved.source.cursors == {"contacts": "2024-06-01T00:00:00Z"}

    # Restart: a new store loaded only from the saved document.
    fresh = InMemoryExtractSourceStore()
    await fresh.upsert(
        StoredExtractSource(
            tenant_id="t",
            source=DltExtractSource.model_validate(saved.source.model_dump(mode="json")),
        )
    )
    reloaded = await fresh.get("t", "warehouse")
    assert reloaded.source.cursors == {"contacts": "2024-06-01T00:00:00Z"}

    table.append({"id": "3", "updated_at": "2024-12-01T00:00:00Z"})
    await run_saved_extract(
        tenant_id="t",
        source=reloaded.source,
        neptune=object(),
        source_factory=factory,
        store=fresh,
    )
    assert seen[1]["starts"] == {"contacts": "2024-06-01T00:00:00Z"}
    assert yielded[1] == ["3"]
    ingested = inst.ingest_structured_rows.await_args_list[-1].args[0]
    assert [row["id"] for row in ingested] == ["3"]
    assert (await fresh.get("t", "warehouse")).source.cursors == {
        "contacts": "2024-12-01T00:00:00Z"
    }


@pytest.mark.asyncio
@patch("infona_client.ingestion.run.refresh_after_write", new_callable=AsyncMock)
@patch("infona_client.ingestion.run.SchemaResolver")
async def test_full_refresh_ignores_cursor_and_replaces_it(mock_resolver_cls, _refresh):
    inst = AsyncMock()
    inst.ingest_structured_rows.return_value = IngestResult(
        rows_in=1, entities_resolved=1, triples_inserted=1, types_created=["Contact"]
    )
    mock_resolver_cls.return_value = inst
    table = [
        {"id": "1", "updated_at": "2024-01-01T00:00:00Z"},
        {"id": "2", "updated_at": "2024-06-01T00:00:00Z"},
    ]
    seen: list[dict] = []

    def factory(spec, _secrets):
        window = spec.incremental
        seen.append(window.model_dump())
        start = None if window.full_refresh else window.starts.get("contacts")
        rows = [r for r in table if start is None or r["updated_at"] > start]
        return _FakeSource([_FakeResource("contacts", rows)])

    store = InMemoryExtractSourceStore()
    source = _sql_source(cursors={"contacts": "2024-12-01T00:00:00Z"})
    await store.upsert(StoredExtractSource(tenant_id="t", source=source))
    from infona_client.ingestion.models import RunExtractSourceRequest

    await run_saved_extract(
        tenant_id="t",
        source=source,
        neptune=object(),
        override=RunExtractSourceRequest(full_refresh=True),
        source_factory=factory,
        store=store,
    )
    assert seen[0]["full_refresh"] is True
    assert seen[0]["starts"] == {}
    stored = (await store.get("t", "warehouse")).source.cursors
    assert stored == {"contacts": "2024-06-01T00:00:00Z"}
    assert "2024-12-01T00:00:00Z" not in stored.values()


@pytest.mark.asyncio
@patch("infona_client.ingestion.run.refresh_after_write", new_callable=AsyncMock)
@patch("infona_client.ingestion.run.SchemaResolver")
async def test_other_connectors_stay_full_pulls(mock_resolver_cls, _refresh):
    inst = AsyncMock()
    inst.ingest_structured_rows.return_value = IngestResult(
        rows_in=1, entities_resolved=1, triples_inserted=1, types_created=["Contact"]
    )
    mock_resolver_cls.return_value = inst
    seen: list = []

    def factory(spec, _secrets):
        seen.append(spec.incremental)
        return _FakeSource(
            [_FakeResource("v1/contacts", [{"id": "1"}, {"id": "2"}])]
        )

    source = DltExtractSource(
        slug="crm",
        kind="rest_api",
        connector_id="hubspot",
        source=DltSourceSpec(
            kind="rest_api",
            base_url="https://api.example.com",
            resources=["v1/contacts"],
        ),
        map={"v1/contacts": DltResourceMap(type="Contact", id_field="id")},
    )
    store = InMemoryExtractSourceStore()
    await store.upsert(StoredExtractSource(tenant_id="t", source=source))
    for _ in range(2):
        current = (await store.get("t", "crm")).source
        await run_saved_extract(
            tenant_id="t",
            source=current,
            neptune=object(),
            source_factory=factory,
            store=store,
        )
    assert seen == [None, None]
    assert (await store.get("t", "crm")).source.cursors == {}


def test_cursors_are_per_resource_and_do_not_move_backward():
    existing = {"contacts": "100", "orders": "9"}
    assert next_cursors(
        existing, {"contacts": "101"}, ["contacts", "orders"], full_refresh=False
    ) == {"contacts": "101", "orders": "9"}
    assert next_cursors(
        existing, {"contacts": "99"}, ["contacts", "orders"], full_refresh=False
    ) == existing
    assert next_cursors(
        existing, {"contacts": "1"}, ["contacts", "orders"], full_refresh=True
    ) == {"contacts": "1"}
    assert is_newer("100", "99")
    assert not is_newer("99", "100")
    assert not is_newer("100", "100")


def test_plan_window_matches_the_three_templates_only():
    sql = _sql_source(cursors={"contacts": "5"})
    window = plan_window(sql, full_refresh=False)
    assert window is not None and window.mode == "sql"
    assert window.starts == {"contacts": "5"}
    assert plan_window(sql, full_refresh=True).starts == {}
    assert plan_window(_sql_source(cursor_column=None), full_refresh=False) is None

    stripe = DltExtractSource(
        slug="pay",
        kind="rest_api",
        connector_id="stripe",
        cursors={"v1/charges": "1700000000"},
        source=DltSourceSpec(
            kind="rest_api",
            base_url="https://api.stripe.com",
            resources=["v1/charges"],
        ),
        map={"v1/charges": DltResourceMap(type="Charge")},
    )
    stripe_window = plan_window(stripe, full_refresh=False)
    assert stripe_window.mode == "stripe"
    assert stripe_window.column == "created"
    assert stripe_window.param == "created[gte]"
    assert stripe_window.starts == {"v1/charges": "1700000000"}

    inferred = stripe.model_copy(update={"connector_id": None})
    assert plan_window(inferred, full_refresh=False).mode == "stripe"
    other = stripe.model_copy(update={"connector_id": "hubspot"})
    assert plan_window(other, full_refresh=False) is None


def test_stripe_filter_and_full_page_do_not_skip():
    window = IncrementalWindow(
        mode="stripe",
        column="created",
        param="created[gte]",
        starts={"v1/charges": "1700000000"},
    )
    spec = bind_incremental(
        DltSourceSpec(
            kind="rest_api",
            base_url="https://api.stripe.com",
            resources=["v1/charges"],
            limit=1000,
        ),
        window,
    )
    params = stripe_endpoint_params(spec, "v1/charges")
    assert params == {"limit": 100, "created[gte]": 1700000000}
    refreshed = window.model_copy(update={"full_refresh": True, "starts": {}})
    assert "created[gte]" not in stripe_endpoint_params(
        bind_incremental(spec, refreshed), "v1/charges"
    )

    full_page = [{"created": i} for i in range(100)]
    kept, cursor = collect_rows(
        full_page, window=window.model_copy(update={"starts": {}}), resource_name="v1/charges", limit=1000
    )
    assert len(kept) == 100
    assert cursor is None

    kept, cursor = collect_rows(
        [{"created": 1700000000}, {"created": 1700000001}],
        window=window,
        resource_name="v1/charges",
        limit=1000,
    )
    assert [row["created"] for row in kept] == [1700000001]
    assert cursor == "1700000001"


def test_sql_incremental_plan_is_exclusive_and_attaches():
    window = IncrementalWindow(
        mode="sql", column="updated_at", starts={"contacts": "5"}
    )
    spec = bind_incremental(
        DltSourceSpec(
            kind="sql",
            dsn="postgresql://user@db.example.com/app",
            resources=["contacts"],
            cursor_column="updated_at",
        ),
        window,
    )
    plan = sql_incremental_plan(spec)
    assert plan["contacts"]["initial_value"] == 5
    assert plan["contacts"]["range_start"] == "open"
    assert plan["contacts"]["row_order"] == "asc"
    assert sql_incremental_plan(
        bind_incremental(spec, window.model_copy(update={"full_refresh": True}))
    ) == {}
    assert "incremental" not in spec.model_dump()
    assert "_incremental" not in DltSourceSpec.model_json_schema()["properties"]

    class _Res:
        def __init__(self):
            self.hint = None

        def apply_hints(self, incremental):
            self.hint = incremental

    class _Src:
        def __init__(self):
            self.resources = {"contacts": _Res()}

    class _Dlt:
        class sources:
            @staticmethod
            def incremental(**kwargs):
                return kwargs

    source = _Src()
    with patch("infona_client.ingestion.dlt_source._import_dlt", return_value=_Dlt):
        _with_sql_incremental(source, spec)
    assert source.resources["contacts"].hint["cursor_path"] == "updated_at"
    assert source.resources["contacts"].hint["initial_value"] == 5

    class _Old:
        class sources:
            @staticmethod
            def incremental(*args, **kwargs):
                if "range_start" in kwargs:
                    raise TypeError("old")
                return {"args": args, "kwargs": kwargs}

    assert _incremental_hint(_Old, plan["contacts"])["kwargs"]["initial_value"] == 5


def test_stripe_rest_config_sends_created_gte_and_keeps_single_page(monkeypatch):
    captured: dict = {}

    def rest_api_source(config):
        captured["config"] = config
        return object()

    module = types.ModuleType("dlt.sources.rest_api")
    module.rest_api_source = rest_api_source
    monkeypatch.setitem(sys.modules, "dlt.sources.rest_api", module)
    monkeypatch.setattr(
        "infona_client.ingestion.dlt_source._import_dlt", lambda: object()
    )
    spec = bind_incremental(
        DltSourceSpec(
            kind="rest_api",
            base_url="https://api.stripe.com",
            auth={"type": "bearer", "token": "sk_test_secret"},
            resources=["v1/charges"],
            limit=1000,
        ),
        IncrementalWindow(
            mode="stripe",
            column="created",
            param="created[gte]",
            starts={"v1/charges": "1700000000"},
        ),
    )
    _build_dlt_source(spec, ResolvedSecrets(token="sk_test_secret"))
    config = captured["config"]
    assert config["client"]["paginator"] == "single_page"
    assert config["client"]["auth"]["type"] == "bearer"
    endpoint = config["resources"][0]["endpoint"]
    assert endpoint["paginator"] == "single_page"
    assert endpoint["params"] == {"limit": 100, "created[gte]": 1700000000}
    assert "sk_test_secret" not in str(endpoint)


def test_catalog_and_docs_list_the_same_incremental_sources():
    incremental = {c.id for c in list_connectors() if c.incremental is not None}
    assert incremental == {"stripe", "postgres", "mysql"}
    stripe = get_connector("stripe").incremental
    assert stripe.mode == "stripe"
    assert stripe.column == "created"
    assert stripe.filter == "created[gte]"
    for cid in ("postgres", "mysql"):
        hint = get_connector(cid).incremental
        assert hint.mode == "sql"
        assert hint.column is None
        assert hint.filter is None
    assert get_connector("hubspot").incremental is None
    assert get_connector("custom_sql").incremental is None
    assert get_connector("custom_rest").incremental is None

    text = (REPO / "docs" / "extract-incremental.md").read_text()
    listed = set()
    for line in text.splitlines():
        if line.startswith("| `") and line.endswith("|"):
            listed.add(line.split("`")[1])
    assert listed == {"stripe", "postgres", "mysql"}
    assert "full pull" in text.lower()


def test_cursor_column_rejected_unless_postgres_or_mysql(client, auth_headers, monkeypatch):
    monkeypatch.setenv("INFONA_SECRETS_KEY", "0" * 64)
    from infona_client.api_registry.crypto import reset_secret_cipher
    from infona_client.ingestion.extract_source_store import reset_extract_source_store

    reset_secret_cipher()
    reset_extract_source_store()

    rejected = client.post(
        "/graphs/test-tenant/extract-sources",
        json={
            "slug": "crm",
            "connector_id": "hubspot",
            "source": {
                "kind": "rest_api",
                "base_url": "https://api.example.com",
                "resources": ["v1/contacts"],
                "cursor_column": "updated_at",
            },
            "map": {"v1/contacts": {"type": "Contact", "id_field": "id"}},
        },
        headers=auth_headers,
    )
    assert rejected.status_code == 422
    assert "cursor_column" in rejected.text

    created = client.post(
        "/graphs/test-tenant/extract-sources",
        json={
            "slug": "warehouse",
            "connector_id": "postgres",
            "source": {
                "kind": "sql",
                "dsn": "postgresql://user:pw@db.example.com/app",
                "resources": ["contacts"],
                "cursor_column": "updated_at",
            },
            "map": {"contacts": {"type": "Contact", "id_field": "id"}},
        },
        headers=auth_headers,
    )
    assert created.status_code == 200, created.text
    body = created.json()
    assert body["connector_id"] == "postgres"
    assert body["cursor_column"] == "updated_at"
    assert body["cursors"] == {}
    assert "pw" not in created.text

    import asyncio

    from infona_client.ingestion.extract_source_store import make_extract_source_store

    async def _plant_cursor() -> None:
        store = make_extract_source_store()
        row = await store.get("test-tenant", "warehouse")
        row.source.cursors["contacts"] = "9"
        await store.upsert(row)

    asyncio.run(_plant_cursor())
    patched = client.patch(
        "/graphs/test-tenant/extract-sources/warehouse",
        json={
            "source": {
                "kind": "sql",
                "dsn": "dsn",
                "resources": ["contacts"],
                "cursor_column": "created_at",
            }
        },
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["cursor_column"] == "created_at"
    assert patched.json()["cursors"] == {}


def test_sql_incremental_refuses_a_source_it_cannot_attach():
    window = IncrementalWindow(mode="sql", column="updated_at", starts={"t": "1"})
    spec = bind_incremental(
        DltSourceSpec(
            kind="sql",
            dsn="postgresql://user@db.example.com/app",
            resources=["t"],
            cursor_column="updated_at",
        ),
        window,
    )

    class _Empty:
        resources = {}

    with patch("infona_client.ingestion.dlt_source._import_dlt", return_value=object()):
        with pytest.raises(DltExtractError, match="could not attach"):
            _with_sql_incremental(_Empty(), spec)
