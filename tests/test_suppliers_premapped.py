"""INF-1002: frozen suppliers schema loads six fragments; ER runs for real.

No API key. The schema file is the only cache. ``rebuild_kg`` is the live
second pass and must match ``examples/suppliers-messy.er-rebuild.txt``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from infona_client.graph.explore_store import list_entities_by_type
from infona_client.graph.ontology_queries import attr_uri, entity_uri
from infona_client.graph.queries import kg_graph_uri
from infona_client.graph.store import get_graph_store
from infona_client.graph.suppliers_snapshot import (
    DEFAULT_KG,
    DEFAULT_TENANT,
    EXPECTED_FRAGMENTS,
    fragment_uris,
    load_mapping,
    load_suppliers_snapshot,
    triples_from_mapping,
)
from infona_client.graph.validity import fetch_current_object_terms
from infona_client.pipeline.conflict import REASON_AUTHORITY
from infona_client.resolver.er.rebuild import rebuild_kg
from infona_client.resolver.er.rebuild_explain import format_rebuild_report

REPO = Path(__file__).resolve().parents[1]
SCHEMA = REPO / "examples" / "prebuilt" / "suppliers_schema.json"
LOADER = REPO / "infona_client" / "graph" / "suppliers_snapshot.py"
OSS_UP = REPO / "scripts" / "oss_up.sh"
GOLDEN = REPO / "examples" / "suppliers-messy.er-rebuild.txt"
README = REPO / "README.md"

ACME = entity_uri("Supplier", "ERP-1001")
HQ = attr_uri("Supplier", "headquarters")
CREDIT = attr_uri("Supplier", "credit_rating")
GRAPH = kg_graph_uri(DEFAULT_TENANT, DEFAULT_KG)


def test_schema_file_caches_inference_only() -> None:
    payload = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert payload["cached_step"] == "schema-inference"
    assert "entity-resolution" in payload["not_cached"]
    assert "merges" not in payload
    assert "signal-richest" not in SCHEMA.read_text(encoding="utf-8")
    mapping = load_mapping(SCHEMA)
    assert mapping.entity_type == "Supplier"
    triples = triples_from_mapping()
    assert len(fragment_uris(triples)) == EXPECTED_FRAGMENTS
    assert ACME in fragment_uris(triples)
    src = LOADER.read_text(encoding="utf-8")
    assert "rebuild_kg(" not in src
    assert "rewrite_subject(" not in src
    assert "insert_facts(" in src


def test_readme_and_oss_up_separate_cache_from_live_er() -> None:
    readme = README.read_text(encoding="utf-8")
    assert "examples/prebuilt/suppliers_schema.json" in readme
    assert "er rebuild --kg suppliers" in readme
    script = OSS_UP.read_text(encoding="utf-8")
    assert "load_prebuilt_suppliers.sh" in script
    tail = [ln for ln in script.splitlines() if ln.strip()][-1]
    assert "er rebuild --kg suppliers" in tail
    for ln in script.splitlines():
        if "er rebuild" in ln:
            assert "log " in ln


@pytest.mark.asyncio
async def test_load_is_unmerged_and_rebuild_matches_golden() -> None:
    store = get_graph_store()
    n = await load_suppliers_snapshot(store, tenant_id=DEFAULT_TENANT, kg=DEFAULT_KG)
    assert n > EXPECTED_FRAGMENTS
    page = await list_entities_by_type(
        tenant_id=DEFAULT_TENANT, kg=DEFAULT_KG, type_name="Supplier"
    )
    ids = {e.id for e in page.entities}
    assert len(ids) == EXPECTED_FRAGMENTS
    assert entity_uri("Supplier", "CRM-4402") in ids
    assert entity_uri("Supplier", "CRM-5503") in ids

    report = await rebuild_kg(None, GRAPH)
    assert report["fragments_absorbed_total"] == 3
    rendered = format_rebuild_report(report, DEFAULT_KG)
    assert rendered == GOLDEN.read_text(encoding="utf-8")

    hq = next(c for c in report["conflicts"] if c["field"] == "headquarters")
    assert hq["winner"]["value"] == "Austin"
    assert hq["reason"] == REASON_AUTHORITY
    credit = report["unresolved"]
    assert len(credit) == 1
    assert credit[0]["field"] == "credit_rating"
    assert {v["value"] for v in credit[0]["values"]} == {"A", "BBB"}

    after = await list_entities_by_type(
        tenant_id=DEFAULT_TENANT, kg=DEFAULT_KG, type_name="Supplier"
    )
    after_ids = {e.id for e in after.entities}
    assert len(after_ids) == 3
    assert ACME in after_ids
    assert entity_uri("Supplier", "CRM-4402") not in after_ids
    assert set(await fetch_current_object_terms(None, GRAPH, ACME, HQ)) == {"Austin"}
    assert set(await fetch_current_object_terms(None, GRAPH, ACME, CREDIT)) == {"A", "BBB"}

    again = await rebuild_kg(None, GRAPH)
    assert again["fragments_absorbed_total"] == 0
