"""Load suppliers-messy fragments with no LLM (INF-1002).

The only cached step is schema inference: ``examples/prebuilt/suppliers_schema.json``
is a frozen :class:`CSVSchemaMapping`. Rows from ``examples/suppliers-messy.csv``
are mapped with :meth:`CSVApplyMixin.apply_mapping` (deterministic) and written
through :func:`insert_facts`, including the ER block index ingest would have
written. Entity resolution is not applied — ``infona er rebuild --kg suppliers``
is the live second pass.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
from pathlib import Path
from typing import Any, Sequence

from infona_client.graph.iri import TYPE_URI_PREFIX
from infona_client.graph.kg_writer import insert_facts
from infona_client.graph.ontology_queries import attr_uri, entity_uri
from infona_client.graph.queries import kg_graph_uri
from infona_client.resolver.csv_apply import CSVApplyMixin
from infona_client.resolver.er.blocking import SparqlBlocker, generate_block_keys
from infona_client.resolver.er.engine import extract_signals
from infona_client.resolver.er.normalize import DefaultNormalizer
from infona_client.resolver.models import CSVSchemaMapping, ColumnRole

DEFAULT_TENANT = "default"
DEFAULT_KG = "suppliers"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
EXPECTED_FRAGMENTS = 6

_REPO_ROOT = Path(__file__).resolve().parents[2]
PREBUILT_DIR = _REPO_ROOT / "examples" / "prebuilt"
SUPPLIERS_CSV = _REPO_ROOT / "examples" / "suppliers-messy.csv"
SCHEMA_JSON = PREBUILT_DIR / "suppliers_schema.json"

_KG_DESCRIPTION = (
    "Suppliers-messy sample. Schema mapping is cached; six fragments, not merged."
)


def schema_path() -> Path:
    override = (os.environ.get("INFONA_SUPPLIERS_SCHEMA") or "").strip()
    if override:
        return Path(override)
    return SCHEMA_JSON


def csv_path() -> Path:
    override = (os.environ.get("INFONA_SUPPLIERS_CSV") or "").strip()
    if override:
        return Path(override)
    return SUPPLIERS_CSV


def load_mapping(path: Path | None = None) -> CSVSchemaMapping:
    """Read the frozen schema. Reject a file that is an ER report in disguise."""
    payload = json.loads((path or schema_path()).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("suppliers schema must be a JSON object")
    if payload.get("cached_step") != "schema-inference":
        raise ValueError(
            "suppliers schema cached_step must be schema-inference "
            "(entity resolution is not cached)"
        )
    for banned in ("merges", "conflicts", "unresolved", "fragments_absorbed"):
        if banned in payload:
            raise ValueError(f"suppliers schema must not carry ER field {banned!r}")
    mapping = CSVSchemaMapping.model_validate(payload["mapping"])
    if mapping.entity_type != "Supplier":
        raise ValueError(
            f"suppliers schema entity_type must be Supplier, got {mapping.entity_type!r}"
        )
    keyed = any(
        c.role == ColumnRole.TYPE_ID and c.column_name == "record_id"
        for c in mapping.columns
    )
    if not keyed:
        raise ValueError("suppliers schema must key Supplier on record_id")
    if mapping.entities:
        raise ValueError("suppliers schema is a single-entity mapping")
    return mapping


def load_rows(path: Path | None = None) -> list[dict[str, str]]:
    with (path or csv_path()).open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def triples_from_mapping(
    rows: Sequence[dict[str, str]] | None = None,
    mapping: CSVSchemaMapping | None = None,
) -> list[tuple[str, str, str]]:
    """Deterministic fragments: one Supplier URI per row, plus ER block keys.

    Does not merge. ``er rebuild`` is the pass that collapses fragments.
    """
    mapping = mapping or load_mapping()
    applied = CSVApplyMixin.apply_mapping(mapping, list(rows or load_rows()))
    if applied.rows_dropped:
        raise ValueError(
            f"suppliers mapping dropped {applied.rows_dropped} rows; expected 0"
        )
    if len(applied.entities) != EXPECTED_FRAGMENTS:
        raise ValueError(
            f"suppliers mapping produced {len(applied.entities)} entities, "
            f"expected {EXPECTED_FRAGMENTS}"
        )
    type_name = mapping.entity_type
    normalizer = DefaultNormalizer()
    triples: list[tuple[str, str, str]] = []
    for entity in applied.entities:
        uri = entity_uri(type_name, entity.id)
        signals = extract_signals(entity)
        label = signals.name or entity.id
        triples.append((uri, RDF_TYPE, f"{TYPE_URI_PREFIX}{type_name}"))
        triples.append((uri, RDFS_LABEL, label))
        for attr in entity.attributes or []:
            if not attr.name or attr.value in (None, ""):
                continue
            triples.append((uri, attr_uri(type_name, attr.name), str(attr.value)))
        normalized = normalizer.normalize(signals)
        triples.extend(
            SparqlBlocker.index_triples(
                uri, normalized, generate_block_keys(normalized)
            )
        )
    return triples


def fragment_uris(triples: Sequence[tuple[str, str, str]]) -> list[str]:
    seen: list[str] = []
    for subject, pred, _obj in triples:
        if pred != RDF_TYPE:
            continue
        uri = subject[1:-1] if subject.startswith("<") and subject.endswith(">") else subject
        if uri not in seen:
            seen.append(uri)
    return seen


async def register_kg(store: Any, tenant_id: str, kg: str, *, n_triples: int) -> None:
    native = getattr(store, "kg_registry_upsert", None)
    if callable(native):
        await native(
            tenant_id,
            kg,
            description=_KG_DESCRIPTION,
            triple_count=n_triples,
            only_if_absent=False,
        )
        return
    from infona_client.graph.kg_registry import upsert_registered_kg
    from infona_client.graph.store import configure_graph_store

    configure_graph_store(store)
    await upsert_registered_kg(
        tenant_id,
        kg,
        description=_KG_DESCRIPTION,
        triple_count=n_triples,
        only_if_absent=False,
    )


async def load_suppliers_snapshot(
    store: Any,
    *,
    tenant_id: str = DEFAULT_TENANT,
    kg: str = DEFAULT_KG,
    triples: Sequence[tuple[str, str, str]] | None = None,
) -> int:
    """Write six unmerged Supplier fragments. Returns the triple count."""
    bootstrap = getattr(store, "bootstrap_schema", None)
    if callable(bootstrap):
        await bootstrap()
    facts = list(triples) if triples is not None else triples_from_mapping()
    if len(fragment_uris(facts)) != EXPECTED_FRAGMENTS:
        raise ValueError(
            f"suppliers load expected {EXPECTED_FRAGMENTS} fragments, "
            f"got {len(fragment_uris(facts))}"
        )
    graph = kg_graph_uri(tenant_id, kg)
    await insert_facts(None, graph, facts, store=store)
    await register_kg(store, tenant_id, kg, n_triples=len(facts))
    return len(facts)


def _resolve_store() -> Any:
    from infona_client.graph.store import get_graph_store

    return get_graph_store()


async def _amain(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Load suppliers-messy fragments from the frozen schema (no LLM). "
            "Does not run entity resolution."
        )
    )
    parser.add_argument("--tenant", default=os.environ.get("INFONA_TENANT") or DEFAULT_TENANT)
    parser.add_argument("--kg", default=os.environ.get("INFONA_SUPPLIERS_KG") or DEFAULT_KG)
    args = parser.parse_args(list(argv) if argv is not None else None)

    store = _resolve_store()
    facts = triples_from_mapping()
    n = await load_suppliers_snapshot(
        store, tenant_id=args.tenant, kg=args.kg, triples=facts
    )
    print(
        f"loaded suppliers-messy fragments: tenant={args.tenant} kg={args.kg} "
        f"entities={len(fragment_uris(facts))} triples={n} "
        f"(schema cached; ER not applied)"
    )
    close = getattr(store, "close", None)
    if callable(close):
        await close()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(_amain(argv))


if __name__ == "__main__":
    sys.exit(main())
