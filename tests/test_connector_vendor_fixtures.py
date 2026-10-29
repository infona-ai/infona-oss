"""Mocked-HTTP checks for the HubSpot, Stripe, and Pipedrive templates.

Drives the real ``rest_api_source`` → :func:`extract_records` path (no
``source_factory``). dlt uses ``requests``, so the mock is ``responses``.
Skip when the ``[dlt]`` extra is not installed.

Fixtures are the vendor's published list examples
(:mod:`tests.connector_doc_fixtures`). No live credentials.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

import pytest

from infona_client.ingestion.catalog import get_connector
from infona_client.ingestion.dlt_source import dlt_available, extract_records
from infona_client.ingestion.models import DltAuthSpec, DltSourceSpec
from infona_client.ingestion.secrets import ResolvedSecrets
from tests.connector_doc_fixtures import DOCUMENTED

pytestmark = pytest.mark.skipif(
    not dlt_available(), reason="infona-client[dlt] not installed"
)

responses = pytest.importorskip("responses")

FIXTURE_TOKEN = "fixture-token"
_METADATA = "http://169.254.169.254/latest/meta-data/"


def _spec(template) -> DltSourceSpec:
    return DltSourceSpec(
        kind="rest_api",
        base_url=template.base_url,
        auth=DltAuthSpec(
            type=template.auth.type,
            token=FIXTURE_TOKEN,
            api_key_header=template.auth.api_key_header,
        ),
        resources=[item["path"] for item in DOCUMENTED[template.id]["resources"]],
        limit=50,
    )


def _url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def _by_path(extracted, path: str):
    want = path.strip("/").replace("/", "_").replace("-", "_").lower()
    for resource in extracted:
        got = resource.name.strip("/").replace("/", "_").replace("-", "_").lower()
        if resource.name == path or got == want:
            return resource
    raise AssertionError(
        f"no resource for {path}; got {[r.name for r in extracted]}"
    )


def _header(call, name: str) -> str:
    want = name.lower()
    for key, value in call.request.headers.items():
        if key.lower() == want:
            return value
    return ""


def _assert_auth(template_id: str, call) -> None:
    contract = DOCUMENTED[template_id]
    if contract["auth_type"] == "bearer":
        assert _header(call, "authorization") == f"Bearer {FIXTURE_TOKEN}"
        return
    assert _header(call, contract["api_key_header"]) == FIXTURE_TOKEN
    assert _header(call, "authorization") == ""
    assert "api_token=" not in call.request.url


def _row_text(row: dict[str, str]) -> str:
    parts = []
    for value in row.values():
        parts.append(value)
        if value.startswith("{") or value.startswith("["):
            try:
                parts.append(json.dumps(json.loads(value)))
            except json.JSONDecodeError:
                pass
    return "\n".join(parts)


@pytest.mark.parametrize("connector_id", ["hubspot", "stripe", "pipedrive"])
def test_template_extracts_doc_example_rows(connector_id: str):
    """One page of each documented list, unwrapped to rows with the example ids."""
    template = get_connector(connector_id)
    contract = DOCUMENTED[connector_id]
    assert template is not None
    assert template.base_url == contract["base_url"]
    assert [r.path for r in template.resources] == [item["path"] for item in contract["resources"]]

    with responses.RequestsMock() as mocked:
        for item in contract["resources"]:
            mocked.add(
                responses.GET,
                _url(template.base_url, item["path"]),
                json=item["body"],
                status=200,
            )
        extracted = extract_records(
            _spec(template),
            secrets=ResolvedSecrets(token=FIXTURE_TOKEN),
        )
        calls = list(mocked.calls)

    assert len(calls) == len(contract["resources"])
    hosts = {urlparse(call.request.url).netloc for call in calls}
    assert hosts == {urlparse(template.base_url).netloc}
    for call in calls:
        _assert_auth(connector_id, call)

    for item in contract["resources"]:
        resource = _by_path(extracted, item["path"])
        assert [row["id"] for row in resource.rows] == list(item["ids"])
        assert item["marker"] in _row_text(resource.rows[0])
        assert "results" not in resource.rows[0]
        assert "paging" not in resource.rows[0]


def test_single_page_does_not_follow_hubspot_next_link_or_metadata_host():
    """The published paging.next.link is another host. A metadata next link is not fetched either."""
    template = get_connector("hubspot")
    assert template is not None
    contacts = DOCUMENTED["hubspot"]["resources"][0]
    published = contacts["body"]["paging"]["next"]["link"]
    assert urlparse(published).netloc == "api.hubspot.com"
    assert urlparse(template.base_url).netloc == "api.hubapi.com"

    off_host = json.loads(json.dumps(contacts["body"]))
    off_host["paging"]["next"]["link"] = _METADATA

    spec = DltSourceSpec(
        kind="rest_api",
        base_url=template.base_url,
        auth=DltAuthSpec(type="bearer", token=FIXTURE_TOKEN),
        resources=["crm/v3/objects/contacts"],
        limit=50,
    )
    with responses.RequestsMock() as mocked:
        mocked.add(
            responses.GET,
            _url(template.base_url, "crm/v3/objects/contacts"),
            json=off_host,
            status=200,
        )
        extracted = extract_records(spec, secrets=ResolvedSecrets(token=FIXTURE_TOKEN))
        calls = list(mocked.calls)

    assert len(calls) == 1
    assert calls[0].request.url.startswith("https://api.hubapi.com/")
    assert "169.254.169.254" not in calls[0].request.url
    assert [row["id"] for row in extracted[0].rows] == ["33451"]


def test_single_page_does_not_follow_stripe_has_more_or_pipedrive_cursor():
    """Stripe ``has_more`` and Pipedrive ``next_cursor`` do not cause a second request."""
    stripe = get_connector("stripe")
    pipedrive = get_connector("pipedrive")
    assert stripe is not None and pipedrive is not None

    customers = json.loads(json.dumps(DOCUMENTED["stripe"]["resources"][0]["body"]))
    customers["has_more"] = True
    persons = json.loads(json.dumps(DOCUMENTED["pipedrive"]["resources"][0]["body"]))
    persons["additional_data"] = {"next_cursor": "cursor-page-2"}

    cases: tuple[tuple[Any, str, dict[str, Any], str], ...] = (
        (stripe, "v1/customers", customers, "cus_NffrFeUfNV2Hib"),
        (pipedrive, "persons", persons, "1"),
    )
    for template, path, body, expected_id in cases:
        spec = DltSourceSpec(
            kind="rest_api",
            base_url=template.base_url,
            auth=DltAuthSpec(
                type=template.auth.type,
                token=FIXTURE_TOKEN,
                api_key_header=template.auth.api_key_header,
            ),
            resources=[path],
            limit=50,
        )
        with responses.RequestsMock() as mocked:
            mocked.add(responses.GET, _url(template.base_url, path), json=body, status=200)
            extracted = extract_records(spec, secrets=ResolvedSecrets(token=FIXTURE_TOKEN))
            assert len(list(mocked.calls)) == 1
        assert [row["id"] for row in extracted[0].rows] == [expected_id]
