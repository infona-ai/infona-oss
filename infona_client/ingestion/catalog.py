"""Connector catalog for the extract family (ONTA-555).

The catalog is **prefill data, not a new engine**. Every entry is a starting
point for the SAME frozen ``DltSourceSpec`` the generic REST / SQL extract
already accepts (ONTA-553): a base URL, an auth *type*, a few resource paths
and the ontology type each one should land as. Picking "HubSpot" in the
Explorer is exactly equivalent to typing those fields by hand — there is no
per-connector code path, no vendor SDK, and no second write path.

Boundary notes (docs/oss_proprietary_boundary.md §33):

* These are **our own templates over the generic dlt REST/SQL source**. They
  are NOT dltHub verified sources and we take no dltHub paid extras. Do not
  describe the catalog as "600 connectors".
* **BYOK only.** A template names the credential the user must paste
  (``auth.label``); it never ships, implies, or provisions a platform key.
* ``verified`` is set only when that template's paths, auth, and response
  envelope were checked against the vendor's public docs with a mocked-HTTP
  fixture (see ``tests/test_connector_vendor_fixtures.py``). It is not a
  live-account check. A template with ``verified`` unset has not been checked.
  Salesforce is deferred: SOQL needs a sandbox org.
* Every REST extract reads the **first page only**. ``_build_dlt_source``
  pins dlt's ``single_page`` paginator so a next-page link cannot hop off
  ``base_url``. REST templates report that as ``pagination="single_page"``.

The canonical route is ``GET /graphs/{tenant}/extract-sources/catalog`` — the
Explorer, the CLI and MCP all read the same list rather than each shipping
their own copy.
"""

from __future__ import annotations

import re
from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator

from infona_client.ingestion.models import DltAuthType, DltSourceKind

#: Grouping used by the Explorer gallery. Purely presentational.
ConnectorCategory = Literal[
    "crm",
    "payments",
    "commerce",
    "support",
    "dev",
    "marketing",
    "project",
    "productivity",
    "database",
    "custom",
]

#: ``{token}`` slots inside a template's ``base_url`` (Shopify store, Zendesk
#: subdomain, ...). The UI renders one field per slot and substitutes before it
#: POSTs; the create route rejects any base_url that still carries one.
PLACEHOLDER_RE = re.compile(r"\{([a-z_]+)\}")


class ConnectorPlaceholder(BaseModel):
    """One workspace-specific value that completes a template's base URL."""

    key: str
    label: str
    example: str = ""
    help: str = ""


class ConnectorResource(BaseModel):
    """A REST path (or SQL table) plus the ontology type it should land as."""

    path: str
    label: str
    suggested_type: str
    id_field: str = "id"
    #: Preselected in the connect drawer. False = offered but off by default.
    default: bool = True


class ConnectorVerification(BaseModel):
    """How a template was checked. Absent on the template means not verified.

    ``method`` names the check (public docs plus a mocked fixture, not a live
    account). ``date`` is the ISO day that check was recorded.
    """

    method: str = Field(min_length=1)
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")


class ConnectorAuth(BaseModel):
    """What credential the user pastes. Always BYOK — never a platform key."""

    type: DltAuthType
    #: What to call the secret in the UI ("Private app token", "Secret key").
    label: str = "API token"
    help: str = ""
    #: Header name when ``type="api_key"`` (mirrors ``DltAuthSpec``).
    api_key_header: Optional[str] = None
    #: What goes in the username field when ``type="basic"``.
    username_label: Optional[str] = None
    #: Prefilled username for basic-auth APIs that want a fixed literal.
    username_default: Optional[str] = None


class ConnectorTemplate(BaseModel):
    """A prefilled starting point for one extract source."""

    id: str
    title: str
    category: ConnectorCategory
    kind: DltSourceKind
    blurb: str
    docs_url: str = ""
    #: REST base URL, possibly carrying ``{placeholder}`` slots. None for SQL.
    base_url: Optional[str] = None
    placeholders: list[ConnectorPlaceholder] = Field(default_factory=list)
    headers: dict[str, str] = Field(default_factory=dict)
    auth: ConnectorAuth
    resources: list[ConnectorResource] = Field(default_factory=list)
    #: True for the two blank tiles ("Custom REST API" / "Custom SQL database").
    custom: bool = False
    #: Free-text caveat surfaced in the drawer (auth quirks, plan requirements).
    note: str = ""
    #: Set only for a template whose paths, auth, and response envelope have a
    #: passing mocked-HTTP fixture. Unset means not verified.
    verified: Optional[ConnectorVerification] = Field(
        default=None,
        description=(
            "Public-docs check (method + date). Set only when a mocked-HTTP "
            "fixture drives the real extract path for this template. Absent "
            "means not verified. Not a live-account check."
        ),
    )
    #: ``single_page`` on every REST template: the extract returns the first
    #: page and does not follow a next link. SQL templates leave this unset.
    pagination: Optional[Literal["single_page"]] = Field(
        default=None,
        description=(
            "single_page on REST templates. The extract route pins dlt's "
            "single_page paginator, so a list endpoint returns its first page "
            "and a next-page link is not followed. SQL templates leave this unset."
        ),
    )

    @model_validator(mode="after")
    def _rest_reads_one_page(self) -> "ConnectorTemplate":
        if self.kind == "rest_api":
            self.pagination = "single_page"
        return self

    def example_base_url(self) -> Optional[str]:
        """``base_url`` with each placeholder replaced by its example value."""
        if not self.base_url:
            return None
        out = self.base_url
        for ph in self.placeholders:
            out = out.replace("{" + ph.key + "}", ph.example or ph.key)
        return out


def list_connectors() -> list[ConnectorTemplate]:
    """Every template, in catalog order (curated first, custom tiles last)."""
    from infona_client.ingestion.catalog_data import CONNECTORS

    return list(CONNECTORS)


def get_connector(connector_id: str) -> Optional[ConnectorTemplate]:
    """One template by id, or None. Ids are stable — the UI stores them."""
    wanted = (connector_id or "").strip().lower()
    for template in list_connectors():
        if template.id == wanted:
            return template
    return None


def unresolved_placeholders(base_url: Optional[str]) -> list[str]:
    """Placeholder keys still present in ``base_url``.

    A source saved with ``https://{store}.myshopify.com`` would fail at run
    time with a confusing DNS error, so the create/update routes reject it up
    front with the slot names the user still has to fill.
    """
    if not base_url:
        return []
    return PLACEHOLDER_RE.findall(base_url)
