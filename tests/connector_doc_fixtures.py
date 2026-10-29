"""Documented list-endpoint bodies for the verified connector templates.

Each payload is trimmed from a public vendor example (cited on the resource).
Pipedrive's OpenAPI publishes those examples inline; HubSpot and Stripe publish
them on the API reference pages named below. No credentials. Values that the
vendor marks as samples stay samples.

The extract reads the first page only. Bodies that include a next link keep
the vendor's link so a test can show it is not requested.
"""

from __future__ import annotations

from typing import Any

VERIFIED_IDS = ("hubspot", "pipedrive", "stripe")
DEFERRED_ID = "salesforce"

# CRM v3 collection from the contacts guide (one published record).
# https://developers.hubspot.com/docs/api-reference/legacy/crm/objects/contacts/guide
_HUBSPOT_CONTACTS: dict[str, Any] = {
    "results": [
        {
            "id": "33451",
            "properties": {
                "createdate": "2022-06-01T14:31:48.469Z",
                "email": "lorelai@thedragonfly.com",
                "firstname": "Lorelai",
                "hs_object_id": "33451",
                "lastmodifieddate": "2025-07-07T20:27:17.947Z",
                "lastname": "Gilmore",
            },
            "createdAt": "2022-06-01T14:31:48.469Z",
            "updatedAt": "2025-07-07T20:27:17.947Z",
            "archived": False,
        }
    ],
    "paging": {
        "next": {
            "after": "33452",
            "link": "https://api.hubspot.com/crm/objects/v3/contacts?limit=1",
        }
    },
}

# Companies and deals use the same CRM collection (`results[].id`). Property
# values below are synthetic; the paths are the documented list endpoints.
# https://developers.hubspot.com/docs/api-reference/legacy/crm/objects/companies/guide
# https://developers.hubspot.com/docs/api-reference/legacy/crm/objects/deals/guide
_HUBSPOT_COMPANIES: dict[str, Any] = {
    "results": [
        {
            "id": "87001",
            "properties": {"name": "Sample Company", "domain": "example.com"},
            "archived": False,
        }
    ]
}
_HUBSPOT_DEALS: dict[str, Any] = {
    "results": [
        {
            "id": "56001",
            "properties": {"dealname": "Sample deal", "amount": "100"},
            "archived": False,
        }
    ]
}

# List envelope from the Stripe API reference. Objects are the published
# samples, trimmed to the id and a few scalar fields.
# https://docs.stripe.com/api/customers/list
# https://docs.stripe.com/api/subscriptions/list
# https://docs.stripe.com/api/products/list
# https://docs.stripe.com/api/charges/list
# https://docs.stripe.com/api/pagination
_STRIPE_CUSTOMERS: dict[str, Any] = {
    "object": "list",
    "url": "/v1/customers",
    "has_more": False,
    "data": [
        {
            "id": "cus_NffrFeUfNV2Hib",
            "object": "customer",
            "email": "jennyrosen@example.com",
            "name": "Jenny Rosen",
        }
    ],
}
_STRIPE_SUBSCRIPTIONS: dict[str, Any] = {
    "object": "list",
    "url": "/v1/subscriptions",
    "has_more": False,
    "data": [
        {
            "id": "sub_1MowQVLkdIwHu7ixeRlqHVzs",
            "object": "subscription",
            "status": "active",
        }
    ],
}
_STRIPE_PRODUCTS: dict[str, Any] = {
    "object": "list",
    "url": "/v1/products",
    "has_more": False,
    "data": [
        {
            "id": "prod_NWjs8kKbJWmuuc",
            "object": "product",
            "name": "Gold Plan",
            "active": True,
        }
    ],
}
_STRIPE_CHARGES: dict[str, Any] = {
    "object": "list",
    "url": "/v1/charges",
    "has_more": False,
    "data": [
        {
            "id": "ch_3MmlLrLkdIwHu7ix0snN0B15",
            "object": "charge",
            "amount": 1099,
            "currency": "usd",
        }
    ],
}

# Pipedrive API v2 OpenAPI examples (server https://api.pipedrive.com/api/v2).
# https://developers.pipedrive.com/docs/api/v1/openapi-v2.yaml
# Persons / organizations / deals list responses are `{success, data: [{id}]}`.
_PIPEDRIVE_PERSONS: dict[str, Any] = {
    "success": True,
    "data": [
        {
            "id": 1,
            "name": "Person Name",
            "first_name": "Person",
            "last_name": "Name",
            "emails": [{"value": "email1@email.com", "primary": True, "label": "work"}],
        }
    ],
}
_PIPEDRIVE_ORGANIZATIONS: dict[str, Any] = {
    "success": True,
    "data": [
        {
            "id": 1,
            "name": "Organization Name",
            "owner_id": 1,
        }
    ],
}
_PIPEDRIVE_DEALS: dict[str, Any] = {
    "success": True,
    "data": [
        {
            "id": 1,
            "title": "Deal Title",
            "value": 200,
            "currency": "USD",
        }
    ],
}


def _res(path: str, body: dict[str, Any], ids: tuple[str, ...], marker: str) -> dict[str, Any]:
    return {"path": path, "body": body, "ids": ids, "marker": marker}


DOCUMENTED: dict[str, dict[str, Any]] = {
    "hubspot": {
        "base_url": "https://api.hubapi.com",
        "auth_type": "bearer",
        "api_key_header": None,
        "resources": (
            _res(
                "crm/v3/objects/contacts",
                _HUBSPOT_CONTACTS,
                ("33451",),
                "lorelai@thedragonfly.com",
            ),
            _res("crm/v3/objects/companies", _HUBSPOT_COMPANIES, ("87001",), "Sample Company"),
            _res("crm/v3/objects/deals", _HUBSPOT_DEALS, ("56001",), "Sample deal"),
        ),
    },
    "stripe": {
        "base_url": "https://api.stripe.com",
        "auth_type": "bearer",
        "api_key_header": None,
        "resources": (
            _res("v1/customers", _STRIPE_CUSTOMERS, ("cus_NffrFeUfNV2Hib",), "Jenny Rosen"),
            _res(
                "v1/subscriptions",
                _STRIPE_SUBSCRIPTIONS,
                ("sub_1MowQVLkdIwHu7ixeRlqHVzs",),
                "active",
            ),
            _res("v1/products", _STRIPE_PRODUCTS, ("prod_NWjs8kKbJWmuuc",), "Gold Plan"),
            _res(
                "v1/charges",
                _STRIPE_CHARGES,
                ("ch_3MmlLrLkdIwHu7ix0snN0B15",),
                "usd",
            ),
        ),
    },
    "pipedrive": {
        "base_url": "https://api.pipedrive.com/api/v2",
        "auth_type": "api_key",
        "api_key_header": "x-api-token",
        "resources": (
            _res("persons", _PIPEDRIVE_PERSONS, ("1",), "Person Name"),
            _res("organizations", _PIPEDRIVE_ORGANIZATIONS, ("1",), "Organization Name"),
            _res("deals", _PIPEDRIVE_DEALS, ("1",), "Deal Title"),
        ),
    },
}
