# Incremental extract

Saved extract runs can window the next read. That is `POST /graphs/{tenant}/extract-sources/{slug}/run` and the `extract` schedule action, which call the same saved-run path. The cursor is stored on the extract source (per resource) and read back on the next run, including after a process restart.

`POST /graphs/{tenant}/ingest/dlt` does not read or write these cursors. That route is a full pull.

## Incremental templates

Exactly these catalog ids window a run. A template's `incremental` field is null for every other id.

| id | what the next run does |
| --- | --- |
| `stripe` | Sends `created[gte]` (unix seconds) set to the stored cursor, the greatest `created` value already ingested, plus `limit` of at most 100. Rows with `created` less than or equal to the cursor are not ingested. The paginator stays `single_page`. The cursor is saved only when the page is shorter than the limit sent; a full page is requested again next run so objects that were not on the page are not skipped. The first run sends no `created[gte]`. |
| `postgres` | Incremental only when the saved source sets `cursor_column` (one SQL identifier). Later runs use dlt incremental with an exclusive lower bound (`range_start` open), ordered ascending on that column: rows greater than the stored per-table cursor. Infona does not keep a dlt pipeline, so the stored cursor is passed as `initial_value` on every run. No `cursor_column` means a full pull. |
| `mysql` | Same as `postgres`. |

`stripe` is selected when `connector_id` is `stripe`, or when `connector_id` is omitted and `base_url` host is `api.stripe.com`. A saved source with some other `connector_id` is not treated as Stripe. `postgres` and `mysql` window only when `connector_id` is that id and `cursor_column` is set. `cursor_column` on any other template is rejected.

## Full pulls

Every other connector id is a full pull of its resources on every run. That includes `custom_rest` and `custom_sql`. A Postgres or MySQL DSN saved as `custom_sql`, or with no `connector_id`, is a full pull.

SSRF checks and BYOK secret handling are unchanged. REST extracts still pin the `single_page` paginator.

## Cursor

Cursors live on the saved source, keyed by resource name. An incremental run stores the greatest cursor value among rows it ingested, and it does not move a cursor backward. A run that ingests no cursor value leaves the stored cursor in place. A full Stripe page also leaves the stored cursor in place, so the next run repeats that page instead of skipping objects that were not on it.

For `postgres` and `mysql` the query is ordered ascending on `cursor_column`, so a run that stops at `limit` continues from the greatest value in that page. For `stripe`, a full page does not store a cursor.

## Full refresh

`POST /graphs/{tenant}/extract-sources/{slug}/run` accepts `full_refresh: true`. That run does not send the stored cursor. After it succeeds, stored cursors for the source's resources are replaced by this run's high-water marks. A resource that produces no mark (no rows, or a full Stripe page) has its stored cursor removed.
