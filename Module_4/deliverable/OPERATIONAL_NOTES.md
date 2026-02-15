# Operational Notes

This page documents runtime behavior and testing expectations for the Module_4 Flask app.

## Busy-State Policy

- The app maintains a shared `PULL_STATE` with `status` values like `idle`, `running`, `done`, and `error`.
- `POST /pull-data`:
  - If `PULL_STATE["status"] == "running"`, returns `409` with `{"busy": true}`.
  - Otherwise, runs the pull pipeline and returns `202` with `{"ok": true}`.
- `POST /update-analysis`:
  - If `PULL_STATE["status"] == "running"`, returns `409` with `{"busy": true}`.
  - Otherwise, returns `200` with `{"ok": true}`.
- Tests inject fake pipeline/query functions via `create_app(...)` to avoid live scrapes.

## Idempotency Strategy

- Data loads are idempotent by URL.
- Before inserting, the loader fetches existing URLs and keeps a `seen_urls` set.
- Any row with a URL already seen is skipped.

## Uniqueness Keys

- **Primary uniqueness key:** `url`
- If `url` is missing, the row is inserted as-is (no dedupe).

## Troubleshooting

- **Missing DATABASE_URL**
  - Error: `KeyError: 'DATABASE_URL'`
  - Fix: set `DATABASE_URL` in your environment, e.g.
    - `export DATABASE_URL="postgresql://localhost/grad_cafe"`

- **Postgres not running**
  - Error: connection refused or timeout.
  - Fix: start Postgres locally or use the GitHub Actions service.

- **pytest only runs a subset**
  - Ensure the marker expression includes the full policy set:
    - `pytest -m "web or buttons or analysis or db or integration"`

- **Coverage fails in CI**
  - Check that `DATABASE_URL` is set and tests import the modules successfully.
  - Re-run locally with:
    - `pytest -m "web or buttons or analysis or db or integration" --cov=src --cov-report=term-missing`

