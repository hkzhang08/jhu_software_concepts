from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import psycopg

try:
    import pika
except ModuleNotFoundError:  # pragma: no cover - test env fallback
    def _missing_pika(*_args, **_kwargs):
        raise ModuleNotFoundError("pika is required to run the worker consumer")

    class _MissingPikaModule:
        URLParameters = staticmethod(_missing_pika)
        BlockingConnection = staticmethod(_missing_pika)

    pika = _MissingPikaModule()

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import db_builders

try:
    from .etl.query_data import fetch_last_seen, get_db_dsn, upsert_last_seen
except ImportError:  # pragma: no cover - script execution path
    from etl.query_data import fetch_last_seen, get_db_dsn, upsert_last_seen


LOGGER = logging.getLogger(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "app_user")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "app_password")
RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/%2F",
)
TASK_EXCHANGE = "tasks"
TASK_QUEUE = "tasks_q"
TASK_ROUTING_KEY = "tasks"
RECONNECT_DELAY_SECONDS = float(os.getenv("RABBITMQ_RECONNECT_DELAY", "5"))

SCRAPE_TASK_NAME = "scrape_new_data"
RECOMPUTE_TASK_NAME = "recompute_analytics"
ANALYTICS_VIEW_NAME = "applicant_analytics_summary"
CREATE_ANALYTICS_VIEW_SQL = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS {ANALYTICS_VIEW_NAME} AS
SELECT
    COUNT(*)::BIGINT AS total_rows,
    COUNT(*) FILTER (WHERE term ILIKE 'Fall 2026')::BIGINT AS fall_2026_count,
    ROUND(
        (
            100.0 * AVG(
                CASE
                    WHEN us_or_international IS NULL THEN 0
                    WHEN us_or_international ILIKE 'American' THEN 0
                    WHEN us_or_international ILIKE 'Other' THEN 0
                    ELSE 1
                END
            )
        )::numeric,
        2
    ) AS international_pct,
    ROUND(
        (
            100.0 * AVG(
                CASE
                    WHEN term ILIKE 'Fall 2026' AND status ILIKE 'Accepted%' THEN 1
                    ELSE 0
                END
            )
        )::numeric,
        2
    ) AS acceptance_pct_fall_2026,
    now() AS refreshed_at
FROM applicants
WITH NO DATA
"""
REFRESH_ANALYTICS_VIEW_SQL = f"REFRESH MATERIALIZED VIEW {ANALYTICS_VIEW_NAME}"
DEFAULT_DATA_FILE = ROOT_DIR / "src" / "llm_new_applicant.json"
DATA_FILE = Path(os.getenv("DATA_FILE", str(DEFAULT_DATA_FILE)))
QUERY_LIMIT = db_builders.clamp_limit(
    os.getenv("QUERY_LIMIT", db_builders.DEFAULT_QUERY_LIMIT)
)
URL_RESULT_ID_RE = re.compile(r"/result/(\d+)")
LEGACY_INSERT_SQL = db_builders.applicants_sql(
    """
    INSERT INTO {table} (
        program,
        comments,
        date_added,
        url,
        status,
        term,
        us_or_international,
        gpa,
        gre,
        gre_v,
        gre_aw,
        degree,
        llm_generated_program,
        llm_generated_university
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
)


def _last_seen_sort_key(value: str):
    """Provide stable numeric-aware sorting for watermark values."""
    raw_value = str(value)
    if raw_value.isdigit():
        return (0, int(raw_value))
    return (1, raw_value)


def _record_last_seen(record: dict) -> str | None:
    """Extract the scraper sort key used for incremental watermarking."""
    for key in ("last_seen", "last_processed_at"):
        if record.get(key) is not None:
            return str(record[key])

    url = db_builders.ftext(record.get("url"))
    if url:
        match = URL_RESULT_ID_RE.search(url)
        if match:
            return match.group(1)

    date_added = db_builders.fdate(record.get("date_added"))
    if date_added is not None:
        return date_added.isoformat()

    return None


def _is_newer(record_last_seen: str | None, since: str | None) -> bool:
    """Return True when a record should be processed for the current watermark."""
    if since is None:
        return True
    if record_last_seen is None:
        return True
    return _last_seen_sort_key(record_last_seen) > _last_seen_sort_key(since)


def _resolve_scrape_since(conn, payload: dict | None = None) -> str | None:
    """Return the watermark for the next incremental scrape request."""
    payload = payload or {}
    if payload.get("since") is not None:
        return str(payload["since"])
    return fetch_last_seen(SCRAPE_TASK_NAME, conn=conn)


def update_last_seen_from_batch(conn, batch: list[dict]) -> str | None:
    """Persist the highest last_seen value observed in a successful batch."""
    last_seen_values = [
        str(record["last_seen"])
        for record in batch
        if record.get("last_seen") is not None
    ]
    if not last_seen_values:
        return None

    newest_last_seen = max(last_seen_values, key=_last_seen_sort_key)
    upsert_last_seen(SCRAPE_TASK_NAME, newest_last_seen, conn=conn)
    return newest_last_seen


def _open_channel():
    """Connect to RabbitMQ and return a durable task channel."""
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(
        exchange=TASK_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    channel.queue_declare(queue=TASK_QUEUE, durable=True)
    channel.queue_bind(
        exchange=TASK_EXCHANGE,
        queue=TASK_QUEUE,
        routing_key=TASK_ROUTING_KEY,
    )
    channel.basic_qos(prefetch_count=1)
    return connection, channel


def _open_db_connection():
    """Open one Postgres connection for a single consumed message."""
    return psycopg.connect(get_db_dsn())


def _load_records_from_file(data_file: Path) -> list[dict]:
    """Load either a JSON array file or a JSONL file of scraper rows."""
    if not data_file.exists():
        raise FileNotFoundError(f"Data file not found: {data_file}")

    with data_file.open("r", encoding="utf-8") as handle:
        while True:
            first_char = handle.read(1)
            if first_char == "":
                return []
            if not first_char.isspace():
                break

        handle.seek(0)
        if first_char == "[":
            records = json.load(handle)
            if not isinstance(records, list):
                raise ValueError("scrape_new_data file must contain a JSON array")
            return records

        records = []
        for line in handle:
            if not line.strip():
                continue
            records.append(json.loads(line))
        return records


def _load_scraper_output(payload: dict | None = None) -> list[dict]:
    """Load records produced by the existing scraper pipeline."""
    payload = payload or {}
    records = payload.get("records")
    if records is None:
        data_file = Path(payload.get("data_file") or DATA_FILE)
        records = _load_records_from_file(data_file)

    if not isinstance(records, list):
        raise ValueError("scrape_new_data records must be provided as a list")

    return records


def _fetch_existing_urls(conn) -> set[str]:
    """Read the currently stored applicant URLs for insert deduplication."""
    with conn.cursor() as cur:
        return db_builders.fetch_existing_urls(cur, QUERY_LIMIT)


def _insert_legacy_rows(conn, rows: list[tuple]) -> int:
    """Insert prepared legacy applicant tuples in the current transaction."""
    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.executemany(LEGACY_INSERT_SQL, rows)
    return len(rows)


def handle_scrape_new_data(conn, payload: dict | None = None) -> int:
    """Load newer scraped rows into the legacy applicants table."""
    since = _resolve_scrape_since(conn, payload)
    scraped_records = _load_scraper_output(payload)
    seen_urls = set(_fetch_existing_urls(conn))
    insert_rows = []
    watermark_batch = []

    for record in scraped_records:
        if not isinstance(record, dict):
            raise ValueError("Each scrape_new_data record must be an object")

        record_last_seen = _record_last_seen(record)
        if not _is_newer(record_last_seen, since):
            continue

        url = db_builders.ftext(record.get("url"))
        if url and url in seen_urls:
            if record_last_seen is not None:
                watermark_batch.append({"last_seen": record_last_seen})
            continue
        if url:
            seen_urls.add(url)

        insert_rows.append(
            db_builders.build_applicant_insert_row(
                record,
                url=url,
                include_llm=True,
            )
        )
        if record_last_seen is not None:
            watermark_batch.append({"last_seen": record_last_seen})

    inserted_count = _insert_legacy_rows(conn, insert_rows)

    # Keep the watermark update as the final DB write in the transaction so
    # it advances only when the insert phase completes successfully.
    newest_last_seen = update_last_seen_from_batch(conn, watermark_batch)
    LOGGER.info(
        "Committed %s applicant rows for %s (previous since=%s, new since=%s).",
        inserted_count,
        SCRAPE_TASK_NAME,
        since,
        newest_last_seen,
    )
    return inserted_count


def handle_recompute_analytics(conn, payload: dict | None = None) -> None:
    """Refresh a legacy-safe analytics materialized view."""
    _ = payload or {}
    with conn.cursor() as cur:
        cur.execute(CREATE_ANALYTICS_VIEW_SQL)
        cur.execute(REFRESH_ANALYTICS_VIEW_SQL)

    LOGGER.info(
        "Refreshed materialized view %s for %s.",
        ANALYTICS_VIEW_NAME,
        RECOMPUTE_TASK_NAME,
    )


def _task_handlers():
    """Return the current task routing map."""
    return {
        SCRAPE_TASK_NAME: handle_scrape_new_data,
        RECOMPUTE_TASK_NAME: handle_recompute_analytics,
    }


def process_task(conn, message: dict) -> int | None:
    """Dispatch one decoded task payload to the matching handler."""
    if not isinstance(message, dict):
        raise ValueError("Task message must decode to an object")

    kind = message.get("kind")
    if not kind:
        raise ValueError("Task message is missing kind")

    payload = message.get("payload") or {}
    if not isinstance(payload, dict):
        raise ValueError("Task payload must be an object")

    handler = _task_handlers().get(kind)
    if handler is None:
        raise ValueError(f"Unsupported task kind: {kind}")
    with conn.transaction():
        return handler(conn, payload)


def on_message(channel, method, _properties, body) -> None:
    """Consume one task message and acknowledge it only after a successful commit."""
    db_connection = None
    try:
        message = json.loads(body.decode("utf-8"))
        db_connection = _open_db_connection()
        process_task(db_connection, message)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        LOGGER.exception("Discarding malformed task message from %s.", TASK_QUEUE)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception:
        LOGGER.exception("Task processing failed; discarding %s.", TASK_QUEUE)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        channel.basic_ack(delivery_tag=method.delivery_tag)
    finally:
        if db_connection is not None and not db_connection.closed:
            db_connection.close()


def consume_forever() -> None:
    """Run the RabbitMQ worker loop until interrupted."""
    while True:
        connection = None
        try:
            connection, channel = _open_channel()
            LOGGER.info(
                "Worker connected to RabbitMQ and consuming from %s.",
                TASK_QUEUE,
            )
            channel.basic_consume(
                queue=TASK_QUEUE,
                on_message_callback=on_message,
            )
            channel.start_consuming()
        except KeyboardInterrupt:
            LOGGER.info("Worker shutdown requested; stopping consume loop.")
            return
        except Exception:
            LOGGER.exception(
                "RabbitMQ connection lost; reconnecting in %s seconds.",
                RECONNECT_DELAY_SECONDS,
            )
            time.sleep(RECONNECT_DELAY_SECONDS)
        finally:
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except Exception:  # pragma: no cover - best effort cleanup
                    LOGGER.exception("Failed to close RabbitMQ connection cleanly.")


def main() -> None:
    """Configure logging and start the long-running worker."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    consume_forever()


if __name__ == "__main__":
    main()
