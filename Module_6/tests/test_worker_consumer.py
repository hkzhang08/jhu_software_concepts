"""Tests for worker task helper behavior."""

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from worker import consumer


def test_resolve_scrape_since_prefers_payload_since(monkeypatch):
    """Explicit payload since should bypass the stored watermark."""

    def should_not_run(_source, conn=None):
        raise AssertionError("fetch_last_seen should not run when since is provided")

    monkeypatch.setattr(consumer, "fetch_last_seen", should_not_run)

    assert consumer._resolve_scrape_since(object(), {"since": "2026-02-28T00:00:00"}) == (
        "2026-02-28T00:00:00"
    )


def test_resolve_scrape_since_uses_watermark_when_since_missing(monkeypatch):
    """Missing payload since should fall back to the stored watermark."""
    called = {}

    sentinel_conn = object()

    def fake_fetch_last_seen(source, conn=None):
        called["source"] = source
        called["conn"] = conn
        return "watermark-value"

    monkeypatch.setattr(consumer, "fetch_last_seen", fake_fetch_last_seen)

    assert consumer._resolve_scrape_since(sentinel_conn, {}) == "watermark-value"
    assert called == {
        "source": consumer.SCRAPE_TASK_NAME,
        "conn": sentinel_conn,
    }


def test_update_last_seen_from_batch_persists_max_value(monkeypatch):
    """Successful batches should store the greatest last_seen value."""
    recorded = {}
    sentinel_conn = object()

    def fake_upsert_last_seen(source, last_seen, conn=None):
        recorded["source"] = source
        recorded["last_seen"] = last_seen
        recorded["conn"] = conn

    monkeypatch.setattr(consumer, "upsert_last_seen", fake_upsert_last_seen)

    batch = [
        {"last_seen": "2026-02-28T00:00:00"},
        {"last_seen": "2026-03-01T00:00:00"},
        {"last_seen": "2026-02-27T00:00:00"},
    ]

    assert (
        consumer.update_last_seen_from_batch(sentinel_conn, batch)
        == "2026-03-01T00:00:00"
    )
    assert recorded == {
        "source": consumer.SCRAPE_TASK_NAME,
        "last_seen": "2026-03-01T00:00:00",
        "conn": sentinel_conn,
    }


def test_update_last_seen_from_batch_skips_empty_batches(monkeypatch):
    """Batches without last_seen values should not write a watermark."""

    def should_not_run(_source, _last_seen, conn=None):
        raise AssertionError("upsert_last_seen should not run for empty batches")

    monkeypatch.setattr(consumer, "upsert_last_seen", should_not_run)

    assert consumer.update_last_seen_from_batch(object(), [{}, {"last_seen": None}]) is None


def test_handle_scrape_new_data_batches_only_newer_normalized_records(monkeypatch):
    """The scrape handler should normalize, filter, batch insert, and then advance the watermark."""
    seen = {}
    sentinel_conn = object()

    def fake_fetch_existing_urls(conn):
        seen["existing_conn"] = conn
        return {"https://www.thegradcafe.com/result/1000"}

    def fake_insert_legacy_rows(conn, rows):
        seen["rows"] = rows
        seen["insert_conn"] = conn
        return len(rows)

    def fake_update_last_seen(conn, batch):
        seen["watermark_conn"] = conn
        seen["watermark_batch"] = batch
        return "1002"

    monkeypatch.setattr(consumer, "_fetch_existing_urls", fake_fetch_existing_urls)
    monkeypatch.setattr(consumer, "_insert_legacy_rows", fake_insert_legacy_rows)
    monkeypatch.setattr(consumer, "update_last_seen_from_batch", fake_update_last_seen)

    older_record = {
        "program": "Computer Science, Example U",
        "comments": "1a/0w/0r",
        "date_added": "February 20, 2026",
        "url": "https://www.thegradcafe.com/result/1001",
        "applicant_status": "Accepted",
        "semester_year_start": "Fall 2026",
        "citizenship": "American",
        "gpa": "GPA 3.80",
        "gre": "330",
        "gre_v": "165",
        "gre_aw": "4.5",
        "masters_or_phd": "Masters",
    }
    newer_record = {
        "program": "Data Science, Example U",
        "comments": "1a/0w/0r",
        "date_added": "February 21, 2026",
        "url": "https://www.thegradcafe.com/result/1002",
        "applicant_status": "Accepted",
        "semester_year_start": "Fall 2026",
        "citizenship": "International",
        "gpa": "GPA 3.90",
        "gre": "331",
        "gre_v": "166",
        "gre_aw": "5.0",
        "masters_or_phd": "Masters",
        "llm-generated-program": "Data Science",
        "llm-generated-university": "Example University",
    }

    result = consumer.handle_scrape_new_data(
        sentinel_conn,
        {
            "since": "1001",
            "records": [
                older_record,
                newer_record,
            ],
        },
    )

    assert result == 1
    assert seen["existing_conn"] is sentinel_conn
    assert seen["insert_conn"] is sentinel_conn
    assert seen["watermark_conn"] is sentinel_conn
    assert seen["rows"] == [
        consumer.db_builders.build_applicant_insert_row(
            newer_record,
            url="https://www.thegradcafe.com/result/1002",
            include_llm=True,
        )
    ]
    assert seen["watermark_batch"] == [
        {"last_seen": "1002"}
    ]


def test_handle_recompute_analytics_refreshes_materialized_view():
    """The recompute handler should create and refresh the analytics view on the same connection."""

    class FakeCursor:
        def __init__(self, statements):
            self.statements = statements

        def __enter__(self):
            return self

        def __exit__(self, exc_type, _exc, _tb):
            return False

        def execute(self, statement):
            self.statements.append(statement)

    class FakeConnection:
        def __init__(self):
            self.statements = []

        def cursor(self):
            return FakeCursor(self.statements)

    fake_conn = FakeConnection()

    consumer.handle_recompute_analytics(fake_conn, {})

    assert fake_conn.statements == [
        consumer.CREATE_ANALYTICS_VIEW_SQL,
        consumer.REFRESH_ANALYTICS_VIEW_SQL,
    ]


def test_open_channel_declares_durable_task_queue_and_prefetch(monkeypatch):
    """Worker channels should be durable and consume one message at a time."""

    class FakeChannel:
        def __init__(self):
            self.calls = []

        def exchange_declare(self, **kwargs):
            self.calls.append(("exchange_declare", kwargs))

        def queue_declare(self, **kwargs):
            self.calls.append(("queue_declare", kwargs))

        def queue_bind(self, **kwargs):
            self.calls.append(("queue_bind", kwargs))

        def basic_qos(self, **kwargs):
            self.calls.append(("basic_qos", kwargs))

    class FakeConnection:
        def __init__(self):
            self.channel_instance = FakeChannel()
            self.is_open = True

        def channel(self):
            return self.channel_instance

    captured = {}

    def fake_url_parameters(url):
        captured["url"] = url
        return f"params:{url}"

    def fake_blocking_connection(params):
        captured["params"] = params
        return FakeConnection()

    monkeypatch.setattr(consumer.pika, "URLParameters", fake_url_parameters)
    monkeypatch.setattr(consumer.pika, "BlockingConnection", fake_blocking_connection)

    connection, channel = consumer._open_channel()

    assert captured["url"] == consumer.RABBITMQ_URL
    assert captured["params"] == f"params:{consumer.RABBITMQ_URL}"
    assert connection.channel_instance is channel
    assert ("exchange_declare", {
        "exchange": consumer.TASK_EXCHANGE,
        "exchange_type": "direct",
        "durable": True,
    }) in channel.calls
    assert ("queue_declare", {
        "queue": consumer.TASK_QUEUE,
        "durable": True,
    }) in channel.calls
    assert ("queue_bind", {
        "exchange": consumer.TASK_EXCHANGE,
        "queue": consumer.TASK_QUEUE,
        "routing_key": consumer.TASK_ROUTING_KEY,
    }) in channel.calls
    assert ("basic_qos", {"prefetch_count": 1}) in channel.calls


def test_on_message_acks_after_successful_processing(monkeypatch):
    """Successful task execution should acknowledge the RabbitMQ delivery."""
    seen = {}

    class FakeChannel:
        def __init__(self):
            self.acked = []
            self.nacked = []

        def basic_ack(self, **kwargs):
            self.acked.append(kwargs)

        def basic_nack(self, **kwargs):
            self.nacked.append(kwargs)

    class FakeDbConnection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    fake_db_connection = FakeDbConnection()

    def fake_process_task(conn, message):
        seen["conn"] = conn
        seen["message"] = message

    monkeypatch.setattr(consumer, "_open_db_connection", lambda: fake_db_connection)
    monkeypatch.setattr(consumer, "process_task", fake_process_task)

    channel = FakeChannel()
    method = SimpleNamespace(delivery_tag="tag-1")
    consumer.on_message(
        channel,
        method,
        None,
        b'{"kind":"scrape_new_data","payload":{"since":"2026-02-28T00:00:00"}}',
    )

    assert seen["message"] == {
        "kind": "scrape_new_data",
        "payload": {"since": "2026-02-28T00:00:00"},
    }
    assert seen["conn"] is fake_db_connection
    assert channel.acked == [{"delivery_tag": "tag-1"}]
    assert channel.nacked == []
    assert fake_db_connection.closed is True


def test_on_message_discards_processing_failures_without_requeue(monkeypatch):
    """Unexpected handler failures should nack without requeueing the task."""

    class FakeChannel:
        def __init__(self):
            self.acked = []
            self.nacked = []

        def basic_ack(self, **kwargs):
            self.acked.append(kwargs)

        def basic_nack(self, **kwargs):
            self.nacked.append(kwargs)

    class FakeDbConnection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    fake_db_connection = FakeDbConnection()

    def boom(_conn, _message):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(consumer, "_open_db_connection", lambda: fake_db_connection)
    monkeypatch.setattr(consumer, "process_task", boom)

    channel = FakeChannel()
    method = SimpleNamespace(delivery_tag="tag-2")
    consumer.on_message(
        channel,
        method,
        None,
        b'{"kind":"scrape_new_data","payload":{}}',
    )

    assert channel.acked == []
    assert channel.nacked == [{"delivery_tag": "tag-2", "requeue": False}]
    assert fake_db_connection.closed is True


def test_on_message_discards_malformed_payloads(monkeypatch):
    """Malformed task messages should be nacked without requeue."""

    class FakeChannel:
        def __init__(self):
            self.acked = []
            self.nacked = []

        def basic_ack(self, **kwargs):
            self.acked.append(kwargs)

        def basic_nack(self, **kwargs):
            self.nacked.append(kwargs)

    def should_not_run(_message):
        raise AssertionError("process_task should not run for malformed JSON")

    monkeypatch.setattr(consumer, "process_task", should_not_run)

    channel = FakeChannel()
    method = SimpleNamespace(delivery_tag="tag-3")
    consumer.on_message(channel, method, None, b"{not-json")

    assert channel.acked == []
    assert channel.nacked == [{"delivery_tag": "tag-3", "requeue": False}]


def test_process_task_routes_scrape_requests_by_kind(monkeypatch):
    """scrape_new_data should dispatch through the task map to the scrape handler."""
    seen = {}

    class FakeTransaction:
        def __init__(self, events):
            self.events = events

        def __enter__(self):
            self.events.append("entered")
            return self

        def __exit__(self, exc_type, _exc, _tb):
            self.events.append(exc_type)
            return False

    class FakeConnection:
        def __init__(self):
            self.events = []

        def transaction(self):
            self.events.append("transaction")
            return FakeTransaction(self.events)

    def fake_scrape(conn, payload):
        seen["conn"] = conn
        seen["payload"] = payload
        return 2

    monkeypatch.setattr(consumer, "handle_scrape_new_data", fake_scrape)

    sentinel_conn = FakeConnection()
    result = consumer.process_task(
        sentinel_conn,
        {"kind": consumer.SCRAPE_TASK_NAME, "payload": {"since": "2026-02-28T00:00:00"}},
    )

    assert result == 2
    assert seen == {
        "conn": sentinel_conn,
        "payload": {"since": "2026-02-28T00:00:00"},
    }
    assert sentinel_conn.events == ["transaction", "entered", None]


def test_process_task_routes_recompute_requests_by_kind(monkeypatch):
    """recompute_analytics should dispatch through the task map to the recompute handler."""
    seen = {}

    class FakeTransaction:
        def __init__(self, events):
            self.events = events

        def __enter__(self):
            self.events.append("entered")
            return self

        def __exit__(self, exc_type, _exc, _tb):
            self.events.append(exc_type)
            return False

    class FakeConnection:
        def __init__(self):
            self.events = []

        def transaction(self):
            self.events.append("transaction")
            return FakeTransaction(self.events)

    def fake_recompute(conn, payload):
        seen["conn"] = conn
        seen["payload"] = payload

    monkeypatch.setattr(consumer, "handle_recompute_analytics", fake_recompute)

    sentinel_conn = FakeConnection()
    result = consumer.process_task(
        sentinel_conn,
        {"kind": consumer.RECOMPUTE_TASK_NAME, "payload": {"refresh": True}},
    )

    assert result is None
    assert seen == {
        "conn": sentinel_conn,
        "payload": {"refresh": True},
    }
    assert sentinel_conn.events == ["transaction", "entered", None]
