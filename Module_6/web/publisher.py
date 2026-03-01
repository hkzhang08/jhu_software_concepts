import json
import os
from datetime import datetime

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "app_user")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "app_password")
RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/%2F",
)
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "applicant.events")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "applicant.ingest")
RABBITMQ_ROUTING_KEY = os.getenv("RABBITMQ_ROUTING_KEY", "applicant.created")
RABBITMQ_TASK_EXCHANGE = os.getenv("RABBITMQ_TASK_EXCHANGE", "tasks")
RABBITMQ_TASK_QUEUE = os.getenv("RABBITMQ_TASK_QUEUE", "tasks_q")
RABBITMQ_TASK_ROUTING_KEY = os.getenv("RABBITMQ_TASK_ROUTING_KEY", "tasks")


class PublishError(RuntimeError):
    """Raised when a RabbitMQ publish attempt fails."""


def publish_message(
    payload: dict,
    *,
    exchange: str = RABBITMQ_EXCHANGE,
    queue: str = RABBITMQ_QUEUE,
    routing_key: str = RABBITMQ_ROUTING_KEY,
) -> None:
    connection = None
    try:
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()

        channel.exchange_declare(
            exchange=exchange,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(
            exchange=exchange,
            queue=queue,
            routing_key=routing_key,
        )

        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
        )
    except Exception as exc:  # pragma: no cover - depends on RabbitMQ availability
        raise PublishError("Failed to publish RabbitMQ message.") from exc
    finally:
        if connection is not None:
            connection.close()


def _open_channel():
    """Open a RabbitMQ channel configured for queued UI task requests."""
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()
    channel.confirm_delivery()
    channel.exchange_declare(
        exchange=RABBITMQ_TASK_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    channel.queue_declare(queue=RABBITMQ_TASK_QUEUE, durable=True)
    channel.queue_bind(
        exchange=RABBITMQ_TASK_EXCHANGE,
        queue=RABBITMQ_TASK_QUEUE,
        routing_key=RABBITMQ_TASK_ROUTING_KEY,
    )
    return connection, channel


def publish_task(
    kind: str,
    payload: dict | None = None,
    headers: dict | None = None,
) -> None:
    """Publish a UI task request to the dedicated task queue."""
    body = json.dumps(
        {
            "kind": kind,
            "ts": datetime.utcnow().isoformat(),
            "payload": payload or {},
        },
        separators=(",", ":"),
    ).encode("utf-8")

    connection = None
    try:
        connection, channel = _open_channel()
        channel.basic_publish(
            exchange=RABBITMQ_TASK_EXCHANGE,
            routing_key=RABBITMQ_TASK_ROUTING_KEY,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                headers=headers or {},
            ),
            mandatory=False,
        )
    except Exception as exc:  # pragma: no cover - depends on RabbitMQ availability
        raise PublishError("Failed to publish RabbitMQ task.") from exc
    finally:
        if connection is not None:
            connection.close()
