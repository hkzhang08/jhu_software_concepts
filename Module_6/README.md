# Module 6 Microservice Baseline

This baseline creates a reproducible containerized stack for future messaging, analytics recompute, and security work.

## Repository Layout

```
module_6/
  docker-compose.yml
  README.md
  docs/
  tests/
  web/
    Dockerfile
    requirements.txt
    run.py
    publisher.py
    app/
  worker/
    Dockerfile
    requirements.txt
    consumer.py
    etl/
      incremental_scraper.py
      query_data.py
  db/
    load_data.py
  data/
    applicant_data.json
```

## Services

- `db`: PostgreSQL 16 with persistent named volume `postgres_data`
- `rabbitmq`: RabbitMQ with management UI
- `web`: Flask API on port `8080`; publishes durable messages to RabbitMQ
- `worker`: RabbitMQ consumer with manual acknowledgments and `prefetch=1`

## Ports

- `8080`: Web API
- `5432`: PostgreSQL
- `5672`: RabbitMQ AMQP
- `15672`: RabbitMQ Management UI

## Quick Start

1. Build and start all services:

```bash
docker compose up -d --build
```

2. Check health:

```bash
docker compose ps
```

3. Load applicant seed data into Postgres:

```bash
docker compose exec web python /opt/project/db/load_data.py
```

4. Publish a test event from the web API:

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "applicant_id": "A2001",
    "name": "Test User",
    "email": "test.user@example.com",
    "program": "MS Computer Science",
    "university": "Johns Hopkins University",
    "status": "queued"
  }'
```

5. Watch worker processing logs:

```bash
docker compose logs -f worker
```

6. Stop services:

```bash
docker compose down
```

To remove volumes too:

```bash
docker compose down -v
```

## RabbitMQ Durable Messaging Setup

- Exchange: `applicant.events` (durable direct exchange)
- Queue: `applicant.ingest` (durable queue)
- Routing key: `applicant.created`
- Published messages use persistent delivery mode (`delivery_mode=2`)

## Registry Links (Base Images)

- Postgres: [https://hub.docker.com/_/postgres](https://hub.docker.com/_/postgres)
- RabbitMQ: [https://hub.docker.com/_/rabbitmq](https://hub.docker.com/_/rabbitmq)
- Python: [https://hub.docker.com/_/python](https://hub.docker.com/_/python)

## Notes

- `web` binds to `0.0.0.0:8080` via `web/run.py`.
- `worker/consumer.py` uses manual `ack`/`nack` and `basic_qos(prefetch_count=1)`.
- `db/load_data.py` loads `data/applicant_data.json` into SQL via upsert.
