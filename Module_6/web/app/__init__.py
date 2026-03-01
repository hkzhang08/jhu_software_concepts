import os
from datetime import datetime, timezone

from flask import Flask, jsonify, request

from publisher import publish_message


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index():
        return jsonify(
            {
                "service": "web",
                "status": "ok",
                "message": "Use POST /publish to enqueue applicant events.",
            }
        )

    @app.get("/health")
    def health():
        return jsonify({"status": "healthy"}), 200

    @app.post("/publish")
    def publish():
        payload = request.get_json(silent=True) or {}
        if not payload:
            return jsonify({"error": "JSON payload is required."}), 400

        payload.setdefault("event_type", "applicant.created")
        payload.setdefault("received_at", datetime.now(timezone.utc).isoformat())
        payload.setdefault("source", "web-api")
        payload.setdefault("environment", os.getenv("FLASK_ENV", "development"))

        publish_message(payload)
        return jsonify({"status": "queued", "payload": payload}), 202

    return app
