"""Package entrypoint for the Flask app.

Exposes an app factory and a ready-to-run Flask instance for CLI/WSGI use.
"""

from .website import create_app

# Create a default app instance so `flask --app src` works out-of-the-box.
app = create_app()

# Re-export public symbols for importers.
__all__ = ["app", "create_app"]

if __name__ == "__main__":
    # Local dev entrypoint: `python -m src` or `python src/__init__.py`.
    app.run(debug=True)
