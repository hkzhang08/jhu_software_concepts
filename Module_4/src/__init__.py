"""Package entrypoint for the Flask app."""

from .website import app

__all__ = ["app"]

if __name__ == "__main__":
    app.run(debug=True)
