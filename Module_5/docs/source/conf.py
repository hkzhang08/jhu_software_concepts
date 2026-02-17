# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

Project = 'Sphinx_Doc'
Copyright = '2026, Helen Zhang'
Author = 'Helen Zhang'
Release = '1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
]
autosummary_generate = True

# Mock heavy or external dependencies during docs build.
autodoc_mock_imports = ["llama_cpp", "huggingface_hub"]

# Ensure the project root is on sys.path for autodoc imports.
import os
import sys
sys.path.insert(0, os.path.abspath("../.."))

# Provide a default DATABASE_URL for autodoc imports.
os.environ.setdefault("DATABASE_URL", "postgresql://localhost/grad_cafe")

# Provide a dummy psycopg connection for autodoc imports to avoid real DB access.
try:
    import psycopg  # type: ignore

    class _DummyCursor:
        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return (0, 0, 0, 0)

        def fetchall(self):
            return []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _DummyConn:
        def cursor(self):
            return _DummyCursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    psycopg.connect = lambda *_args, **_kwargs: _DummyConn()
except Exception:
    pass

templates_path = ['../_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = []
