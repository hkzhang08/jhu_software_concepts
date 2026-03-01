.. Grad Cafe Analytics Application documentation master file, created by
   sphinx-quickstart on Sun Feb 15 15:25:04 2026.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Grad Cafe Analytics Application
===============================

This project is a Flask-based web application that allows users to
view and analyze graduate admissions data.
The application includes page routing, analysis views, and
data processing modules.

Key features
------------

- Flask routes for analysis pages and data refresh actions.
- ETL pipeline: scrape → clean → load → analyze.
- PostgreSQL-backed metrics for the analysis view.
- Fully marked pytest suite with 100% coverage.

Documentation
-------------

This site is built with Sphinx and reStructuredText.
See the
`reStructuredText <https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html>`_
documentation for syntax details.
For Sphinx hosting and publishing guidance, see
`Read the Docs <https://docs.readthedocs.io/en/stable/>`_.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   about.rst
   prerequisites.rst
   api_scrape.rst
   api_clean.rst
   api_load_data.rst
   api_query_table.rst
   api_website.rst
   api_llm_hosting.rst
