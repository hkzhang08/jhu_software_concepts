Prerequisites
=============

This page describes the environment and dependencies needed to run the
application and tests.

Installation instructions
-------------------------

Python version
--------------

- Python 3.13 is recommended (matches CI).

1. Create and activate a virtual environment::

      python3 -m venv .venv
      . .venv/bin/activate

2. Install Python dependencies::

      python3 -m pip install -r requirements.txt

   This installs the Flask app, database drivers, and test tooling.

3. Set required environment variables::

      export DATABASE_URL="postgresql://localhost/grad_cafe"

4. Run the test suite (optional)::

      pytest -m "web or buttons or analysis or db or integration"

Running the app
---------------

From the project root::

      export DATABASE_URL="postgresql://localhost/grad_cafe"
      flask --app src run

Alternatively::

      python -m src
