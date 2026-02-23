# Mini Standardizer API — Flask (Replit-friendly)

Tiny Flask API that standardizes degree program + university names and appends
two new fields to each row:
- `llm-generated-program`
- `llm-generated-university`

By default this runs in deterministic fallback mode (no local model runtime
required). If optional local LLM dependencies are installed separately, the
same API can still use them.

## Quickstart (Replit)

1. Create a new **Python** Repl.
2. Upload these files (or import the zip).
3. Install deps (recommended in a dedicated venv):
   ```bash
   python -m pip install -r requirements.txt
   ```
4. Run the API server:
   ```bash
   python app.py --serve
   ```

5. Test locally (replace the URL with your Replit web URL when deployed):
   ```bash
   curl -s -X POST http://localhost:8000/standardize      -H "Content-Type: application/json"      -d @sample_data.json | jq .
   ```

## CLI mode (no server)

```bash
python app.py --file cleaned_applicant_data.json --stdout > full_out.jsonl
```

## Config (env vars)

- `MODEL_REPO`, `MODEL_FILE`, `N_THREADS`, `N_CTX`, `N_GPU_LAYERS`
  are only used when optional local LLM dependencies are installed.

## Notes
- Rules-first fallback keeps output deterministic when no local model runtime
  is installed.
- Extend the few-shots and the fallback patterns in `app.py` for higher accuracy on your dataset.
