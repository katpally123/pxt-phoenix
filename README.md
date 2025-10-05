# PXT Phoenix (Pyodide Edition)

Zero-backend, zero-storage dashboard. Users upload CSVs, Python (via Pyodide) runs entirely in the browser, and the page renders the results. Refresh clears everything.

## What’s inside
- `index.html` – UI shell + Pyodide runtime (CDN)
- `app.js` – loads Pyodide, passes uploaded files into Python, renders outputs
- `engine_pyodide.py` – your pandas logic (runs in-browser, no disk I/O)
- `settings.json` – department mappings and markers
- `style.css` – minimal styles (replace with your own)

## How to publish on GitHub Pages
**Option A (fastest):** put these files in a `/docs` folder in your repo, then in GitHub → Settings → Pages → Source: `main` / `/docs`.

**Option B:** keep them in `/web` and use a Pages GitHub Action.

## How to use (end-user)
1. Open your Pages URL.
2. Click **Upload daily CSVs** and select your 3–4 files (they never leave the device).
3. Click **Build data**. The page renders Department Summary, VET/VTO, and Swaps.
4. Refresh clears everything (ephemeral).

## Where to put your real logic
Open `engine_pyodide.py` and replace the placeholder logic in `build_all(...)` with your existing pandas code from Colab. The function returns a single JSON-like `dict`:

```python
{
  "generated_at": "...",
  "dept_summary": {...},
  "presence_map": {...},
  "vet_vto": {...},
  "swaps": {...}
}
```

Your JS renderers already expect these shapes, so you can keep your existing UI.
