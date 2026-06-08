# Index Valuation Watch

A Python web app for index valuation lookup, charts, watchlists, focus metrics, and threshold status. It uses public data sources including funddb/韭圈儿 where available.

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/j62303937-del/index-valuation-watch)

## Local Run

```powershell
pip install -r requirements.txt
python server.py
```

Open `http://127.0.0.1:8787/`.

## Cloud Deploy On Render

Click the `Deploy to Render` button above, or:

1. Open Render and choose `New` -> `Blueprint`.
2. Select this GitHub repository.
3. Render will read `render.yaml`, install `requirements.txt`, and run `python server.py`.

After deployment, Render will provide a public HTTPS URL that can be opened from any device and network.

## Notes

Watchlist cache is stored in `watchlist-store.json` on the server filesystem. On free/ephemeral cloud instances this file may reset when the service restarts.
