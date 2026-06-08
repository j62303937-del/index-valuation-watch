# Index Valuation Watch

A Python web app for index valuation lookup, charts, watchlists, focus metrics, and threshold status. It uses public data sources including funddb/韭圈儿 where available.

## Local Run

```powershell
pip install -r requirements.txt
python server.py
```

Open `http://127.0.0.1:8787/`.

## Cloud Deploy On Render

1. Push this repository to GitHub.
2. Open Render and choose `New` -> `Blueprint`.
3. Select this GitHub repository.
4. Render will read `render.yaml`, install `requirements.txt`, and run `python server.py`.

After deployment, Render will provide a public HTTPS URL that can be opened from any device and network.

## Notes

Watchlist cache is stored in `watchlist-store.json` on the server filesystem. On free/ephemeral cloud instances this file may reset when the service restarts.
