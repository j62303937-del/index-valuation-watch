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

Watchlists, cached valuation snapshots, thresholds, axis ranges, and alert rules are stored in SQLite. By default the database file is `index-watch.db` beside `server.py`.

Set `INDEX_WATCH_DB` to use another database path, for example a mounted persistent disk path on Render.

The app schedules a Beijing-time 20:00 daily refresh for saved indexes. If the cloud service is asleep or restarted and misses 20:00, the next startup or page visit will detect the missed refresh and run a background catch-up update.

Render free instances may sleep and may use ephemeral storage. For durable long-term settings, attach a persistent disk or migrate the database to managed Postgres.
