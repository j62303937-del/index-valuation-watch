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

## Persistent Cloud Backup

Render's default filesystem can be reset after redeploys. To keep watchlists, focus metrics, valuation lines, alert rules, and cached snapshots across redeploys, configure a GitHub backup:

```text
GITHUB_BACKUP_TOKEN=your_fine_grained_token_with_contents_read_write
GITHUB_BACKUP_REPO=j62303937-del/index-valuation-watch
GITHUB_BACKUP_PATH=cloud-data/index-watch-backup.json
GITHUB_BACKUP_BRANCH=main
```

When the local SQLite database is empty after a redeploy, the app restores from this backup automatically. Every watchlist or settings save updates the backup.

## Email Alerts

Alert rules support these trigger directions: greater than or less than opportunity, median, or danger values.

Recommended free email path on Render: use Resend's HTTPS API. It avoids SMTP ports that some cloud hosts block.

```text
RESEND_API_KEY=your_resend_api_key
RESEND_FROM=Index Watch <onboarding@resend.dev>
ALERT_EMAIL_TO=694301103@qq.com
```

Resend's default `onboarding@resend.dev` sender is useful for first tests. For production, add and verify your own sending domain in Resend.

Set these Render environment variables to enable email delivery:

```text
ALERT_EMAIL_HOST=smtp.qq.com
ALERT_EMAIL_PORT=465
ALERT_EMAIL_USER=your_sender@qq.com
ALERT_EMAIL_PASSWORD=your_smtp_authorization_code
ALERT_EMAIL_FROM=your_sender@qq.com
ALERT_EMAIL_TO=694301103@qq.com
ALERT_EMAIL_TLS=true
```

For QQ Mail, `ALERT_EMAIL_PASSWORD` should be the SMTP authorization code, not the login password.

If the cloud provider blocks SMTP, configure PushPlus as an HTTPS fallback:

```text
PUSHPLUS_TOKEN=your_pushplus_token
```

When SMTP fails and `PUSHPLUS_TOKEN` is present, the app sends the alert through PushPlus instead.
