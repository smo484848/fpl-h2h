# FPL Team Comparison

Head-to-head Fantasy Premier League analysis. Search by manager name, team name, entry ID, or FPL URL.

## Local development

```
npm install
npm start
```

Opens at http://localhost:3001. The search database (`search.db`) is created automatically on first run. The background crawler starts indexing the FPL Overall League immediately.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3001` | Server port |
| `DB_PATH` | `./search.db` | SQLite database path |
| `CRAWLER_ENABLED` | `true` | Background Overall League crawler (`false` to disable) |
| `CRAWLER_MAX_PAGES` | `2000` | Max crawler pages per server session |
| `CRAWLER_DELAY_MS` | `2000` | Delay (ms) between crawler requests |
| `LEAGUE_DELAY_MS` | `600` | Delay (ms) between league expansion requests |
| `ADMIN_TOKEN` | *(unset)* | Bearer token for `/admin/crawler`. If unset, admin is localhost-only. |

Copy `.env.example` for a full reference with notes.

## Deploy to Railway

1. Push to GitHub
2. Create a new Railway project from the repo
3. Attach a **volume** (e.g. mounted at `/data`)
4. Set environment variable: `DB_PATH=/data/search.db`
5. Deploy — Railway auto-detects `npm start` and sets `PORT`

The database is created on first boot. If you have a local `search.db` with existing indexed teams, upload it to the volume path before starting.

## Deploy to Render / VPS

**Render:** Create a Node web service, attach a persistent disk (e.g. `/var/data`), set `DB_PATH=/var/data/search.db`.

**VPS:** Clone, `npm install`, `npm start`. For TLS, put Caddy or nginx in front:

```
# Caddy one-liner
reverse_proxy localhost:3001
```

## Persistence

The app uses a single SQLite file for the search index. This file must survive restarts — use a persistent volume or a stable filesystem path. Without it, the crawler starts over from scratch on each deploy.

The database grows at roughly 5 MB per 50,000 indexed teams. The crawler indexes ~50 teams per page, at 1 page every 2 seconds by default.

## Admin

Crawler control is available at `/admin/crawler`:

```
# Status
curl http://localhost:3001/admin/crawler

# Pause
curl "http://localhost:3001/admin/crawler?action=pause"

# Resume
curl "http://localhost:3001/admin/crawler?action=resume"
```

On a deployed instance with `ADMIN_TOKEN` set:

```
curl -H "Authorization: Bearer YOUR_TOKEN" https://your-app.example.com/admin/crawler
```

## Architecture

- **Frontend:** Single-file React app (`index.html`), transpiled in-browser via Babel
- **Backend:** Node.js HTTP server (`server.js`) — proxies FPL API, serves search, runs crawler
- **Database:** SQLite + FTS5 via `better-sqlite3` (`search-db.js`)
- **Dependencies:** `better-sqlite3` only (requires Node >= 18)
