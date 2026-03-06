// FPL Proxy Server with SQLite Team Search Index — node server.js
const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const searchDB = require("./search-db");

// ──────────────────────────────────────────────
// Runtime configuration (all overridable via env)
// ──────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || "3001", 10);
const FPL_HOST = "fantasy.premierleague.com";
const CRAWLER_ENABLED = process.env.CRAWLER_ENABLED !== "false";         // default: true
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || null;                     // null = localhost-only

// ──────────────────────────────────────────────
// FPL API fetcher (server-side, for crawling)
// ──────────────────────────────────────────────

function fplGet(apiPath) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: FPL_HOST,
      path: apiPath,
      method: "GET",
      headers: {
        "User-Agent": "Mozilla/5.0",
        Accept: "application/json",
      },
    };
    const req = https.request(options, (res) => {
      let body = "";
      res.on("data", (chunk) => (body += chunk));
      res.on("end", () => {
        if (res.statusCode !== 200) {
          reject(new Error(`FPL API ${res.statusCode}: ${apiPath}`));
          return;
        }
        try {
          resolve(JSON.parse(body));
        } catch (e) {
          reject(new Error(`JSON parse error: ${apiPath}`));
        }
      });
    });
    req.on("error", reject);
    req.setTimeout(10000, () => {
      req.destroy();
      reject(new Error(`Timeout: ${apiPath}`));
    });
    req.end();
  });
}

// ──────────────────────────────────────────────
// Proxy → auto-index interceptors
// ──────────────────────────────────────────────

function tryIndexFromEntry(data) {
  if (data && data.id && data.player_first_name) {
    const manager = (data.player_first_name + " " + data.player_last_name).trim();
    searchDB.upsertTeam({
      entryId: data.id,
      managerName: manager,
      teamName: data.name || "",
      overallRank: data.summary_overall_rank || null,
      source: "proxy",
      confidence: 90,
      verificationLevel: 2, // Direct server fetch from FPL API
    });
    // Queue league expansion
    scheduleLeagueIndex(data.id, data.leagues);
  }
}

function tryIndexFromLeagueStandings(data) {
  const results = data?.standings?.results;
  if (!Array.isArray(results)) return;
  const batch = [];
  for (const entry of results) {
    if (entry.entry && entry.player_name) {
      batch.push({
        entryId: entry.entry,
        managerName: entry.player_name,
        teamName: entry.entry_name || "",
        overallRank: entry.rank || null,
        source: "league",
        confidence: 70,
        verificationLevel: 1,
      });
    }
  }
  if (batch.length > 0) {
    searchDB.upsertTeamBatch(batch);
  }
}

// ──────────────────────────────────────────────
// Background league expansion (controlled queue)
// ──────────────────────────────────────────────

const leagueQueue = {
  items: [],                          // League IDs waiting to be indexed
  processing: false,
  indexed: new Set(),                 // Already-indexed league IDs (this session)
  errors: 0,                         // Consecutive errors
  totalProcessed: 0,
  totalFailed: 0,
  paused: false,
};

const LEAGUE_CONFIG = {
  maxPerEntry: 5,                    // Max leagues queued per entry lookup
  maxPages: 10,                      // Max pages per league (500 members)
  maxQueueSize: 50,                  // Don't let queue grow unbounded
  delayBetweenLeagues: parseInt(process.env.LEAGUE_DELAY_MS || "600", 10),
  delayBetweenPages: 350,            // ms between pages within a league
  maxConsecutiveErrors: 3,           // Pause after this many consecutive failures
  pauseDuration: 60000,              // 1 min pause on error spike
};

function scheduleLeagueIndex(entryId, leaguesObj) {
  if (!leaguesObj || leagueQueue.paused) return;
  const classic = leaguesObj.classic || [];

  // Sort: smaller leagues first (more relevant, faster)
  const eligible = classic
    .filter(l => l.entry_can_leave && !leagueQueue.indexed.has(l.id))
    .sort((a, b) => (a.num_entries || 999) - (b.num_entries || 999));

  let queued = 0;
  for (const league of eligible) {
    if (queued >= LEAGUE_CONFIG.maxPerEntry) break;
    if (leagueQueue.items.length >= LEAGUE_CONFIG.maxQueueSize) break;
    // Skip very large leagues (>1000 members) — diminishing returns
    if (league.num_entries && league.num_entries > 1000) continue;

    leagueQueue.indexed.add(league.id);
    leagueQueue.items.push(league.id);
    queued++;
  }

  if (queued > 0) drainLeagueQueue();
}

async function drainLeagueQueue() {
  if (leagueQueue.processing || leagueQueue.paused) return;
  leagueQueue.processing = true;

  while (leagueQueue.items.length > 0 && !leagueQueue.paused) {
    const leagueId = leagueQueue.items.shift();
    try {
      await indexLeagueMembers(leagueId);
      leagueQueue.errors = 0;
      leagueQueue.totalProcessed++;
    } catch (e) {
      leagueQueue.errors++;
      leagueQueue.totalFailed++;
      console.log(`[LEAGUE] Error indexing ${leagueId}: ${e.message} (${leagueQueue.errors} consecutive)`);

      if (leagueQueue.errors >= LEAGUE_CONFIG.maxConsecutiveErrors) {
        console.log(`[LEAGUE] Pausing for ${LEAGUE_CONFIG.pauseDuration / 1000}s after ${leagueQueue.errors} errors`);
        leagueQueue.paused = true;
        setTimeout(() => {
          leagueQueue.paused = false;
          leagueQueue.errors = 0;
          if (leagueQueue.items.length > 0) drainLeagueQueue();
        }, LEAGUE_CONFIG.pauseDuration);
        break;
      }
    }
    await new Promise((r) => setTimeout(r, LEAGUE_CONFIG.delayBetweenLeagues));
  }

  leagueQueue.processing = false;
}

async function indexLeagueMembers(leagueId) {
  let page = 1;
  const batch = [];
  while (page <= LEAGUE_CONFIG.maxPages) {
    const data = await fplGet(
      `/api/leagues-classic/${leagueId}/standings/?page_standings=${page}`
    );
    const results = data?.standings?.results;
    if (!Array.isArray(results) || results.length === 0) break;
    for (const entry of results) {
      if (entry.entry && entry.player_name) {
        batch.push({
          entryId: entry.entry,
          managerName: entry.player_name,
          teamName: entry.entry_name || "",
          overallRank: null,
          source: "league",
          confidence: 65,
          verificationLevel: 1,
        });
      }
    }
    if (!data.standings.has_next) break;
    page++;
    await new Promise((r) => setTimeout(r, LEAGUE_CONFIG.delayBetweenPages));
  }
  if (batch.length > 0) {
    searchDB.upsertTeamBatch(batch);
    console.log(`[LEAGUE] +${batch.length} from league ${leagueId} (queue: ${leagueQueue.items.length})`);
  }
}

// ──────────────────────────────────────────────
// Background Overall League Crawler (controlled)
// ──────────────────────────────────────────────

const crawler = {
  running: false,
  paused: false,
  consecutiveErrors: 0,
  totalErrors: 0,
  pagesThisSession: 0,
  startedAt: null,
};

const CRAWLER_CONFIG = {
  maxPagesPerSession: parseInt(process.env.CRAWLER_MAX_PAGES || "2000", 10),
  delayBetweenPages: parseInt(process.env.CRAWLER_DELAY_MS || "2000", 10),
  maxConsecutiveErrors: 5,           // Pause after this many
  shortPauseDuration: 30000,         // 30s on single error
  longPauseDuration: 300000,         // 5 min on error spike
  progressLogInterval: 50,           // Log every N pages
};

async function startOverallCrawler() {
  if (crawler.running) return;
  crawler.running = true;
  crawler.startedAt = Date.now();
  crawler.pagesThisSession = 0;

  let page = parseInt(searchDB.getCrawlerState("overall_page") || "1", 10);
  console.log(`[CRAWLER] Starting from page ${page}`);

  while (crawler.pagesThisSession < CRAWLER_CONFIG.maxPagesPerSession && !crawler.paused) {
    try {
      const data = await fplGet(
        `/api/leagues-classic/314/standings/?page_standings=${page}`
      );
      const results = data?.standings?.results;
      if (!Array.isArray(results) || results.length === 0) {
        console.log(`[CRAWLER] Empty results at page ${page}, stopping`);
        break;
      }

      const batch = results
        .filter((e) => e.entry && e.player_name)
        .map((e) => ({
          entryId: e.entry,
          managerName: e.player_name,
          teamName: e.entry_name || "",
          overallRank: e.rank || null,
          source: "crawler",
          confidence: 60,
          verificationLevel: 1,
        }));

      if (batch.length > 0) searchDB.upsertTeamBatch(batch);

      page++;
      crawler.pagesThisSession++;
      crawler.consecutiveErrors = 0;
      searchDB.setCrawlerState("overall_page", String(page));

      if (!data.standings.has_next) {
        console.log(`[CRAWLER] Reached end of Overall league at page ${page - 1}`);
        break;
      }

      // Progress logging
      if (crawler.pagesThisSession % CRAWLER_CONFIG.progressLogInterval === 0) {
        const stats = searchDB.getStats();
        const elapsed = Math.round((Date.now() - crawler.startedAt) / 1000);
        console.log(`[CRAWLER] Page ${page} — ${stats.total} teams — ${crawler.pagesThisSession} pages in ${elapsed}s — errors: ${crawler.totalErrors}`);
      }

      await new Promise((r) => setTimeout(r, CRAWLER_CONFIG.delayBetweenPages));
    } catch (e) {
      crawler.consecutiveErrors++;
      crawler.totalErrors++;

      if (crawler.consecutiveErrors >= CRAWLER_CONFIG.maxConsecutiveErrors) {
        console.log(`[CRAWLER] ${crawler.consecutiveErrors} consecutive errors. Pausing ${CRAWLER_CONFIG.longPauseDuration / 1000}s...`);
        await new Promise((r) => setTimeout(r, CRAWLER_CONFIG.longPauseDuration));
        crawler.consecutiveErrors = 0;
      } else {
        // Exponential backoff with jitter
        const backoff = Math.min(
          CRAWLER_CONFIG.shortPauseDuration,
          3000 * Math.pow(2, crawler.consecutiveErrors - 1)
        );
        const jitter = Math.floor(Math.random() * 2000);
        console.log(`[CRAWLER] Error page ${page}: ${e.message}. Retrying in ${Math.round((backoff + jitter) / 1000)}s`);
        await new Promise((r) => setTimeout(r, backoff + jitter));
      }
    }
  }

  const stats = searchDB.getStats();
  const elapsed = Math.round((Date.now() - crawler.startedAt) / 1000);
  console.log(`[CRAWLER] Session done. ${stats.total} teams. ${crawler.pagesThisSession} pages in ${elapsed}s. Errors: ${crawler.totalErrors}. Next page: ${page}`);
  crawler.running = false;
}

// ──────────────────────────────────────────────
// Client submission validation
// ──────────────────────────────────────────────

const CLIENT_SUBMIT_MAX_BATCH = 10;
const clientSubmitTracker = { count: 0, windowStart: Date.now() };
const CLIENT_RATE_LIMIT = 60;       // Max submissions per minute
const CLIENT_RATE_WINDOW = 60000;

function validateClientSubmission(teams) {
  // Must be an array
  if (!Array.isArray(teams)) return { ok: false, error: "Expected array" };

  // Cap batch size
  if (teams.length > CLIENT_SUBMIT_MAX_BATCH) {
    return { ok: false, error: `Batch too large (max ${CLIENT_SUBMIT_MAX_BATCH})` };
  }

  // Rate limit
  const now = Date.now();
  if (now - clientSubmitTracker.windowStart > CLIENT_RATE_WINDOW) {
    clientSubmitTracker.count = 0;
    clientSubmitTracker.windowStart = now;
  }
  if (clientSubmitTracker.count + teams.length > CLIENT_RATE_LIMIT) {
    return { ok: false, error: "Rate limit exceeded" };
  }

  // Validate each row
  const valid = [];
  for (const t of teams) {
    const id = Number(t.entryId);
    const name = String(t.managerName || "").trim();
    const team = String(t.teamName || "").trim();

    // Reject bad rows
    if (!id || id <= 0 || id > 100000000) continue;
    if (!name || name.length < 2 || name.length > 100) continue;
    if (team.length > 100) continue;
    // Reject suspicious patterns
    if (/[<>{}]/.test(name) || /[<>{}]/.test(team)) continue;

    valid.push({
      entryId: id,
      managerName: name,
      teamName: team,
      source: "client",
      confidence: 20,
      verificationLevel: 0, // Unverified — lowest trust
    });
  }

  clientSubmitTracker.count += valid.length;
  return { ok: true, teams: valid };
}

// ──────────────────────────────────────────────
// HTTP Server
// ──────────────────────────────────────────────

const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

  // Handle CORS preflight
  if (req.method === "OPTIONS") {
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    res.writeHead(204);
    res.end();
    return;
  }

  // ─── Search endpoint ───
  if (req.url.startsWith("/search-teams")) {
    const start = Date.now();
    const url = new URL(req.url, `http://localhost:${PORT}`);
    const q = url.searchParams.get("q") || "";
    const limit = Math.min(parseInt(url.searchParams.get("limit") || "20", 10), 50);

    const results = searchDB.searchTeams(q, limit);
    const latencyMs = Date.now() - start;

    // Record metric
    searchDB.recordMetric("search", {
      query: q,
      resultCount: results.length,
      latencyMs,
    });

    const stats = searchDB.getStats();
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        query: q,
        count: results.length,
        indexSize: stats.total,
        latencyMs,
        results,
      })
    );
    return;
  }

  // ─── Search quality metrics (POST from client) ───
  if (req.url === "/search-metric" && req.method === "POST") {
    let body = "";
    req.on("data", (chunk) => {
      if (body.length > 2000) return;
      body += chunk;
    });
    req.on("end", () => {
      try {
        const data = JSON.parse(body);
        const event = String(data.event || "").slice(0, 30);
        if (event && ["select", "compare_after_search", "abandon"].includes(event)) {
          searchDB.recordMetric("search_" + event, {
            position: data.position != null ? Number(data.position) : null,
            entryId: data.entryId ? Number(data.entryId) : null,
            score: data.score ? Number(data.score) : null,
            query: data.query ? String(data.query).slice(0, 50) : null,
          });
        }
      } catch (e) {}
      res.writeHead(204);
      res.end();
    });
    return;
  }

  // ─── Crawler control endpoint ───
  if (req.url.startsWith("/admin/crawler")) {
    // Auth: token-based if ADMIN_TOKEN is set, otherwise localhost-only
    if (ADMIN_TOKEN) {
      const authHeader = req.headers.authorization || "";
      if (authHeader !== `Bearer ${ADMIN_TOKEN}`) {
        res.writeHead(401, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Invalid or missing admin token" }));
        return;
      }
    } else {
      const remoteAddr = req.socket.remoteAddress;
      if (remoteAddr !== "127.0.0.1" && remoteAddr !== "::1" && remoteAddr !== "::ffff:127.0.0.1") {
        res.writeHead(403, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Admin endpoints are localhost-only" }));
        return;
      }
    }
    const url = new URL(req.url, `http://localhost:${PORT}`);
    const action = url.searchParams.get("action");

    if (action === "pause") {
      crawler.paused = true;
      leagueQueue.paused = true;
      console.log("[ADMIN] Crawler and league expansion paused");
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "paused", crawler: crawler.paused, leagues: leagueQueue.paused }));
    } else if (action === "resume") {
      crawler.paused = false;
      leagueQueue.paused = false;
      leagueQueue.errors = 0;
      console.log("[ADMIN] Crawler and league expansion resumed");
      // Restart crawler if it had stopped
      if (!crawler.running) setTimeout(() => startOverallCrawler(), 1000);
      if (leagueQueue.items.length > 0 && !leagueQueue.processing) drainLeagueQueue();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "resumed", crawler: crawler.paused, leagues: leagueQueue.paused }));
    } else {
      // Default: return status
      const page = searchDB.getCrawlerState("overall_page") || "1";
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        crawler: { page: parseInt(page, 10), running: crawler.running, paused: crawler.paused, pagesThisSession: crawler.pagesThisSession, totalErrors: crawler.totalErrors },
        leagues: { queueLength: leagueQueue.items.length, processing: leagueQueue.processing, paused: leagueQueue.paused, totalProcessed: leagueQueue.totalProcessed },
      }));
    }
    return;
  }

  // ─── Index stats endpoint ───
  if (req.url === "/index-stats") {
    const stats = searchDB.getStats();
    const metrics = searchDB.getMetricsSummary();
    const freshness = searchDB.getFreshnessDistribution();
    const page = searchDB.getCrawlerState("overall_page") || "1";
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        ...stats,
        freshness,
        crawler: {
          page: parseInt(page, 10),
          running: crawler.running,
          paused: crawler.paused,
          pagesThisSession: crawler.pagesThisSession,
          consecutiveErrors: crawler.consecutiveErrors,
          totalErrors: crawler.totalErrors,
          startedAt: crawler.startedAt,
        },
        leagueExpansion: {
          queueLength: leagueQueue.items.length,
          processing: leagueQueue.processing,
          paused: leagueQueue.paused,
          totalProcessed: leagueQueue.totalProcessed,
          totalFailed: leagueQueue.totalFailed,
          indexedCount: leagueQueue.indexed.size,
        },
        metrics,
      })
    );
    return;
  }

  // ─── Client submission endpoint (POST) ───
  if (req.url === "/index-teams" && req.method === "POST") {
    let body = "";
    req.on("data", (chunk) => {
      // Cap body size at 50KB
      if (body.length > 50000) return;
      body += chunk;
    });
    req.on("end", () => {
      try {
        const parsed = JSON.parse(body);
        const validation = validateClientSubmission(parsed);

        if (!validation.ok) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: validation.error }));
          return;
        }

        if (validation.teams.length > 0) {
          searchDB.upsertTeamBatch(validation.teams);
        }

        const stats = searchDB.getStats();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ accepted: validation.teams.length, indexSize: stats.total }));
      } catch (e) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Invalid JSON" }));
      }
    });
    return;
  }

  // ─── Proxy /api/* to FPL (with index interception) ───
  if (req.url.startsWith("/api/")) {
    const options = {
      hostname: FPL_HOST,
      path: req.url,
      method: "GET",
      headers: {
        "User-Agent": "Mozilla/5.0",
        Accept: "application/json",
      },
    };

    const proxy = https.request(options, (fplRes) => {
      const isEntryEndpoint = /^\/api\/entry\/\d+\/?(\?|$)/.test(req.url);
      const isLeagueStandings = /^\/api\/leagues-classic\/\d+\/standings/.test(req.url);

      if (isEntryEndpoint || isLeagueStandings) {
        // Buffer to index, then forward
        let body = "";
        fplRes.on("data", (chunk) => (body += chunk));
        fplRes.on("end", () => {
          res.writeHead(fplRes.statusCode, { "Content-Type": "application/json" });
          res.end(body);
          if (fplRes.statusCode === 200) {
            try {
              const data = JSON.parse(body);
              if (isEntryEndpoint) tryIndexFromEntry(data);
              if (isLeagueStandings) tryIndexFromLeagueStandings(data);
            } catch (e) {}
          }
        });
      } else {
        res.writeHead(fplRes.statusCode, { "Content-Type": "application/json" });
        fplRes.pipe(res);
      }
    });

    proxy.on("error", (e) => {
      res.writeHead(500);
      res.end(JSON.stringify({ error: e.message }));
    });

    proxy.end();
    return;
  }

  // ─── Serve static HTML ───
  const urlPath = req.url.split("?")[0];
  const file =
    urlPath === "/mobile" || urlPath === "/mobile/"
      ? "index-mobile.html"
      : "index.html";
  const filePath = path.join(__dirname, file);
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end(file + " not found — make sure server.js and " + file + " are in the same folder.");
      return;
    }
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(data);
  });
});

// ──────────────────────────────────────────────
// Startup
// ──────────────────────────────────────────────

// 1. Verify index.html is present (fail fast, not on first request)
const INDEX_HTML = path.join(__dirname, "index.html");
if (!fs.existsSync(INDEX_HTML)) {
  console.error("[FATAL] index.html not found in " + __dirname);
  console.error("        Make sure server.js and index.html are in the same directory.");
  process.exit(1);
}

// 2. Open database (fail fast if path is bad or permissions are wrong)
try {
  searchDB.initDB();
} catch (e) {
  const dbPath = process.env.DB_PATH || path.join(__dirname, "search.db");
  console.error("[FATAL] Could not open database: " + e.message);
  console.error("        DB_PATH: " + dbPath);
  console.error("        Check that the directory exists and is writable.");
  process.exit(1);
}

// 3. Run one-time migrations (non-fatal — these are best-effort)
try { searchDB.migrateFromJSON(); } catch (e) {
  console.warn("[WARN] JSON migration failed: " + e.message);
}
const CRAWLER_STATE_FILE = path.join(__dirname, "crawler-state.json");
try {
  if (fs.existsSync(CRAWLER_STATE_FILE) && !searchDB.getCrawlerState("overall_page")) {
    const old = JSON.parse(fs.readFileSync(CRAWLER_STATE_FILE, "utf8"));
    if (old.page) {
      searchDB.setCrawlerState("overall_page", String(old.page));
      console.log(`[CRAWLER] Migrated state from JSON: page ${old.page}`);
      fs.renameSync(CRAWLER_STATE_FILE, CRAWLER_STATE_FILE + ".migrated");
    }
  }
} catch (e) {}

// 4. Start HTTP server
server.on("error", (e) => {
  if (e.code === "EADDRINUSE") {
    console.error(`[FATAL] Port ${PORT} is already in use.`);
    console.error("        Stop the other process or set a different PORT.");
  } else {
    console.error("[FATAL] Server error: " + e.message);
  }
  process.exit(1);
});

server.listen(PORT, "0.0.0.0", () => {
  const stats = searchDB.getStats();
  console.log("");
  console.log("  FPL Team Comparison");
  console.log("  ───────────────────────────────────");
  console.log(`  URL:      http://0.0.0.0:${PORT}`);
  console.log(`  DB:       ${process.env.DB_PATH || "./search.db"} (${stats.total.toLocaleString()} teams)`);
  console.log(`  Crawler:  ${CRAWLER_ENABLED ? "enabled" : "disabled"}`);
  console.log(`  Admin:    ${ADMIN_TOKEN ? "token auth" : "localhost-only"}`);
  console.log("  ───────────────────────────────────");
  console.log("");

  // Start background crawler after 5 second delay (if enabled)
  if (CRAWLER_ENABLED) {
    setTimeout(() => startOverallCrawler(), 5000);
  } else {
    console.log("[CRAWLER] Disabled via CRAWLER_ENABLED=false");
  }
  // Run metric cleanup on startup and then every 6 hours
  searchDB.cleanupOldMetrics();
  setInterval(() => searchDB.cleanupOldMetrics(), 6 * 3600000);
});

// Clean shutdown
process.on("SIGINT", () => {
  console.log("\n[DB] Shutting down...");
  searchDB.closeDB();
  process.exit(0);
});

process.on("SIGTERM", () => {
  searchDB.closeDB();
  process.exit(0);
});
