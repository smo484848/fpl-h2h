// search-db.js — SQLite + FTS5 search index for FPL teams
// Provides: initDB, upsertTeam, searchTeams, getStats, recordMetric, getCrawlerState, setCrawlerState, migrateFromJSON

const Database = require("better-sqlite3");
const path = require("path");
const fs = require("fs");

const DB_PATH = process.env.DB_PATH || path.join(__dirname, "search.db");
const JSON_INDEX_PATH = path.join(__dirname, "team-index.json");

let db = null;

// ──────────────────────────────────────────────
// Initialisation & schema
// ──────────────────────────────────────────────

function initDB() {
  // Validate DB path — check parent directory exists
  const dbDir = path.dirname(DB_PATH);
  if (!fs.existsSync(dbDir)) {
    throw new Error(`DB directory does not exist: ${dbDir} (DB_PATH=${DB_PATH})`);
  }

  db = new Database(DB_PATH);

  // Performance pragmas
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("cache_size = -8000"); // 8 MB cache

  // Create tables (idempotent)
  db.exec(`
    CREATE TABLE IF NOT EXISTS teams (
      entry_id           INTEGER PRIMARY KEY,
      manager_name       TEXT NOT NULL,
      team_name          TEXT NOT NULL DEFAULT '',
      overall_rank       INTEGER,
      first_seen_at      INTEGER NOT NULL,
      last_seen_at       INTEGER NOT NULL,
      times_seen         INTEGER NOT NULL DEFAULT 1,
      source             TEXT NOT NULL DEFAULT 'crawler',
      confidence         INTEGER NOT NULL DEFAULT 50,
      verification_level INTEGER NOT NULL DEFAULT 0
    );

    CREATE INDEX IF NOT EXISTS idx_teams_manager ON teams(manager_name COLLATE NOCASE);
    CREATE INDEX IF NOT EXISTS idx_teams_team    ON teams(team_name COLLATE NOCASE);
    CREATE INDEX IF NOT EXISTS idx_teams_rank    ON teams(overall_rank);
    CREATE INDEX IF NOT EXISTS idx_teams_lastseen ON teams(last_seen_at);

    CREATE TABLE IF NOT EXISTS crawler_state (
      key   TEXT PRIMARY KEY,
      value TEXT
    );

    CREATE TABLE IF NOT EXISTS metrics (
      id    INTEGER PRIMARY KEY AUTOINCREMENT,
      ts    INTEGER NOT NULL,
      event TEXT NOT NULL,
      data  TEXT
    );
  `);

  // Create FTS5 virtual table (idempotent — wrapped in try/catch because
  // CREATE VIRTUAL TABLE IF NOT EXISTS isn't supported in all SQLite builds)
  try {
    db.exec(`
      CREATE VIRTUAL TABLE teams_fts USING fts5(
        manager_name, team_name,
        content=teams, content_rowid=entry_id,
        tokenize='unicode61 remove_diacritics 2'
      );
    `);
  } catch (e) {
    // Table already exists — that's fine
    if (!e.message.includes("already exists")) throw e;
  }

  // Triggers to keep FTS in sync (idempotent via IF NOT EXISTS workaround)
  ensureTriggers();

  // Rebuild FTS if it's empty but teams table has data
  const teamCount = db.prepare("SELECT COUNT(*) as c FROM teams").get().c;
  let ftsCount = 0;
  try {
    ftsCount = db.prepare("SELECT COUNT(*) as c FROM teams_fts").get().c;
  } catch (e) {
    ftsCount = 0;
  }
  if (teamCount > 0 && ftsCount === 0) {
    console.log("[DB] Rebuilding FTS index...");
    rebuildFTS();
  }

  console.log(`[DB] Opened ${DB_PATH} — ${teamCount} teams indexed`);
  return db;
}

function ensureTriggers() {
  // Drop and recreate to ensure they're correct
  db.exec(`
    DROP TRIGGER IF EXISTS teams_ai;
    DROP TRIGGER IF EXISTS teams_au;
    DROP TRIGGER IF EXISTS teams_ad;

    CREATE TRIGGER teams_ai AFTER INSERT ON teams BEGIN
      INSERT INTO teams_fts(rowid, manager_name, team_name)
      VALUES (new.entry_id, new.manager_name, new.team_name);
    END;

    CREATE TRIGGER teams_au AFTER UPDATE OF manager_name, team_name ON teams BEGIN
      DELETE FROM teams_fts WHERE rowid = old.entry_id;
      INSERT INTO teams_fts(rowid, manager_name, team_name)
      VALUES (new.entry_id, new.manager_name, new.team_name);
    END;

    CREATE TRIGGER teams_ad AFTER DELETE ON teams BEGIN
      DELETE FROM teams_fts WHERE rowid = old.entry_id;
    END;
  `);
}

function rebuildFTS() {
  db.exec(`
    DELETE FROM teams_fts;
    INSERT INTO teams_fts(rowid, manager_name, team_name)
    SELECT entry_id, manager_name, team_name FROM teams;
  `);
}

// ──────────────────────────────────────────────
// Upsert
// ──────────────────────────────────────────────
//
// UPSERT VERIFICATION RULES (explicit documentation):
//
// A new record with ANY verification level (0, 1, 2) can CREATE a row.
// All rules below govern UPDATES to an existing row on conflict(entry_id).
//
// STALENESS WINDOW: If existing data hasn't been refreshed in 30+ days,
// a level-1+ source can update names and rank even if the existing row
// has higher verification. This prevents stale level-2 metadata from
// lingering forever when teams rename. Level-0 (client) never benefits
// from this — only real FPL API data (level 1+) can refresh stale rows.
// verification_level itself is NOT downgraded — only display data refreshes.
//
// ┌──────────────────────┬──────────────────────────────────────────────────────┐
// │ Field                │ Update Rule                                         │
// ├──────────────────────┼──────────────────────────────────────────────────────┤
// │ manager_name         │ Updated when:                                       │
// │                      │  • incoming verification > existing, OR             │
// │                      │  • same level with newer timestamp, OR              │
// │                      │  • existing is stale (30d+) AND incoming >= level 1 │
// │                      │ ⇒ Strong data wins, but stale data yields to fresh │
// │                      │   real API data after 30 days.                      │
// ├──────────────────────┼──────────────────────────────────────────────────────┤
// │ team_name            │ Same rule as manager_name.                          │
// ├──────────────────────┼──────────────────────────────────────────────────────┤
// │ overall_rank         │ Updated when incoming rank IS NOT NULL AND:         │
// │                      │  • incoming verification >= existing, OR            │
// │                      │  • existing is stale (30d+) AND incoming >= level 1 │
// │                      │ ⇒ A null rank never overwrites a non-null rank.    │
// ├──────────────────────┼──────────────────────────────────────────────────────┤
// │ last_seen_at         │ Always takes MAX(existing, incoming).               │
// │                      │ ⇒ Any source advances the "freshness" timestamp.   │
// ├──────────────────────┼──────────────────────────────────────────────────────┤
// │ times_seen           │ Always incremented (+1) regardless of source.       │
// │                      │ ⇒ Any touch increases the sighting count.          │
// ├──────────────────────┼──────────────────────────────────────────────────────┤
// │ source               │ Updated ONLY when incoming verificationLevel >      │
// │                      │ existing.                                           │
// │                      │ ⇒ Source label tracks the strongest origin.        │
// │                      │ ⇒ NOT updated on staleness refresh — preserves     │
// │                      │   the record of strongest-ever origin.              │
// ├──────────────────────┼──────────────────────────────────────────────────────┤
// │ confidence           │ Updated when incoming confidence > existing.        │
// │                      │ ⇒ Confidence only ratchets upward, never down.    │
// ├──────────────────────┼──────────────────────────────────────────────────────┤
// │ verification_level   │ Updated ONLY when incoming > existing.              │
// │                      │ ⇒ Verification only ratchets upward (0→1→2).      │
// │                      │ ⇒ NOT downgraded on staleness refresh.             │
// └──────────────────────┴──────────────────────────────────────────────────────┘
//
// Verification levels:
//   0 = client-submitted (unverified, lowest trust)
//   1 = league standings / background crawler (real FPL data, indirect)
//   2 = direct proxy fetch of /api/entry/{id}/ (highest trust)
//

const upsertStmt = () => db.prepare(`
  INSERT INTO teams (entry_id, manager_name, team_name, overall_rank,
                     first_seen_at, last_seen_at, times_seen, source,
                     confidence, verification_level)
  VALUES (@entryId, @managerName, @teamName, @overallRank,
          @now, @now, 1, @source, @confidence, @verificationLevel)
  ON CONFLICT(entry_id) DO UPDATE SET
    manager_name       = CASE
                           WHEN @verificationLevel > verification_level
                             OR (@verificationLevel = verification_level AND @now > last_seen_at)
                             OR (@verificationLevel >= 1 AND (@now - last_seen_at) > 2592000000)
                           THEN @managerName
                           ELSE manager_name
                         END,
    team_name          = CASE
                           WHEN @verificationLevel > verification_level
                             OR (@verificationLevel = verification_level AND @now > last_seen_at)
                             OR (@verificationLevel >= 1 AND (@now - last_seen_at) > 2592000000)
                           THEN @teamName
                           ELSE team_name
                         END,
    overall_rank       = CASE
                           WHEN @overallRank IS NOT NULL
                             AND (@verificationLevel >= verification_level
                               OR (@verificationLevel >= 1 AND (@now - last_seen_at) > 2592000000))
                           THEN @overallRank
                           ELSE overall_rank
                         END,
    last_seen_at       = MAX(last_seen_at, @now),
    times_seen         = times_seen + 1,
    source             = CASE
                           WHEN @verificationLevel > verification_level THEN @source
                           ELSE source
                         END,
    confidence         = CASE
                           WHEN @confidence > confidence THEN @confidence
                           ELSE confidence
                         END,
    verification_level = CASE
                           WHEN @verificationLevel > verification_level THEN @verificationLevel
                           ELSE verification_level
                         END
`);

let _upsertStmt = null;

function upsertTeam({
  entryId,
  managerName,
  teamName = "",
  overallRank = null,
  source = "crawler",
  confidence = 50,
  verificationLevel = 0,
}) {
  if (!entryId || !managerName) return;
  if (!_upsertStmt) _upsertStmt = upsertStmt();
  _upsertStmt.run({
    entryId: Number(entryId),
    managerName: String(managerName).trim(),
    teamName: String(teamName).trim(),
    overallRank: overallRank != null ? Number(overallRank) : null,
    now: Date.now(),
    source,
    confidence: Math.max(0, Math.min(100, Number(confidence))),
    verificationLevel: Math.max(0, Math.min(2, Number(verificationLevel))),
  });
}

// Batch upsert (wrapped in a transaction for speed)
function upsertTeamBatch(teams) {
  const tx = db.transaction((rows) => {
    for (const t of rows) {
      upsertTeam(t);
    }
  });
  tx(teams);
}

// ──────────────────────────────────────────────
// Search
// ──────────────────────────────────────────────

function searchTeams(query, limit = 20) {
  query = (query || "").trim();
  if (!query || query.length < 2) return [];

  const now = Date.now();
  const candidates = new Map(); // entryId → result obj

  // ── Strategy 1: FTS5 prefix search ──
  // Build FTS query: each token gets a prefix wildcard
  const tokens = query
    .replace(/[^\w\s]/g, "") // strip punctuation
    .split(/\s+/)
    .filter((t) => t.length >= 2);

  if (tokens.length > 0) {
    const ftsQuery = tokens.map((t) => '"' + t + '"*').join(" ");
    try {
      const ftsResults = db
        .prepare(
          `
        SELECT t.entry_id, t.manager_name, t.team_name, t.overall_rank,
               t.last_seen_at, t.times_seen, t.source, t.confidence,
               t.verification_level
        FROM teams_fts f
        JOIN teams t ON t.entry_id = f.rowid
        WHERE teams_fts MATCH @query
        LIMIT 100
      `
        )
        .all({ query: ftsQuery });

      for (const row of ftsResults) {
        candidates.set(row.entry_id, row);
      }
    } catch (e) {
      // FTS query syntax error — fall through to LIKE
    }
  }

  // ── Strategy 2: LIKE fallback for partial/contains matches ──
  // Only if FTS didn't find enough
  if (candidates.size < limit) {
    const likePattern = "%" + query.replace(/%/g, "").replace(/_/g, "") + "%";
    const likeResults = db
      .prepare(
        `
      SELECT entry_id, manager_name, team_name, overall_rank,
             last_seen_at, times_seen, source, confidence, verification_level
      FROM teams
      WHERE (manager_name LIKE @pattern COLLATE NOCASE
             OR team_name LIKE @pattern COLLATE NOCASE)
      LIMIT 100
    `
      )
      .all({ pattern: likePattern });

    for (const row of likeResults) {
      if (!candidates.has(row.entry_id)) {
        candidates.set(row.entry_id, row);
      }
    }
  }

  // ── Score and rank all candidates ──
  const qNorm = normalizeStr(query);
  const qTokens = qNorm.split(" ").filter((t) => t.length >= 2);
  const results = [];

  for (const [entryId, row] of candidates) {
    const mNorm = normalizeStr(row.manager_name);
    const tNorm = normalizeStr(row.team_name);

    // Text relevance score (0–100)
    const textScore = Math.max(
      scoreText(qNorm, qTokens, mNorm),
      scoreText(qNorm, qTokens, tNorm)
    );

    if (textScore === 0) continue;

    // Match reason
    const mScore = scoreText(qNorm, qTokens, mNorm);
    const tScore = scoreText(qNorm, qTokens, tNorm);
    const matchReason = getMatchReason(
      qNorm,
      qTokens,
      mNorm,
      tNorm,
      mScore,
      tScore
    );

    // Freshness signal (0–10): full marks if seen in last 24h, decays over 30 days
    const ageMs = now - row.last_seen_at;
    const ageDays = ageMs / 86400000;
    const freshness = Math.max(0, 10 - ageDays / 3);

    // Verification signal (0–10)
    const verifyScore = row.verification_level * 5; // 0, 5, or 10

    // Confidence signal (0–10)
    const confScore = row.confidence / 10;

    // Rank signal (0–5): small tiebreaker, NOT dominant
    let rankSignal = 0;
    if (row.overall_rank && row.overall_rank > 0) {
      // Top 10k gets 5, top 100k gets 4, top 1M gets 2, rest gets 0.5
      if (row.overall_rank <= 10000) rankSignal = 5;
      else if (row.overall_rank <= 100000) rankSignal = 4;
      else if (row.overall_rank <= 1000000) rankSignal = 2;
      else rankSignal = 0.5;
    }

    // Composite score: text match dominates
    const composite =
      textScore * 0.6 +
      freshness * 1.5 +
      verifyScore * 1.0 +
      confScore * 0.5 +
      rankSignal * 0.4;

    results.push({
      entryId,
      managerName: row.manager_name,
      teamName: row.team_name,
      overallRank: row.overall_rank,
      score: Math.round(composite * 10) / 10,
      source: row.source,
      lastSeenAt: row.last_seen_at,
      verificationLevel: row.verification_level,
      matchReason,
    });
  }

  // Sort: composite score desc, then entryId asc for stability
  results.sort((a, b) => b.score - a.score || a.entryId - b.entryId);
  return results.slice(0, limit);
}

// ── Text scoring helpers ──

function normalizeStr(s) {
  return (s || "").toLowerCase().trim().replace(/\s+/g, " ");
}

function scoreText(qNorm, qTokens, text) {
  if (!qNorm || !text) return 0;
  if (text === qNorm) return 100;
  if (text.startsWith(qNorm)) return 80;

  const tTokens = text.split(" ");
  if (
    qTokens.length > 1 &&
    qTokens.every((qt) => tTokens.some((tt) => tt.startsWith(qt)))
  )
    return 70;

  if (text.includes(qNorm)) return 60;
  if (qTokens.every((qt) => text.includes(qt))) return 50;
  if (
    qTokens.some(
      (qt) => qt.length >= 2 && tTokens.some((tt) => tt.startsWith(qt))
    )
  )
    return 30;

  return 0;
}

function getMatchReason(qNorm, qTokens, mNorm, tNorm, mScore, tScore) {
  const bestField = mScore >= tScore ? "manager" : "team";
  const bestScore = Math.max(mScore, tScore);

  if (bestScore >= 100)
    return bestField === "manager"
      ? "Exact manager name"
      : "Exact team name";
  if (bestScore >= 80)
    return bestField === "manager"
      ? "Manager name starts with query"
      : "Team name starts with query";
  if (bestScore >= 70) return "All search tokens match";
  if (bestScore >= 60)
    return bestField === "manager"
      ? "Manager name contains query"
      : "Team name contains query";
  if (bestScore >= 50) return "All tokens found";
  if (bestScore >= 30) return "Partial token match";
  return "Weak match";
}

// ──────────────────────────────────────────────
// Migration from JSON index
// ──────────────────────────────────────────────

function migrateFromJSON() {
  if (!fs.existsSync(JSON_INDEX_PATH)) {
    console.log("[DB] No JSON index to migrate");
    return 0;
  }

  try {
    const raw = JSON.parse(fs.readFileSync(JSON_INDEX_PATH, "utf8"));
    let entries = [];

    if (Array.isArray(raw)) {
      // Format: [[entryId, {m, t}], ...]
      entries = raw.map(([id, data]) => ({
        entryId: Number(id),
        managerName: data.m || "",
        teamName: data.t || "",
      }));
    } else if (typeof raw === "object") {
      entries = Object.entries(raw).map(([id, data]) => ({
        entryId: Number(id),
        managerName: data.m || "",
        teamName: data.t || "",
      }));
    }

    if (entries.length === 0) {
      console.log("[DB] JSON index was empty");
      return 0;
    }

    // Check how many we already have
    const existing = db.prepare("SELECT COUNT(*) as c FROM teams").get().c;
    if (existing >= entries.length) {
      console.log(
        `[DB] SQLite already has ${existing} teams (JSON has ${entries.length}), skipping migration`
      );
      return 0;
    }

    console.log(
      `[DB] Migrating ${entries.length} teams from JSON to SQLite...`
    );

    const batchRows = entries.map((e) => ({
      entryId: e.entryId,
      managerName: e.managerName,
      teamName: e.teamName,
      overallRank: null,
      source: "crawler",
      confidence: 40, // Legacy data, moderate confidence
      verificationLevel: 1, // It came from real FPL API responses
    }));

    upsertTeamBatch(batchRows);

    console.log(
      `[DB] Migration complete. ${db.prepare("SELECT COUNT(*) as c FROM teams").get().c} teams in SQLite`
    );

    // Rename JSON file rather than deleting — keep as backup
    const backupPath = JSON_INDEX_PATH + ".migrated";
    if (!fs.existsSync(backupPath)) {
      fs.renameSync(JSON_INDEX_PATH, backupPath);
      console.log(`[DB] JSON index backed up to ${path.basename(backupPath)}`);
    }

    return entries.length;
  } catch (e) {
    console.error("[DB] Migration error:", e.message);
    return 0;
  }
}

// ──────────────────────────────────────────────
// Crawler state
// ──────────────────────────────────────────────

function getCrawlerState(key) {
  const row = db
    .prepare("SELECT value FROM crawler_state WHERE key = ?")
    .get(key);
  return row ? row.value : null;
}

function setCrawlerState(key, value) {
  db.prepare(
    "INSERT OR REPLACE INTO crawler_state (key, value) VALUES (?, ?)"
  ).run(key, String(value));
}

// ──────────────────────────────────────────────
// Metrics (lightweight)
// ──────────────────────────────────────────────

function recordMetric(event, data = {}) {
  try {
    db.prepare("INSERT INTO metrics (ts, event, data) VALUES (?, ?, ?)").run(
      Date.now(),
      event,
      JSON.stringify(data)
    );
  } catch (e) {
    // Never let metrics break the app
  }
}

function getMetricsSummary() {
  try {
    const hourAgo = Date.now() - 3600000;
    const dayAgo = Date.now() - 86400000;

    const searchesHour = db
      .prepare(
        "SELECT COUNT(*) as c FROM metrics WHERE event = 'search' AND ts > ?"
      )
      .get(hourAgo).c;
    const searchesDay = db
      .prepare(
        "SELECT COUNT(*) as c FROM metrics WHERE event = 'search' AND ts > ?"
      )
      .get(dayAgo).c;
    const zeroResultsDay = db
      .prepare(
        "SELECT COUNT(*) as c FROM metrics WHERE event = 'search' AND ts > ? AND json_extract(data, '$.resultCount') = 0"
      )
      .get(dayAgo).c;
    const avgLatency = db
      .prepare(
        "SELECT AVG(json_extract(data, '$.latencyMs')) as avg FROM metrics WHERE event = 'search' AND ts > ?"
      )
      .get(hourAgo).avg;

    // Search quality metrics
    const selectsDay = db
      .prepare(
        "SELECT COUNT(*) as c FROM metrics WHERE event = 'search_select' AND ts > ?"
      )
      .get(dayAgo).c;
    const avgPosition = db
      .prepare(
        "SELECT AVG(json_extract(data, '$.position')) as avg FROM metrics WHERE event = 'search_select' AND ts > ? AND json_extract(data, '$.position') IS NOT NULL"
      )
      .get(dayAgo).avg;
    const firstPickRate = selectsDay > 0
      ? db.prepare(
          "SELECT COUNT(*) as c FROM metrics WHERE event = 'search_select' AND ts > ? AND json_extract(data, '$.position') = 0"
        ).get(dayAgo).c
      : 0;
    const comparesAfterSearch = db
      .prepare(
        "SELECT COUNT(*) as c FROM metrics WHERE event = 'search_compare_after_search' AND ts > ?"
      )
      .get(dayAgo).c;

    return {
      searchesLastHour: searchesHour,
      searchesLastDay: searchesDay,
      zeroResultRateDay:
        searchesDay > 0
          ? Math.round((zeroResultsDay / searchesDay) * 100)
          : 0,
      avgLatencyMs: avgLatency ? Math.round(avgLatency) : null,
      selectsLastDay: selectsDay,
      avgSelectedPosition: avgPosition != null ? Math.round(avgPosition * 10) / 10 : null,
      firstPickRateDay:
        selectsDay > 0
          ? Math.round((firstPickRate / selectsDay) * 100)
          : 0,
      comparesAfterSearchDay: comparesAfterSearch,
    };
  } catch (e) {
    return {};
  }
}

// ──────────────────────────────────────────────
// Stats
// ──────────────────────────────────────────────

function getStats() {
  const total = db.prepare("SELECT COUNT(*) as c FROM teams").get().c;
  const verified2 = db
    .prepare("SELECT COUNT(*) as c FROM teams WHERE verification_level = 2")
    .get().c;
  const verified1 = db
    .prepare("SELECT COUNT(*) as c FROM teams WHERE verification_level = 1")
    .get().c;
  const verified0 = db
    .prepare("SELECT COUNT(*) as c FROM teams WHERE verification_level = 0")
    .get().c;
  const sources = db
    .prepare(
      "SELECT source, COUNT(*) as c FROM teams GROUP BY source ORDER BY c DESC"
    )
    .all();

  return {
    total,
    byVerification: { direct: verified2, league: verified1, client: verified0 },
    bySources: sources.reduce((acc, r) => {
      acc[r.source] = r.c;
      return acc;
    }, {}),
  };
}

// ──────────────────────────────────────────────
// Freshness distribution (for /index-stats)
// ──────────────────────────────────────────────

function getFreshnessDistribution() {
  try {
    const now = Date.now();
    const day1 = now - 86400000;       // 1 day ago
    const day7 = now - 7 * 86400000;   // 7 days ago
    const day30 = now - 30 * 86400000; // 30 days ago

    const last24h = db
      .prepare("SELECT COUNT(*) as c FROM teams WHERE last_seen_at > ?")
      .get(day1).c;
    const last7d = db
      .prepare("SELECT COUNT(*) as c FROM teams WHERE last_seen_at > ? AND last_seen_at <= ?")
      .get(day7, day1).c;
    const last30d = db
      .prepare("SELECT COUNT(*) as c FROM teams WHERE last_seen_at > ? AND last_seen_at <= ?")
      .get(day30, day7).c;
    const older = db
      .prepare("SELECT COUNT(*) as c FROM teams WHERE last_seen_at <= ?")
      .get(day30).c;

    return { last24h, last7d, last30d, older };
  } catch (e) {
    return { last24h: 0, last7d: 0, last30d: 0, older: 0 };
  }
}

// ──────────────────────────────────────────────
// Metric retention cleanup (30-day cap)
// ──────────────────────────────────────────────

function cleanupOldMetrics() {
  try {
    const cutoff = Date.now() - 30 * 86400000; // 30 days ago
    const result = db
      .prepare("DELETE FROM metrics WHERE ts < ?")
      .run(cutoff);
    if (result.changes > 0) {
      console.log(`[DB] Cleaned up ${result.changes} metrics older than 30 days`);
    }
    return result.changes;
  } catch (e) {
    console.error("[DB] Metric cleanup error:", e.message);
    return 0;
  }
}

// ──────────────────────────────────────────────
// Cleanup / close
// ──────────────────────────────────────────────

function closeDB() {
  if (db) {
    try {
      db.close();
    } catch (e) {}
    db = null;
    _upsertStmt = null;
  }
}

module.exports = {
  initDB,
  upsertTeam,
  upsertTeamBatch,
  searchTeams,
  migrateFromJSON,
  getCrawlerState,
  setCrawlerState,
  recordMetric,
  getMetricsSummary,
  getStats,
  getFreshnessDistribution,
  cleanupOldMetrics,
  closeDB,
};
