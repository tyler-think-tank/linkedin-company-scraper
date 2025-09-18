// server.js
// LinkedIn Company Posts -> Apify proxy with MySQL persistence + daily gate
//
// Endpoints:
//   GET /latest?company=<slug-or-url>&page=<number>&force=<true|false>
//     - Returns ONLY the newest post
//     - Will call Apify at most once per company per day (unless force=true)
//   GET /scrape?company=<slug-or-url>&page=<number>&force=<true|false>
//     - Returns many items from the actor (still saves only the newest one to DB)
//
// Stores (per your request):
//   - Author logo, author name, follower count
//   - Post URL and post text
//   - Any media URLs (with width/height if present)

"use strict";

const express = require("express");
const cors = require("cors");
const rateLimit = require("express-rate-limit");
const dotenv = require("dotenv");
const winston = require("winston");
const { query, validationResult } = require("express-validator");
const { ApifyClient } = require("apify-client");
const mysql = require("mysql2/promise");

dotenv.config();

// ---- Config ----
const PORT = parseInt(process.env.PORT || "10000", 10);
const BASE_API_URL = process.env.BASE_API_URL || "";
const APIFY_TOKEN = process.env.APIFY_TOKEN;
const APIFY_ACTOR_ID =
  process.env.APIFY_ACTOR_ID || "apimaestro/linkedin-company-posts";
// Daily gate: only one run per company per this many ms (default 12h)
const DAILY_WINDOW_MS = parseInt(
  process.env.DAILY_WINDOW_MS || (12 * 60 * 60 * 1000).toString(),
  10
);

// DB config
const DB_HOST = process.env.DB_HOST;
const DB_USER = process.env.DB_USER;
const DB_PASS = process.env.DB_PASS;
const DB_NAME = process.env.DB_NAME;
const DB_PORT = parseInt(process.env.DB_PORT || "3306", 10);

// Basic validation
if (!APIFY_TOKEN) {
  console.error("Missing APIFY_TOKEN env var.");
  process.exit(1);
}
if (!DB_HOST || !DB_USER || !DB_PASS || !DB_NAME) {
  console.error("Missing DB env vars (DB_HOST, DB_USER, DB_PASS, DB_NAME).");
  process.exit(1);
}

// ---- Logger ----
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      ({ level, message, timestamp }) =>
        `${timestamp} [${level.toUpperCase()}] ${message}`
    )
  ),
  transports: [new winston.transports.Console()],
});

// ---- Express ----
const app = express();
app.set("trust proxy", 1);
app.use(cors());
app.use(express.json());

app.use(
  rateLimit({
    windowMs: 15 * 60 * 1000,
    max: parseInt(process.env.RATE_LIMIT_MAX || "60", 10),
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => req.ip,
    skip: (req) => req.path.endsWith("/health"),
  })
);

// ---- Apify client (v1) ----
const client = new ApifyClient({ token: APIFY_TOKEN });

// ---- MySQL pool & schema init ----
let pool;

async function initDb() {
  pool = await mysql.createPool({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASS,
    database: DB_NAME,
    port: DB_PORT,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
  });

  // Create tables if not exist
  await pool.query(`
    CREATE TABLE IF NOT EXISTS companies (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_key VARCHAR(255) NOT NULL UNIQUE,
      author_name VARCHAR(255) NULL,
      author_logo_url TEXT NULL,
      follower_count INT NULL,
      updated_at TIMESTAMP NULL DEFAULT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS posts (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      company_id INT NOT NULL,
      activity_urn VARCHAR(255) NULL,
      full_urn VARCHAR(255) NULL,
      post_url TEXT NOT NULL,
      post_text MEDIUMTEXT NULL,
      posted_at DATETIME NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uq_post_url (post_url(255)),
      INDEX idx_company_created (company_id, created_at),
      CONSTRAINT fk_posts_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS media (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      post_id BIGINT NOT NULL,
      url TEXT NOT NULL,
      width INT NULL,
      height INT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_post (post_id),
      CONSTRAINT fk_media_post FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  logger.info("DB initialized & schema ensured.");
}

async function getCompanyByKey(companyKey) {
  const [rows] = await pool.query(
    `SELECT * FROM companies WHERE company_key = ? LIMIT 1`,
    [companyKey]
  );
  return rows[0] || null;
}

async function upsertCompany({
  companyKey,
  authorName,
  authorLogoUrl,
  followerCount,
}) {
  const existing = await getCompanyByKey(companyKey);
  if (existing) {
    await pool.query(
      `UPDATE companies SET author_name=?, author_logo_url=?, follower_count=?, updated_at=NOW() WHERE id=?`,
      [
        authorName || existing.author_name,
        authorLogoUrl || existing.author_logo_url,
        followerCount ?? existing.follower_count,
        existing.id,
      ]
    );
    return existing.id;
  } else {
    const [res] = await pool.query(
      `INSERT INTO companies (company_key, author_name, author_logo_url, follower_count, updated_at) VALUES (?,?,?,?,NOW())`,
      [
        companyKey,
        authorName || null,
        authorLogoUrl || null,
        followerCount ?? null,
      ]
    );
    return res.insertId;
  }
}

async function insertPostIfNew({
  companyId,
  activityUrn,
  fullUrn,
  postUrl,
  postText,
  postedAt,
}) {
  // Check by unique post_url
  const [found] = await pool.query(
    `SELECT id FROM posts WHERE post_url = ? LIMIT 1`,
    [postUrl]
  );
  if (found.length) {
    return found[0].id; // already saved
  }
  const [res] = await pool.query(
    `INSERT INTO posts (company_id, activity_urn, full_urn, post_url, post_text, posted_at) VALUES (?,?,?,?,?,?)`,
    [
      companyId,
      activityUrn || null,
      fullUrn || null,
      postUrl,
      postText || null,
      postedAt ? new Date(postedAt) : null,
    ]
  );
  return res.insertId;
}

async function replaceMediaForPost(postId, mediaItems) {
  await pool.query(`DELETE FROM media WHERE post_id = ?`, [postId]);
  if (!Array.isArray(mediaItems) || mediaItems.length === 0) return;
  const values = mediaItems.map((m) => [
    postId,
    m.url,
    m.width || null,
    m.height || null,
  ]);
  await pool.query(
    `INSERT INTO media (post_id, url, width, height) VALUES ${values
      .map(() => "(?, ?, ?, ?)")
      .join(", ")}`,
    values.flat()
  );
}

async function getLatestSavedFromDb(companyKey) {
  const [rows] = await pool.query(
    `
    SELECT p.id as post_id, c.company_key, c.author_name, c.author_logo_url, c.follower_count,
           p.activity_urn, p.full_urn, p.post_url, p.post_text, p.posted_at, p.created_at
      FROM companies c
      JOIN posts p ON p.company_id = c.id
     WHERE c.company_key = ?
     ORDER BY p.created_at DESC
     LIMIT 1
    `,
    [companyKey]
  );
  if (!rows.length) return null;

  const post = rows[0];
  const [mediaRows] = await pool.query(
    `SELECT url, width, height FROM media WHERE post_id = ?`,
    [post.post_id]
  );
  post.media = mediaRows || [];
  return post;
}

async function latestSavedIsFresh(companyKey, withinMs) {
  const [rows] = await pool.query(
    `
    SELECT p.created_at
      FROM companies c
      JOIN posts p ON p.company_id = c.id
     WHERE c.company_key = ?
     ORDER BY p.created_at DESC
     LIMIT 1
    `,
    [companyKey]
  );
  if (!rows.length) return false;
  const created = new Date(rows[0].created_at).getTime();
  return Date.now() - created < withinMs;
}

// ---- Apify actor helpers ----

// v1 client: pass the INPUT OBJECT DIRECTLY to .call(inputObject)
async function runApifyActorRaw({ company, page = 1 }) {
  // Per actor docs: expects "company" and "page_number"
  const input = {
    company_name: company, // e.g. "the-think-tank_2" or "https://www.linkedin.com/company/the-think-tank_2/"
    page_number: page, // default = 1
    limit: 2,
  };

  const run = await client.actor(APIFY_ACTOR_ID).call(input);
  logger.info(
    `Apify run: actor=${APIFY_ACTOR_ID} runId=${
      run?.id || "n/a"
    } company=${company} page=${page}`
  );
  return run;
}

async function getAllItemsFromRun(run) {
  if (run?.defaultDatasetId) {
    const { items } = await client
      .dataset(run.defaultDatasetId)
      .listItems({ clean: true, limit: 500 });
    return items || [];
  }
  if (run?.output) return run.output;
  return [];
}

async function getLatestItemFromRun(run) {
  if (run?.defaultDatasetId) {
    const { items } = await client
      .dataset(run.defaultDatasetId)
      .listItems({ clean: true, limit: 1, desc: true });
    return items?.[0] || null;
  }
  if (Array.isArray(run?.output) && run.output.length) {
    // naive newest-first by posted_at/date
    const pick = (x) => x?.posted_at?.date || x?.posted_at || x?.date || null;
    return (
      [...run.output].sort(
        (a, b) => new Date(pick(b) || 0) - new Date(pick(a) || 0)
      )[0] || null
    );
  }
  return null;
}

// Save a single item (latest) to DB only if it's actually new
async function saveLatestToDb(companyKey, item) {
  if (!item) return null;

  // Check if this post is already the latest in our DB
  const existingLatest = await getLatestSavedFromDb(companyKey);
  if (existingLatest && existingLatest.post_url === item.post_url) {
    // Same post URL, just return the existing record
    return existingLatest;
  }

  const authorName = item?.author?.name || null;
  const authorLogoUrl = item?.author?.logo_url || null;
  const followerCount = Number.isFinite(item?.author?.follower_count)
    ? item.author.follower_count
    : null;

  const companyId = await upsertCompany({
    companyKey,
    authorName,
    authorLogoUrl,
    followerCount,
  });

  const postId = await insertPostIfNew({
    companyId,
    activityUrn: item.activity_urn || null,
    fullUrn: item.full_urn || null,
    postUrl: item.post_url,
    postText: item.text || null,
    postedAt: item?.posted_at?.date || null,
  });

  // media
  const mediaItems = (item?.media?.items || [])
    .filter((m) => m?.url)
    .map((m) => ({
      url: m.url,
      width: m.width || null,
      height: m.height || null,
    }));

  await replaceMediaForPost(postId, mediaItems);

  // return the saved record for convenience
  return await getLatestSavedFromDb(companyKey);
}

// ---- Router ----
const router = express.Router();

router.get("/", (_req, res) => {
  const base = BASE_API_URL || "";
  res.json({
    message: "LinkedIn Company Posts via Apify (persisted, daily gate)",
    version: "3.0.0",
    actor: APIFY_ACTOR_ID,
    endpoints: {
      latest: `${base}/latest?company=the-think-tank_2&page=1`,
      scrape: `${base}/scrape?company=the-think-tank_2&page=1`,
      health: `${base}/health`,
    },
    notes: [
      "Runs Apify at most once per company per day (configurable via DAILY_WINDOW_MS).",
      "Persists author + post + media to MySQL; subsequent calls serve from DB.",
      "Use force=true to bypass the daily gate for manual refresh.",
    ],
  });
});

router.get("/health", async (_req, res) => {
  try {
    const [db] = await pool.query("SELECT 1 AS ok");
    await client.user().getUser(); // token sanity check
    res.json({
      status: "OK",
      db: db[0].ok === 1,
      actor: APIFY_ACTOR_ID,
      timestamp: new Date().toISOString(),
    });
  } catch (e) {
    res.status(500).json({ status: "ERR", error: e.message });
  }
});

// Returns MANY items from actor; still respects 'force' for daily gate on saving newest
router.get(
  "/scrape",
  [
    query("company")
      .notEmpty()
      .trim()
      .withMessage("company is required (slug or full URL)"),
    query("page").optional().isInt({ min: 1 }).toInt(),
    query("force").optional().isBoolean().toBoolean(),
  ],
  async (req, res) => {
    const errors = validationResult(req);
    if (!errors.isEmpty())
      return res.status(400).json({ errors: errors.array() });

    const company = req.query.company;
    const page = req.query.page ? parseInt(req.query.page, 10) : 1;
    const force = !!req.query.force;

    try {
      // If not forced and fresh exists, return DB copy
      if (!force && (await latestSavedIsFresh(company, DAILY_WINDOW_MS))) {
        const saved = await getLatestSavedFromDb(company);
        return res.json({ company, page, source: "db", latest: saved });
      }

      const run = await runApifyActorRaw({ company, page });
      const allItems = await getAllItemsFromRun(run);
      // Save only the latest item
      const latestItem = await getLatestItemFromRun(run);
      const saved = await saveLatestToDb(company, latestItem);

      res.json({
        company,
        page,
        source: "apify",
        latest_saved: saved,
        results: allItems,
      });
    } catch (err) {
      logger.warn(
        `Apify /scrape failed for ${company} p${page}: ${err.message}`
      );
      res.status(502).json({ error: "Apify run failed", detail: err.message });
    }
  }
);

// Returns ONLY newest item; daily gate by default
router.get(
  "/latest",
  [
    query("company")
      .notEmpty()
      .trim()
      .withMessage("company is required (slug or full URL)"),
    query("page").optional().isInt({ min: 1 }).toInt(),
    query("force").optional().isBoolean().toBoolean(),
  ],
  async (req, res) => {
    const errors = validationResult(req);
    if (!errors.isEmpty())
      return res.status(400).json({ errors: errors.array() });

    const company = req.query.company;
    const page = req.query.page ? parseInt(req.query.page, 10) : 1;
    const force = !!req.query.force;

    try {
      // Daily gate
      if (!force && (await latestSavedIsFresh(company, DAILY_WINDOW_MS))) {
        const saved = await getLatestSavedFromDb(company);
        return res.json({ company, page, source: "db", result: saved });
      }

      // Fresh run
      const run = await runApifyActorRaw({ company, page });
      const latest = await getLatestItemFromRun(run);
      const saved = await saveLatestToDb(company, latest);

      res.json({ company, page, source: "apify", result: saved });
    } catch (err) {
      logger.warn(
        `Apify /latest failed for ${company} p${page}: ${err.message}`
      );
      res.status(502).json({ error: "Apify run failed", detail: err.message });
    }
  }
);

// Mount
if (BASE_API_URL && BASE_API_URL.trim() !== "") {
  logger.info(`Mounting router at ${BASE_API_URL}`);
  app.use(BASE_API_URL, router);
} else {
  logger.info("Mounting router at /");
  app.use("/", router);
}

// 404
app.use((_, res) => res.status(404).json({ error: "Not Found" }));

// Start
(async () => {
  await initDb();
  app.listen(PORT, () => {
    logger.info(`Server listening on :${PORT} (actor=${APIFY_ACTOR_ID})`);
  });
})();
