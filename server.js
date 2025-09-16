// server.js
// Minimal API that proxies Apify actor: apimaestro/linkedin-company-posts
// Endpoints:
//   GET /scrape?company=<slug-or-url>&page=<number>     -> returns many items
//   GET /latest?company=<slug-or-url>&page=<number>     -> returns ONLY the newest item

"use strict";

const express = require("express");
const cors = require("cors");
const rateLimit = require("express-rate-limit");
const dotenv = require("dotenv");
const winston = require("winston");
const { query, validationResult } = require("express-validator");
// IMPORTANT: using v1 client (unscoped)
const { ApifyClient } = require("apify-client");

dotenv.config();

const PORT = parseInt(process.env.PORT || "10000", 10);
const BASE_API_URL = process.env.BASE_API_URL || "";
const APIFY_TOKEN = process.env.APIFY_TOKEN; // required
const APIFY_ACTOR_ID =
  process.env.APIFY_ACTOR_ID || "apimaestro/linkedin-company-posts";
const COMPANY_CACHE_TTL_MS = parseInt(
  process.env.COMPANY_CACHE_TTL_MS || "180000",
  10
); // 3 min
const APIFY_TIMEOUT_SECS = parseInt(
  process.env.APIFY_TIMEOUT_SECS || "120",
  10
); // actor run timeout

if (!APIFY_TOKEN) {
  console.error("Missing APIFY_TOKEN env var. Set it in Render > Environment.");
  process.exit(1);
}

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

const app = express();
app.set("trust proxy", 1);
app.use(cors());
app.use(express.json());

// Rate limit
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

// In-memory cache
const cache = new Map(); // key -> { ts, data }
const getCache = (k) => {
  const v = cache.get(k);
  if (!v) return null;
  if (Date.now() - v.ts > COMPANY_CACHE_TTL_MS) {
    cache.delete(k);
    return null;
  }
  return v.data;
};
const setCache = (k, data) => cache.set(k, { ts: Date.now(), data });

const client = new ApifyClient({ token: APIFY_TOKEN });

async function runApifyActorRaw({ company, page = 1 }) {
  // Exact keys from the actor docs
  const input = {
    company_name: company, // e.g. "the-think-tank_2" or "https://www.linkedin.com/company/the-think-tank_2/"
    page_number: page, // default = 1
    limit: 2,
  };

  // apify-client v1: pass input directly
  const run = await client.actor(APIFY_ACTOR_ID).call(input);

  logger.info(
    `Apify run started: actor=${APIFY_ACTOR_ID} runId=${
      run?.id || "n/a"
    } company=${company} page=${page}`
  );

  return run;
}

/** Returns ALL items from a run (used by /scrape) */
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

/** Returns ONLY the latest (newest) item from a run (used by /latest) */
async function getLatestItemFromRun(run) {
  if (run?.defaultDatasetId) {
    const { items } = await client
      .dataset(run.defaultDatasetId)
      .listItems({ clean: true, limit: 1, desc: true });
    return items?.[0] || null;
  }
  if (Array.isArray(run?.output) && run.output.length) {
    const pickDate = (x) =>
      x?.publishedAt ||
      x?.postDate ||
      x?.postedAt ||
      x?.createdAt ||
      x?.date ||
      null;
    const sorted = [...run.output].sort((a, b) => {
      const da = new Date(pickDate(a) || 0).getTime();
      const db = new Date(pickDate(b) || 0).getTime();
      return db - da;
    });
    return sorted[0] || null;
  }
  return null;
}

// Router
const router = express.Router();

router.get("/", (_req, res) => {
  const base = BASE_API_URL || "";
  res.json({
    message: "LinkedIn Company Posts via Apify (No Cookies actor)",
    version: "1.1.1",
    actor: APIFY_ACTOR_ID,
    endpoints: {
      scrape_all: `${base}/scrape?company=the-think-tank_2&page=1`,
      latest_one: `${base}/latest?company=the-think-tank_2&page=1`,
      health: `${base}/health`,
    },
    notes: [
      "Using apify-client v1: pass input DIRECTLY to .call(input).",
      "Use /latest to fetch ONLY the newest post (1 item).",
      "Short TTL cache (default 3 min) on both endpoints.",
    ],
  });
});

router.get("/health", async (_req, res) => {
  try {
    const me = await client.user().getUser(); // token sanity check
    res.json({
      status: "OK",
      actor: APIFY_ACTOR_ID,
      user: me?.username || null,
      timestamp: new Date().toISOString(),
    });
  } catch (e) {
    res.status(500).json({ status: "ERR", error: e.message });
  }
});

// Returns MANY items (cached)
router.get(
  "/scrape",
  [
    query("company")
      .notEmpty()
      .trim()
      .withMessage("company is required (slug or full URL)"),
    query("page").optional().isInt({ min: 1 }).toInt(),
  ],
  async (req, res) => {
    const errors = validationResult(req);
    if (!errors.isEmpty())
      return res.status(400).json({ errors: errors.array() });

    const company = req.query.company;
    const page = req.query.page ? parseInt(req.query.page, 10) : 1;
    const cacheKey = `all:${company}:${page}`;

    const cached = getCache(cacheKey);
    if (cached)
      return res.json({ company, page, cached: true, results: cached });

    try {
      const run = await runApifyActorRaw({ company, page });
      const items = await getAllItemsFromRun(run);
      setCache(cacheKey, items);
      res.json({ company, page, cached: false, results: items });
    } catch (err) {
      logger.warn(
        `Apify /scrape failed for ${company} p${page}: ${err.message}`
      );
      res.status(502).json({ error: "Apify run failed", detail: err.message });
    }
  }
);

// Returns ONLY the newest item (cached)
router.get(
  "/latest",
  [
    query("company")
      .notEmpty()
      .trim()
      .withMessage("company is required (slug or full URL)"),
    query("page").optional().isInt({ min: 1 }).toInt(),
  ],
  async (req, res) => {
    const errors = validationResult(req);
    if (!errors.isEmpty())
      return res.status(400).json({ errors: errors.array() });

    const company = req.query.company;
    const page = req.query.page ? parseInt(req.query.page, 10) : 1;
    const cacheKey = `latest:${company}:${page}`;

    const cached = getCache(cacheKey);
    if (cached)
      return res.json({ company, page, cached: true, result: cached });

    try {
      const run = await runApifyActorRaw({ company, page });
      const latest = await getLatestItemFromRun(run);
      setCache(cacheKey, latest);
      res.json({ company, page, cached: false, result: latest });
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

app.listen(PORT, () => {
  logger.info(`Server listening on :${PORT} (actor=${APIFY_ACTOR_ID})`);
});
