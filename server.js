const express = require("express");
const rateLimit = require("express-rate-limit");
const winston = require("winston");
const dotenv = require("dotenv");
const { query, validationResult } = require("express-validator");
const cors = require("cors");

dotenv.config();

const NODE_ENV = process.env.NODE_ENV || 'development';

// Puppeteer setup optimized for Render deployment
const puppeteer = require("puppeteer");

const app = express();

// Configure Express to trust proxy (more secure for cPanel hosting)
// Trust only the first proxy (cPanel's reverse proxy)
app.set('trust proxy', 1);

const PORT = process.env.PORT || 3000;
const BASE_API_URL = process.env.BASE_API_URL || '';
const LI_AT = process.env.LI_AT;
const JSESSIONID = process.env.JSESSIONID;
const LINKEDIN_EMAIL = process.env.LINKEDIN_EMAIL;
const LINKEDIN_PASSWORD = process.env.LINKEDIN_PASSWORD;
const MIN_DELAY = parseInt(process.env.MIN_DELAY) || 2000;  // Reduced from 15s to 2s
const MAX_DELAY = parseInt(process.env.MAX_DELAY) || 5000;   // Reduced from 60s to 5s
const MAX_RETRY_DELAY = parseInt(process.env.MAX_RETRY_DELAY) || 30000; // Reduced from 10min to 30s

// Logger setup
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: "error.log", level: "error" }),
    new winston.transports.File({ filename: "combined.log" }),
    new winston.transports.Console(),
  ],
});

// Middleware
app.use(cors());
app.use(express.json());

// Debug middleware to log all requests to error.log
app.use((req, res, next) => {
  logger.error(`DEBUG REQUEST: ${new Date().toISOString()} - ${req.method} ${req.url}`);
  logger.error(`DEBUG BASE_API_URL: "${BASE_API_URL}"`);
  logger.error(`DEBUG Original URL: ${req.originalUrl}`);
  logger.error(`DEBUG Request Path: ${req.path}`);
  logger.error(`DEBUG Headers: ${JSON.stringify(req.headers)}`);
  next();
});
// Rate limiting with proxy support
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 50,
  message: { error: "Too many requests, please try again later." },
  standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
  legacyHeaders: false, // Disable the `X-RateLimit-*` headers
  // Custom key generator to handle proxy properly
  keyGenerator: (req) => {
    // Use the real IP from X-Forwarded-For (first IP) or fallback to connection IP
    return req.ip;
  },
  skip: (req) => {
    // Optional: Skip rate limiting for health checks
    return req.path === '/health';
  },
});

app.use(limiter);

// Random delay function
function randomDelay(min, max) {
  return new Promise((resolve) =>
    setTimeout(resolve, Math.random() * (max - min) + min)
  );
}

// Cookie cache to avoid unnecessary refreshes
let cookieCache = null;
let cookieCacheTime = 0;
const COOKIE_CACHE_DURATION = 30 * 60 * 1000; // 30 minutes

// Cookie refresh function (enhanced debugging)
async function refreshCookies() {
  // Check if we have cached cookies that are still valid
  const now = Date.now();
  if (cookieCache && (now - cookieCacheTime) < COOKIE_CACHE_DURATION) {
    logger.info("Using cached cookies");
    return cookieCache;
  }

  if (!LINKEDIN_EMAIL || !LINKEDIN_PASSWORD) {
    logger.warn("No LinkedIn credentials provided; using existing cookies");
    if (!LI_AT || !JSESSIONID) {
      throw new Error(
        "No valid LI_AT or JSESSIONID in .env and no credentials provided"
      );
    }
    const cookies = { li_at: LI_AT, jsessionid: JSESSIONID };
    cookieCache = cookies;
    cookieCacheTime = now;
    return cookies;
  }

  let browser;
  try {
    browser = await puppeteer.launch({
      headless: "new",
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-accelerated-2d-canvas",
        "--disable-gpu",
        "--disable-web-security",
        "--disable-features=WebRtc,MediaStream",
        "--no-zygote",
        "--disable-background-media",
      ],
    });
    const page = await browser.newPage();

    await page.setViewport({
      width: 1366 + Math.floor(Math.random() * 100),
      height: 768 + Math.floor(Math.random() * 100),
    });
    await page.setExtraHTTPHeaders({
      "Accept-Language": "en-US,en;q=0.9",
      "Accept-Encoding": "gzip, deflate, br",
      DNT: "1",
      Connection: "keep-alive",
      "Upgrade-Insecure-Requests": "1",
    });

    logger.info("Navigating directly to LinkedIn login page");
    await page.goto("https://www.linkedin.com/login", {
      waitUntil: "domcontentloaded",
      timeout: 20000,
    });
    await randomDelay(200, 500);

    logger.info("Entering login credentials");
    await page.type("#username", LINKEDIN_EMAIL, { delay: 50 });
    await page.type("#password", LINKEDIN_PASSWORD, { delay: 50 });
    await page.click('[type="submit"]');

    // Wait for navigation or check if we're already logged in
    try {
      await page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 });
    } catch (navigationError) {
      const currentUrl = page.url();
      const htmlSnippet = await page.evaluate(() =>
        document.body.innerHTML.substring(0, 500)
      );
      logger.info(
        `Login navigation failed. Current URL: ${currentUrl}, HTML: ${htmlSnippet}`
      );

      // Check if we're already on a LinkedIn authenticated page (feed, etc.)
      if (currentUrl.includes("linkedin.com/feed") || currentUrl.includes("linkedin.com/in/")) {
        logger.info("Login successful - already on authenticated LinkedIn page");
        // Continue with cookie extraction
      } else if (
        currentUrl.includes("checkpoint") ||
        (await page.$("#twoFactorInput")) ||
        (await page.$(".checkpoint-container"))
      ) {
        throw new Error(
          "CAPTCHA or security check detected - please update LI_AT and JSESSIONID manually"
        );
      } else {
        // Check for login error (e.g., incorrect password)
        const errorMessage = await page.evaluate(() => {
          const error = document.querySelector(
            ".error, .alert, #error-for-username, #error-for-password"
          );
          return error ? error.innerText : null;
        });
        if (errorMessage) {
          throw new Error(`Login failed: ${errorMessage}`);
        }

        throw new Error(
          "Login navigation timeout - check credentials or LinkedIn login page"
        );
      }
    }

    await randomDelay(200, 500);

    const cookies = await page.cookies();
    const liAt = cookies.find((c) => c.name === "li_at")?.value;
    const jsessionid = cookies.find((c) => c.name === "JSESSIONID")?.value;

    if (!liAt || !jsessionid) {
      const htmlSnippet = await page.evaluate(() =>
        document.body.innerHTML.substring(0, 500)
      );
      logger.error(
        `Failed to retrieve cookies. Page URL: ${page.url()}, HTML: ${htmlSnippet}`
      );
      throw new Error("Failed to retrieve li_at or JSESSIONID after login");
    }

    logger.info("Successfully refreshed LinkedIn cookies");
    const freshCookies = { li_at: liAt, jsessionid: jsessionid };

    // Cache the cookies
    cookieCache = freshCookies;
    cookieCacheTime = Date.now();

    return freshCookies;
  } catch (error) {
    logger.error(`Cookie refresh failed: ${error.message}`);
    throw error;
  } finally {
    if (browser) await browser.close();
  }
}

// Scrape function
async function scrapeCompanyPosts(companyName, maxPosts = 5) {
  let browser;
  try {
    const { default: retry } = await import("p-retry");

    // Simple Puppeteer launch - let it handle Chrome automatically
    browser = await puppeteer.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-web-security",
        "--disable-features=WebRtc,MediaStream",
        "--no-first-run",
        "--no-default-browser-check",
      ],
    });

    // Get cookies once outside the retry loop
    let cookies;
    try {
      cookies = await refreshCookies();
    } catch (error) {
      logger.warn(
        `Falling back to provided cookies due to refresh failure: ${error.message}`
      );
      if (!LI_AT || !JSESSIONID) {
        throw new Error(
          "No valid LI_AT or JSESSIONID in .env and cookie refresh failed"
        );
      }
      cookies = { li_at: LI_AT, jsessionid: JSESSIONID };
    }

    const posts = await retry(
      async (attempt) => {
        const page = await browser.newPage();

        await page.setViewport({
          width: 1366 + Math.floor(Math.random() * 100),
          height: 768 + Math.floor(Math.random() * 100),
        });
        await page.setExtraHTTPHeaders({
          "Accept-Language": "en-US,en;q=0.9",
          "Accept-Encoding": "gzip, deflate, br",
          DNT: "1",
          Connection: "keep-alive",
          "Upgrade-Insecure-Requests": "1",
        });

        await page.setCookie(
          { name: "li_at", value: cookies.li_at, domain: ".linkedin.com" },
          {
            name: "JSESSIONID",
            value: cookies.jsessionid,
            domain: ".linkedin.com",
          },
          {
            name: "bcookie",
            value: "v=2&f5e01239-089a-4a8a-863c-0d36ff5be4fb",
            domain: ".linkedin.com",
          },
          {
            name: "bscookie",
            value:
              "v=1&202508141500209f070c49-62f9-43e7-8a12-e3648f8a42e0AQH-Q8rR3rPSrQo4V8xO2Kx5Au8MgmT_",
            domain: ".linkedin.com",
          },
          {
            name: "lidc",
            value:
              "b=TB83:s=T:r=T:a=T:p=T:g=4098:u=1478:x=1:i=1757690422:t=1757776803:v=2:sig=AQF3mWN9k-ft4K-2PKjiDuuyXxXPvfLM",
            domain: ".linkedin.com",
          }
        );

        await randomDelay(200, 500);

        const url = `https://www.linkedin.com/company/${companyName}/posts/?feedView=all`;
        logger.info(`Navigating to ${url} (attempt ${attempt})`);
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

        // Wait specifically for posts to load instead of full network idle
        await page.waitForSelector(
          ".feed-shared-update-v2, .occludable-update",
          { timeout: 15000 }
        ).catch(() => {
          // If posts don't load quickly, continue anyway - the error handling below will catch it
        });

        await page.mouse.move(Math.random() * 800, Math.random() * 600);

        const isRateLimited = await page.evaluate(() => {
          return (
            document.body.innerText.includes("HTTP ERROR 429") ||
            document.body.innerText.includes("Too Many Requests")
          );
        });
        if (isRateLimited) {
          const waitTime = Math.min(
            Math.pow(2, attempt) * 60000,
            MAX_RETRY_DELAY
          );
          logger.warn(
            `Rate limit (429) detected on attempt ${attempt}. Waiting ${
              waitTime / 60000
            } minutes.`
          );
          throw new Error(`Rate limited - retry after ${waitTime}ms`);
        }

        const isLoginPage = await page.evaluate(() => {
          return (
            document.querySelector('input[name="session_key"]') !== null ||
            document.body.innerText.includes(
              "Sign in to your LinkedIn account"
            ) ||
            document.body.innerText.includes("Agree & Join LinkedIn") ||
            document.body.innerText.includes("one-time link")
          );
        });

        if (isLoginPage) {
          const html = await page.evaluate(() => document.body.innerHTML);
          logger.error(
            `Login page detected for ${companyName}: ${html.substring(
              0,
              500
            )}...`
          );
          throw new Error(
            "Redirected to login page. Please update LI_AT and JSESSIONID manually."
          );
        }

        // Check if posts are already loaded, otherwise wait briefly
        const postsExist = await page.$(".feed-shared-update-v2, .occludable-update");
        if (!postsExist) {
          await page
            .waitForSelector(
              ".feed-shared-update-v2, .occludable-update",
              { timeout: 10000 }
            )
            .catch(async () => {
              const html = await page.evaluate(() => document.body.innerHTML);
              logger.error(
                `No posts found for ${companyName}: ${html.substring(0, 500)}...`
              );
              throw new Error("No posts found or company page does not exist");
            });
        }

        // Scroll intelligently until we have enough posts
        let currentPostCount = 0;
        let scrollAttempts = 0;
        const maxScrollAttempts = 5;

        do {
          const newPostCount = await page.evaluate(() => {
            return document.querySelectorAll(".feed-shared-update-v2, .occludable-update").length;
          });

          if (newPostCount >= maxPosts) {
            break;
          }

          if (newPostCount === currentPostCount && scrollAttempts > 1) {
            // No new posts loaded after scrolling, break to avoid infinite loop
            break;
          }

          currentPostCount = newPostCount;

          await page.evaluate(() =>
            window.scrollTo(0, document.body.scrollHeight)
          );
          await randomDelay(500, 1000);
          scrollAttempts++;
        } while (scrollAttempts < maxScrollAttempts);

        const posts = await page.evaluate((maxPosts) => {
          // Use more specific selector to avoid duplicates
          const postElements = document.querySelectorAll(
            ".feed-shared-update-v2:not(.feed-shared-update-v2 .feed-shared-update-v2)"
          );
          const results = [];
          const processedUrns = new Set();

          console.log(`Found ${postElements.length} post elements`);

          // Process posts until we have maxPosts valid ones
          let i = 0;
          while (results.length < maxPosts && i < postElements.length) {
            const post = postElements[i];

            // Check for duplicate posts using URN
            const urn = post.getAttribute("data-urn") || post.getAttribute("data-id");
            if (urn && processedUrns.has(urn)) {
              console.log(`Skipping duplicate post with URN: ${urn}`);
              i++;
              continue;
            }
            if (urn) processedUrns.add(urn);

            // Enhanced text extraction with more selectors
            const text =
              post
                .querySelector(
                  ".feed-shared-update-v2__description-wrapper .attributed-text-segment-list__content, .feed-shared-text .attributed-text-segment-list__content, .feed-shared-update-v2__description .attributed-text-segment-list__content"
                )
                ?.innerText?.trim() ||
              post
                .querySelector(
                  ".feed-shared-update-v2__description-wrapper, .feed-shared-text, .feed-shared-inline-show-more-text"
                )
                ?.innerText?.trim() || null;

            // Enhanced author extraction
            const author =
              post
                .querySelector(
                  ".feed-shared-actor__name .visually-hidden"
                )
                ?.innerText?.trim() ||
              post
                .querySelector(
                  ".feed-shared-actor__name, .update-components-actor__name"
                )
                ?.innerText?.trim() || null;

            // Fixed date extraction - target actual time elements
            let date = null;
            const timeElement = post.querySelector("time.feed-shared-actor__sub-description");
            if (timeElement) {
              date = timeElement.getAttribute("datetime") || timeElement.innerText?.trim();
            } else {
              // Fallback to other date selectors
              const dateEl = post.querySelector(".feed-shared-actor__sub-description, .feed-shared-actor__description");
              if (dateEl && !dateEl.innerText.includes("Feed post number")) {
                date = dateEl.innerText?.trim();
              }
            }

            // Enhanced likes extraction
            const likes =
              post
                .querySelector(
                  ".social-details-social-counts__reactions-count, .feed-shared-social-action-bar__reaction-count"
                )
                ?.innerText?.trim() || "0";

            // Enhanced comments extraction
            const comments =
              post
                .querySelector(
                  ".social-details-social-counts__comments, .feed-shared-social-action-bar__comment-count"
                )
                ?.innerText?.split(" ")[0] || "0";

            // Improved permalink extraction
            let permalink = null;
            if (urn && urn.includes("urn:li:activity:")) {
              const activityId = urn.replace("urn:li:activity:", "");
              permalink = `https://www.linkedin.com/feed/update/urn:li:activity:${activityId}/`;
            } else {
              // Try to find permalink from share button or other elements
              const shareBtn = post.querySelector("[aria-label*='Share']");
              if (shareBtn) {
                const parentLink = shareBtn.closest('a');
                if (parentLink && parentLink.href) {
                  permalink = parentLink.href;
                }
              }
            }

            // Enhanced image extraction
            let imageUrl = null;
            const image = post.querySelector(
              ".feed-shared-image img, .feed-shared-update-v2__content img"
            );
            if (image) {
              imageUrl =
                image.getAttribute("src") ||
                image.getAttribute("data-delayed-url") ||
                image.getAttribute("data-src");
            }

            // Enhanced video extraction
            let videoUrl = null;
            const video = post.querySelector(
              ".feed-shared-video video, .feed-shared-update-v2__content video"
            );
            if (video) {
              videoUrl =
                video.getAttribute("src") ||
                video.querySelector("source")?.getAttribute("src");
            }

            // Only include posts that have meaningful content
            const hasContent = text || author || imageUrl || videoUrl || (date && !date.includes("Feed post number"));

            if (hasContent) {
              results.push({
                text,
                author,
                date,
                likes,
                comments,
                permalink,
                imageUrl,
                videoUrl,
              });
            } else {
              console.log(`Skipping empty post ${i}:`, {
                text: !!text,
                author: !!author,
                date: date,
                imageUrl: !!imageUrl,
                videoUrl: !!videoUrl
              });
            }
            i++;
          }

          console.log(`Returning ${results.length} posts after filtering`);
          return results;
        }, maxPosts);

        if (posts.length === 0) {
          logger.warn(`No valid posts extracted for ${companyName}`);
        }

        logger.info(
          `Scraped ${posts.length} posts for company: ${companyName}`
        );
        return posts;
      },
      {
        retries: 2,  // Reduced retries for faster failure
        minTimeout: 2000,  // Faster retry timing
        maxTimeout: MAX_RETRY_DELAY,
        onFailedAttempt: (error) => {
          const errorDetails = error.message || (error instanceof Error ? error.stack : JSON.stringify(error, Object.getOwnPropertyNames(error))) || 'Unknown error';
          logger.warn(
            `Retry attempt failed for ${companyName}: ${errorDetails}`
          );
        },
      }
    );

    return posts;
  } catch (error) {
    logger.error(`Error scraping ${companyName}: ${error.message}`);
    throw error;
  } finally {
    if (browser) await browser.close();
  }
}

// Create router for API routes
const router = express.Router();

// Root API endpoint
router.get("/", (req, res) => {
  const baseUrl = BASE_API_URL || '';
  res.json({
    message: "LinkedIn Company Scraper API",
    version: "1.0.0",
    endpoints: {
      scrape: `${baseUrl}/scrape?company=COMPANY_NAME`,
      health: `${baseUrl}/health`
    }
  });
});

// Health check endpoint
router.get("/health", (req, res) => {
  res.json({ status: "OK", timestamp: new Date().toISOString() });
});

// Scraping endpoint
router.get(
  "/scrape",
  [
    query("company")
      .notEmpty()
      .trim()
      .withMessage("Company parameter is required"),
  ],
  async (req, res) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const companyName = req.query.company;

    try {
      const posts = await scrapeCompanyPosts(companyName);
      res.json({ company: companyName, posts });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }
);

// Mount router on base path
// If BASE_API_URL is empty, mount at root for custom subdomain (api.intranet.thinktank.org.uk)
if (BASE_API_URL) {
  logger.error(`DEBUG: Mounting router on path: "${BASE_API_URL}"`);
  app.use(BASE_API_URL, router);
} else {
  logger.error(`DEBUG: Mounting router on root path "/"`);
  app.use('/', router);
}

// Error handling for invalid routes
app.use((_, res) => {
  res.status(404).json({ error: "Not Found" });
});

// Start server
app.listen(PORT, () => {
  logger.info(`Server running on port ${PORT}`);
});
