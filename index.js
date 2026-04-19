const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const { parse } = require("csv-parse/sync");
const { stringify } = require("csv-stringify/sync");
const { spawnSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const {
  buildApolloBatchFromCombinedUrl,
  runApolloScrape,
  writeApolloInputCsv,
} = require("./apollo_camoufox");

puppeteer.use(StealthPlugin());

const KASPR_EXTENSION_ID = "kkfgenjfpmoegefcckjklfjieepogfhg";
const RESOLVED_EXTENSION = resolveKasprExtensionPath();

function parseBooleanEnv(value, fallback = false) {
  if (value == null || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

function parseHumanizeEnv(value, fallback = true) {
  if (value == null || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;

  const numeric = Number(normalized);
  if (Number.isFinite(numeric) && numeric >= 0) {
    return numeric;
  }

  return fallback;
}

function normalizePipelineMode(value) {
  const normalized = (value || "").trim().toLowerCase();
  if (!normalized) return "";
  if (["scrape", "scrape-only", "scrape_only"].includes(normalized)) return "scrape";
  if (["email", "email-only", "email_only"].includes(normalized)) return "email";
  if (["apollo-only", "apollo_only"].includes(normalized)) return "apollo-only";
  if (["apollo-full", "apollo_full"].includes(normalized)) return "apollo-full";
  if (["full", "pipeline", "all"].includes(normalized)) return "full";
  return normalized;
}

// ─── CONFIG ───────────────────────────────────────────────────────────
const CONFIG = {
  kasprEmail: process.env.KASPR_EMAIL || "ajakki@purdue.edu",
  kasprPassword: process.env.KASPR_PASSWORD || "Ashwin@387",

  extensionPath: RESOLVED_EXTENSION.path,
  extensionSource: RESOLVED_EXTENSION.source,
  extensionCandidates: RESOLVED_EXTENSION.candidates,

  userDataDir:
    process.env.CHROME_USER_DATA_DIR ||
    path.join(process.env.HOME, "Library/Application Support/Google/Chrome"),

  inputCsv: process.env.INPUT_CSV || "linkedin_urls.csv",
  urlColumn: process.env.URL_COLUMN || "linkedinUrl",
  outputCsv: process.env.OUTPUT_CSV || "results.csv",

  // Delay between API calls (ms) — randomized between min and max
  minDelay: parseInt(process.env.MIN_DELAY_MS || "250", 10),
  maxDelay: parseInt(process.env.MAX_DELAY_MS || "750", 10),

  // Short setup waits to let the extension attach without dragging the run
  browserReadyDelay: parseInt(process.env.BROWSER_READY_DELAY_MS || "1000", 10),
  profileSettleDelay: parseInt(process.env.PROFILE_SETTLE_DELAY_MS || "250", 10),
  revealPollInterval: parseInt(process.env.REVEAL_POLL_INTERVAL_MS || "250", 10),
  postShowDelay: parseInt(process.env.POST_SHOW_DELAY_MS || "750", 10),
  pageNavigationTimeout: parseInt(process.env.PAGE_NAVIGATION_TIMEOUT_MS || "10000", 10),

  // Max profiles to process per run
  maxProfiles: parseInt(process.env.MAX_PROFILES || "9999", 10),

  // How long to wait for Kaspr widget to appear and finish loading (ms)
  kaspWidgetTimeout: 60_000,

  autoEmailEnabled: parseBooleanEnv(process.env.AUTO_EMAIL_AFTER_SCRAPE, true),
  pipelineMode:
    normalizePipelineMode(process.env.PIPELINE_MODE) ||
    (parseBooleanEnv(process.env.AUTO_EMAIL_AFTER_SCRAPE, true) ? "full" : "scrape"),
  autoEmailDir: path.resolve(
    expandHome(process.env.AUTO_EMAIL_DIR || path.join(process.cwd(), "auto_email"))
  ),
  autoEmailPython:
    process.env.AUTO_EMAIL_PYTHON ||
    [
      path.join(process.cwd(), "auto_email", "venv", "bin", "python"),
      "python3",
    ].find((candidate) => candidate === "python3" || fs.existsSync(candidate)),
  autoEmailSender: process.env.AUTO_EMAIL_SENDER || "",
  autoEmailDryRun: parseBooleanEnv(process.env.AUTO_EMAIL_DRY_RUN, false),
  autoEmailPace: process.env.AUTO_EMAIL_PACE || "0.4",
  autoEmailMax: process.env.AUTO_EMAIL_MAX || "",
  autoEmailContactsCsv: path.resolve(
    expandHome(
      process.env.AUTO_EMAIL_CONTACTS_CSV ||
        path.join(process.cwd(), "auto_email", "contacts_from_scrape.csv")
    )
  ),
  autoEmailResultsCsv: path.resolve(
    expandHome(
      process.env.AUTO_EMAIL_RESULTS_CSV ||
        path.join(process.cwd(), "auto_email", "eligible_results.csv")
    )
  ),
  autoEmailTemplate: path.resolve(
    expandHome(
      process.env.AUTO_EMAIL_TEMPLATE ||
        path.join(process.cwd(), "auto_email", "templates", "sample.txt")
    )
  ),
  autoEmailSourceTracking: expandHome(process.env.AUTO_EMAIL_SOURCE_TRACKING || ""),

  apolloInputCsv: path.resolve(
    expandHome(
      process.env.APOLLO_INPUT_CSV || path.join(process.cwd(), "runtime", "apollo", "apollo_input.csv")
    )
  ),
  apolloCombinedUrl: process.env.APOLLO_COMBINED_URL || "",
  apolloGeneratedInputCsv: path.resolve(
    expandHome(
      process.env.APOLLO_GENERATED_INPUT_CSV ||
        path.join(process.cwd(), "runtime", "apollo", "generated_input.csv")
    )
  ),
  apolloProfileDir: path.resolve(
    expandHome(
      process.env.APOLLO_PROFILE_DIR || path.join(process.cwd(), "runtime", "apollo", "profile")
    )
  ),
  apolloFirefoxProfileDir: expandHome(process.env.APOLLO_FIREFOX_PROFILE_DIR || ""),
  apolloHeadless: parseBooleanEnv(process.env.APOLLO_HEADLESS, false),
  apolloHumanize: parseHumanizeEnv(process.env.APOLLO_HUMANIZE, true),
  apolloMaxPagesPerOrg: parseInt(process.env.APOLLO_MAX_PAGES_PER_ORG || "5", 10),
  apolloSettleMs: parseInt(process.env.APOLLO_SETTLE_MS || "3000", 10),
  apolloActionMinWaitMs: parseInt(process.env.APOLLO_ACTION_MIN_WAIT_MS || "1500", 10),
  apolloActionMaxWaitMs: parseInt(process.env.APOLLO_ACTION_MAX_WAIT_MS || "4000", 10),
  apolloPageTimeoutMs: parseInt(process.env.APOLLO_PAGE_TIMEOUT_MS || "30000", 10),
  apolloResultsSelectorTimeoutMs: parseInt(
    process.env.APOLLO_RESULTS_SELECTOR_TIMEOUT_MS || "15000",
    10
  ),
  apolloRawOutputDir: path.resolve(
    expandHome(
      process.env.APOLLO_RAW_OUTPUT_DIR || path.join(process.cwd(), "runtime", "apollo", "raw")
    )
  ),
  apolloRawFilePrefix: process.env.APOLLO_RAW_FILE_PREFIX || "linkedin_urls",
  apolloCanonicalOutputCsv: path.resolve(
    expandHome(
      process.env.APOLLO_CANONICAL_OUTPUT_CSV ||
        path.join(process.cwd(), "runtime", "apollo", "linkedin_urls.csv")
    )
  ),
  apolloMaxUrls: parseInt(process.env.APOLLO_MAX_URLS || "9999", 10),
  apolloResultsCsv: path.resolve(
    expandHome(
      process.env.APOLLO_RESULTS_CSV ||
        path.join(process.cwd(), "runtime", "apollo", "apollo_full_results.csv")
    )
  ),
};

// ─── HELPERS ──────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function expandHome(filePath) {
  if (!filePath) return null;
  if (filePath === "~") return process.env.HOME;
  if (filePath.startsWith("~/")) {
    return path.join(process.env.HOME, filePath.slice(2));
  }
  return filePath;
}

function isDirectory(filePath) {
  try {
    return fs.statSync(filePath).isDirectory();
  } catch {
    return false;
  }
}

function isExtensionDirectory(filePath) {
  return isDirectory(filePath) && fs.existsSync(path.join(filePath, "manifest.json"));
}

function compareVersionLikeStrings(a, b) {
  const aParts = a.split(/[._-]/);
  const bParts = b.split(/[._-]/);
  const maxLength = Math.max(aParts.length, bParts.length);

  for (let i = 0; i < maxLength; i++) {
    const left = aParts[i] || "";
    const right = bParts[i] || "";
    const leftNumber = /^\d+$/.test(left) ? Number(left) : Number.NaN;
    const rightNumber = /^\d+$/.test(right) ? Number(right) : Number.NaN;

    if (!Number.isNaN(leftNumber) && !Number.isNaN(rightNumber) && leftNumber !== rightNumber) {
      return leftNumber - rightNumber;
    }

    const textCompare = left.localeCompare(right);
    if (textCompare !== 0) {
      return textCompare;
    }
  }

  return 0;
}

function listCandidateUserDataDirs() {
  const candidates = [
    process.env.CHROME_USER_DATA_DIR,
    path.join(process.env.HOME, "Library/Application Support/Google/Chrome"),
    path.join(process.env.HOME, "Library/Application Support/net.imput.helium"),
    path.join(process.cwd(), "chrome-data"),
    path.join(process.cwd(), ".chrome_profile"),
  ];

  return [...new Set(candidates.filter(Boolean).map((candidate) => path.resolve(expandHome(candidate))))];
}

function readConfiguredExtensionPaths(userDataDir) {
  const defaultProfileDir = path.join(userDataDir, "Default");
  const preferenceFiles = ["Secure Preferences", "Preferences"];
  const configuredPaths = [];

  for (const fileName of preferenceFiles) {
    const preferencePath = path.join(defaultProfileDir, fileName);
    if (!fs.existsSync(preferencePath)) continue;

    try {
      const raw = JSON.parse(fs.readFileSync(preferencePath, "utf8"));
      const configuredPath = raw?.extensions?.settings?.[KASPR_EXTENSION_ID]?.path;
      if (!configuredPath) continue;

      if (path.isAbsolute(configuredPath)) {
        configuredPaths.push({
          path: configuredPath,
          source: `${preferencePath} (absolute configured path)`,
        });
        continue;
      }

      configuredPaths.push({
        path: path.resolve(defaultProfileDir, configuredPath),
        source: `${preferencePath} (relative to Default profile)`,
      });
      configuredPaths.push({
        path: path.resolve(defaultProfileDir, "Extensions", configuredPath),
        source: `${preferencePath} (relative to Default/Extensions)`,
      });
      configuredPaths.push({
        path: path.resolve(userDataDir, configuredPath),
        source: `${preferencePath} (relative to user data dir)`,
      });
    } catch {}
  }

  return configuredPaths;
}

function listInstalledExtensionPaths(userDataDir) {
  const extensionRoots = [
    path.join(userDataDir, "Default", "Extensions", KASPR_EXTENSION_ID),
    path.join(userDataDir, "Extensions", KASPR_EXTENSION_ID),
  ];
  const foundPaths = [];

  for (const extensionRoot of extensionRoots) {
    if (isExtensionDirectory(extensionRoot)) {
      foundPaths.push(extensionRoot);
    }

    if (!isDirectory(extensionRoot)) continue;

    const versionDirs = fs
      .readdirSync(extensionRoot, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => path.join(extensionRoot, entry.name))
      .filter((candidatePath) => isExtensionDirectory(candidatePath))
      .sort((left, right) =>
        compareVersionLikeStrings(path.basename(right), path.basename(left))
      );

    foundPaths.push(...versionDirs);
  }

  return foundPaths;
}

function resolveKasprExtensionPath() {
  const explicitPath = expandHome(process.env.KASPR_EXTENSION_PATH);
  if (explicitPath) {
    const resolvedPath = path.resolve(explicitPath);
    return {
      path: isExtensionDirectory(resolvedPath) ? resolvedPath : null,
      source: isExtensionDirectory(resolvedPath) ? "KASPR_EXTENSION_PATH" : null,
      candidates: [{ path: resolvedPath, source: "KASPR_EXTENSION_PATH" }],
    };
  }

  const candidates = [];
  const seen = new Set();

  function addCandidate(candidatePath, source) {
    if (!candidatePath) return;

    const resolvedPath = path.resolve(expandHome(candidatePath));
    if (seen.has(resolvedPath)) return;

    seen.add(resolvedPath);
    candidates.push({ path: resolvedPath, source });
  }

  addCandidate(path.join(process.cwd(), "kaspr_extension"), "repo-local unpacked extension");

  for (const userDataDir of listCandidateUserDataDirs()) {
    for (const configuredPath of readConfiguredExtensionPaths(userDataDir)) {
      addCandidate(configuredPath.path, configuredPath.source);
    }

    for (const installedPath of listInstalledExtensionPaths(userDataDir)) {
      addCandidate(installedPath, `Installed Kaspr extension under ${userDataDir}`);
    }
  }

  const match = candidates.find((candidate) => isExtensionDirectory(candidate.path));
  return {
    path: match ? match.path : null,
    source: match ? match.source : null,
    candidates,
  };
}

function randomDelay() {
  const span = Math.max(CONFIG.maxDelay - CONFIG.minDelay, 0);
  const delay = Math.floor(Math.random() * (span + 1)) + CONFIG.minDelay;
  console.log(`  Waiting ${(delay / 1000).toFixed(0)}s before next profile...`);
  return sleep(delay);
}

function readInputCsv(filePath = CONFIG.inputCsv) {
  let raw = fs.readFileSync(filePath, "utf-8");
  if (raw.charCodeAt(0) === 0xFEFF) raw = raw.slice(1); // strip BOM
  const records = parse(raw, { columns: true, skip_empty_lines: true, trim: true });
  const rows = records.filter((r) => {
    if (!r[CONFIG.urlColumn] || !r[CONFIG.urlColumn].includes("linkedin.com/in/")) return false;
    if (!r.Name || r.Name.trim().length < 2) return false;
    if (!r.Company || r.Company.trim().length < 2) return false;
    return true;
  });
  console.log(`Found ${rows.length} LinkedIn URLs in ${filePath}`);
  return rows;
}

function loadExistingResults(filePath = CONFIG.outputCsv) {
  return loadResultsFrom(filePath);
}

function loadResultsFrom(filePath) {
  if (!fs.existsSync(filePath)) return {};
  const raw = fs.readFileSync(filePath, "utf-8");
  const records = parse(raw, { columns: true, skip_empty_lines: true });
  const map = {};
  for (const r of records) {
    const url = r[CONFIG.urlColumn] || r.linkedinUrl || r.linkedin_url;
    if (url) map[url] = r;
  }
  return map;
}

function saveResult(results, filePath = CONFIG.outputCsv) {
  saveResultsTo(filePath, results);
  syncAutoEmailEligibility(results, { silent: true, sourcePath: filePath });
}

function saveResultsTo(filePath, results) {
  const rows = Object.values(results);
  if (rows.length === 0) return;
  const csv = stringify(rows, { header: true });
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, csv);
}

function hasFoundEmail(record) {
  return (record?.status || "").trim() === "found" && Boolean((record?.email || "").trim());
}

function getScrapedAtTime(record) {
  const timestamp = Date.parse(record?.scraped_at || "");
  return Number.isFinite(timestamp) ? timestamp : Number.NEGATIVE_INFINITY;
}

function choosePreferredResult(existingRecord, incomingRecord) {
  if (!existingRecord) return incomingRecord;

  const existingFound = hasFoundEmail(existingRecord);
  const incomingFound = hasFoundEmail(incomingRecord);

  if (incomingFound && !existingFound) return incomingRecord;
  if (existingFound && !incomingFound) return existingRecord;

  return getScrapedAtTime(incomingRecord) >= getScrapedAtTime(existingRecord)
    ? incomingRecord
    : existingRecord;
}

function mergeResultsMaps(existingResults, incomingResults) {
  const merged = { ...existingResults };
  for (const [url, record] of Object.entries(incomingResults)) {
    merged[url] = choosePreferredResult(merged[url], record);
  }
  return merged;
}

function syncAutoEmailEligibility(
  currentResults,
  { silent = false, sourcePath = CONFIG.outputCsv } = {}
) {
  if (!currentResults || Object.keys(currentResults).length === 0) return;

  const targetPath = path.resolve(CONFIG.autoEmailResultsCsv);
  const resolvedSourcePath = path.resolve(sourcePath);
  if (targetPath === resolvedSourcePath) return;

  const existingEligibleResults = loadResultsFrom(targetPath);
  const mergedEligibleResults = mergeResultsMaps(existingEligibleResults, currentResults);

  saveResultsTo(targetPath, mergedEligibleResults);

  if (!silent) {
    const eligibleCount = Object.values(mergedEligibleResults).filter(hasFoundEmail).length;
    console.log(
      `Eligible email pool updated: ${eligibleCount} found emails in ${targetPath}`
    );
  }
}

function getEmailResultsPath() {
  if (process.env.OUTPUT_CSV) {
    return path.resolve(CONFIG.outputCsv);
  }
  return path.resolve(CONFIG.autoEmailResultsCsv);
}

// ─── KASPR WIDGET SCRAPING ───────────────────────────────────────────

async function findKasprWidget(page) {
  // The Kaspr widget uses #KasprPlugin as its main container
  try {
    await page.waitForSelector("#KasprPlugin", { timeout: CONFIG.kaspWidgetTimeout });
    return "#KasprPlugin";
  } catch {}
  return null;
}

async function waitForWidgetReady(page) {
  try {
    await page.waitForFunction(
      () => {
        const el = document.querySelector("#KasprPlugin");
        if (!el) return true; // no widget, bail
        const text = el.innerText;
        return text.includes("Contact Information") ||
               text.includes("Sorry") ||
               text.includes("Export") ||
               text.trim().length > 50;
      },
      { timeout: CONFIG.kaspWidgetTimeout }
    );
    return true;
  } catch {
    console.log(`  (Widget did not load after ${(CONFIG.kaspWidgetTimeout / 1000).toFixed(0)}s)`);
    return false;
  }
}

async function extractKasprData(page) {
  let emails = [];
  let phones = [];

  const widget = await findKasprWidget(page);
  if (!widget) {
    console.log("  Kaspr widget not found on this page.");
    return { emails, phones };
  }
  console.log("  Kaspr widget found.");

  // Wait for "Searching contact information..." to finish
  console.log("  Waiting for widget to load...");
  await waitForWidgetReady(page);

  // Dump widget state for debugging
  const widgetState = await page.evaluate(() => {
    const el = document.querySelector("#KasprPlugin");
    if (!el) return "no widget";
    return el.innerText.substring(0, 300);
  });
  console.log(`  Widget text: ${widgetState.replace(/\n/g, " | ").substring(0, 200)}`);

  // Step 1: Click "Reveal contact details" button inside #KasprPlugin
  // The button has class "btn sm step1" and is inside div.btn-in.searching-contact-info
  try {
    const revealPos = await page.evaluate(() => {
      const plugin = document.querySelector("#KasprPlugin");
      if (!plugin) return null;
      // Try the specific class first
      const btn = plugin.querySelector(".btn-in button.step1") || plugin.querySelector("button.step1");
      if (btn) {
        const rect = btn.getBoundingClientRect();
        if (rect.width > 0 && rect.height > 0) {
          return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2, text: btn.textContent.trim() };
        }
      }
      // Fallback: find by text
      const all = plugin.querySelectorAll("button");
      for (const el of all) {
        if (el.textContent.trim().toLowerCase().includes("reveal contact")) {
          const rect = el.getBoundingClientRect();
          if (rect.width > 0 && rect.height > 0) {
            return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2, text: el.textContent.trim() };
          }
        }
      }
      return null;
    });
    if (revealPos) {
      await page.mouse.click(revealPos.x, revealPos.y);
      console.log(`  Clicked '${revealPos.text}'`);
      // Wait for masked emails (***@) to disappear or Show buttons to go away (max 10s)
      const revealStart = Date.now();
      while (Date.now() - revealStart < 10000) {
        const stillMasked = await page.evaluate(() => {
          const plugin = document.querySelector("#KasprPlugin");
          if (!plugin) return false;
          return plugin.innerText.includes("***@") || plugin.querySelector("button.show-btn") !== null;
        });
        if (!stillMasked) break;
        await sleep(CONFIG.revealPollInterval);
      }
    } else {
      console.log("  No 'Reveal' button found (may already be revealed).");
    }
  } catch (err) {
    console.log(`  Error clicking reveal: ${err.message}`);
  }

  // Step 2: If still masked, click Show button as fallback
  try {
    const showBtn = await page.evaluate(() => {
      const plugin = document.querySelector("#KasprPlugin");
      if (!plugin) return null;
      if (!plugin.innerText.includes("***@")) return null; // already unmasked
      const btn = plugin.querySelector("button.show-btn");
      if (!btn) return null;
      const rect = btn.getBoundingClientRect();
      return rect.width > 0 ? { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2 } : null;
    });
    if (showBtn) {
      await page.mouse.click(showBtn.x, showBtn.y);
      console.log("  Still masked — clicked Show button as fallback");
      await sleep(CONFIG.postShowDelay);
    }
  } catch {}

  // Step 3: Extract emails from inside #KasprPlugin
  try {
    emails = await page.evaluate(() => {
      const plugin = document.querySelector("#KasprPlugin");
      if (!plugin) return [];
      const found = new Set();
      const emailRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;

      const walker = document.createTreeWalker(plugin, NodeFilter.SHOW_TEXT);
      while (walker.nextNode()) {
        const text = walker.currentNode.textContent;
        if (text.includes("@")) {
          const matches = text.match(emailRegex);
          if (matches) matches.forEach((m) => found.add(m.toLowerCase()));
        }
      }

      plugin.querySelectorAll('a[href^="mailto:"]').forEach((el) => {
        const email = el.href.replace("mailto:", "").split("?")[0].trim();
        if (email.includes("@")) found.add(email.toLowerCase());
      });

      const ignore = ["@linkedin.com", "@licdn.com", "noreply@", "support@", "@kaspr"];
      return [...found].filter((e) => !ignore.some((i) => e.includes(i)));
    });
  } catch (err) {
    console.log(`  Error extracting emails: ${err.message}`);
  }

  return { emails, phones };
}

const PERSONAL_DOMAINS = [
  "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
  "icloud.com", "me.com", "mac.com", "live.com", "msn.com",
  "protonmail.com", "mail.com", "zoho.com", "ymail.com", "comcast.net",
  "verizon.net", "att.net", "sbcglobal.net", "cox.net", "earthlink.net",
];

function pickBestEmail(emails, company) {
  if (emails.length <= 1) return emails[0] || "";

  const companyLower = (company || "").toLowerCase().replace(/[^a-z0-9]/g, "");

  // Separate work vs personal
  const work = emails.filter((e) => {
    const domain = e.split("@")[1];
    return !PERSONAL_DOMAINS.some((pd) => domain === pd);
  });
  const pool = work.length > 0 ? work : emails;

  // Prefer email whose domain contains the company name
  if (companyLower.length > 2) {
    const companyMatch = pool.find((e) => {
      const domain = e.split("@")[1].replace(/[^a-z0-9]/g, "");
      return domain.includes(companyLower) || companyLower.includes(domain.split(".")[0]);
    });
    if (companyMatch) return companyMatch;
  }

  return pool[0];
}

async function extractProfileName(page) {
  try {
    const name = await page.$eval("h1", (el) => el.textContent.trim());
    return name;
  } catch {
    return "";
  }
}

function handleInterceptedRequest(req) {
  const reqUrl = req.url();
  if (reqUrl.includes("linkedin.com/in/")) {
    const profileSlug = reqUrl.match(/linkedin\.com\/in\/([^/?#]+)/)?.[1] || "unknown";
    req.respond({
      status: 200,
      contentType: "text/html",
      body: `<!DOCTYPE html>
<html><head><title>${profileSlug} | LinkedIn</title></head>
<body>
  <div class="scaffold-layout">
    <div class="pv-top-card">
      <h1>${profileSlug}</h1>
      <div class="pv-top-card--list">LinkedIn Member</div>
    </div>
  </div>
</body></html>`,
    });
    return;
  }

  if (
    reqUrl.includes("linkedin.com") &&
    !reqUrl.includes("kaspr") &&
    !reqUrl.includes("api.kaspr.io")
  ) {
    req.abort();
    return;
  }

  req.continue();
}

async function attachInterception(page) {
  await page.setRequestInterception(true);
  page.on("request", handleInterceptedRequest);
}

async function resetPage(browser, page) {
  try {
    await page.close();
  } catch {}

  const freshPage = await browser.newPage();
  await attachInterception(freshPage);
  return freshPage;
}

function getScrapeOutputCsvForMode(pipelineMode) {
  if (process.env.OUTPUT_CSV) {
    return path.resolve(CONFIG.outputCsv);
  }
  if (pipelineMode === "apollo-full") {
    return path.resolve(CONFIG.apolloResultsCsv);
  }
  return path.resolve(CONFIG.outputCsv);
}

function resolveApolloInputCsv() {
  if (!CONFIG.apolloCombinedUrl) {
    return {
      inputCsv: CONFIG.apolloInputCsv,
      generated: false,
    };
  }

  const batch = buildApolloBatchFromCombinedUrl(
    CONFIG.apolloCombinedUrl,
    CONFIG.apolloMaxPagesPerOrg
  );
  writeApolloInputCsv(CONFIG.apolloGeneratedInputCsv, batch.urls);

  console.log("Apollo combined URL input detected. Using APOLLO_COMBINED_URL precedence.");
  console.log(`Apollo combined URL org ids: ${batch.orgIds.length}`);
  console.log(`Apollo page fanout per org: ${batch.pageCount}`);
  console.log(`Apollo generated URLs: ${batch.urls.length}`);
  console.log(`Apollo generated input CSV: ${CONFIG.apolloGeneratedInputCsv}`);

  return {
    inputCsv: CONFIG.apolloGeneratedInputCsv,
    generated: true,
    orgCount: batch.orgIds.length,
    pageCount: batch.pageCount,
    urlCount: batch.urls.length,
  };
}

async function runApolloStage() {
  console.log("Starting Apollo Camoufox stage...");
  const inputConfig = resolveApolloInputCsv();
  const result = await runApolloScrape({
    inputCsv: inputConfig.inputCsv,
    profileDir: CONFIG.apolloProfileDir,
    firefoxProfileDir: CONFIG.apolloFirefoxProfileDir || null,
    headless: CONFIG.apolloHeadless,
    humanize: CONFIG.apolloHumanize,
    settleMs: CONFIG.apolloSettleMs,
    actionMinWaitMs: CONFIG.apolloActionMinWaitMs,
    actionMaxWaitMs: CONFIG.apolloActionMaxWaitMs,
    pageTimeoutMs: CONFIG.apolloPageTimeoutMs,
    resultsSelectorTimeoutMs: CONFIG.apolloResultsSelectorTimeoutMs,
    rawOutputDir: CONFIG.apolloRawOutputDir,
    rawFilePrefix: CONFIG.apolloRawFilePrefix,
    canonicalOutputCsv: CONFIG.apolloCanonicalOutputCsv,
    maxUrls: CONFIG.apolloMaxUrls,
  });
  console.log(
    `Apollo stage complete: ${result.canonicalRowCount} canonical LinkedIn row(s) written to ${result.canonicalOutputCsv}`
  );
  return result;
}

function runAutoEmailFollowUp(resultsPath = getEmailResultsPath()) {
  const helperPath = path.join(CONFIG.autoEmailDir, "run_after_scrape.py");
  if (!fs.existsSync(helperPath)) {
    throw new Error(`Auto-email helper not found: ${helperPath}`);
  }

  const command = [
    helperPath,
    "--results",
    path.resolve(resultsPath),
    "--contacts-out",
    CONFIG.autoEmailContactsCsv,
    "--template",
    CONFIG.autoEmailTemplate,
  ];

  if (CONFIG.autoEmailSourceTracking) {
    command.push(
      "--source-tracking",
      path.resolve(CONFIG.autoEmailSourceTracking)
    );
  }

  if (CONFIG.autoEmailSender) {
    command.push("--sender", CONFIG.autoEmailSender);
  }
  if (CONFIG.autoEmailDryRun) {
    command.push("--dry-run");
  }
  if (CONFIG.autoEmailPace) {
    command.push("--pace", CONFIG.autoEmailPace);
  }
  if (CONFIG.autoEmailMax) {
    command.push("--max", CONFIG.autoEmailMax);
  }

  console.log("\nStarting auto_email follow-up...");
  const result = spawnSync(CONFIG.autoEmailPython, command, {
    stdio: "inherit",
    cwd: process.cwd(),
  });

  if (result.status !== 0) {
    throw new Error(
      `auto_email follow-up failed with exit code ${result.status ?? "unknown"}`
    );
  }
}

// ─── MAIN ─────────────────────────────────────────────────────────────

async function main() {
  if (!["scrape", "email", "full", "apollo-only", "apollo-full"].includes(CONFIG.pipelineMode)) {
    console.error(
      `ERROR: Unsupported PIPELINE_MODE '${CONFIG.pipelineMode}'. Use 'scrape', 'email', 'full', 'apollo-only', or 'apollo-full'.`
    );
    process.exit(1);
  }

  console.log(`Pipeline mode: ${CONFIG.pipelineMode}`);

  if (CONFIG.pipelineMode === "email") {
    const emailResultsPath = getEmailResultsPath();
    if (!fs.existsSync(emailResultsPath)) {
      console.error(`ERROR: Results CSV not found for email mode: ${emailResultsPath}`);
      process.exit(1);
    }
    runAutoEmailFollowUp(emailResultsPath);
    return;
  }

  let scrapeInputCsv = path.resolve(CONFIG.inputCsv);
  const scrapeOutputCsv = getScrapeOutputCsvForMode(CONFIG.pipelineMode);

  if (["apollo-only", "apollo-full"].includes(CONFIG.pipelineMode)) {
    const apolloResult = await runApolloStage();
    scrapeInputCsv = apolloResult.canonicalOutputCsv;

    if (CONFIG.pipelineMode === "apollo-only") {
      console.log("\nApollo-only mode complete. Skipping Kaspr and auto_email stages.");
      return;
    }
  }

  if (!CONFIG.extensionPath) {
    console.error("ERROR: Could not find a usable Kaspr extension path.");
    console.error("Set KASPR_EXTENSION_PATH or restore one of these candidate locations:");
    for (const candidate of CONFIG.extensionCandidates) {
      console.error(`  - ${candidate.path} (${candidate.source})`);
    }
    process.exit(1);
  }

  console.log(`Using Kaspr extension: ${CONFIG.extensionPath}`);
  if (CONFIG.extensionSource) {
    console.log(`Extension source: ${CONFIG.extensionSource}`);
  }

  if (!fs.existsSync(scrapeInputCsv)) {
    console.error(`ERROR: Input CSV file not found: ${scrapeInputCsv}`);
    process.exit(1);
  }

  const inputRows = readInputCsv(scrapeInputCsv);
  if (inputRows.length === 0) {
    console.error("No valid LinkedIn URLs found in the CSV.");
    process.exit(1);
  }

  const existingResults = loadExistingResults(scrapeOutputCsv);
  const urlsToProcess = inputRows.filter((r) => !existingResults[r[CONFIG.urlColumn]]);
  console.log(
    `${urlsToProcess.length} new URLs to process (${Object.keys(existingResults).length} already done)`
  );

  if (urlsToProcess.length === 0) {
    console.log("All URLs already processed. Nothing to do.");
    syncAutoEmailEligibility(existingResults, { sourcePath: scrapeOutputCsv });
    if (["full", "apollo-full"].includes(CONFIG.pipelineMode)) {
      console.log("Using eligible results for auto_email follow-up.");
      runAutoEmailFollowUp(CONFIG.autoEmailResultsCsv);
    }
    return;
  }

  const batch = urlsToProcess.slice(0, CONFIG.maxProfiles);
  console.log(`Processing ${batch.length} profiles in this run (max: ${CONFIG.maxProfiles})\n`);

  // Launch browser with Kaspr extension
  const browser = await puppeteer.launch({
    headless: false,
    args: [
      `--disable-extensions-except=${CONFIG.extensionPath}`,
      `--load-extension=${CONFIG.extensionPath}`,
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--profile-directory=Default",
    ],
    userDataDir: CONFIG.userDataDir,
    defaultViewport: null,
  });

  await sleep(CONFIG.browserReadyDelay);

  const pages = await browser.pages();
  let page = pages[0] || (await browser.newPage());
  await attachInterception(page);

  console.log("Browser ready with request interception.\n");

  // Process each profile
  let processed = 0;
  for (const row of batch) {
    processed++;
    const url = row[CONFIG.urlColumn];
    const profileId = url.match(/linkedin\.com\/in\/([^/?#]+)/)?.[1];
    if (!profileId) {
      console.log(`[${processed}/${batch.length}] Invalid URL: ${url}`);
      continue;
    }

    const displayName = row.Name || profileId;
    console.log(`\n[${processed}/${batch.length}] ${displayName} (${row.Company || ""})`);

    let attempt = 0;
    while (attempt < 2) {
      try {
        await page.goto(`https://www.linkedin.com/in/${profileId}/`, {
          waitUntil: "domcontentloaded",
          timeout: CONFIG.pageNavigationTimeout,
        });
        await sleep(CONFIG.profileSettleDelay); // Let Kaspr extension settle

        const { emails, phones } = await extractKasprData(page);

        console.log(
          `  ${displayName} → ${emails.length > 0 ? emails.join(", ") : "no email"}`
        );

        const bestEmail = pickBestEmail(emails, row.Company);
        existingResults[url] = {
          ...row,
          email: bestEmail,
          all_emails: emails.join("; "),
          phones: phones.join("; "),
          status: emails.length > 0 ? "found" : "no_email",
          scraped_at: new Date().toISOString(),
        };
        saveResult(existingResults, scrapeOutputCsv);
        break;
      } catch (err) {
        const retryable = /Navigation timeout|ERR_ABORTED/.test(err.message || "");
        const shouldRetry = retryable && attempt === 0;

        if (shouldRetry) {
          console.log(`  Navigation failed (${err.message}). Resetting page and retrying once...`);
          page = await resetPage(browser, page);
          attempt++;
          continue;
        }

        console.error(`  Error: ${err.message}`);
        existingResults[url] = {
          ...row,
          email: "",
          all_emails: "",
          phones: "",
          status: "error",
          scraped_at: new Date().toISOString(),
        };
        saveResult(existingResults, scrapeOutputCsv);
        break;
      }
    }

    // Random delay between profiles (skip after last one)
    if (processed < batch.length) {
      await randomDelay();
    }
  }

  console.log(`\nDone! Results saved to ${scrapeOutputCsv}`);
  console.log(`Processed: ${processed}, Total in results: ${Object.keys(existingResults).length}`);
  syncAutoEmailEligibility(existingResults, { sourcePath: scrapeOutputCsv });

  await browser.close();
  if (["full", "apollo-full"].includes(CONFIG.pipelineMode)) {
    runAutoEmailFollowUp(CONFIG.autoEmailResultsCsv);
  } else {
    console.log("\nScrape-only mode complete. Skipping auto_email follow-up.");
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
