const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const { parse } = require("csv-parse/sync");
const { stringify } = require("csv-stringify/sync");
const fs = require("fs");
const path = require("path");

puppeteer.use(StealthPlugin());

const KASPR_EXTENSION_ID = "kkfgenjfpmoegefcckjklfjieepogfhg";
const RESOLVED_EXTENSION = resolveKasprExtensionPath();

// ─── CONFIG ───────────────────────────────────────────────────────────
const CONFIG = {
  extensionPath: RESOLVED_EXTENSION.path,
  extensionSource: RESOLVED_EXTENSION.source,
  extensionCandidates: RESOLVED_EXTENSION.candidates,

  userDataDir:
    process.env.CHROME_USER_DATA_DIR ||
    path.join(process.env.HOME, "Library/Application Support/Google/Chrome"),

  inputCsv: process.env.INPUT_CSV || "linkedin_urls.csv",
  urlColumn: process.env.URL_COLUMN || "linkedin_url",
  outputCsv: process.env.OUTPUT_CSV || "results.csv",

  // Delay between profile visits (ms) — randomized between min and max
  minDelay: 5_000,
  maxDelay: 15_000,

  // Max profiles to process per run
  maxProfiles: parseInt(process.env.MAX_PROFILES || "9999", 10),

  // How long to wait for Kaspr widget to appear (ms)
  kaspWidgetTimeout: 15_000,
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
  const delay =
    Math.floor(Math.random() * (CONFIG.maxDelay - CONFIG.minDelay)) +
    CONFIG.minDelay;
  console.log(`  Waiting ${(delay / 1000).toFixed(0)}s before next profile...`);
  return sleep(delay);
}

function readInputCsv() {
  const raw = fs.readFileSync(CONFIG.inputCsv, "utf-8");
  const records = parse(raw, { columns: true, skip_empty_lines: true, trim: true });
  const urls = records
    .map((r) => r[CONFIG.urlColumn])
    .filter((u) => u && u.includes("linkedin.com/in/"));
  console.log(`Found ${urls.length} LinkedIn URLs in ${CONFIG.inputCsv}`);
  return urls;
}

function loadExistingResults() {
  if (!fs.existsSync(CONFIG.outputCsv)) return {};
  const raw = fs.readFileSync(CONFIG.outputCsv, "utf-8");
  const records = parse(raw, { columns: true, skip_empty_lines: true });
  const map = {};
  for (const r of records) {
    map[r.linkedin_url] = r;
  }
  return map;
}

function saveResult(results) {
  const rows = Object.values(results);
  if (rows.length === 0) return;
  const csv = stringify(rows, { header: true });
  fs.writeFileSync(CONFIG.outputCsv, csv);
}

// ─── KASPR WIDGET SCRAPING ───────────────────────────────────────────

async function findKasprWidget(page) {
  const selectors = [".kaspr-wk", ".kaspr-popup", ".kspr-popup-wrapper", '[class*="kaspr"]', '[class*="kspr"]'];

  // Race all selectors in parallel — first one to match wins (single 15s timeout total, not per selector)
  try {
    const result = await Promise.race([
      ...selectors.map(sel =>
        page.waitForSelector(sel, { timeout: CONFIG.kaspWidgetTimeout }).then(() => sel)
      ),
      // Also check for text-based detection every 2s
      (async () => {
        const start = Date.now();
        while (Date.now() - start < CONFIG.kaspWidgetTimeout) {
          const found = await page.evaluate(() => {
            const all = document.querySelectorAll("*");
            for (const el of all) {
              const text = el.textContent.trim().toLowerCase();
              if (text.includes("reveal contact details") || text.includes("b2b email")) {
                return true;
              }
            }
            return false;
          });
          if (found) return "text-based";
          await sleep(2000);
        }
        throw new Error("text-based timeout");
      })(),
    ]);
    return result;
  } catch {}

  return null;
}

async function extractKasprData(page) {
  let emails = [];
  let phones = [];

  const widget = await findKasprWidget(page);
  if (!widget) {
    console.log("  Kaspr widget not found on this page.");
    return { emails, phones };
  }
  console.log(`  Kaspr widget found (${widget})`);

  // Wait for widget to fully render
  await sleep(3000);

  // Step 1: Click "Reveal contact details" button using real mouse click
  try {
    const revealPos = await page.evaluate(() => {
      const all = document.querySelectorAll("button, a, div, span, [role='button']");
      for (const el of all) {
        if (el.textContent.trim().toLowerCase().includes("reveal contact")) {
          const rect = el.getBoundingClientRect();
          if (rect.width > 0 && rect.height > 0) {
            return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2 };
          }
        }
      }
      return null;
    });
    if (revealPos) {
      await page.mouse.click(revealPos.x, revealPos.y);
      console.log("  Clicked 'Reveal contact details'");
      await sleep(5000);
    }
  } catch (err) {
    console.log(`  Error clicking reveal: ${err.message}`);
  }

  // Step 2: Click the "Show" button near "B2B email" only (save credits) using real mouse click
  try {
    const showPos = await page.evaluate(() => {
      const all = [...document.querySelectorAll("*")];

      // Find the element that says "B2B email"
      let b2bSection = null;
      for (const el of all) {
        if (el.children.length <= 2 && el.textContent.trim().toLowerCase().includes("b2b email")) {
          b2bSection = el;
          break;
        }
      }

      if (b2bSection) {
        // Walk up to find a container, then look for a "Show" button
        let container = b2bSection;
        for (let depth = 0; depth < 8; depth++) {
          const buttons = container.querySelectorAll("button, a, span, div, [role='button']");
          for (const btn of buttons) {
            const txt = btn.textContent.trim();
            if (txt.toLowerCase() === "show") {
              const rect = btn.getBoundingClientRect();
              if (rect.width > 0 && rect.height > 0) {
                return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2, strategy: "b2b" };
              }
            }
          }
          container = container.parentElement;
          if (!container) break;
        }
      }

      // Fallback: click the first visible "Show" button on the page
      for (const el of all) {
        const txt = el.textContent.trim().toLowerCase();
        if (txt === "show" && el.offsetParent !== null) {
          const rect = el.getBoundingClientRect();
          if (rect.width > 0 && rect.height > 0) {
            return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2, strategy: "first-show" };
          }
        }
      }

      return null;
    });

    if (showPos) {
      await page.mouse.click(showPos.x, showPos.y);
      console.log(`  Clicked 'Show' for B2B email (strategy: ${showPos.strategy}) — waiting for data...`);
      await sleep(5000);
    } else {
      console.log("  No 'Show' button found (may already be revealed).");
      await sleep(2000);
    }
  } catch (err) {
    console.log(`  Error clicking show: ${err.message}`);
    await sleep(2000);
  }

  // Step 3: Extract emails from anywhere on the page
  try {
    emails = await page.evaluate(() => {
      const found = new Set();
      const emailRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;

      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
      while (walker.nextNode()) {
        const text = walker.currentNode.textContent;
        if (text.includes("@")) {
          const matches = text.match(emailRegex);
          if (matches) matches.forEach((m) => found.add(m.toLowerCase()));
        }
      }

      document.querySelectorAll('a[href^="mailto:"]').forEach((el) => {
        const email = el.href.replace("mailto:", "").split("?")[0].trim();
        if (email.includes("@")) found.add(email.toLowerCase());
      });

      const ignore = ["@linkedin.com", "@licdn.com", "noreply@", "support@"];
      return [...found].filter((e) => !ignore.some((i) => e.includes(i)));
    });
  } catch (err) {
    console.log(`  Error extracting emails: ${err.message}`);
  }

  return { emails, phones };
}

async function extractProfileName(page) {
  try {
    const name = await page.$eval("h1", (el) => el.textContent.trim());
    return name;
  } catch {
    return "";
  }
}

// ─── MAIN ─────────────────────────────────────────────────────────────

async function main() {
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

  if (!fs.existsSync(CONFIG.inputCsv)) {
    console.error(`ERROR: Input CSV file not found: ${CONFIG.inputCsv}`);
    process.exit(1);
  }

  const urls = readInputCsv();
  if (urls.length === 0) {
    console.error("No valid LinkedIn URLs found in the CSV.");
    process.exit(1);
  }

  const existingResults = loadExistingResults();
  const urlsToProcess = urls.filter((u) => !existingResults[u]);
  console.log(
    `${urlsToProcess.length} new URLs to process (${Object.keys(existingResults).length} already done)`
  );

  if (urlsToProcess.length === 0) {
    console.log("All URLs already processed. Nothing to do.");
    return;
  }

  const batch = urlsToProcess.slice(0, CONFIG.maxProfiles);
  console.log(`Processing ${batch.length} profiles in this run (max: ${CONFIG.maxProfiles})`);

  const browser = await puppeteer.launch({
    headless: false,
    args: [
      `--disable-extensions-except=${CONFIG.extensionPath}`,
      `--load-extension=${CONFIG.extensionPath}`,
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--start-maximized",
      "--profile-directory=Default",
    ],
    userDataDir: CONFIG.userDataDir,
    defaultViewport: null,
  });

  const page = await browser.newPage();

  await page.setUserAgent(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  );

  console.log("Navigating to LinkedIn...");
  await page.goto("https://www.linkedin.com/feed/", { waitUntil: "domcontentloaded", timeout: 60000 });

  const currentUrl = page.url();
  if (currentUrl.includes("/login") || currentUrl.includes("/authwall") || currentUrl.includes("/checkpoint")) {
    console.log("\n╔══════════════════════════════════════════════════════════╗");
    console.log("║  Please log in to LinkedIn manually in the browser.     ║");
    console.log("║  The script will continue once you're on the feed page. ║");
    console.log("╚══════════════════════════════════════════════════════════╝\n");

    const loginTimeout = Date.now() + 300_000;
    while (Date.now() < loginTimeout) {
      await sleep(3000);
      const url = page.url();
      if (url.includes("/feed") || url.includes("/in/") || url.includes("/mynetwork")) {
        break;
      }
    }
    await sleep(3000);
    console.log("Logged in successfully!");
  } else {
    console.log("Already logged into LinkedIn.");
  }

  // Process each profile
  let processed = 0;
  for (const url of batch) {
    processed++;
    console.log(`\n[${processed}/${batch.length}] Visiting: ${url}`);

    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
      await sleep(4000); // Let the page and Kaspr extension settle

      const name = await extractProfileName(page);
      console.log(`  Name: ${name || "(not found)"}`);

      const { emails, phones } = await extractKasprData(page);

      const result = {
        linkedin_url: url,
        name,
        emails: emails.join("; "),
        phones: phones.join("; "),
        status: emails.length > 0 ? "found" : "no_email",
        scraped_at: new Date().toISOString(),
      };

      existingResults[url] = result;
      saveResult(existingResults);

      if (emails.length > 0) {
        console.log(`  ✅ Emails: ${emails.join(", ")}`);
      } else {
        console.log("  ❌ No emails found.");
      }
      if (phones.length > 0) {
        console.log(`  Phones: ${phones.join(", ")}`);
      }
    } catch (err) {
      console.error(`  Error processing ${url}: ${err.message}`);
      existingResults[url] = {
        linkedin_url: url,
        name: "",
        emails: "",
        phones: "",
        status: "error",
        scraped_at: new Date().toISOString(),
      };
      saveResult(existingResults);
    }

    // Random delay between profiles (skip after last one)
    if (processed < batch.length) {
      await randomDelay();
    }
  }

  console.log(`\nDone! Results saved to ${CONFIG.outputCsv}`);
  console.log(`Processed: ${processed}, Total in results: ${Object.keys(existingResults).length}`);

  await browser.close();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
