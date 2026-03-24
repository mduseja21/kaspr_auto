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
  kasprEmail: process.env.KASPR_EMAIL || "ajakki@purdue.edu",
  kasprPassword: process.env.KASPR_PASSWORD || "Ashwin@387",

  extensionPath: RESOLVED_EXTENSION.path,
  extensionSource: RESOLVED_EXTENSION.source,
  extensionCandidates: RESOLVED_EXTENSION.candidates,

  userDataDir:
    process.env.CHROME_USER_DATA_DIR ||
    path.join(process.env.HOME, "Library/Application Support/Google/Chrome"),

  inputCsv: process.env.INPUT_CSV || "linkedin_urls.csv",
  urlColumn: process.env.URL_COLUMN || "linkedin_url",
  outputCsv: process.env.OUTPUT_CSV || "results.csv",

  // Delay between API calls (ms) — randomized between min and max
  minDelay: 1_000,
  maxDelay: 3_000,

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
      { timeout: 15000 }
    );
    return true;
  } catch {
    console.log("  (Widget did not load after 15s)");
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
        await sleep(500);
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
      await sleep(2000);
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

  await sleep(3000);

  const pages = await browser.pages();
  const page = pages[0] || (await browser.newPage());

  // Enable request interception — serve a fake LinkedIn page so we don't
  // actually hit LinkedIn, but the extension content script still activates
  await page.setRequestInterception(true);
  page.on("request", (req) => {
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
    } else if (
      reqUrl.includes("linkedin.com") &&
      !reqUrl.includes("kaspr") &&
      !reqUrl.includes("api.kaspr.io")
    ) {
      req.abort();
    } else {
      req.continue();
    }
  });

  console.log("Browser ready with request interception.\n");

  // Process each profile
  let processed = 0;
  for (const url of batch) {
    processed++;
    const profileId = url.match(/linkedin\.com\/in\/([^/?#]+)/)?.[1];
    if (!profileId) {
      console.log(`[${processed}/${batch.length}] Invalid URL: ${url}`);
      continue;
    }

    console.log(`\n[${processed}/${batch.length}] ${profileId}`);

    try {
      await page.goto(`https://www.linkedin.com/in/${profileId}/`, {
        waitUntil: "domcontentloaded",
        timeout: 10000,
      });
      await sleep(500); // Let Kaspr extension settle

      const { emails, phones } = await extractKasprData(page);

      console.log(
        `  ${profileId} → ${emails.length > 0 ? emails.join(", ") : "no email"}`
      );

      existingResults[url] = {
        linkedin_url: url,
        name: profileId,
        emails: emails.join("; "),
        phones: phones.join("; "),
        status: emails.length > 0 ? "found" : "no_email",
        scraped_at: new Date().toISOString(),
      };
      saveResult(existingResults);
    } catch (err) {
      console.error(`  Error: ${err.message}`);
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
