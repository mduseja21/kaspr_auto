const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const { parse } = require("csv-parse/sync");
const { stringify } = require("csv-stringify/sync");
const fs = require("fs");
const path = require("path");

puppeteer.use(StealthPlugin());

// ─── CONFIG ───────────────────────────────────────────────────────────
const CONFIG = {
  extensionPath:
    process.env.KASPR_EXTENSION_PATH ||
    path.join(
      process.env.HOME,
      "Library/Application Support/Google/Chrome/Default/Extensions/kkfgenjfpmoegefcckjklfjieepogfhg/2.0.19_0"
    ),

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
    console.error("ERROR: Set KASPR_EXTENSION_PATH");
    process.exit(1);
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

  // Launch browser with extension
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

  // First, check if we're logged into LinkedIn
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
