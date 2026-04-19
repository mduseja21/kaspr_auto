const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");
const { parse } = require("csv-parse/sync");
const { stringify } = require("csv-stringify/sync");

const RAW_COLUMNS = ["Name", "Title", "Company", "linkedinUrl"];
const APOLLO_PAGE_PARAM = "page";
const APOLLO_ORG_PARAM = "organizationIds[]";
const APOLLO_RESULT_SELECTORS = [
  '[role="row"] [data-testid="contact-name-cell"] a',
  '[role="gridcell"][aria-colindex="1"] [data-testid="contact-name-cell"] a',
];
const APOLLO_LOGIN_HINTS = [
  "sign in",
  "log in",
  "continue with google",
  "forgot your password",
  "forgot password",
  "enter your work email",
];
const APOLLO_COOKIE_QUERY = `
  select
    name,
    value,
    host,
    path,
    expiry,
    isSecure,
    isHttpOnly
  from moz_cookies
  where host like '%apollo.io%'
  order by host, name
`;

function normalizeCell(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function stripBom(raw) {
  return raw.charCodeAt(0) === 0xfeff ? raw.slice(1) : raw;
}

function isLikelyApolloSearchUrl(value) {
  return /(?:app\.)?apollo\.io\/#\/people\?/i.test(String(value || "").trim());
}

function isValidLinkedInProfileUrl(value) {
  try {
    const url = new URL(String(value || "").trim());
    return /(^|\.)linkedin\.com$/i.test(url.hostname) && /^\/in\/[^/?#]+/i.test(url.pathname);
  } catch {
    return false;
  }
}

function normalizeLinkedInUrl(value) {
  if (!isValidLinkedInProfileUrl(value)) return "";

  const url = new URL(String(value).trim());
  url.hash = "";
  url.search = "";
  return url.toString();
}

function trimTrailingEmptyCells(cells) {
  const trimmed = [...cells];
  while (trimmed.length > 0 && trimmed[trimmed.length - 1] === "") {
    trimmed.pop();
  }
  return trimmed;
}

function buildRawFileName(prefix, index) {
  return `${prefix}${index === 1 ? "" : index}.csv`;
}

function isApolloOrgParam(key) {
  return key === APOLLO_ORG_PARAM || key === "organizationIds";
}

function ensureDirectory(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function cleanupRawOutputDir(dirPath, prefix) {
  ensureDirectory(dirPath);
  const pattern = new RegExp(`^${prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(\\d+)?\\.csv$`);
  for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
    if (entry.isFile() && pattern.test(entry.name)) {
      fs.unlinkSync(path.join(dirPath, entry.name));
    }
  }
}

function randomIntBetween(min, max) {
  const normalizedMin = Math.max(0, Number(min) || 0);
  const normalizedMax = Math.max(normalizedMin, Number(max) || normalizedMin);
  const span = normalizedMax - normalizedMin;
  return normalizedMin + Math.floor(Math.random() * (span + 1));
}

async function waitRandomActionDelay(page, config, reason = "") {
  const minWaitMs =
    Number.isFinite(config.actionMinWaitMs) && config.actionMinWaitMs >= 0
      ? config.actionMinWaitMs
      : config.settleMs;
  const maxWaitMs =
    Number.isFinite(config.actionMaxWaitMs) && config.actionMaxWaitMs >= minWaitMs
      ? config.actionMaxWaitMs
      : minWaitMs;

  if (maxWaitMs <= 0) return 0;

  const delay = randomIntBetween(minWaitMs, maxWaitMs);
  if (reason) {
    console.log(`  Waiting ${(delay / 1000).toFixed(2)}s ${reason}...`);
  }
  await page.waitForTimeout(delay);
  return delay;
}

function parseApolloPeopleSearchUrl(urlString) {
  if (!isLikelyApolloSearchUrl(urlString)) {
    throw new Error(`Invalid Apollo people-search URL: ${urlString}`);
  }

  let url;
  try {
    url = new URL(String(urlString).trim());
  } catch {
    throw new Error(`Invalid Apollo URL: ${urlString}`);
  }

  const rawHash = url.hash.startsWith("#") ? url.hash.slice(1) : url.hash;
  const [hashPath = "", hashQuery = ""] = rawHash.split("?");
  if (!/^\/people$/i.test(hashPath)) {
    throw new Error(`Apollo URL must target #/people search: ${urlString}`);
  }

  const orderedEntries = [...new URLSearchParams(hashQuery).entries()];
  return {
    url,
    hashPath,
    orderedEntries,
  };
}

function extractApolloOrganizationIds(orderedEntries) {
  const seen = new Set();
  const orgIds = [];

  for (const [key, value] of orderedEntries) {
    if (!isApolloOrgParam(key)) continue;
    const normalized = normalizeCell(value);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    orgIds.push(normalized);
  }

  return orgIds;
}

function buildApolloUrlForOrgAndPage(parsedUrl, orgId, pageNumber) {
  const params = new URLSearchParams();
  let insertedPage = false;
  let insertedOrg = false;

  for (const [key, value] of parsedUrl.orderedEntries) {
    if (key === APOLLO_PAGE_PARAM) {
      if (!insertedPage) {
        params.append(APOLLO_PAGE_PARAM, String(pageNumber));
        insertedPage = true;
      }
      continue;
    }

    if (isApolloOrgParam(key)) {
      if (!insertedOrg) {
        params.append(APOLLO_ORG_PARAM, orgId);
        insertedOrg = true;
      }
      continue;
    }

    params.append(key, value);
  }

  if (!insertedPage) {
    params.append(APOLLO_PAGE_PARAM, String(pageNumber));
  }
  if (!insertedOrg) {
    params.append(APOLLO_ORG_PARAM, orgId);
  }

  const nextUrl = new URL(parsedUrl.url.toString());
  nextUrl.hash = `${parsedUrl.hashPath}?${params.toString()}`;
  return nextUrl.toString();
}

function buildApolloBatchFromCombinedUrl(combinedUrl, maxPagesPerOrg) {
  const parsedUrl = parseApolloPeopleSearchUrl(combinedUrl);
  const orgIds = extractApolloOrganizationIds(parsedUrl.orderedEntries);

  if (orgIds.length === 0) {
    throw new Error("Apollo combined URL did not contain any organizationIds[] values.");
  }

  const normalizedMaxPages = Number(maxPagesPerOrg);
  if (!Number.isInteger(normalizedMaxPages) || normalizedMaxPages <= 0) {
    throw new Error(
      `APOLLO_MAX_PAGES_PER_ORG must be a positive integer. Received: ${maxPagesPerOrg}`
    );
  }

  const urls = [];
  for (const orgId of orgIds) {
    for (let pageNumber = 1; pageNumber <= normalizedMaxPages; pageNumber++) {
      urls.push(buildApolloUrlForOrgAndPage(parsedUrl, orgId, pageNumber));
    }
  }

  return {
    orgIds,
    pageCount: normalizedMaxPages,
    urls,
  };
}

function readApolloInputCsv(filePath) {
  const raw = stripBom(fs.readFileSync(filePath, "utf-8"));
  const records = parse(raw, {
    columns: false,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
    relax_quotes: true,
  });

  const urls = [];
  for (const record of records) {
    const cells = Array.isArray(record) ? record : [record];
    const url = cells.map(normalizeCell).find(isLikelyApolloSearchUrl);
    if (!url) continue;
    urls.push(url);
  }

  return urls;
}

function writeApolloInputCsv(filePath, urls) {
  ensureDirectory(path.dirname(filePath));
  const csv = urls.length > 0 ? stringify(urls.map((url) => [url]), { header: false }) : "";
  fs.writeFileSync(filePath, csv);
}

function normalizeCookieExpiry(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return -1;
  return numeric > 1e12 ? Math.floor(numeric / 1000) : Math.floor(numeric);
}

function readApolloCookiesFromFirefoxProfile(profileDir) {
  const cookiesDbPath = path.join(profileDir, "cookies.sqlite");
  if (!fs.existsSync(cookiesDbPath)) {
    throw new Error(`Firefox cookies database not found: ${cookiesDbPath}`);
  }

  let rawJson = "[]";
  try {
    rawJson = execFileSync("sqlite3", ["-json", cookiesDbPath, APOLLO_COOKIE_QUERY], {
      encoding: "utf8",
    });
  } catch (error) {
    if (error?.code === "ENOENT") {
      throw new Error(
        "sqlite3 is required to import Apollo cookies from Firefox. Install sqlite3 or sign in directly in the Camoufox profile."
      );
    }
    throw new Error(
      `Failed to read Apollo cookies from Firefox profile ${profileDir}: ${error.message || error}`
    );
  }

  const rows = JSON.parse(rawJson || "[]");
  return rows.map((row) => ({
    name: row.name,
    value: row.value,
    domain: row.host,
    path: row.path || "/",
    expires: normalizeCookieExpiry(row.expiry),
    secure: Boolean(Number(row.isSecure)),
    httpOnly: Boolean(Number(row.isHttpOnly)),
  }));
}

async function importApolloFirefoxCookies(context, config) {
  if (!config.firefoxProfileDir) return;

  const cookies = readApolloCookiesFromFirefoxProfile(config.firefoxProfileDir);
  if (cookies.length === 0) {
    console.log(
      `Apollo Firefox cookie import: no Apollo cookies found in ${config.firefoxProfileDir}.`
    );
    return;
  }

  await context.addCookies(cookies);
  console.log(
    `Apollo Firefox cookie import: imported ${cookies.length} Apollo cookie(s) from ${config.firefoxProfileDir}.`
  );
}

async function launchApolloContext(config) {
  const { Camoufox } = await import("camoufox-js");

  try {
    return await Camoufox({
      headless: config.headless,
      humanize: config.humanize,
      user_data_dir: config.profileDir,
    });
  } catch (error) {
    const message = String(error?.message || error);
    if (/camoufox fetch|version information not found|not installed/i.test(message)) {
      throw new Error(
        "Camoufox browser is not installed. Run `npm run apollo:fetch-browser` before using apollo-only/apollo-full."
      );
    }
    throw error;
  }
}

async function getApolloPageDiagnostics(page) {
  const diagnostics = await page.evaluate((selectors) => {
    const bodyText = document.body?.innerText || "";
    return {
      title: document.title || "",
      text: bodyText.slice(0, 2000),
      hasResultSelectors: selectors.some((selector) => document.querySelectorAll(selector).length > 0),
    };
  }, APOLLO_RESULT_SELECTORS);

  return {
    url: page.url(),
    ...diagnostics,
  };
}

function looksLikeApolloLogin(diagnostics) {
  const combined = `${diagnostics.url} ${diagnostics.title} ${diagnostics.text}`.toLowerCase();
  return !diagnostics.hasResultSelectors && (
    combined.includes("/login") ||
    combined.includes("/signin") ||
    APOLLO_LOGIN_HINTS.some((hint) => combined.includes(hint))
  );
}

async function assertApolloSessionReady(page) {
  const diagnostics = await getApolloPageDiagnostics(page);
  if (looksLikeApolloLogin(diagnostics)) {
    throw new Error(
      "Apollo session is not ready. Open Apollo in the Camoufox profile and sign in, or set APOLLO_FIREFOX_PROFILE_DIR to a logged-in Firefox profile before running apollo-only/apollo-full."
    );
  }
  return diagnostics;
}

async function waitForApolloResults(page, timeout) {
  try {
    await page.waitForFunction(
      (selectors) => selectors.some((selector) => document.querySelectorAll(selector).length > 0),
      APOLLO_RESULT_SELECTORS,
      { timeout }
    );
    return true;
  } catch {
    return false;
  }
}

function parseApolloPersonId(personPath) {
  const match = String(personPath || "").match(/\/people\/([^/?#]+)/i);
  return match ? match[1] : "";
}

async function extractApolloVisibleRows(page) {
  return page.evaluate(() => {
    const normalize = (value) => String(value || "").replace(/\s+/g, " ").trim();

    function extractName(anchor, fallbackCell) {
      if (anchor) {
        const directText = Array.from(anchor.childNodes)
          .filter((node) => node.nodeType === Node.TEXT_NODE)
          .map((node) => node.textContent || "")
          .join(" ");
        const normalizedDirectText = normalize(directText);
        if (normalizedDirectText) return normalizedDirectText;
      }
      return normalize(fallbackCell?.innerText || "");
    }

    const rows = [];
    for (const row of Array.from(document.querySelectorAll('[role="row"]'))) {
      const nameCell = row.querySelector('[role="gridcell"][aria-colindex="1"]');
      const nameAnchor =
        nameCell?.querySelector('[data-testid="contact-name-cell"] a') ||
        nameCell?.querySelector('a[data-to^="/people/"]') ||
        nameCell?.querySelector('a[href^="#/people/"]');

      const personPath =
        nameAnchor?.getAttribute("data-to") ||
        nameAnchor?.getAttribute("href") ||
        "";

      if (!/\/people\//i.test(personPath)) continue;

      const titleCell = row.querySelector('[role="gridcell"][aria-colindex="2"]');
      const companyCell = row.querySelector('[role="gridcell"][aria-colindex="3"]');

      rows.push({
        Name: extractName(nameAnchor, nameCell),
        Title: normalize(titleCell?.innerText || ""),
        Company: normalize(companyCell?.innerText || ""),
        personPath,
      });
    }

    return rows;
  });
}

async function fetchApolloPersonPayload(page, personId) {
  return page.evaluate(async ({ personId, cacheKey }) => {
    const response = await fetch(`/api/v1/people/${personId}?cacheKey=${cacheKey}`, {
      credentials: "include",
    });
    if (!response.ok) {
      throw new Error(`Apollo person fetch failed for ${personId}: ${response.status}`);
    }
    return response.json();
  }, { personId, cacheKey: Date.now() });
}

async function hydrateApolloRowsWithLinkedIn(page, rows, config) {
  const hydratedRows = [];

  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    const personId = parseApolloPersonId(row.personPath);
    if (!personId) {
      hydratedRows.push([row.Name || "", row.Title || "", row.Company || "", ""]);
      continue;
    }

    if (index > 0) {
      await waitRandomActionDelay(page, config, "between Apollo person detail fetches");
    }

    try {
      const payload = await fetchApolloPersonPayload(page, personId);
      const person = payload?.person || {};
      const organization = person?.organization || {};

      hydratedRows.push([
        normalizeCell(row.Name || person.name || `${person.first_name || ""} ${person.last_name || ""}`),
        normalizeCell(row.Title || person.title || ""),
        normalizeCell(row.Company || organization.name || ""),
        normalizeCell(person.linkedin_url || ""),
      ]);
    } catch (error) {
      console.log(`  Apollo person details unavailable for ${personId}: ${error.message}`);
      hydratedRows.push([row.Name || "", row.Title || "", row.Company || "", ""]);
    }
  }

  return hydratedRows.filter((row) => row.some(Boolean));
}

function writeRawRows(filePath, rawRows) {
  const normalizedRows = rawRows
    .map((row) => trimTrailingEmptyCells(row.map(normalizeCell)))
    .filter((row) => row.some(Boolean));

  ensureDirectory(path.dirname(filePath));
  const csv = normalizedRows.length > 0 ? stringify(normalizedRows, { header: false }) : "";
  fs.writeFileSync(filePath, csv);
}

function buildCanonicalRows(rawRows) {
  const deduped = new Map();

  for (const rawRow of rawRows) {
    const [Name = "", Title = "", Company = "", linkedinUrl = ""] = rawRow.map(normalizeCell);
    const normalizedLinkedIn = normalizeLinkedInUrl(linkedinUrl);
    if (!normalizedLinkedIn) continue;
    if (deduped.has(normalizedLinkedIn)) continue;

    deduped.set(normalizedLinkedIn, {
      Name,
      Title,
      Company,
      linkedinUrl: normalizedLinkedIn,
    });
  }

  return [...deduped.values()];
}

function writeCanonicalRows(filePath, rows) {
  ensureDirectory(path.dirname(filePath));
  const csv = stringify(rows, {
    header: true,
    columns: RAW_COLUMNS,
  });
  fs.writeFileSync(filePath, csv);
}

async function runApolloScrape(config) {
  if (!fs.existsSync(config.inputCsv)) {
    throw new Error(`Apollo input CSV file not found: ${config.inputCsv}`);
  }

  const urls = readApolloInputCsv(config.inputCsv);
  if (urls.length === 0) {
    throw new Error(`No valid Apollo search URLs found in ${config.inputCsv}`);
  }

  const batch = urls.slice(0, config.maxUrls);
  cleanupRawOutputDir(config.rawOutputDir, config.rawFilePrefix);
  ensureDirectory(config.profileDir);

  console.log(`Apollo stage: ${batch.length} Apollo URL(s) to process (max: ${config.maxUrls})`);

  const context = await launchApolloContext(config);
  await importApolloFirefoxCookies(context, config);
  const page = context.pages()[0] || (await context.newPage());
  const allRawRows = [];
  const rawFiles = [];

  try {
    for (let index = 0; index < batch.length; index++) {
      const url = batch[index];
      const rawFilePath = path.join(
        config.rawOutputDir,
        buildRawFileName(config.rawFilePrefix, index + 1)
      );

      console.log(`\n[Apollo ${index + 1}/${batch.length}] ${url}`);

      let rawRows = [];
      let attempt = 0;
      while (attempt < 2) {
        try {
          await page.goto(url, {
            waitUntil: "domcontentloaded",
            timeout: config.pageTimeoutMs,
          });
          await waitRandomActionDelay(page, config, "after Apollo navigation");

          await assertApolloSessionReady(page);

          const foundResults = await waitForApolloResults(page, config.resultsSelectorTimeoutMs);
          if (!foundResults) {
            console.log("  Apollo results did not render before timeout. Capturing current page anyway...");
            await assertApolloSessionReady(page);
          }

          const visibleRows = await extractApolloVisibleRows(page);
          rawRows = await hydrateApolloRowsWithLinkedIn(page, visibleRows, config);
          console.log(`  Captured ${rawRows.length} raw Apollo row(s).`);
          break;
        } catch (error) {
          const retryable = /timeout|ERR_ABORTED|Navigation/i.test(error?.message || "");
          const shouldRetry = retryable && attempt === 0;

          if (shouldRetry) {
            console.log(`  Apollo navigation failed (${error.message}). Retrying once...`);
            attempt++;
            continue;
          }

          throw error;
        }
      }

      writeRawRows(rawFilePath, rawRows);
      rawFiles.push(rawFilePath);
      allRawRows.push(...rawRows);
    }
  } finally {
    await context.close();
  }

  const canonicalRows = buildCanonicalRows(allRawRows);
  writeCanonicalRows(config.canonicalOutputCsv, canonicalRows);

  console.log(`\nApollo raw output dir: ${config.rawOutputDir}`);
  console.log(`Apollo canonical output: ${config.canonicalOutputCsv}`);
  console.log(`Apollo canonical rows: ${canonicalRows.length}`);

  if (canonicalRows.length === 0) {
    throw new Error(
      `Apollo scrape completed, but no usable LinkedIn URLs were found. Check raw output under ${config.rawOutputDir}.`
    );
  }

  return {
    rawOutputDir: config.rawOutputDir,
    rawFiles,
    canonicalOutputCsv: config.canonicalOutputCsv,
    rawRowCount: allRawRows.length,
    canonicalRowCount: canonicalRows.length,
    urlCount: batch.length,
  };
}

module.exports = {
  buildApolloBatchFromCombinedUrl,
  buildCanonicalRows,
  buildRawFileName,
  normalizeLinkedInUrl,
  readApolloInputCsv,
  runApolloScrape,
  writeApolloInputCsv,
};
