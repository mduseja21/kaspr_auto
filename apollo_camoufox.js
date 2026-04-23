const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");
const { parse } = require("csv-parse/sync");
const { stringify } = require("csv-stringify/sync");

const RAW_COLUMNS = ["Name", "Title", "Company", "linkedinUrl"];
const ORG_MATCH_REPORT_COLUMNS = [
  "firmName",
  "firmCacheKey",
  "status",
  "orgId",
  "matchedCompanyName",
  "matchReason",
];
const APOLLO_PAGE_PARAM = "page";
const APOLLO_ORG_PARAM = "organizationIds[]";
const APOLLO_BOOTSTRAP_URL = "https://app.apollo.io/#/people";
const APOLLO_COMPANY_SEARCH_PATH = "/api/v1/mixed_companies/search";
const APOLLO_COMPANY_LOOKUP_PER_PAGE = 5;
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
const COMPANY_ENTITY_SUFFIX_TOKENS = new Set([
  "ab",
  "ag",
  "aps",
  "as",
  "bv",
  "co",
  "company",
  "corp",
  "corporation",
  "gmbh",
  "inc",
  "incorporated",
  "kk",
  "limited",
  "llc",
  "llp",
  "lp",
  "ltd",
  "nv",
  "oy",
  "plc",
  "pte",
  "sa",
  "sas",
  "spa",
  "srl",
  "sro",
]);

function normalizeCell(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function stripBom(raw) {
  return raw.charCodeAt(0) === 0xfeff ? raw.slice(1) : raw;
}

function isLikelyApolloSearchUrl(value) {
  return /(?:app\.)?apollo\.io\/#\/people(?:\?|$)/i.test(String(value || "").trim());
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
  const match = url.pathname.match(/^\/in\/([^/?#]+)/i);
  if (!match) return "";
  const profileSlug = match[1];
  return `https://www.linkedin.com/in/${profileSlug}/`;
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

function dedupeNormalizedValues(values) {
  const deduped = [];
  const seen = new Set();

  for (const value of values) {
    const normalized = normalizeCell(value);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    deduped.push(normalized);
  }

  return deduped;
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

function buildApolloBatchFromOrganizationIds(templateUrl, organizationIds, maxPagesPerOrg) {
  const parsedUrl = parseApolloPeopleSearchUrl(templateUrl);
  const orgIds = dedupeNormalizedValues(organizationIds);
  if (orgIds.length === 0) {
    throw new Error("Apollo org batch generation requires at least one organization id.");
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

function buildApolloBatchFromCombinedUrl(combinedUrl, maxPagesPerOrg) {
  const parsedUrl = parseApolloPeopleSearchUrl(combinedUrl);
  const orgIds = extractApolloOrganizationIds(parsedUrl.orderedEntries);

  if (orgIds.length === 0) {
    throw new Error("Apollo combined URL did not contain any organizationIds[] values.");
  }

  return buildApolloBatchFromOrganizationIds(combinedUrl, orgIds, maxPagesPerOrg);
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

function readApolloFirmInputCsv(filePath) {
  const raw = stripBom(fs.readFileSync(filePath, "utf-8"));
  const records = parse(raw, {
    columns: false,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
    relax_quotes: true,
  });

  const firms = [];
  for (const record of records) {
    const cells = Array.isArray(record) ? record : [record];
    const firmName = cells.map(normalizeCell).find(Boolean);
    if (!firmName) continue;
    firms.push(firmName);
  }

  return firms;
}

function normalizeApolloFirmCacheKey(value) {
  const normalizedCompanyName = normalizeApolloCompanyName(value);
  if (normalizedCompanyName) return normalizedCompanyName;

  return normalizeCell(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/['’]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function buildApolloFirmLookupEntries(firms) {
  const entries = new Map();

  for (const firmName of firms) {
    const normalizedFirmName = normalizeCell(firmName);
    if (!normalizedFirmName) continue;

    const firmCacheKey = normalizeApolloFirmCacheKey(normalizedFirmName);
    if (!firmCacheKey) continue;

    if (entries.has(firmCacheKey)) {
      const existing = entries.get(firmCacheKey);
      existing.firmName = normalizedFirmName;
      existing.inputCount += 1;
      continue;
    }

    entries.set(firmCacheKey, {
      firmName: normalizedFirmName,
      firmCacheKey,
      inputCount: 1,
    });
  }

  return [...entries.values()];
}

function normalizeApolloOrgMatchRow(row) {
  const firmName = normalizeCell(row?.firmName || "");
  const firmCacheKey = normalizeApolloFirmCacheKey(
    row?.firmCacheKey || firmName || row?.matchedCompanyName || ""
  );
  if (!firmCacheKey) return null;

  return {
    firmName: firmName || firmCacheKey,
    firmCacheKey,
    status: normalizeCell(row?.status || "").toLowerCase() === "resolved" ? "resolved" : "unresolved",
    orgId: normalizeCell(row?.orgId || ""),
    matchedCompanyName: normalizeCell(row?.matchedCompanyName || ""),
    matchReason: normalizeCell(row?.matchReason || ""),
  };
}

function readApolloOrgMatchReport(filePath) {
  const cache = new Map();
  if (!filePath || !fs.existsSync(filePath)) {
    return cache;
  }

  const raw = stripBom(fs.readFileSync(filePath, "utf-8"));
  if (!normalizeCell(raw)) {
    return cache;
  }

  const records = parse(raw, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
    relax_quotes: true,
  });

  for (const record of records) {
    const normalizedRow = normalizeApolloOrgMatchRow(record);
    if (!normalizedRow) continue;
    cache.set(normalizedRow.firmCacheKey, normalizedRow);
  }

  return cache;
}

function upsertApolloOrgMatchCache(cache, row) {
  const normalizedRow = normalizeApolloOrgMatchRow(row);
  if (!normalizedRow) return null;
  cache.set(normalizedRow.firmCacheKey, normalizedRow);
  return normalizedRow;
}

function writeApolloInputCsv(filePath, urls) {
  ensureDirectory(path.dirname(filePath));
  const csv = urls.length > 0 ? stringify(urls.map((url) => [url]), { header: false }) : "";
  fs.writeFileSync(filePath, csv);
}

function writeApolloOrgMatchReport(filePath, rows) {
  ensureDirectory(path.dirname(filePath));
  const normalizedRows = rows
    .map((row) => normalizeApolloOrgMatchRow(row))
    .filter(Boolean);
  const csv = stringify(normalizedRows, {
    header: true,
    columns: ORG_MATCH_REPORT_COLUMNS,
  });
  fs.writeFileSync(filePath, csv);
}

function tokenizeApolloCompanyName(value) {
  const normalized = normalizeCell(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/['’]/g, "")
    .replace(/[^a-z0-9]+/g, " ");

  const tokens = normalized.split(/\s+/).filter(Boolean);

  if (tokens[0] === "the" && tokens.length > 1) {
    tokens.shift();
  }

  while (tokens.length > 1 && COMPANY_ENTITY_SUFFIX_TOKENS.has(tokens[tokens.length - 1])) {
    tokens.pop();
  }

  return tokens;
}

function normalizeApolloCompanyName(value) {
  return tokenizeApolloCompanyName(value).join(" ");
}

function sharedTokenPrefixLength(leftTokens, rightTokens) {
  const limit = Math.min(leftTokens.length, rightTokens.length);
  let index = 0;
  while (index < limit && leftTokens[index] === rightTokens[index]) {
    index += 1;
  }
  return index;
}

function scoreApolloOrganizationCandidate(firmName, candidateName) {
  const firmTokens = tokenizeApolloCompanyName(firmName);
  const candidateTokens = tokenizeApolloCompanyName(candidateName);
  const firmNormalized = firmTokens.join(" ");
  const candidateNormalized = candidateTokens.join(" ");

  if (!firmNormalized || !candidateNormalized) {
    return {
      score: 0,
      reason: "missing_normalized_name",
    };
  }

  if (firmNormalized === candidateNormalized) {
    return {
      score: 100,
      reason: "exact_normalized_name_match",
    };
  }

  if (firmNormalized.replace(/\s+/g, "") === candidateNormalized.replace(/\s+/g, "")) {
    return {
      score: 95,
      reason: "punctuation_variant_name_match",
    };
  }

  const prefixLength = sharedTokenPrefixLength(firmTokens, candidateTokens);
  const shorterLength = Math.min(firmTokens.length, candidateTokens.length);
  const longerLength = Math.max(firmTokens.length, candidateTokens.length);
  if (
    shorterLength >= 2 &&
    longerLength === shorterLength + 1 &&
    prefixLength === shorterLength
  ) {
    return {
      score: 85,
      reason: "near_exact_prefix_name_match",
    };
  }

  const extraCandidateTokens = candidateTokens.length - firmTokens.length;
  if (
    firmTokens.length >= 1 &&
    prefixLength === firmTokens.length &&
    extraCandidateTokens >= 1 &&
    extraCandidateTokens <= 3
  ) {
    return {
      score: 65 - Math.max(0, extraCandidateTokens - 1) * 5,
      reason: "relaxed_prefix_name_match",
    };
  }

  return {
    score: 0,
    reason: "no_exactish_name_match",
  };
}

function selectApolloOrganizationMatch(firmName, candidates) {
  const normalizedFirmName = normalizeApolloCompanyName(firmName);
  if (!normalizedFirmName) {
    return {
      firmName,
      status: "unresolved",
      orgId: "",
      matchedCompanyName: "",
      matchReason: "empty_firm_name",
    };
  }

  const validCandidates = candidates.filter((candidate) => candidate.orgId && candidate.name);
  if (validCandidates.length === 0) {
    return {
      firmName,
      status: "unresolved",
      orgId: "",
      matchedCompanyName: "",
      matchReason: "no_candidates_returned",
    };
  }

  const scoredCandidates = validCandidates
    .map((candidate) => {
      const score = scoreApolloOrganizationCandidate(firmName, candidate.name);
      return {
        ...score,
        candidate,
      };
    })
    .filter((candidate) => candidate.score > 0)
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      return left.candidate.name.localeCompare(right.candidate.name);
    });

  if (scoredCandidates.length === 0) {
    return {
      firmName,
      status: "unresolved",
      orgId: "",
      matchedCompanyName: "",
      matchReason: "no_exactish_company_name_match",
    };
  }

  const topCandidate = scoredCandidates[0];
  if (
    topCandidate.reason === "relaxed_prefix_name_match" &&
    scoredCandidates.length > 1
  ) {
    const relaxedCandidates = scoredCandidates
      .filter((candidate) => candidate.reason === "relaxed_prefix_name_match")
      .slice(0, 3)
      .map((candidate) => candidate.candidate.name)
      .join(" | ");
    return {
      firmName,
      status: "unresolved",
      orgId: "",
      matchedCompanyName: "",
      matchReason: `ambiguous_relaxed_prefix_name_match: ${relaxedCandidates}`,
    };
  }

  const tiedCandidates = scoredCandidates.filter((candidate) => candidate.score === topCandidate.score);

  if (tiedCandidates.length > 1) {
    const ambiguousNames = tiedCandidates
      .slice(0, 3)
      .map((candidate) => candidate.candidate.name)
      .join(" | ");
    return {
      firmName,
      status: "unresolved",
      orgId: "",
      matchedCompanyName: "",
      matchReason: `ambiguous_${topCandidate.reason}: ${ambiguousNames}`,
    };
  }

  return {
    firmName,
    status: "resolved",
    orgId: topCandidate.candidate.orgId,
    matchedCompanyName: topCandidate.candidate.name,
    matchReason: topCandidate.reason,
  };
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

async function openApolloPage(config, bootstrapUrl = APOLLO_BOOTSTRAP_URL) {
  ensureDirectory(config.profileDir);

  const context = await launchApolloContext(config);
  try {
    await importApolloFirefoxCookies(context, config);
    const page = context.pages()[0] || (await context.newPage());

    if (bootstrapUrl) {
      await page.goto(bootstrapUrl, {
        waitUntil: "domcontentloaded",
        timeout: config.pageTimeoutMs,
      });
      await waitRandomActionDelay(page, config, "after Apollo bootstrap navigation");
      await assertApolloSessionReady(page);
    }

    return {
      context,
      page,
    };
  } catch (error) {
    await context.close();
    throw error;
  }
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

async function fetchApolloCompanySearchPayload(page, firmName, perPage = APOLLO_COMPANY_LOOKUP_PER_PAGE) {
  return page.evaluate(async ({ path, firmName, perPage, cacheKey }) => {
    const response = await fetch(path, {
      method: "POST",
      credentials: "include",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        q_organization_name: firmName,
        page: 1,
        per_page: perPage,
        cacheKey,
      }),
    });

    if (!response.ok) {
      throw new Error(`Apollo company lookup failed for "${firmName}": ${response.status}`);
    }

    return response.json();
  }, {
    path: APOLLO_COMPANY_SEARCH_PATH,
    firmName,
    perPage,
    cacheKey: Date.now(),
  });
}

function extractApolloCompanyCandidates(payload) {
  const rawCandidates = Array.isArray(payload?.organizations)
    ? payload.organizations
    : Array.isArray(payload?.accounts)
      ? payload.accounts
      : [];

  const deduped = new Map();
  for (const candidate of rawCandidates) {
    const orgId = normalizeCell(
      candidate?.id || candidate?.organization_id || candidate?.organization?.id || ""
    );
    const name = normalizeCell(
      candidate?.name || candidate?.organization_name || candidate?.organization?.name || ""
    );
    if (!orgId || !name || deduped.has(orgId)) continue;

    deduped.set(orgId, {
      orgId,
      name,
    });
  }

  return [...deduped.values()];
}

async function prepareApolloFirmInput(config) {
  const reportRows = [];
  let context = null;
  let firms = [];
  let uniqueFirmEntries = [];
  let orgMatchCache = new Map();
  let cacheHitCount = 0;
  let cacheMissCount = 0;
  let refreshedCount = 0;
  let apolloLookupCount = 0;
  let loadedCacheCount = 0;

  try {
    orgMatchCache = readApolloOrgMatchReport(config.orgMatchReportCsv);
    loadedCacheCount = orgMatchCache.size;

    if (!config.firmInputCsv) {
      throw new Error("Apollo firm lookup requires a firm input CSV path.");
    }
    if (!config.templateUrl) {
      throw new Error(
        "APOLLO_FIRM_INPUT_CSV requires APOLLO_COMBINED_URL as the Apollo people-search template URL."
      );
    }
    if (!fs.existsSync(config.firmInputCsv)) {
      throw new Error(`Apollo firm input CSV file not found: ${config.firmInputCsv}`);
    }

    firms = readApolloFirmInputCsv(config.firmInputCsv);
    if (firms.length === 0) {
      throw new Error(`No firm names found in ${config.firmInputCsv}`);
    }

    uniqueFirmEntries = buildApolloFirmLookupEntries(firms);

    console.log(
      `Apollo firm lookup: ${firms.length} input row(s), ${uniqueFirmEntries.length} unique firm name(s) to resolve.`
    );
    console.log(`Apollo firm lookup cache: loaded ${loadedCacheCount} cached firm match(es).`);

    let page = null;
    async function ensureApolloLookupPage() {
      if (page) return page;
      const apollo = await openApolloPage(config);
      context = apollo.context;
      page = apollo.page;
      return page;
    }

    for (let index = 0; index < uniqueFirmEntries.length; index++) {
      const firmEntry = uniqueFirmEntries[index];
      const cachedRow = orgMatchCache.get(firmEntry.firmCacheKey);

      if (cachedRow && !config.forceRefreshOrgMatches) {
        const reusedRow = upsertApolloOrgMatchCache(orgMatchCache, {
          ...cachedRow,
          firmName: firmEntry.firmName,
          firmCacheKey: firmEntry.firmCacheKey,
        });
        cacheHitCount += 1;
        reportRows.push(reusedRow);

        if (reusedRow.status === "resolved") {
          console.log(
            `  [Firm ${index + 1}/${uniqueFirmEntries.length}] ${firmEntry.firmName} -> cache hit ${reusedRow.orgId} (${reusedRow.matchedCompanyName})`
          );
        } else {
          console.log(
            `  [Firm ${index + 1}/${uniqueFirmEntries.length}] ${firmEntry.firmName} -> cache hit unresolved (${reusedRow.matchReason})`
          );
        }
        continue;
      }

      if (cachedRow) {
        refreshedCount += 1;
      } else {
        cacheMissCount += 1;
      }

      const lookupPage = await ensureApolloLookupPage();
      if (apolloLookupCount > 0) {
        await waitRandomActionDelay(lookupPage, config, "between Apollo company lookups");
      }

      try {
        const payload = await fetchApolloCompanySearchPayload(lookupPage, firmEntry.firmName);
        const candidates = extractApolloCompanyCandidates(payload);
        const result = upsertApolloOrgMatchCache(
          orgMatchCache,
          selectApolloOrganizationMatch(firmEntry.firmName, candidates)
        );
        apolloLookupCount += 1;
        reportRows.push(result);

        if (cachedRow) {
          console.log(
            `  [Firm ${index + 1}/${uniqueFirmEntries.length}] ${firmEntry.firmName} -> refreshed ${result.status === "resolved" ? `${result.orgId} (${result.matchedCompanyName})` : `unresolved (${result.matchReason})`}`
          );
        } else {
          if (result.status === "resolved") {
            console.log(
              `  [Firm ${index + 1}/${uniqueFirmEntries.length}] ${firmEntry.firmName} -> ${result.orgId} (${result.matchedCompanyName})`
            );
          } else {
            console.log(
              `  [Firm ${index + 1}/${uniqueFirmEntries.length}] ${firmEntry.firmName} -> unresolved (${result.matchReason})`
            );
          }
        }
      } catch (error) {
        const result = upsertApolloOrgMatchCache(orgMatchCache, {
          firmName: firmEntry.firmName,
          firmCacheKey: firmEntry.firmCacheKey,
          status: "unresolved",
          orgId: "",
          matchedCompanyName: "",
          matchReason: `lookup_error: ${normalizeCell(error?.message || error)}`,
        });
        apolloLookupCount += 1;
        reportRows.push(result);
        if (cachedRow) {
          console.log(
            `  [Firm ${index + 1}/${uniqueFirmEntries.length}] ${firmEntry.firmName} -> refreshed unresolved (${result.matchReason})`
          );
        } else {
          console.log(
            `  [Firm ${index + 1}/${uniqueFirmEntries.length}] ${firmEntry.firmName} -> unresolved (${result.matchReason})`
          );
        }
      }
    }

    console.log(
      `Apollo firm lookup cache summary: ${cacheHitCount} hit(s), ${cacheMissCount} miss(es), ${refreshedCount} refresh(es), ${apolloLookupCount} Apollo lookup(s).`
    );
  } finally {
    if (config.orgMatchReportCsv && (loadedCacheCount > 0 || reportRows.length > 0)) {
      writeApolloOrgMatchReport(config.orgMatchReportCsv, [...orgMatchCache.values()]);
    }
    if (context) {
      await context.close();
    }
  }

  const resolvedOrgIds = dedupeNormalizedValues(
    reportRows
      .filter((row) => row.status === "resolved")
      .map((row) => row.orgId)
  );

  if (resolvedOrgIds.length === 0) {
    throw new Error(
      `Apollo firm lookup completed, but no firms resolved to org ids. Check match report at ${config.orgMatchReportCsv}.`
    );
  }

  const batch = buildApolloBatchFromOrganizationIds(
    config.templateUrl,
    resolvedOrgIds,
    config.maxPagesPerOrg
  );
  writeApolloInputCsv(config.generatedInputCsv, batch.urls);

  return {
    firmCount: firms.length,
    uniqueFirmCount: uniqueFirmEntries.length,
    resolvedFirmCount: reportRows.filter((row) => row.status === "resolved").length,
    unresolvedFirmCount: reportRows.filter((row) => row.status !== "resolved").length,
    cacheHitCount,
    cacheMissCount,
    refreshedCount,
    apolloLookupCount,
    loadedCacheCount,
    orgCount: batch.orgIds.length,
    pageCount: batch.pageCount,
    urlCount: batch.urls.length,
    generatedInputCsv: config.generatedInputCsv,
    orgMatchReportCsv: config.orgMatchReportCsv,
  };
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

  console.log(`Apollo stage: ${batch.length} Apollo URL(s) to process (max: ${config.maxUrls})`);

  const { context, page } = await openApolloPage(config, null);
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
    canonicalRows,
    rawRowCount: allRawRows.length,
    canonicalRowCount: canonicalRows.length,
    urlCount: batch.length,
  };
}

module.exports = {
  buildApolloBatchFromCombinedUrl,
  buildApolloBatchFromOrganizationIds,
  buildCanonicalRows,
  buildApolloFirmLookupEntries,
  buildRawFileName,
  normalizeApolloCompanyName,
  normalizeApolloFirmCacheKey,
  normalizeLinkedInUrl,
  prepareApolloFirmInput,
  readApolloInputCsv,
  readApolloFirmInputCsv,
  readApolloOrgMatchReport,
  runApolloScrape,
  selectApolloOrganizationMatch,
  writeApolloInputCsv,
  writeApolloOrgMatchReport,
};
