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
const APOLLO_TITLE_PARAM = "personTitles[]";
const APOLLO_BOOTSTRAP_URL = "https://app.apollo.io/#/people";
const APOLLO_COMPANY_SEARCH_PATH = "/api/v1/mixed_companies/search";
const APOLLO_PEOPLE_SEARCH_PATH = "/api/v1/mixed_people/search";
const APOLLO_COMPANY_LOOKUP_PER_PAGE = 5;
const APOLLO_PEOPLE_SEARCH_PER_PAGE = 25;
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

function getProgressPath(rawOutputDir) {
  return path.join(rawOutputDir, "apollo_progress.json");
}

function loadApolloProgress(rawOutputDir) {
  const progressPath = getProgressPath(rawOutputDir);
  if (!fs.existsSync(progressPath)) return { completedUrls: {} };
  try {
    return JSON.parse(fs.readFileSync(progressPath, "utf-8"));
  } catch {
    return { completedUrls: {} };
  }
}

function saveApolloProgress(rawOutputDir, progress) {
  ensureDirectory(rawOutputDir);
  fs.writeFileSync(getProgressPath(rawOutputDir), JSON.stringify(progress, null, 2));
}

function clearApolloProgress(rawOutputDir) {
  const progressPath = getProgressPath(rawOutputDir);
  if (fs.existsSync(progressPath)) fs.unlinkSync(progressPath);
}

function extractApolloUrlKey(url) {
  try {
    const parsed = new URL(url);
    const hashQuery = (parsed.hash || "").replace(/^#\/people\??/, "");
    const params = new URLSearchParams(hashQuery);
    return {
      orgId: params.get(APOLLO_ORG_PARAM) || "",
      title: params.get(APOLLO_TITLE_PARAM) || "",
      page: parseInt(params.get(APOLLO_PAGE_PARAM) || "1", 10),
      comboKey: `${params.get(APOLLO_ORG_PARAM) || ""}::${params.get(APOLLO_TITLE_PARAM) || ""}`,
    };
  } catch {
    return { orgId: "", title: "", page: 1, comboKey: "" };
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

function extractApolloPersonTitles(orderedEntries) {
  const seen = new Set();
  const titles = [];

  for (const [key, value] of orderedEntries) {
    if (key !== APOLLO_TITLE_PARAM) continue;
    const normalized = normalizeCell(value);
    if (!normalized || seen.has(normalized.toLowerCase())) continue;
    seen.add(normalized.toLowerCase());
    titles.push(normalized);
  }

  return titles;
}

function buildApolloUrlForOrgAndPage(parsedUrl, orgId, pageNumber, title) {
  const params = new URLSearchParams();
  let insertedPage = false;
  let insertedOrg = false;
  let insertedTitle = false;

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

    if (title && key === APOLLO_TITLE_PARAM) {
      if (!insertedTitle) {
        params.append(APOLLO_TITLE_PARAM, title);
        insertedTitle = true;
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
  if (title && !insertedTitle) {
    params.append(APOLLO_TITLE_PARAM, title);
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

  const titles = extractApolloPersonTitles(parsedUrl.orderedEntries);
  const fanOutByTitle = titles.length > 1;

  const urls = [];
  for (const orgId of orgIds) {
    if (fanOutByTitle) {
      for (const title of titles) {
        for (let pageNumber = 1; pageNumber <= normalizedMaxPages; pageNumber++) {
          urls.push(buildApolloUrlForOrgAndPage(parsedUrl, orgId, pageNumber, title));
        }
      }
    } else {
      for (let pageNumber = 1; pageNumber <= normalizedMaxPages; pageNumber++) {
        urls.push(buildApolloUrlForOrgAndPage(parsedUrl, orgId, pageNumber, null));
      }
    }
  }

  return {
    orgIds,
    titles,
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
    .replace(/[‘’]/g, "")
    .replace(/[^a-z0-9]+/g, " ");

  const tokens = normalized.split(/\s+/).filter(Boolean);

  if (tokens[0] === "the" && tokens.length > 1) {
    tokens.shift();
  }

  // Rejoin split legal suffixes like "L.P." -> ["l", "p"] back to "lp"
  if (
    tokens.length >= 3 &&
    tokens[tokens.length - 2].length === 1 &&
    tokens[tokens.length - 1].length === 1
  ) {
    const joined = tokens[tokens.length - 2] + tokens[tokens.length - 1];
    if (COMPANY_ENTITY_SUFFIX_TOKENS.has(joined)) {
      tokens.splice(tokens.length - 2, 2, joined);
    }
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
    const uniqueNormalizedNames = new Set(
      tiedCandidates.map((c) => normalizeApolloCompanyName(c.candidate.name))
    );
    if (uniqueNormalizedNames.size === 1) {
      return {
        firmName,
        status: "resolved",
        orgId: topCandidate.candidate.orgId,
        matchedCompanyName: topCandidate.candidate.name,
        matchReason: `${topCandidate.reason}_deduped_variants`,
      };
    }

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

function getApolloAddons() {
  const capsolverDir = path.join(process.cwd(), "runtime", "apollo", "extensions", "capsolver");
  if (fs.existsSync(path.join(capsolverDir, "manifest.json"))) {
    return [capsolverDir];
  }
  return [];
}

async function launchApolloContext(config) {
  const { Camoufox } = await import("camoufox-js");
  const addons = getApolloAddons();

  try {
    return await Camoufox({
      headless: config.headless,
      humanize: config.humanize,
      user_data_dir: config.profileDir,
      addons: addons.length > 0 ? addons : undefined,
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

async function detectCloudflareChallenge(page) {
  return page.evaluate(() => {
    const body = document.body?.innerText || "";
    const title = document.title || "";
    const combined = (body + " " + title).toLowerCase();
    const hasTurnstile = !!document.querySelector("#challenge-running, #challenge-stage, .cf-turnstile, [data-sitekey]");
    const hasText = combined.includes("verify you are human") ||
      combined.includes("checking your browser") ||
      combined.includes("just a moment") ||
      combined.includes("cloudflare");
    return hasTurnstile || hasText;
  });
}

async function waitForCloudflareResolution(page) {
  console.log("  Cloudflare challenge detected. Waiting for resolution (Capsolver or manual)...");

  while (true) {
    await new Promise((r) => setTimeout(r, 3000));
    const stillBlocked = await detectCloudflareChallenge(page);
    if (!stillBlocked) {
      console.log("  Cloudflare challenge resolved!");
      await new Promise((r) => setTimeout(r, 2000));
      return true;
    }
  }
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

      if (await detectCloudflareChallenge(page)) {
        await waitForCloudflareResolution(page);
      }

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

const APOLLO_URL_TO_API_PARAM_MAP = {
  "personTitles[]": "person_titles",
  "personLocations[]": "person_locations",
  "contactEmailStatusV2[]": "contact_email_status",
  "prospectedByCurrentTeam[]": "prospected_by_current_team",
};

function extractSearchFiltersFromTemplateUrl(templateUrl) {
  const parsed = parseApolloPeopleSearchUrl(templateUrl);
  const filters = {};

  for (const [urlParam, apiParam] of Object.entries(APOLLO_URL_TO_API_PARAM_MAP)) {
    const values = parsed.orderedEntries
      .filter(([key]) => key === urlParam)
      .map(([, value]) => normalizeCell(value))
      .filter(Boolean);
    if (values.length > 0) filters[apiParam] = values;
  }

  return filters;
}

async function fetchApolloPeopleSearch(page, { orgId, filters, pageNumber = 1, perPage = APOLLO_PEOPLE_SEARCH_PER_PAGE }) {
  return page.evaluate(async ({ path, params, cacheKey }) => {
    const response = await fetch(path, {
      method: "POST",
      credentials: "include",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({ ...params, cacheKey }),
    });

    if (!response.ok) {
      throw new Error(`Apollo people search failed: ${response.status}`);
    }

    return response.json();
  }, {
    path: APOLLO_PEOPLE_SEARCH_PATH,
    params: {
      page: pageNumber,
      per_page: perPage,
      organization_ids: [orgId],
      ...filters,
    },
    cacheKey: Date.now(),
  });
}

function extractPeopleFromSearchResponse(response) {
  const people = Array.isArray(response?.people) ? response.people
    : Array.isArray(response?.contacts) ? response.contacts
    : [];

  return people.map((person) => {
    const personId = normalizeCell(person?.id || "");
    const name = normalizeCell(
      person?.name || `${person?.first_name || ""} ${person?.last_name || ""}`.trim()
    );
    const title = normalizeCell(person?.title || "");
    const orgName = normalizeCell(
      person?.organization?.name || person?.organization_name || ""
    );
    const linkedinUrl = normalizeCell(person?.linkedin_url || "");

    return { personId, Name: name, Title: title, Company: orgName, linkedinUrl };
  }).filter((p) => p.personId);
}

function getPaginationFromSearchResponse(response) {
  return {
    totalEntries: response?.pagination?.total_entries || 0,
    totalPages: response?.pagination?.total_pages || 0,
    currentPage: response?.pagination?.page || 1,
  };
}

function buildApolloSearchQuery(firmName) {
  const normalized = normalizeApolloCompanyName(firmName);
  return normalized || normalizeCell(firmName);
}

async function fetchApolloCompanySearchPayload(page, firmName, perPage = APOLLO_COMPANY_LOOKUP_PER_PAGE) {
  const searchQuery = buildApolloSearchQuery(firmName);
  return page.evaluate(async ({ path, searchQuery, perPage, cacheKey }) => {
    const response = await fetch(path, {
      method: "POST",
      credentials: "include",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        q_organization_name: searchQuery,
        page: 1,
        per_page: perPage,
        cacheKey,
      }),
    });

    if (!response.ok) {
      throw new Error(`Apollo company lookup failed for "${searchQuery}": ${response.status}`);
    }

    return response.json();
  }, {
    path: APOLLO_COMPANY_SEARCH_PATH,
    searchQuery,
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

    const excludedCompanies = Array.isArray(config.excludedCompanies) ? config.excludedCompanies : [];
    if (excludedCompanies.length > 0) {
      const beforeCount = firms.length;
      firms = firms.filter((firm) => {
        const lower = normalizeCell(firm).toLowerCase();
        return !excludedCompanies.some((ex) => lower.includes(ex));
      });
      const skipped = beforeCount - firms.length;
      if (skipped > 0) {
        console.log(`Apollo firm lookup: filtered out ${skipped} excluded firm(s).`);
      }
      if (firms.length === 0) {
        throw new Error("All firms were excluded. Check excluded_companies.txt.");
      }
    }

    uniqueFirmEntries = buildApolloFirmLookupEntries(firms);

    console.log(
      `Apollo firm lookup: ${firms.length} input row(s), ${uniqueFirmEntries.length} unique firm name(s) to resolve.`
    );
    console.log(`Apollo firm lookup cache: loaded ${loadedCacheCount} cached firm match(es).`);

    let page = null;
    const CONSECUTIVE_ERROR_BACKOFF_THRESHOLD = 3;
    const MAX_BACKOFF_ATTEMPTS = 4;
    const BASE_BACKOFF_MS = 15000;
    let consecutiveErrors = 0;

    async function ensureApolloLookupPage() {
      if (page) return page;
      const apollo = await openApolloPage(config);
      context = apollo.context;
      page = apollo.page;
      return page;
    }

    async function resetApolloLookupPage() {
      if (context) {
        try { await context.close(); } catch {}
        context = null;
        page = null;
      }
      return ensureApolloLookupPage();
    }

    for (let index = 0; index < uniqueFirmEntries.length; index++) {
      const firmEntry = uniqueFirmEntries[index];
      const cachedRow = orgMatchCache.get(firmEntry.firmCacheKey);

      const isRetryableCache = cachedRow && cachedRow.status !== "resolved";
      if (cachedRow && !config.forceRefreshOrgMatches && !isRetryableCache) {
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

      let lookupPage = await ensureApolloLookupPage();
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
        consecutiveErrors = 0;
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
        consecutiveErrors += 1;
        apolloLookupCount += 1;

        if (consecutiveErrors >= CONSECUTIVE_ERROR_BACKOFF_THRESHOLD) {
          let recovered = false;

          for (let attempt = 1; attempt <= MAX_BACKOFF_ATTEMPTS; attempt++) {
            const backoffMs = BASE_BACKOFF_MS * Math.pow(2, attempt - 1);
            console.log(
              `  Rate limit detected (${consecutiveErrors} consecutive errors). Backing off ${(backoffMs / 1000).toFixed(0)}s (attempt ${attempt}/${MAX_BACKOFF_ATTEMPTS})...`
            );
            await new Promise((r) => setTimeout(r, backoffMs));

            lookupPage = await resetApolloLookupPage();

            if (await detectCloudflareChallenge(lookupPage)) {
              console.log("  Cloudflare challenge detected during firm resolution...");
              await waitForCloudflareResolution(lookupPage);
            }

            try {
              const retryPayload = await fetchApolloCompanySearchPayload(lookupPage, firmEntry.firmName);
              const retryCandidates = extractApolloCompanyCandidates(retryPayload);
              const retryResult = upsertApolloOrgMatchCache(
                orgMatchCache,
                selectApolloOrganizationMatch(firmEntry.firmName, retryCandidates)
              );
              reportRows.push(retryResult);
              consecutiveErrors = 0;
              recovered = true;
              console.log(
                `  [Firm ${index + 1}/${uniqueFirmEntries.length}] ${firmEntry.firmName} -> recovered: ${retryResult.status === "resolved" ? `${retryResult.orgId} (${retryResult.matchedCompanyName})` : `unresolved (${retryResult.matchReason})`}`
              );
              break;
            } catch (retryError) {
              console.log(`  Backoff retry ${attempt} failed: ${normalizeCell(retryError?.message || retryError)}`);
            }
          }

          if (!recovered) {
            const remaining = uniqueFirmEntries.length - index;
            console.log(
              `  Apollo rate limit: could not recover after ${MAX_BACKOFF_ATTEMPTS} backoff attempts. Skipping remaining ${remaining} firm(s). Re-run to retry from cache.`
            );
            break;
          }
        } else {
          const result = upsertApolloOrgMatchCache(orgMatchCache, {
            firmName: firmEntry.firmName,
            firmCacheKey: firmEntry.firmCacheKey,
            status: "unresolved",
            orgId: "",
            matchedCompanyName: "",
            matchReason: `lookup_error: ${normalizeCell(error?.message || error)}`,
          });
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

function readRawCsvRows(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const raw = stripBom(fs.readFileSync(filePath, "utf-8"));
  if (!normalizeCell(raw)) return [];
  return parse(raw, {
    columns: false,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
    relax_quotes: true,
  });
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

  // Parse search filters from the first URL (they share the same base filters)
  const searchFilters = extractSearchFiltersFromTemplateUrl(urls[0]);

  // Extract unique titles and org IDs across all URLs
  const titles = dedupeNormalizedValues(
    urls.map((url) => extractApolloUrlKey(url).title).filter(Boolean)
  );
  const orgIds = dedupeNormalizedValues(
    urls.map((url) => extractApolloUrlKey(url).orgId).filter(Boolean)
  );
  const combos = [];
  for (const orgId of orgIds) {
    if (titles.length > 1) {
      for (const title of titles) {
        combos.push({ orgId, title, comboKey: `${orgId}::${title}` });
      }
    } else {
      combos.push({ orgId, title: titles[0] || "", comboKey: `${orgId}::${titles[0] || ""}` });
    }
  }

  const progress = loadApolloProgress(config.rawOutputDir);
  const completedCombos = new Set(
    Object.keys(progress.completedUrls).filter((key) => progress.completedUrls[key].completed)
  );
  const pendingCombos = combos.filter((c) => !completedCombos.has(c.comboKey));

  console.log(
    `Apollo stage: ${combos.length} org+title combo(s), ${completedCombos.size} already completed, ${pendingCombos.length} remaining.`
  );

  const allCanonicalRows = [];
  const rawFiles = [];

  // Reload canonical rows from previously completed combos
  for (const comboKey of completedCombos) {
    const info = progress.completedUrls[comboKey];
    const rawFilePath = path.join(config.rawOutputDir, `${config.rawFilePrefix}${comboKey.replace(/[^a-zA-Z0-9]/g, "_")}.csv`);
    if (fs.existsSync(rawFilePath)) {
      const existing = readRawCsvRows(rawFilePath);
      const canonical = buildCanonicalRows(existing);
      allCanonicalRows.push(...canonical);
      rawFiles.push(rawFilePath);
    }
  }

  if (allCanonicalRows.length > 0 && typeof config.onRowsScraped === "function") {
    config.onRowsScraped(allCanonicalRows);
    console.log(`Reloaded ${allCanonicalRows.length} canonical row(s) from previous progress into tracking.`);
  }

  if (pendingCombos.length > 0) {
    let { context, page } = await openApolloPage(config, APOLLO_BOOTSTRAP_URL);

    const SCRAPE_CONSECUTIVE_ERROR_THRESHOLD = 3;
    const SCRAPE_PAUSE_MS = 300000;
    const SCRAPE_MAX_PAUSE_RETRIES = 3;
    const SESSION_ROTATION_INTERVAL = 100;
    const BREAK_INTERVAL_MIN = 50;
    const BREAK_INTERVAL_MAX = 80;

    let consecutiveErrors = 0;
    let successCount = 0;
    let combosSinceBreak = 0;
    let nextBreakAt = randomIntBetween(BREAK_INTERVAL_MIN, BREAK_INTERVAL_MAX);

    try {
      for (let comboIndex = 0; comboIndex < combos.length; comboIndex++) {
        const combo = combos[comboIndex];
        if (completedCombos.has(combo.comboKey)) continue;

        const titleFilter = combo.title
          ? { ...searchFilters, person_titles: [combo.title] }
          : searchFilters;

        console.log(
          `\n[Apollo ${comboIndex + 1}/${combos.length}] org=${combo.orgId} title="${combo.title}"`
        );

        let comboRawRows = [];
        let totalPages = 1;

        const maxPages = Number.isFinite(config.maxPagesPerOrg) ? config.maxPagesPerOrg : 5;
        for (let pageNum = 1; pageNum <= Math.min(totalPages, maxPages); pageNum++) {
          await waitRandomActionDelay(page, config, `before API call (page ${pageNum})`);

          try {
            const response = await fetchApolloPeopleSearch(page, {
              orgId: combo.orgId,
              filters: titleFilter,
              pageNumber: pageNum,
            });

            const pagination = getPaginationFromSearchResponse(response);
            totalPages = pagination.totalPages;

            const people = extractPeopleFromSearchResponse(response);
            console.log(
              `  Page ${pageNum}/${totalPages}: ${people.length} people (${pagination.totalEntries} total entries)`
            );
            if (people.length === 0) {
              break;
            }

            for (const person of people) {
              comboRawRows.push([
                person.Name || "",
                person.Title || "",
                person.Company || "",
                person.linkedinUrl || "",
              ]);
            }

            consecutiveErrors = 0;
          } catch (error) {
            consecutiveErrors++;
            console.log(`  API error on page ${pageNum}: ${error.message}`);

            if (consecutiveErrors >= SCRAPE_CONSECUTIVE_ERROR_THRESHOLD) {
              let recovered = false;
              for (let pauseAttempt = 1; pauseAttempt <= SCRAPE_MAX_PAUSE_RETRIES; pauseAttempt++) {
                console.log(
                  `  Rate limit detected (${consecutiveErrors} consecutive errors). Pausing ${(SCRAPE_PAUSE_MS / 60000).toFixed(0)} min (attempt ${pauseAttempt}/${SCRAPE_MAX_PAUSE_RETRIES})...`
                );
                await new Promise((r) => setTimeout(r, SCRAPE_PAUSE_MS));
                console.log("  Refreshing browser session...");
                await context.close();
                ({ context, page } = await openApolloPage(config, APOLLO_BOOTSTRAP_URL));

                try {
                  const retryResponse = await fetchApolloPeopleSearch(page, {
                    orgId: combo.orgId,
                    filters: titleFilter,
                    pageNumber: pageNum,
                  });
                  const retryPeople = extractPeopleFromSearchResponse(retryResponse);
                  if (retryPeople.length > 0) {
                    console.log(`  Recovered after pause! ${retryPeople.length} people on page ${pageNum}.`);
                    consecutiveErrors = 0;
                    recovered = true;
                    // Continue with this page's results
                    totalPages = getPaginationFromSearchResponse(retryResponse).totalPages;
                    for (const person of retryPeople) {
                      comboRawRows.push([person.Name || "", person.Title || "", person.Company || "", person.linkedinUrl || ""]);
                    }
                    break;
                  }
                } catch (retryErr) {
                  console.log(`  Pause retry ${pauseAttempt} failed: ${retryErr.message}`);
                }
              }
              if (!recovered) {
                console.log("  Could not recover. Skipping remaining pages for this combo.");
                break;
              }
            } else {
              break; // Skip remaining pages for this combo on error
            }
          }
        }

        // Save raw rows for this combo
        const rawFilePath = path.join(config.rawOutputDir, `${config.rawFilePrefix}${combo.comboKey.replace(/[^a-zA-Z0-9]/g, "_")}.csv`);
        writeRawRows(rawFilePath, comboRawRows);
        rawFiles.push(rawFilePath);

        const comboCanonical = buildCanonicalRows(comboRawRows);
        allCanonicalRows.push(...comboCanonical);

        if (comboCanonical.length > 0 && typeof config.onRowsScraped === "function") {
          config.onRowsScraped(comboCanonical);
        }

        progress.completedUrls[combo.comboKey] = {
          completedAt: new Date().toISOString(),
          rowCount: comboRawRows.length,
          canonicalCount: comboCanonical.length,
          completed: true,
        };
        saveApolloProgress(config.rawOutputDir, progress);

        console.log(`  Combo complete: ${comboRawRows.length} raw, ${comboCanonical.length} canonical`);

        if (comboCanonical.length > 0) {
          successCount++;
        }
      }
    } finally {
      await context.close();
    }
  }

  // Dedupe all canonical rows
  const deduped = new Map();
  for (const row of allCanonicalRows) {
    const url = normalizeLinkedInUrl(row.linkedinUrl || "");
    if (url && !deduped.has(url)) deduped.set(url, row);
  }
  const canonicalRows = [...deduped.values()];
  writeCanonicalRows(config.canonicalOutputCsv, canonicalRows);

  // Keep progress so next run with different titles skips already-completed combos
  // clearApolloProgress(config.rawOutputDir);

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
    rawRowCount: allCanonicalRows.length,
    canonicalRowCount: canonicalRows.length,
    urlCount: combos.length,
  };
}

module.exports = {
  buildApolloBatchFromCombinedUrl,
  buildApolloBatchFromOrganizationIds,
  buildApolloSearchQuery,
  buildCanonicalRows,
  buildApolloFirmLookupEntries,
  buildRawFileName,
  clearApolloProgress,
  extractApolloUrlKey,
  extractSearchFiltersFromTemplateUrl,
  fetchApolloPeopleSearch,
  loadApolloProgress,
  normalizeApolloCompanyName,
  normalizeApolloFirmCacheKey,
  normalizeLinkedInUrl,
  prepareApolloFirmInput,
  readApolloInputCsv,
  readApolloFirmInputCsv,
  readApolloOrgMatchReport,
  runApolloScrape,
  saveApolloProgress,
  selectApolloOrganizationMatch,
  writeApolloInputCsv,
  writeApolloOrgMatchReport,
};
