const fs = require("fs");
const path = require("path");
const { parse } = require("csv-parse/sync");
const { stringify } = require("csv-stringify/sync");

const MASTER_TRACKING_COLUMNS = [
  "linkedinUrl",
  "Name",
  "Title",
  "Company",
  "email",
  "all_emails",
  "phones",
  "kaspr_status",
  "kaspr_scraped_at",
  "email_send_status",
  "email_sent_at",
  "email_last_attempt_at",
  "email_last_error",
  "email_sender_account",
  "read_receipt",
  "reply_detected",
  "reply_at",
  "source_stage",
  "discovered_at",
  "updated_at",
];

const LEGACY_TRACKING_COLUMNS = [
  "email",
  "name",
  "company_name",
  "sent_at",
  "sender_account",
  "status",
  "read_receipt",
  "reply_detected",
  "reply_at",
];

const RESULTS_EXPORT_COLUMNS = [
  "Name",
  "Title",
  "Company",
  "linkedinUrl",
  "email",
  "all_emails",
  "phones",
  "status",
  "scraped_at",
];

const KASPR_COMPLETED_STATUSES = new Set(["found", "no_email"]);

function normalizeCell(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeEmail(value) {
  const normalized = normalizeCell(value).toLowerCase();
  return normalized.includes("@") ? normalized : "";
}

function normalizeBooleanString(value, fallback = "False") {
  if (value == null || value === "") return fallback;
  const normalized = normalizeCell(value).toLowerCase();
  if (["true", "1", "yes", "y"].includes(normalized)) return "True";
  if (["false", "0", "no", "n"].includes(normalized)) return "False";
  return fallback;
}

function splitMultiValue(value) {
  return String(value || "")
    .split(/[;,]/)
    .map((part) => normalizeCell(part))
    .filter(Boolean);
}

function joinMultiValue(values) {
  const deduped = [];
  const seen = new Set();

  for (const value of values) {
    const normalized = normalizeCell(value);
    if (!normalized) continue;
    const key = normalized.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(normalized);
  }

  return deduped.join("; ");
}

function ensureDirectory(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function stripBom(raw) {
  return raw.charCodeAt(0) === 0xfeff ? raw.slice(1) : raw;
}

function readCsvHeader(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const raw = stripBom(fs.readFileSync(filePath, "utf8"));
  if (!normalizeCell(raw)) return [];

  const headerRows = parse(raw, {
    columns: false,
    skip_empty_lines: true,
    trim: true,
    to_line: 1,
    relax_column_count: true,
    relax_quotes: true,
  });

  return Array.isArray(headerRows[0]) ? headerRows[0].map((cell) => normalizeCell(cell)) : [];
}

function readCsvRows(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const raw = stripBom(fs.readFileSync(filePath, "utf8"));
  if (!normalizeCell(raw)) return [];

  return parse(raw, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
    relax_quotes: true,
  });
}

function normalizeLinkedInUrl(value) {
  try {
    const url = new URL(String(value || "").trim());
    if (!/(^|\.)linkedin\.com$/i.test(url.hostname)) return "";
    const match = url.pathname.match(/^\/in\/([^/?#]+)/i);
    if (!match) return "";
    const profileSlug = match[1];
    return `https://www.linkedin.com/in/${profileSlug}/`;
  } catch {
    return "";
  }
}

function getNowIso() {
  return new Date().toISOString();
}

function emptyMasterTrackingRow() {
  return Object.fromEntries(MASTER_TRACKING_COLUMNS.map((column) => [column, ""]));
}

function isLegacyTrackingHeader(header) {
  if (!header || header.length === 0) return false;
  return LEGACY_TRACKING_COLUMNS.every((column) => header.includes(column)) && !header.includes("linkedinUrl");
}

function isMasterTrackingHeader(header) {
  if (!header || header.length === 0) return false;
  return header.includes("linkedinUrl") && header.includes("kaspr_status");
}

function extractPrimaryEmail(row) {
  const direct = normalizeEmail(row.email || row.Email || "");
  if (direct) return direct;

  const multiValueFields = [
    row.all_emails,
    row.emails,
    row.Email,
  ];

  for (const value of multiValueFields) {
    const match = splitMultiValue(value).map(normalizeEmail).find(Boolean);
    if (match) return match;
  }

  return "";
}

function extractAllEmails(row) {
  const values = [];
  const primary = extractPrimaryEmail(row);
  if (primary) values.push(primary);

  for (const email of splitMultiValue(row.all_emails || row.emails || row.Email || "")) {
    const normalized = normalizeEmail(email);
    if (normalized) values.push(normalized);
  }

  return joinMultiValue(values);
}

function extractPhones(row) {
  return joinMultiValue(splitMultiValue(row.phones || row.Phones || ""));
}

function normalizeMasterTrackingRow(row) {
  const nowIso = getNowIso();
  const linkedinUrl = normalizeLinkedInUrl(
    row.linkedinUrl || row.linkedin_url || row.LinkedinUrl || row[0] || ""
  );
  const email = extractPrimaryEmail(row);
  const allEmails = joinMultiValue([email, ...splitMultiValue(extractAllEmails(row))]);

  return {
    ...emptyMasterTrackingRow(),
    linkedinUrl,
    Name: normalizeCell(row.Name || row.name || ""),
    Title: normalizeCell(row.Title || row.title || ""),
    Company: normalizeCell(row.Company || row.company || row.company_name || ""),
    email,
    all_emails: allEmails,
    phones: extractPhones(row),
    kaspr_status: normalizeCell(row.kaspr_status || row.status || "").toLowerCase(),
    kaspr_scraped_at: normalizeCell(row.kaspr_scraped_at || row.scraped_at || ""),
    email_send_status: normalizeCell(row.email_send_status || ""),
    email_sent_at: normalizeCell(row.email_sent_at || row.sent_at || ""),
    email_last_attempt_at: normalizeCell(row.email_last_attempt_at || row.sent_at || ""),
    email_last_error: normalizeCell(row.email_last_error || ""),
    email_sender_account: normalizeCell(row.email_sender_account || row.sender_account || ""),
    read_receipt: normalizeBooleanString(row.read_receipt, "False"),
    reply_detected: normalizeBooleanString(row.reply_detected, "False"),
    reply_at: normalizeCell(row.reply_at || ""),
    source_stage: normalizeCell(row.source_stage || ""),
    discovered_at: normalizeCell(row.discovered_at || row.kaspr_scraped_at || row.scraped_at || nowIso),
    updated_at: normalizeCell(row.updated_at || nowIso),
  };
}

function mergeMasterTrackingRows(existingRow, incomingRow) {
  const nowIso = getNowIso();
  const existing = existingRow ? normalizeMasterTrackingRow(existingRow) : emptyMasterTrackingRow();
  const incoming = normalizeMasterTrackingRow(incomingRow);

  const preferredEmail = incoming.email || existing.email;
  const mergedAllEmails = joinMultiValue([
    preferredEmail,
    ...splitMultiValue(existing.all_emails),
    ...splitMultiValue(incoming.all_emails),
  ]);

  let emailLastError = incoming.email_last_error || existing.email_last_error;
  if (incoming.email_send_status === "sent") {
    emailLastError = "";
  }

  return {
    ...existing,
    linkedinUrl: incoming.linkedinUrl || existing.linkedinUrl,
    Name: incoming.Name || existing.Name,
    Title: incoming.Title || existing.Title,
    Company: incoming.Company || existing.Company,
    email: preferredEmail,
    all_emails: mergedAllEmails,
    phones: joinMultiValue([...splitMultiValue(existing.phones), ...splitMultiValue(incoming.phones)]),
    kaspr_status: incoming.kaspr_status || existing.kaspr_status,
    kaspr_scraped_at: incoming.kaspr_scraped_at || existing.kaspr_scraped_at,
    email_send_status: incoming.email_send_status || existing.email_send_status,
    email_sent_at: incoming.email_sent_at || existing.email_sent_at,
    email_last_attempt_at: incoming.email_last_attempt_at || existing.email_last_attempt_at,
    email_last_error: emailLastError,
    email_sender_account: incoming.email_sender_account || existing.email_sender_account,
    read_receipt:
      incoming.read_receipt !== "False" || !existing.read_receipt
        ? incoming.read_receipt
        : existing.read_receipt,
    reply_detected:
      incoming.reply_detected !== "False" || !existing.reply_detected
        ? incoming.reply_detected
        : existing.reply_detected,
    reply_at: incoming.reply_at || existing.reply_at,
    source_stage: incoming.source_stage || existing.source_stage,
    discovered_at: existing.discovered_at || incoming.discovered_at || nowIso,
    updated_at: incoming.updated_at || nowIso,
  };
}

function buildMasterTrackingMap(rows) {
  const map = {};
  for (const row of rows) {
    const normalized = normalizeMasterTrackingRow(row);
    if (!normalized.linkedinUrl) continue;
    map[normalized.linkedinUrl] = mergeMasterTrackingRows(map[normalized.linkedinUrl], normalized);
  }
  return map;
}

function readMasterTrackingMap(filePath) {
  return buildMasterTrackingMap(readCsvRows(filePath));
}

function writeMasterTrackingMap(filePath, trackingMap) {
  ensureDirectory(path.dirname(filePath));
  const rows = Object.values(trackingMap).map((row) => normalizeMasterTrackingRow(row));
  rows.sort((left, right) => left.linkedinUrl.localeCompare(right.linkedinUrl));
  const csv = stringify(rows, {
    header: true,
    columns: MASTER_TRACKING_COLUMNS,
  });
  fs.writeFileSync(filePath, csv);
}

function buildMasterSeedRowFromAnyRow(row, sourceStage = "") {
  const normalized = normalizeMasterTrackingRow({
    ...row,
    source_stage: sourceStage || row.source_stage || "",
  });
  return normalized.linkedinUrl ? normalized : null;
}

function upsertTrackingRows(trackingMap, rows, sourceStage = "") {
  for (const row of rows) {
    const seedRow = buildMasterSeedRowFromAnyRow(row, sourceStage);
    if (!seedRow) continue;
    trackingMap[seedRow.linkedinUrl] = mergeMasterTrackingRows(trackingMap[seedRow.linkedinUrl], seedRow);
  }
  return trackingMap;
}

function buildEmailIndex(trackingMap) {
  const emailIndex = new Map();

  for (const row of Object.values(trackingMap)) {
    const emails = joinMultiValue([
      row.email,
      ...splitMultiValue(row.all_emails),
    ]);

    for (const email of splitMultiValue(emails)) {
      const normalized = normalizeEmail(email);
      if (!normalized) continue;
      const matches = emailIndex.get(normalized) || [];
      matches.push(row.linkedinUrl);
      emailIndex.set(normalized, matches);
    }
  }

  return emailIndex;
}

function mapLegacyEmailStatus(status) {
  const normalized = normalizeCell(status).toLowerCase();
  if (normalized === "sent") return "sent";
  if (normalized === "bounced") return "bounced";
  if (normalized.startsWith("failed")) return "failed";
  return normalized;
}

function buildMigrationSeedRows(seedPaths = [], seedRows = []) {
  const collected = [...seedRows];

  for (const seedPath of seedPaths) {
    if (!seedPath || !fs.existsSync(seedPath)) continue;
    for (const row of readCsvRows(seedPath)) {
      collected.push(row);
    }
  }

  return collected;
}

function buildLegacyBackupPath(trackingPath) {
  const timestamp = getNowIso().replace(/[:.]/g, "-");
  const parsed = path.parse(trackingPath);
  return path.join(parsed.dir, `${parsed.name}.legacy-backup-${timestamp}${parsed.ext || ".csv"}`);
}

function migrateLegacyTrackingFile({ trackingPath, seedPaths = [], seedRows = [] }) {
  const legacyRows = readCsvRows(trackingPath);
  const backupPath = buildLegacyBackupPath(trackingPath);
  fs.copyFileSync(trackingPath, backupPath);

  const trackingMap = {};
  upsertTrackingRows(trackingMap, buildMigrationSeedRows(seedPaths, seedRows), "migration_seed");

  const emailIndex = buildEmailIndex(trackingMap);
  let importedCount = 0;
  let unmatchedCount = 0;

  for (const legacyRow of legacyRows) {
    const email = normalizeEmail(legacyRow.email);
    if (!email) {
      unmatchedCount += 1;
      continue;
    }

    const matches = [...new Set(emailIndex.get(email) || [])];
    if (matches.length !== 1) {
      unmatchedCount += 1;
      continue;
    }

    const linkedinUrl = matches[0];
    trackingMap[linkedinUrl] = mergeMasterTrackingRows(trackingMap[linkedinUrl], {
      linkedinUrl,
      Name: normalizeCell(legacyRow.name || ""),
      Company: normalizeCell(legacyRow.company_name || ""),
      email,
      all_emails: email,
      email_send_status: mapLegacyEmailStatus(legacyRow.status || ""),
      email_sent_at: normalizeCell(legacyRow.sent_at || ""),
      email_last_attempt_at: normalizeCell(legacyRow.sent_at || ""),
      email_last_error:
        mapLegacyEmailStatus(legacyRow.status || "") === "failed"
          ? normalizeCell(legacyRow.status || "")
          : "",
      email_sender_account: normalizeCell(legacyRow.sender_account || ""),
      read_receipt: normalizeBooleanString(legacyRow.read_receipt, "False"),
      reply_detected: normalizeBooleanString(legacyRow.reply_detected, "False"),
      reply_at: normalizeCell(legacyRow.reply_at || ""),
      source_stage: "legacy_migration",
    });
    importedCount += 1;
  }

  writeMasterTrackingMap(trackingPath, trackingMap);

  return {
    trackingMap,
    backupPath,
    importedCount,
    unmatchedCount,
    seededRowCount: Object.keys(trackingMap).length,
  };
}

function loadOrCreateTrackingState({ trackingPath, seedPaths = [], seedRows = [] }) {
  const header = readCsvHeader(trackingPath);

  if (!fs.existsSync(trackingPath) || header.length === 0) {
    const trackingMap = {};
    upsertTrackingRows(trackingMap, buildMigrationSeedRows(seedPaths, seedRows), "bootstrap_seed");
    return {
      trackingMap,
      migratedLegacy: false,
      migrationSummary: null,
    };
  }

  if (isLegacyTrackingHeader(header)) {
    const migrationSummary = migrateLegacyTrackingFile({
      trackingPath,
      seedPaths,
      seedRows,
    });
    return {
      trackingMap: migrationSummary.trackingMap,
      migratedLegacy: true,
      migrationSummary,
    };
  }

  if (!isMasterTrackingHeader(header)) {
    throw new Error(
      `Unsupported tracking CSV schema at ${trackingPath}. Expected master tracking columns or the legacy email-only schema.`
    );
  }

  return {
    trackingMap: readMasterTrackingMap(trackingPath),
    migratedLegacy: false,
    migrationSummary: null,
  };
}

function buildResultsExportRows(trackingRows, { onlyWithEmail = false } = {}) {
  const rows = [];

  for (const row of trackingRows) {
    const normalized = normalizeMasterTrackingRow(row);
    if (!normalized.linkedinUrl) continue;
    if (onlyWithEmail && !normalized.email) continue;

    rows.push({
      Name: normalized.Name,
      Title: normalized.Title,
      Company: normalized.Company,
      linkedinUrl: normalized.linkedinUrl,
      email: normalized.email,
      all_emails: normalized.all_emails,
      phones: normalized.phones,
      status: normalized.kaspr_status,
      scraped_at: normalized.kaspr_scraped_at,
    });
  }

  rows.sort((left, right) => left.linkedinUrl.localeCompare(right.linkedinUrl));
  return rows;
}

function writeResultsExport(filePath, trackingRows, options = {}) {
  ensureDirectory(path.dirname(filePath));
  const csv = stringify(buildResultsExportRows(trackingRows, options), {
    header: true,
    columns: RESULTS_EXPORT_COLUMNS,
  });
  fs.writeFileSync(filePath, csv);
}

function writeTrackingStateArtifacts({
  trackingPath,
  trackingMap,
  resultsExportPath = "",
  eligibleExportPath = "",
  resultsFilterUrls = null,
}) {
  writeMasterTrackingMap(trackingPath, trackingMap);

  const allRows = Object.values(trackingMap);

  if (resultsExportPath) {
    const filteredRows = Array.isArray(resultsFilterUrls)
      ? allRows.filter((row) => resultsFilterUrls.includes(row.linkedinUrl))
      : allRows;
    writeResultsExport(resultsExportPath, filteredRows);
  }

  if (eligibleExportPath) {
    writeResultsExport(eligibleExportPath, allRows, { onlyWithEmail: true });
  }
}

function hasCompletedKasprScrape(row) {
  return KASPR_COMPLETED_STATUSES.has(normalizeCell(row?.kaspr_status || "").toLowerCase());
}

module.exports = {
  LEGACY_TRACKING_COLUMNS,
  MASTER_TRACKING_COLUMNS,
  RESULTS_EXPORT_COLUMNS,
  buildMasterSeedRowFromAnyRow,
  buildResultsExportRows,
  hasCompletedKasprScrape,
  loadOrCreateTrackingState,
  mergeMasterTrackingRows,
  normalizeCell,
  normalizeEmail,
  normalizeLinkedInUrl,
  readCsvRows,
  upsertTrackingRows,
  writeResultsExport,
  writeTrackingStateArtifacts,
};
