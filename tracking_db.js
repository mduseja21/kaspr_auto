const Database = require("better-sqlite3");
const fs = require("fs");
const path = require("path");
const { stringify } = require("csv-stringify/sync");

const {
  MASTER_TRACKING_COLUMNS,
  RESULTS_EXPORT_COLUMNS,
  buildMasterSeedRowFromAnyRow,
  mergeMasterTrackingRows,
  normalizeMasterTrackingRow,
  readCsvRows,
} = require("./tracking_state");

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS tracking (
  linkedinUrl TEXT PRIMARY KEY NOT NULL,
  Name TEXT NOT NULL DEFAULT '',
  Title TEXT NOT NULL DEFAULT '',
  Company TEXT NOT NULL DEFAULT '',
  email TEXT NOT NULL DEFAULT '',
  all_emails TEXT NOT NULL DEFAULT '',
  phones TEXT NOT NULL DEFAULT '',
  kaspr_status TEXT NOT NULL DEFAULT '',
  kaspr_scraped_at TEXT NOT NULL DEFAULT '',
  email_send_status TEXT NOT NULL DEFAULT '',
  email_sent_at TEXT NOT NULL DEFAULT '',
  email_last_attempt_at TEXT NOT NULL DEFAULT '',
  email_last_error TEXT NOT NULL DEFAULT '',
  email_sender_account TEXT NOT NULL DEFAULT '',
  read_receipt TEXT NOT NULL DEFAULT 'False',
  reply_detected TEXT NOT NULL DEFAULT 'False',
  reply_at TEXT NOT NULL DEFAULT '',
  source_stage TEXT NOT NULL DEFAULT '',
  discovered_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_tracking_email ON tracking(email);
CREATE INDEX IF NOT EXISTS idx_tracking_send_status ON tracking(email_send_status);
CREATE INDEX IF NOT EXISTS idx_tracking_company ON tracking(Company);
`;

const UPSERT_COLUMNS = MASTER_TRACKING_COLUMNS.join(", ");
const UPSERT_PLACEHOLDERS = MASTER_TRACKING_COLUMNS.map(() => "?").join(", ");
const UPSERT_SQL = `INSERT OR REPLACE INTO tracking (${UPSERT_COLUMNS}) VALUES (${UPSERT_PLACEHOLDERS})`;
const SELECT_ONE_SQL = "SELECT * FROM tracking WHERE linkedinUrl = ?";
const SELECT_ALL_SQL = "SELECT * FROM tracking ORDER BY linkedinUrl";
const SELECT_BY_EMAIL_SQL = "SELECT * FROM tracking WHERE email = ? LIMIT 1";

function ensureDirectory(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function rowToParams(row) {
  return MASTER_TRACKING_COLUMNS.map((col) => row[col] ?? "");
}

function rowFromSqlite(sqliteRow) {
  if (!sqliteRow) return null;
  const row = {};
  for (const col of MASTER_TRACKING_COLUMNS) {
    row[col] = sqliteRow[col] ?? "";
  }
  return row;
}

function openTrackingDb(dbPath) {
  ensureDirectory(path.dirname(dbPath));
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("busy_timeout = 5000");
  db.exec(SCHEMA_SQL);

  db._stmts = {
    selectOne: db.prepare(SELECT_ONE_SQL),
    selectAll: db.prepare(SELECT_ALL_SQL),
    selectByEmail: db.prepare(SELECT_BY_EMAIL_SQL),
    upsert: db.prepare(UPSERT_SQL),
  };

  return db;
}

function closeTrackingDb(db) {
  if (db && db.open) db.close();
}

function getRow(db, linkedinUrl) {
  return rowFromSqlite(db._stmts.selectOne.get(linkedinUrl));
}

function getRowByEmail(db, email) {
  if (!email) return null;
  return rowFromSqlite(db._stmts.selectByEmail.get(email.toLowerCase()));
}

function loadAllRows(db) {
  const map = {};
  for (const sqliteRow of db._stmts.selectAll.iterate()) {
    const row = rowFromSqlite(sqliteRow);
    if (row && row.linkedinUrl) {
      map[row.linkedinUrl] = row;
    }
  }
  return map;
}

function upsertRow(db, incomingRow) {
  const normalized = normalizeMasterTrackingRow(incomingRow);
  if (!normalized.linkedinUrl) return null;

  const existing = getRow(db, normalized.linkedinUrl);
  const merged = mergeMasterTrackingRows(existing, normalized);
  db._stmts.upsert.run(rowToParams(merged));
  return merged;
}

function upsertRows(db, rows, sourceStage = "") {
  const insertMany = db.transaction((rowList) => {
    for (const row of rowList) {
      const seed = buildMasterSeedRowFromAnyRow(row, sourceStage);
      if (!seed) continue;
      const existing = getRow(db, seed.linkedinUrl);
      const merged = mergeMasterTrackingRows(existing, seed);
      db._stmts.upsert.run(rowToParams(merged));
    }
  });
  insertMany(rows);
}

function countRows(db) {
  return db.prepare("SELECT COUNT(*) as count FROM tracking").get().count;
}

function countRowsWithEmail(db) {
  return db.prepare("SELECT COUNT(*) as count FROM tracking WHERE email != ''").get().count;
}

function queryRowsByStatus(db, statusField, statusValue) {
  const rows = [];
  const stmt = db.prepare(`SELECT * FROM tracking WHERE "${statusField}" = ?`);
  for (const sqliteRow of stmt.iterate(statusValue)) {
    const row = rowFromSqlite(sqliteRow);
    if (row) rows.push(row);
  }
  return rows;
}

function buildResultsExportRow(row) {
  return {
    Name: row.Name,
    Title: row.Title,
    Company: row.Company,
    linkedinUrl: row.linkedinUrl,
    email: row.email,
    all_emails: row.all_emails,
    phones: row.phones,
    status: row.kaspr_status,
    scraped_at: row.kaspr_scraped_at,
  };
}

function exportToCsv(db, csvPath, { onlyWithEmail = false, filterUrls = null } = {}) {
  ensureDirectory(path.dirname(csvPath));

  let sql = "SELECT * FROM tracking";
  const conditions = [];
  if (onlyWithEmail) conditions.push("email != ''");
  if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");
  sql += " ORDER BY linkedinUrl";

  const stmt = db.prepare(sql);
  const rows = [];
  for (const sqliteRow of stmt.iterate()) {
    const row = rowFromSqlite(sqliteRow);
    if (!row) continue;
    if (Array.isArray(filterUrls) && !filterUrls.includes(row.linkedinUrl)) continue;
    rows.push(buildResultsExportRow(row));
  }

  const csv = stringify(rows, { header: true, columns: RESULTS_EXPORT_COLUMNS });
  fs.writeFileSync(csvPath, csv);
  return rows.length;
}

function buildMigrationBackupPath(csvPath) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const parsed = path.parse(csvPath);
  return path.join(parsed.dir, `${parsed.name}.migrated-to-db-${timestamp}${parsed.ext || ".csv"}`);
}

function migrateFromCsv(db, csvPath) {
  if (!csvPath || !fs.existsSync(csvPath)) {
    return { importedCount: 0, backupPath: null };
  }

  const rows = readCsvRows(csvPath);
  if (rows.length === 0) {
    return { importedCount: 0, backupPath: null };
  }

  upsertRows(db, rows, "csv_migration");

  const backupPath = buildMigrationBackupPath(csvPath);
  fs.renameSync(csvPath, backupPath);

  return { importedCount: rows.length, backupPath };
}

module.exports = {
  closeTrackingDb,
  countRows,
  countRowsWithEmail,
  exportToCsv,
  getRow,
  getRowByEmail,
  loadAllRows,
  migrateFromCsv,
  openTrackingDb,
  queryRowsByStatus,
  upsertRow,
  upsertRows,
};
