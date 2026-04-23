#!/usr/bin/env python3
"""SQLite-backed tracking state for the unified master tracking DB."""

import csv
import os
import shutil
import sqlite3
from datetime import datetime, timezone

try:
    from .master_tracking import (
        MASTER_TRACKING_HEADERS,
        merge_rows,
        normalize_email,
        normalize_linkedin_url,
        normalize_master_row,
        now_iso,
    )
except ImportError:
    from master_tracking import (
        MASTER_TRACKING_HEADERS,
        merge_rows,
        normalize_email,
        normalize_linkedin_url,
        normalize_master_row,
        now_iso,
    )

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(BASE_DIR, "tracking.db")
ENV_DB = os.getenv("AUTO_EMAIL_TRACKING_DB", "").strip()

SCHEMA_SQL = """
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
"""

UPSERT_COLUMNS = ", ".join(MASTER_TRACKING_HEADERS)
UPSERT_PLACEHOLDERS = ", ".join("?" for _ in MASTER_TRACKING_HEADERS)
UPSERT_SQL = f"INSERT OR REPLACE INTO tracking ({UPSERT_COLUMNS}) VALUES ({UPSERT_PLACEHOLDERS})"


def default_db_path():
    return ENV_DB or DEFAULT_DB


def _row_to_params(row):
    return tuple(row.get(col, "") for col in MASTER_TRACKING_HEADERS)


def _row_from_sqlite(sqlite_row):
    if sqlite_row is None:
        return None
    return {col: (sqlite_row[col] or "") for col in MASTER_TRACKING_HEADERS}


def open_tracking_db(db_path=None):
    db_path = db_path or default_db_path()
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA_SQL)
    return conn


def close_tracking_db(conn):
    if conn:
        conn.close()


def get_row(conn, linkedin_url):
    normalized = normalize_linkedin_url(linkedin_url)
    if not normalized:
        return None
    cursor = conn.execute(
        "SELECT * FROM tracking WHERE linkedinUrl = ?", (normalized,)
    )
    return _row_from_sqlite(cursor.fetchone())


def get_row_by_email(conn, email):
    normalized = normalize_email(email)
    if not normalized:
        return None
    cursor = conn.execute(
        "SELECT * FROM tracking WHERE email = ? LIMIT 1", (normalized,)
    )
    return _row_from_sqlite(cursor.fetchone())


def load_all_rows(conn):
    cursor = conn.execute("SELECT * FROM tracking ORDER BY linkedinUrl")
    rows = []
    for sqlite_row in cursor:
        row = _row_from_sqlite(sqlite_row)
        if row:
            rows.append(row)
    return rows


def upsert_row(conn, updates, linkedin_url="", email=""):
    normalized = normalize_master_row(updates)
    lookup_url = normalize_linkedin_url(
        linkedin_url or normalized.get("linkedinUrl", "")
    )
    lookup_email = normalize_email(email or normalized.get("email", ""))

    existing = None
    if lookup_url:
        existing = get_row(conn, lookup_url)
    if existing is None and lookup_email:
        existing = get_row_by_email(conn, lookup_email)

    merged = merge_rows(existing, normalized)
    if not merged.get("linkedinUrl"):
        return None

    conn.execute(UPSERT_SQL, _row_to_params(merged))
    conn.commit()
    return merged


def upsert_rows(conn, rows, source_stage=""):
    for row in rows:
        normalized = normalize_master_row({
            **row,
            "source_stage": source_stage or row.get("source_stage", ""),
        })
        if not normalized.get("linkedinUrl"):
            continue
        existing = get_row(conn, normalized["linkedinUrl"])
        merged = merge_rows(existing, normalized)
        conn.execute(UPSERT_SQL, _row_to_params(merged))
    conn.commit()


def count_rows(conn):
    cursor = conn.execute("SELECT COUNT(*) FROM tracking")
    return cursor.fetchone()[0]


def count_rows_with_email(conn):
    cursor = conn.execute("SELECT COUNT(*) FROM tracking WHERE email != ''")
    return cursor.fetchone()[0]


def query_rows_by_status(conn, status_field, status_value):
    cursor = conn.execute(
        f'SELECT * FROM tracking WHERE "{status_field}" = ?', (status_value,)
    )
    rows = []
    for sqlite_row in cursor:
        row = _row_from_sqlite(sqlite_row)
        if row:
            rows.append(row)
    return rows


def export_to_csv(conn, csv_path, only_with_email=False):
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)

    sql = "SELECT * FROM tracking"
    if only_with_email:
        sql += " WHERE email != ''"
    sql += " ORDER BY linkedinUrl"

    cursor = conn.execute(sql)
    rows = []
    for sqlite_row in cursor:
        row = _row_from_sqlite(sqlite_row)
        if row:
            rows.append(row)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MASTER_TRACKING_HEADERS)
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def migrate_from_csv(conn, csv_path):
    if not csv_path or not os.path.exists(csv_path):
        return {"imported_count": 0, "backup_path": None}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return {"imported_count": 0, "backup_path": None}

    upsert_rows(conn, rows, "csv_migration")

    timestamp = now_iso().replace(":", "-").replace(".", "-")
    base, ext = os.path.splitext(csv_path)
    backup_path = f"{base}.migrated-to-db-{timestamp}{ext or '.csv'}"
    shutil.move(csv_path, backup_path)

    return {"imported_count": len(rows), "backup_path": backup_path}
