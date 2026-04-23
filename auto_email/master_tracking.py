#!/usr/bin/env python3
"""Shared helpers for the unified master tracking CSV."""

import csv
import os
import re
from urllib.parse import urlparse
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKING_FILE = os.path.join(BASE_DIR, "tracking.csv")
TRACKING_DB_FILE = os.path.join(BASE_DIR, "tracking.db")
ENV_TRACKING_FILE = os.getenv("AUTO_EMAIL_TRACKING_CSV", "").strip()
ENV_TRACKING_DB = os.getenv("AUTO_EMAIL_TRACKING_DB", "").strip()

MASTER_TRACKING_HEADERS = [
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
]

LEGACY_TRACKING_HEADERS = [
    "email",
    "name",
    "company_name",
    "sent_at",
    "sender_account",
    "status",
    "read_receipt",
    "reply_detected",
    "reply_at",
]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def default_tracking_path():
    return ENV_TRACKING_DB or TRACKING_DB_FILE


def default_tracking_csv_path():
    return ENV_TRACKING_FILE or TRACKING_FILE


def is_db_path(path):
    return str(path or "").endswith(".db")


def normalize_text(value):
    return " ".join(str(value or "").split()).strip()


def normalize_email(value):
    email = normalize_text(value).lower()
    return email if "@" in email else ""


def normalize_linkedin_url(value):
    raw_value = normalize_text(value)
    if not raw_value:
        return ""

    try:
        parsed = urlparse(raw_value)
    except ValueError:
        return ""

    hostname = (parsed.hostname or "").lower()
    if hostname != "linkedin.com" and not hostname.endswith(".linkedin.com"):
        return ""

    match = re.match(r"^/in/([^/?#]+)", parsed.path or "", re.IGNORECASE)
    if not match:
        return ""

    profile_slug = match.group(1)
    return f"https://www.linkedin.com/in/{profile_slug}/"


def normalize_bool_str(value, fallback="False"):
    if value in (None, ""):
        return fallback
    normalized = normalize_text(value).lower()
    if normalized in {"true", "1", "yes", "y"}:
        return "True"
    if normalized in {"false", "0", "no", "n"}:
        return "False"
    return fallback


def split_multi_value(value):
    parts = []
    for raw_part in str(value or "").replace(";", ",").split(","):
        part = normalize_text(raw_part)
        if part:
            parts.append(part)
    return parts


def join_multi_value(values):
    deduped = []
    seen = set()
    for value in values:
        normalized = normalize_text(value)
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return "; ".join(deduped)


def iter_row_emails(row):
    emails = []
    primary = normalize_email(row.get("email", ""))
    if primary:
        emails.append(primary)
    for email in split_multi_value(row.get("all_emails", "")):
        normalized = normalize_email(email)
        if normalized:
            emails.append(normalized)
    return list(dict.fromkeys(emails))


def _empty_row():
    return {header: "" for header in MASTER_TRACKING_HEADERS}


def normalize_master_row(row):
    now = now_iso()
    primary_email = normalize_email(row.get("email", ""))
    all_emails = join_multi_value([primary_email, *iter_row_emails(row)])
    if not primary_email:
        primary_email = normalize_email(split_multi_value(all_emails)[0] if all_emails else "")

    return {
        **_empty_row(),
        "linkedinUrl": normalize_linkedin_url(row.get("linkedinUrl", row.get("linkedin_url", ""))),
        "Name": normalize_text(row.get("Name", row.get("name", ""))),
        "Title": normalize_text(row.get("Title", row.get("title", ""))),
        "Company": normalize_text(row.get("Company", row.get("company_name", row.get("company", "")))),
        "email": primary_email,
        "all_emails": all_emails,
        "phones": join_multi_value(split_multi_value(row.get("phones", ""))),
        "kaspr_status": normalize_text(row.get("kaspr_status", row.get("status", ""))).lower(),
        "kaspr_scraped_at": normalize_text(row.get("kaspr_scraped_at", row.get("scraped_at", ""))),
        "email_send_status": normalize_text(row.get("email_send_status", "")),
        "email_sent_at": normalize_text(row.get("email_sent_at", row.get("sent_at", ""))),
        "email_last_attempt_at": normalize_text(
            row.get("email_last_attempt_at", row.get("sent_at", ""))
        ),
        "email_last_error": normalize_text(row.get("email_last_error", "")),
        "email_sender_account": normalize_text(
            row.get("email_sender_account", row.get("sender_account", ""))
        ),
        "read_receipt": normalize_bool_str(row.get("read_receipt"), "False"),
        "reply_detected": normalize_bool_str(row.get("reply_detected"), "False"),
        "reply_at": normalize_text(row.get("reply_at", "")),
        "source_stage": normalize_text(row.get("source_stage", "")),
        "discovered_at": normalize_text(
            row.get("discovered_at", row.get("kaspr_scraped_at", row.get("scraped_at", now)))
        ),
        "updated_at": normalize_text(row.get("updated_at", now)),
    }


def merge_rows(existing, incoming):
    existing_row = normalize_master_row(existing or {})
    incoming_row = normalize_master_row(incoming or {})

    email = incoming_row["email"] or existing_row["email"]
    all_emails = join_multi_value([email, *iter_row_emails(existing_row), *iter_row_emails(incoming_row)])
    email_last_error = incoming_row["email_last_error"] or existing_row["email_last_error"]
    if incoming_row["email_send_status"] == "sent":
        email_last_error = ""

    return {
        **existing_row,
        "linkedinUrl": incoming_row["linkedinUrl"] or existing_row["linkedinUrl"],
        "Name": incoming_row["Name"] or existing_row["Name"],
        "Title": incoming_row["Title"] or existing_row["Title"],
        "Company": incoming_row["Company"] or existing_row["Company"],
        "email": email,
        "all_emails": all_emails,
        "phones": join_multi_value(
            [*split_multi_value(existing_row["phones"]), *split_multi_value(incoming_row["phones"])]
        ),
        "kaspr_status": incoming_row["kaspr_status"] or existing_row["kaspr_status"],
        "kaspr_scraped_at": incoming_row["kaspr_scraped_at"] or existing_row["kaspr_scraped_at"],
        "email_send_status": incoming_row["email_send_status"] or existing_row["email_send_status"],
        "email_sent_at": incoming_row["email_sent_at"] or existing_row["email_sent_at"],
        "email_last_attempt_at": incoming_row["email_last_attempt_at"]
        or existing_row["email_last_attempt_at"],
        "email_last_error": email_last_error,
        "email_sender_account": incoming_row["email_sender_account"]
        or existing_row["email_sender_account"],
        "read_receipt": (
            incoming_row["read_receipt"]
            if incoming_row["read_receipt"] == "True" or not existing_row["read_receipt"]
            else existing_row["read_receipt"]
        ),
        "reply_detected": (
            incoming_row["reply_detected"]
            if incoming_row["reply_detected"] == "True" or not existing_row["reply_detected"]
            else existing_row["reply_detected"]
        ),
        "reply_at": incoming_row["reply_at"] or existing_row["reply_at"],
        "source_stage": incoming_row["source_stage"] or existing_row["source_stage"],
        "discovered_at": existing_row["discovered_at"] or incoming_row["discovered_at"] or now_iso(),
        "updated_at": incoming_row["updated_at"] or now_iso(),
    }


def _tracking_header(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if row:
                return [normalize_text(cell) for cell in row]
    return []


def load_tracking(path=None):
    path = path or default_tracking_path()

    if is_db_path(path):
        try:
            from .tracking_db import open_tracking_db, load_all_rows, close_tracking_db
        except ImportError:
            from tracking_db import open_tracking_db, load_all_rows, close_tracking_db
        conn = open_tracking_db(path)
        try:
            return load_all_rows(conn)
        finally:
            close_tracking_db(conn)

    if not os.path.exists(path):
        return []

    header = _tracking_header(path)
    if header and all(column in header for column in LEGACY_TRACKING_HEADERS) and "linkedinUrl" not in header:
        raise RuntimeError(
            f"Legacy tracking.csv detected at {path}. Run the main Node pipeline once to migrate it into the unified master schema."
        )

    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    merged = {}
    for row in rows:
        normalized = normalize_master_row(row)
        linkedin_url = normalized["linkedinUrl"]
        if not linkedin_url:
            continue
        merged[linkedin_url] = merge_rows(merged.get(linkedin_url), normalized)

    return list(merged.values())


def save_tracking(rows, path=None):
    path = path or default_tracking_path()

    if is_db_path(path):
        try:
            from .tracking_db import open_tracking_db, upsert_rows, close_tracking_db
        except ImportError:
            from tracking_db import open_tracking_db, upsert_rows, close_tracking_db
        conn = open_tracking_db(path)
        try:
            upsert_rows(conn, rows)
        finally:
            close_tracking_db(conn)
        return

    normalized_rows = []
    merged = {}
    for row in rows:
        normalized = normalize_master_row(row)
        linkedin_url = normalized["linkedinUrl"]
        if not linkedin_url:
            continue
        merged[linkedin_url] = merge_rows(merged.get(linkedin_url), normalized)

    normalized_rows = sorted(merged.values(), key=lambda row: row["linkedinUrl"])

    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MASTER_TRACKING_HEADERS)
        writer.writeheader()
        writer.writerows(normalized_rows)


def find_tracking_row(rows, linkedin_url="", email=""):
    normalized_linkedin = normalize_linkedin_url(linkedin_url)
    normalized_email = normalize_email(email)

    if normalized_linkedin:
        for row in rows:
            if normalize_linkedin_url(row.get("linkedinUrl")) == normalized_linkedin:
                return row

    if normalized_email:
        for row in rows:
            if normalized_email in iter_row_emails(row):
                return row

    return None


def upsert_tracking_row(rows, updates, linkedin_url="", email=""):
    row = find_tracking_row(rows, linkedin_url=linkedin_url, email=email)
    if row is not None:
        merged = merge_rows(row, updates)
        row.clear()
        row.update(merged)
        return row

    normalized = normalize_master_row(updates)
    if not normalized["linkedinUrl"]:
        return None

    rows.append(normalized)
    return rows[-1]
