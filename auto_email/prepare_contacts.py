#!/usr/bin/env python3
"""Build sender contacts from the unified master tracking CSV."""

import argparse
import csv
from pathlib import Path

try:
    from .master_tracking import default_tracking_path, load_tracking, normalize_email
except ImportError:
    from master_tracking import default_tracking_path, load_tracking, normalize_email

DEFAULT_INPUT = default_tracking_path()
DEFAULT_OUTPUT = "auto_email/contacts_from_scrape.csv"
OUTPUT_HEADERS = ["name", "company_name", "email", "title", "linkedin_url", "source_status"]


def load_tracking_rows(path):
    return load_tracking(str(path))


def build_contacts(rows):
    seen_emails = set()
    contacts = []

    for row in rows:
        kaspr_status = row.get("kaspr_status", row.get("status", "")).strip().lower()
        if kaspr_status != "found":
            continue

        email = normalize_email(row.get("email"))
        if not email:
            continue
        if email in seen_emails:
            continue

        seen_emails.add(email)
        contacts.append(
            {
                "name": row.get("Name", "").strip(),
                "company_name": row.get("Company", "").strip(),
                "email": email,
                "title": row.get("Title", "").strip(),
                "linkedin_url": row.get("linkedinUrl", row.get("linkedin_url", "")).strip(),
                "source_status": row.get("kaspr_status", row.get("status", "")).strip(),
            }
        )

    return contacts


def write_contacts(path, contacts):
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADERS)
        writer.writeheader()
        writer.writerows(contacts)


def main():
    parser = argparse.ArgumentParser(
        description="Convert master tracking rows into auto_email contacts."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help=f"Path to master tracking CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Path to output contacts CSV (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    rows = load_tracking_rows(args.input)
    contacts = build_contacts(rows)
    write_contacts(args.output, contacts)

    print(f"Read {len(rows)} master tracking rows from {args.input}")
    print(f"Wrote {len(contacts)} contacts to {args.output}")


if __name__ == "__main__":
    main()
