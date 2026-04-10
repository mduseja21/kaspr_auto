#!/usr/bin/env python3
"""Convert scraper results into the contact shape used by auto_email."""

import argparse
import csv
from pathlib import Path


DEFAULT_INPUT = "results.csv"
DEFAULT_OUTPUT = "auto_email/contacts_from_scrape.csv"
OUTPUT_HEADERS = ["name", "company_name", "email", "title", "linkedin_url", "source_status"]


def normalize_email(raw_email):
    email = (raw_email or "").strip().lower()
    if not email or "@" not in email:
        return ""
    return email


def load_scrape_rows(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def build_contacts(rows):
    seen_emails = set()
    contacts = []

    for row in rows:
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
                "source_status": row.get("status", "").strip(),
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
        description="Convert Kaspr scraper results into auto_email contacts."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help=f"Path to scraper results CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Path to output contacts CSV (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    rows = load_scrape_rows(args.input)
    contacts = build_contacts(rows)
    write_contacts(args.output, contacts)

    print(f"Read {len(rows)} scrape rows from {args.input}")
    print(f"Wrote {len(contacts)} contacts to {args.output}")


if __name__ == "__main__":
    main()
