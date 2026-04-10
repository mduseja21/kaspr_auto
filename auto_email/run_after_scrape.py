#!/usr/bin/env python3
"""Prepare contacts from scrape results and kick off the email sender."""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from prepare_contacts import build_contacts, load_scrape_rows, write_contacts

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS = BASE_DIR.parent / "results.csv"
DEFAULT_CONTACTS = BASE_DIR / "contacts_from_scrape.csv"
DEFAULT_TEMPLATE = BASE_DIR / "templates" / "sample.txt"
DEFAULT_TRACKING = BASE_DIR / "tracking.csv"
DEFAULT_SOURCE_TRACKING = ""


def sync_file(source_path, destination_path, label):
    if not source_path or not destination_path:
        print(f"{label}: disabled")
        return False

    source = Path(source_path)
    destination = Path(destination_path)

    if not source.exists():
        print(f"{label}: source not found, skipping ({source})")
        return False

    if source.resolve() == destination.resolve():
        print(f"{label}: source and destination are the same file, skipping")
        return False

    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    print(f"{label}: copied {source} -> {destination}")
    return True


def run_email_sender(args):
    command = [
        sys.executable,
        str(BASE_DIR / "send_emails.py"),
        "--contacts",
        str(args.contacts_out),
        "--template",
        str(args.template),
    ]

    if args.sender:
        command.extend(["--sender", args.sender])
    if args.dry_run:
        command.append("--dry-run")
    if args.pace is not None:
        command.extend(["--pace", str(args.pace)])
    if args.max is not None:
        command.extend(["--max", str(args.max)])
    if args.attach:
        command.append("--attach")
        command.extend(args.attach)

    subprocess.run(command, cwd=BASE_DIR, check=True)


def main():
    parser = argparse.ArgumentParser(
        description="Turn scrape results into contacts and launch the email sender."
    )
    parser.add_argument("--results", default=str(DEFAULT_RESULTS), help="Path to scraper results CSV.")
    parser.add_argument("--contacts-out", default=str(DEFAULT_CONTACTS), help="Output contacts CSV path.")
    parser.add_argument("--template", default=str(DEFAULT_TEMPLATE), help="Email template path.")
    parser.add_argument(
        "--source-tracking",
        default=DEFAULT_SOURCE_TRACKING,
        help="Optional tracking CSV to sync from before sending, and back to after sending.",
    )
    parser.add_argument("--sender", default="", help="Optional sender override for send_emails.py.")
    parser.add_argument("--dry-run", action="store_true", help="Preview emails without sending.")
    parser.add_argument("--pace", type=float, default=None, help="Average minutes between emails.")
    parser.add_argument("--max", type=int, default=None, help="Max emails to send in this run.")
    parser.add_argument("--attach", nargs="*", default=[], help="Optional attachments to send.")
    args = parser.parse_args()

    args.results = Path(args.results).resolve()
    args.contacts_out = Path(args.contacts_out).resolve()
    args.template = Path(args.template).resolve()
    args.source_tracking = str(Path(args.source_tracking).resolve()) if args.source_tracking else ""

    sync_file(args.source_tracking, DEFAULT_TRACKING, "Tracking sync in")

    rows = load_scrape_rows(args.results)
    contacts = build_contacts(rows)
    write_contacts(args.contacts_out, contacts)
    print(f"Prepared {len(contacts)} contacts from {args.results}")

    if not contacts:
        print("No contacts with emails were found. Skipping send step.")
        return

    run_email_sender(args)
    sync_file(DEFAULT_TRACKING, args.source_tracking, "Tracking sync back")


if __name__ == "__main__":
    main()
