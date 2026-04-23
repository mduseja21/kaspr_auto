#!/usr/bin/env python3
"""Check inbox for replies from tracked recipients and print a summary."""

import argparse
import imaplib
import sys
from datetime import datetime, timezone

try:
    from .config import get_provider
except ImportError:
    from config import get_provider
try:
    from .master_tracking import (
        default_tracking_path,
        is_db_path,
        load_tracking as load_master_tracking,
        normalize_email,
        save_tracking,
    )
except ImportError:
    from master_tracking import (
        default_tracking_path,
        is_db_path,
        load_tracking as load_master_tracking,
        normalize_email,
        save_tracking,
    )
try:
    from .tracking_db import open_tracking_db, close_tracking_db, upsert_row as db_upsert_row
except ImportError:
    from tracking_db import open_tracking_db, close_tracking_db, upsert_row as db_upsert_row


def load_tracking(tracking_path):
    rows = load_master_tracking(tracking_path)
    if not rows:
        print("No master tracking rows found. Run the scraper or sender first.")
        sys.exit(1)
    return rows


def check_replies(provider_name=None, tracking_path=None):
    tracking_path = tracking_path or default_tracking_path()
    provider = get_provider(provider_name)
    rows = load_tracking(tracking_path)

    # Build set of recipient emails we're tracking (only sent ones)
    tracked_emails = {
        normalize_email(r["email"])
        for r in rows
        if r["email_send_status"] == "sent" and r["reply_detected"] == "False"
    }

    if not tracked_emails:
        print("No pending replies to check.")
        print_summary(rows)
        return

    print(f"Connecting to {provider['imap_host']} as {provider['address']}...")

    with imaplib.IMAP4_SSL(provider["imap_host"], provider["imap_port"]) as mail:
        mail.login(provider["address"], provider["password"])
        mail.select("INBOX")

        found_replies = set()

        for target_email in tracked_emails:
            # Search for emails FROM this recipient
            status, data = mail.search(None, f'(FROM "{target_email}")')
            if status != "OK" or not data[0]:
                continue

            msg_ids = data[0].split()
            if msg_ids:
                found_replies.add(target_email)

    # Update tracking
    now = datetime.now(timezone.utc).isoformat()
    updated = 0
    for row in rows:
        if normalize_email(row["email"]) in found_replies and row["reply_detected"] == "False":
            row["reply_detected"] = "True"
            row["reply_at"] = now
            updated += 1

    if updated:
        if is_db_path(tracking_path):
            db_conn = open_tracking_db(tracking_path)
            try:
                for row in rows:
                    if row.get("reply_detected") == "True":
                        db_upsert_row(db_conn, row, linkedin_url=row.get("linkedinUrl", ""))
            finally:
                close_tracking_db(db_conn)
        else:
            save_tracking(rows, tracking_path)
        print(f"\nUpdated {updated} contact(s) with reply detected.")
    else:
        print("\nNo new replies found.")

    print_summary(rows)


def print_summary(rows):
    sent_rows = [r for r in rows if r["email_send_status"] == "sent"]
    total_sent = len(sent_rows)
    replied = sum(1 for r in sent_rows if r["reply_detected"] == "True")
    no_reply = total_sent - replied
    failed = sum(1 for r in rows if r["email_send_status"] == "failed")

    print("\n" + "=" * 40)
    print("        EMAIL CAMPAIGN SUMMARY")
    print("=" * 40)
    print(f"  Total sent:      {total_sent}")
    print(f"  Replies:         {replied}")
    print(f"  No reply yet:    {no_reply}")
    print(f"  Failed to send:  {failed}")
    if total_sent > 0:
        print(f"  Reply rate:      {replied / total_sent * 100:.1f}%")
    print("=" * 40)


def main():
    parser = argparse.ArgumentParser(description="Check for email replies")
    parser.add_argument(
        "--sender", default=None,
        help="Email provider to check: gmail or outlook (default from .env)",
    )
    parser.add_argument(
        "--tracking",
        default=default_tracking_path(),
        help="Path to master tracking CSV file",
    )
    args = parser.parse_args()
    check_replies(args.sender, args.tracking)


if __name__ == "__main__":
    main()
