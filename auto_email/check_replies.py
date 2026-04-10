#!/usr/bin/env python3
"""Check inbox for replies from tracked recipients and print a summary."""

import argparse
import csv
import email
import imaplib
import os
import sys
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parseaddr

from config import get_provider

TRACKING_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tracking.csv")


def load_tracking():
    if not os.path.exists(TRACKING_FILE):
        print("No tracking file found. Run send_emails.py first.")
        sys.exit(1)
    with open(TRACKING_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_tracking(rows):
    if not rows:
        return
    with open(TRACKING_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def extract_sender_address(msg):
    from_header = msg.get("From", "")
    _, addr = parseaddr(from_header)
    return addr.lower()


def check_replies(provider_name=None):
    provider = get_provider(provider_name)
    rows = load_tracking()

    # Build set of recipient emails we're tracking (only sent ones)
    tracked_emails = {
        r["email"].lower()
        for r in rows
        if r["status"] == "sent" and r["reply_detected"] == "False"
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
        if row["email"].lower() in found_replies and row["reply_detected"] == "False":
            row["reply_detected"] = "True"
            row["reply_at"] = now
            updated += 1

    if updated:
        save_tracking(rows)
        print(f"\nUpdated {updated} contact(s) with reply detected.")
    else:
        print("\nNo new replies found.")

    print_summary(rows)


def print_summary(rows):
    sent_rows = [r for r in rows if r["status"] == "sent"]
    total_sent = len(sent_rows)
    replied = sum(1 for r in sent_rows if r["reply_detected"] == "True")
    no_reply = total_sent - replied
    failed = sum(1 for r in rows if r["status"].startswith("failed"))

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
    args = parser.parse_args()
    check_replies(args.sender)


if __name__ == "__main__":
    main()
