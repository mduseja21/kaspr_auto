#!/usr/bin/env python3
"""Check inbox for replies to sent emails and update tracking.csv."""

import argparse
import json
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone

try:
    from .outlook_auth import get_oauth2_token
    from .config import get_provider
except ImportError:
    from outlook_auth import get_oauth2_token
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
    for row in rows:
        row.setdefault("read_receipt", "False")
        row.setdefault("reply_detected", "False")
    return rows


def search_inbox(token, query, top=50):
    """Search inbox via Graph API."""
    params = urllib.parse.urlencode({
        "$top": top,
        "$select": "subject,from,receivedDateTime,isRead",
        "$filter": query,
        "$orderby": "receivedDateTime desc",
    })
    url = f"https://graph.microsoft.com/v1.0/me/messages?{params}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read()).get("value", [])


def search_replies(token, email_addr, sent_after):
    """Search for emails from a specific address received after we sent ours."""
    # Use $filter to find messages from this sender after our sent date
    filter_q = (
        f"from/emailAddress/address eq '{email_addr}' "
        f"and receivedDateTime ge {sent_after}"
    )
    try:
        return search_inbox(token, filter_q)
    except Exception:
        return []


def check_read_receipts(token):
    """Search for read receipt messages in inbox."""
    try:
        filter_q = "contains(subject, 'Read:') or contains(subject, 'read receipt')"
        return search_inbox(token, filter_q)
    except Exception:
        return []


def print_table(rows):
    sent_rows = [r for r in rows if r["email_send_status"] == "sent"]
    if not sent_rows:
        print("No sent emails found in tracking.")
        return

    print(f"\n{'Name':<15} {'Company':<20} {'Email':<35} {'Read':>6} {'Reply':>6}")
    print("-" * 86)
    for row in sent_rows:
        read = "Yes" if row.get("read_receipt") == "True" else "-"
        replied = "Yes" if row.get("reply_detected") == "True" else "-"
        print(f"{row['Name']:<15} {row['Company']:<20} {row['email']:<35} {read:>6} {replied:>6}")

    total = len(sent_rows)
    reads = len([r for r in sent_rows if r.get("read_receipt") == "True"])
    replies = len([r for r in sent_rows if r.get("reply_detected") == "True"])
    print("-" * 86)
    print(f"Total sent: {total} | Read receipts: {reads} | Replies: {replies}")


def main():
    parser = argparse.ArgumentParser(description="Check inbox for replies to sent emails and update tracking.csv.")
    parser.add_argument(
        "--tracking",
        default=default_tracking_path(),
        help="Path to master tracking CSV file",
    )
    args = parser.parse_args()

    rows = load_tracking(args.tracking)
    sent_rows = [r for r in rows if r["email_send_status"] == "sent"]

    if not sent_rows:
        print("No sent emails to check.")
        return

    provider = get_provider()
    token = get_oauth2_token(provider["address"])

    print(f"Checking replies for {len(sent_rows)} sent emails...")
    updated = 0

    for row in rows:
        if row["email_send_status"] != "sent":
            continue

        email_addr = row["email"]

        # Skip if already marked as replied
        if row.get("reply_detected") == "True":
            continue

        # Search for replies from this person after we sent the email
        replies = search_replies(token, email_addr, row["email_sent_at"][:10])
        if replies:
            row["reply_detected"] = "True"
            row["reply_at"] = replies[0]["receivedDateTime"]
            print(f"  REPLY found: {email_addr} ({replies[0]['subject'][:50]})")
            updated += 1

    # Check for read receipts
    receipts = check_read_receipts(token)
    for receipt in receipts:
        # Read receipts have subject like "Read: Original Subject"
        subj = receipt.get("subject", "")
        for row in rows:
            if row["email_send_status"] != "sent" or row.get("read_receipt") == "True":
                continue
            if row["Name"] in subj or normalize_email(row["email"]).split("@")[0] in receipt.get("from", {}).get("emailAddress", {}).get("address", ""):
                row["read_receipt"] = "True"
                print(f"  READ receipt: {row['email']}")
                updated += 1

    if updated:
        if is_db_path(args.tracking):
            db_conn = open_tracking_db(args.tracking)
            try:
                for row in rows:
                    db_upsert_row(db_conn, row, linkedin_url=row.get("linkedinUrl", ""))
            finally:
                close_tracking_db(db_conn)
        else:
            save_tracking(rows, args.tracking)
        print(f"\nUpdated {updated} entries in tracking")
    else:
        print("\nNo new replies or read receipts found.")

    print_table(rows)


if __name__ == "__main__":
    main()
