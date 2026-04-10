#!/usr/bin/env python3
"""Check inbox for replies to sent emails and update tracking.csv."""

import csv
import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone

from outlook_auth import get_oauth2_token
from config import get_provider

TRACKING_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tracking.csv")
TRACKING_HEADERS = [
    "email", "name", "company_name", "sent_at",
    "sender_account", "status", "read_receipt", "reply_detected", "reply_at",
]


def load_tracking():
    if not os.path.exists(TRACKING_FILE):
        return []
    with open(TRACKING_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # Ensure all headers exist in each row
    for row in rows:
        for h in TRACKING_HEADERS:
            if h not in row:
                row[h] = ""
    return rows


def save_tracking(rows):
    with open(TRACKING_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TRACKING_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


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
    sent_rows = [r for r in rows if r["status"] == "sent"]
    if not sent_rows:
        print("No sent emails found in tracking.")
        return

    print(f"\n{'Name':<15} {'Company':<20} {'Email':<35} {'Read':>6} {'Reply':>6}")
    print("-" * 86)
    for row in sent_rows:
        read = "Yes" if row.get("read_receipt") == "True" else "-"
        replied = "Yes" if row.get("reply_detected") == "True" else "-"
        print(f"{row['name']:<15} {row['company_name']:<20} {row['email']:<35} {read:>6} {replied:>6}")

    total = len(sent_rows)
    reads = len([r for r in sent_rows if r.get("read_receipt") == "True"])
    replies = len([r for r in sent_rows if r.get("reply_detected") == "True"])
    print("-" * 86)
    print(f"Total sent: {total} | Read receipts: {reads} | Replies: {replies}")


def main():
    rows = load_tracking()
    sent_rows = [r for r in rows if r["status"] == "sent"]

    if not sent_rows:
        print("No sent emails to check.")
        return

    provider = get_provider()
    token = get_oauth2_token(provider["address"])

    print(f"Checking replies for {len(sent_rows)} sent emails...")
    updated = 0

    for row in rows:
        if row["status"] != "sent":
            continue

        email_addr = row["email"]

        # Skip if already marked as replied
        if row.get("reply_detected") == "True":
            continue

        # Search for replies from this person after we sent the email
        replies = search_replies(token, email_addr, row["sent_at"][:10])
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
            if row["status"] != "sent" or row.get("read_receipt") == "True":
                continue
            if row["name"] in subj or row["email"].split("@")[0] in receipt.get("from", {}).get("emailAddress", {}).get("address", ""):
                row["read_receipt"] = "True"
                print(f"  READ receipt: {row['email']}")
                updated += 1

    if updated:
        save_tracking(rows)
        print(f"\nUpdated {updated} entries in tracking.csv")
    else:
        print("\nNo new replies or read receipts found.")

    print_table(rows)


if __name__ == "__main__":
    main()
