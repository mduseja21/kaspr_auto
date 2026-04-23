#!/usr/bin/env python3
"""Scan inbox for bounces & read receipts, update tracking.csv, and delete notifications."""

import argparse
import json
import re
import urllib.request
import urllib.parse

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
    return load_master_tracking(tracking_path)


def graph_get(token, url):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())


def graph_delete(token, msg_id):
    req = urllib.request.Request(
        f"https://graph.microsoft.com/v1.0/me/messages/{msg_id}",
        headers={"Authorization": f"Bearer {token}"},
        method="DELETE",
    )
    urllib.request.urlopen(req)


def get_all_messages(token, top=500):
    """Fetch up to `top` recent messages."""
    all_msgs = []
    params = urllib.parse.urlencode({
        "$top": min(top, 200),
        "$select": "id,subject,from,receivedDateTime,bodyPreview",
        "$orderby": "receivedDateTime desc",
    })
    url = f"https://graph.microsoft.com/v1.0/me/messages?{params}"

    while url and len(all_msgs) < top:
        data = graph_get(token, url)
        all_msgs.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    return all_msgs


def main():
    parser = argparse.ArgumentParser(
        description="Scan inbox for bounces & read receipts, update tracking.csv, and delete notifications."
    )
    parser.add_argument(
        "--tracking",
        default=default_tracking_path(),
        help="Path to master tracking CSV file",
    )
    args = parser.parse_args()

    provider = get_provider()
    token = get_oauth2_token(provider["address"])
    rows = load_tracking(args.tracking)

    # Build lookup by email
    tracking_by_email = {}
    for row in rows:
        email = normalize_email(row["email"])
        if email:
            tracking_by_email[email] = row

    print("Fetching inbox messages...")
    messages = get_all_messages(token)
    print(f"Found {len(messages)} messages")

    bounced_emails = set()
    read_emails = set()
    to_delete = []

    for msg in messages:
        subj = msg.get("subject", "") or ""
        from_addr = msg.get("from", {}).get("emailAddress", {}).get("address", "")
        preview = msg.get("bodyPreview", "") or ""
        mid = msg["id"]

        # Bounce / Undeliverable
        if ("Undeliverable" in subj or "Returned mail" in subj or
                "Delivery has failed" in subj or
                ("delivery" in subj.lower() and "fail" in subj.lower())):
            # Extract recipient email from preview
            found = re.findall(r"[\w.+-]+@[\w-]+\.[\w.-]+", preview)
            for email in found:
                email_lower = email.lower()
                if email_lower in tracking_by_email:
                    bounced_emails.add(email_lower)
            to_delete.append(mid)

        # Read receipt
        elif subj.startswith("Read:"):
            # The from address is who read it
            from_lower = from_addr.lower()
            if from_lower in tracking_by_email:
                read_emails.add(from_lower)
            to_delete.append(mid)

    # Update tracking
    updated = 0
    for email in bounced_emails:
        row = tracking_by_email[email]
        if row["email_send_status"] == "sent":
            row["email_send_status"] = "bounced"
            updated += 1

    for email in read_emails:
        row = tracking_by_email[email]
        if row.get("read_receipt") != "True":
            row["read_receipt"] = "True"
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

    print(f"\nTracking updated:")
    print(f"  Bounced: {len(bounced_emails)} emails marked as bounced")
    print(f"  Read receipts: {len(read_emails)} emails marked as read")

    # Delete notifications
    print(f"\nDeleting {len(to_delete)} notification emails from inbox...")
    deleted = 0
    failed = 0
    for mid in to_delete:
        try:
            graph_delete(token, mid)
            deleted += 1
        except Exception as e:
            failed += 1

    print(f"Deleted: {deleted} | Failed: {failed}")

    # Print summary table
    sent_rows = [r for r in rows if r["email_send_status"] in ("sent", "bounced")]
    total = len([r for r in rows if r["email_send_status"] == "sent"])
    bounced = len([r for r in rows if r["email_send_status"] == "bounced"])
    reads = len([r for r in rows if r.get("read_receipt") == "True"])
    replies = len([r for r in rows if r.get("reply_detected") == "True"])

    print(f"\n{'='*60}")
    print(f"  Total sent: {total} | Bounced: {bounced} | Read: {reads} | Replies: {replies}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
