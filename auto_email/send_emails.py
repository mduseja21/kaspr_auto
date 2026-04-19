#!/usr/bin/env python3
"""Send templated emails to contacts from a CSV file."""

import argparse
import base64
import collections
import csv
import json
import mimetypes
import os
import random
import smtplib
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, date, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import DEFAULT_SENDER, get_provider
from outlook_auth import get_oauth2_token
from gmail_auth import get_gmail_service, send_email_gmail_api

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKING_FILE = os.path.join(BASE_DIR, "tracking.csv")
TRACKING_HEADERS = [
    "email", "name", "company_name", "sent_at",
    "sender_account", "status", "read_receipt", "reply_detected", "reply_at",
]
EXCLUDED_COMPANIES_FILE = os.path.join(BASE_DIR, "excluded_companies.txt")
DEFAULT_TEMPLATE = os.path.join(BASE_DIR, "templates", "sample.txt")
DEFAULT_CONTACTS = os.path.join(BASE_DIR, "contacts.csv")
MAX_PER_COMPANY = 15
LIMIT_WINDOW_DAYS = 3


def load_contacts(path):
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # Normalize column names to {name}, {company_name}, {email}
    normalized = []
    for row in rows:
        if "First Name" in row:
            # Take only the first email if multiple are semicolon-separated
            raw_email = row.get("Email", "").strip()
            email = raw_email.split(";")[0].strip()
            normalized.append({
                "name": row["First Name"],
                "company_name": row.get("Company", ""),
                "email": email,
            })
        else:
            normalized.append(row)
    return normalized


def load_template(path):
    with open(path, encoding="utf-8") as f:
        lines = f.read().strip().splitlines()

    subject_line = lines[0]
    if not subject_line.lower().startswith("subject:"):
        print("WARNING: First line of template should start with 'Subject:'")
        subject = subject_line.strip()
    else:
        subject = subject_line.split(":", 1)[1].strip()

    body = "\n".join(lines[1:]).strip()
    return subject, body


def load_excluded_companies():
    if not os.path.exists(EXCLUDED_COMPANIES_FILE):
        return set()
    with open(EXCLUDED_COMPANIES_FILE, encoding="utf-8") as f:
        return {
            line.strip().lower()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        }


def get_recent_company_counts():
    """Count how many emails were sent to each company in the last LIMIT_WINDOW_DAYS days."""
    counts = collections.Counter()
    if not os.path.exists(TRACKING_FILE):
        return counts
    cutoff = datetime.now(timezone.utc).timestamp() - (LIMIT_WINDOW_DAYS * 86400)
    with open(TRACKING_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["status"] != "sent":
                continue
            try:
                sent_ts = datetime.fromisoformat(row["sent_at"]).timestamp()
            except ValueError:
                continue
            if sent_ts >= cutoff:
                counts[row["company_name"].lower()] += 1
    return counts


def already_sent(email_addr):
    if not os.path.exists(TRACKING_FILE):
        return False
    with open(TRACKING_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["email"].lower() == email_addr.lower() and row["status"] == "sent":
                return True
    return False


def append_tracking(row_dict):
    file_exists = os.path.exists(TRACKING_FILE)
    with open(TRACKING_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TRACKING_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row_dict)


def render(template_str, contact):
    return template_str.format_map(contact)


def parse_sender_names(sender_arg):
    if sender_arg is None:
        return [DEFAULT_SENDER]

    names = [name.strip().lower() for name in sender_arg.split(",") if name.strip()]
    if not names:
        raise ValueError("At least one sender must be provided.")

    # Preserve order while removing duplicates.
    return list(dict.fromkeys(names))


def build_sender_states(sender_arg):
    sender_names = parse_sender_names(sender_arg)
    sender_states = []

    for name in sender_names:
        provider = get_provider(name)
        sender_states.append({
            "name": name,
            "provider": provider,
            "auth_method": provider.get("auth_method"),
            "token": None,
            "server": None,
            "gmail_service": None,
        })

    return sender_states


def send_email_smtp(smtp_conn, from_addr, to_addr, subject, body, attachments=None):
    msg = MIMEMultipart("mixed")
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    for filepath in (attachments or []):
        filename = os.path.basename(filepath)
        with open(filepath, "rb") as f:
            part = MIMEApplication(f.read(), Name=filename)
        part["Content-Disposition"] = f'attachment; filename="{filename}"'
        msg.attach(part)

    smtp_conn.sendmail(from_addr, to_addr, msg.as_string())


def send_email_graph(token, from_addr, to_addr, subject, body, attachments=None):
    """Send email via Microsoft Graph API with read/delivery receipt requests."""
    message = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body,
            },
            "toRecipients": [
                {"emailAddress": {"address": to_addr}}
            ],
            "isDeliveryReceiptRequested": True,
            "isReadReceiptRequested": True,
            "attachments": [],
        },
        "saveToSentItems": "true",
    }

    for filepath in (attachments or []):
        filename = os.path.basename(filepath)
        content_type = mimetypes.guess_type(filepath)[0] or "application/octet-stream"
        with open(filepath, "rb") as f:
            content_bytes = base64.b64encode(f.read()).decode("utf-8")
        message["message"]["attachments"].append({
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": filename,
            "contentType": content_type,
            "contentBytes": content_bytes,
        })

    data = json.dumps(message).encode("utf-8")
    req = urllib.request.Request(
        "https://graph.microsoft.com/v1.0/me/sendMail",
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Graph API error {e.code}: {error_body}")


def main():
    parser = argparse.ArgumentParser(description="Send templated emails")
    parser.add_argument(
        "--sender", default=None,
        help="Email provider(s) to use: gmail, outlook, or a comma-separated "
             "list to alternate from one queue (default from .env)",
    )
    parser.add_argument(
        "--template", default=DEFAULT_TEMPLATE,
        help="Path to email template file",
    )
    parser.add_argument(
        "--contacts", default=DEFAULT_CONTACTS,
        help="Path to contacts CSV file",
    )
    parser.add_argument(
        "--attach", nargs="*", default=[],
        help="File(s) to attach (e.g. --attach resume.pdf)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview emails without sending",
    )
    parser.add_argument(
        "--pace", type=float, default=0.4,
        help="Average minutes between emails (adds random jitter ±40%%). "
             "E.g. --pace 0.5 sends about every 18-42 seconds, "
             "--pace 3 sends ~1 email every 2-4 minutes. Default is 0.4. 0 = no delay.",
    )
    parser.add_argument(
        "--max", type=int, default=0,
        help="Max number of emails to send in this run. 0 = no limit.",
    )
    args = parser.parse_args()

    # Validate attachments exist
    for path in args.attach:
        if not os.path.isfile(path):
            print(f"ERROR: Attachment not found: {path}")
            sys.exit(1)

    sender_states = build_sender_states(args.sender)
    contacts = load_contacts(args.contacts)
    subject_tpl, body_tpl = load_template(args.template)
    auth_method_names = []
    for state in sender_states:
        auth_method_names.append(
            {"gmail_api": "Gmail API", "oauth2": "Microsoft Graph API"}.get(
                state["auth_method"], "SMTP"
            )
        )
    excluded = load_excluded_companies()
    company_counts = get_recent_company_counts()

    if not contacts:
        print("No contacts found.")
        return

    if excluded:
        print(f"Excluded companies: {', '.join(sorted(excluded))}")
    print("Sender(s): " + ", ".join(
        f"{state['name']}<{state['provider']['address']}>"
        for state in sender_states
    ))
    print(f"Template: {args.template}")
    print(f"Contacts: {len(contacts)}")
    print(f"Per-company limit: {MAX_PER_COMPANY} per {LIMIT_WINDOW_DAYS} days")
    print(f"Method(s): {', '.join(auth_method_names)}")
    if args.pace > 0:
        print(f"Pace: ~1 email every {args.pace} min (±40% jitter)")
    if args.attach:
        print(f"Attachments: {', '.join(os.path.basename(a) for a in args.attach)}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("-" * 50)

    if args.dry_run:
        sender_index = 0
        for contact in contacts:
            company_key = contact["company_name"].lower()
            if any(ex in company_key for ex in excluded):
                print(f"\nSKIP (excluded company): {contact['email']} [{contact['company_name']}]")
                continue
            if company_counts[company_key] >= MAX_PER_COMPANY:
                print(f"\nSKIP (limit {MAX_PER_COMPANY}/{LIMIT_WINDOW_DAYS}d): {contact['email']} [{contact['company_name']}]")
                continue
            if already_sent(contact["email"]):
                print(f"\nSKIP (already sent): {contact['email']}")
                continue
            sender_state = sender_states[sender_index % len(sender_states)]
            sender_index += 1
            subj = render(subject_tpl, contact)
            body = render(body_tpl, contact)
            print(f"\nTo: {contact['email']}")
            print(f"From: {sender_state['provider']['address']} ({sender_state['name']})")
            print(f"Subject: {subj}")
            print(f"Body:\n{body}")
            if args.attach:
                print(f"Attachments: {', '.join(os.path.basename(a) for a in args.attach)}")
            print("-" * 50)
            company_counts[company_key] += 1
        print("\nDry run complete. No emails sent.")
        return

    # Authenticate
    for state in sender_states:
        provider = state["provider"]
        auth_method = state["auth_method"]

        if auth_method == "gmail_api":
            state["gmail_service"] = get_gmail_service()
        elif auth_method == "oauth2":
            state["token"] = get_oauth2_token(provider["address"])
        else:
            server = smtplib.SMTP(provider["smtp_host"], provider["smtp_port"])
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(provider["address"], provider["password"])
            state["server"] = server

    sent = 0
    skipped = 0
    failed = 0
    limited = 0
    sender_index = 0

    try:
        for contact in contacts:
            email_addr = contact["email"]
            company_key = contact["company_name"].lower()

            # Check excluded companies
            if any(ex in company_key for ex in excluded):
                print(f"SKIP (excluded company): {email_addr} [{contact['company_name']}]")
                skipped += 1
                continue

            # Check per-company daily limit
            if company_counts[company_key] >= MAX_PER_COMPANY:
                print(f"SKIP (limit {MAX_PER_COMPANY}/{LIMIT_WINDOW_DAYS}d): {email_addr} [{contact['company_name']}]")
                limited += 1
                continue

            if already_sent(email_addr):
                print(f"SKIP (already sent): {email_addr}")
                skipped += 1
                continue

            sender_state = sender_states[sender_index % len(sender_states)]
            sender_index += 1
            provider = sender_state["provider"]
            auth_method = sender_state["auth_method"]
            subj = render(subject_tpl, contact)
            body = render(body_tpl, contact)

            try:
                if auth_method == "gmail_api":
                    send_email_gmail_api(sender_state["gmail_service"], provider["address"], email_addr, subj, body, args.attach)
                elif auth_method == "oauth2":
                    send_email_graph(sender_state["token"], provider["address"], email_addr, subj, body, args.attach)
                else:
                    send_email_smtp(sender_state["server"], provider["address"], email_addr, subj, body, args.attach)

                append_tracking({
                    "email": email_addr,
                    "name": contact["name"],
                    "company_name": contact["company_name"],
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                    "sender_account": provider["address"],
                    "status": "sent",
                    "read_receipt": "False",
                    "reply_detected": "False",
                    "reply_at": "",
                })
                company_counts[company_key] += 1
                print(f"SENT: {email_addr} [{contact['company_name']}] via {provider['address']}")
                sent += 1

                if args.max > 0 and sent >= args.max:
                    print(f"\nReached --max {args.max}. Stopping.")
                    raise StopIteration

                if args.pace > 0:
                    delay = args.pace * 60 * random.uniform(0.6, 1.4)
                    next_time = datetime.now() + __import__('datetime').timedelta(seconds=delay)
                    print(f"  Waiting {delay/60:.1f} min (next ~{next_time.strftime('%H:%M')})...")
                    sys.stdout.flush()
                    time.sleep(delay)
            except Exception as e:
                print(f"FAILED: {email_addr} — {e}")
                append_tracking({
                    "email": email_addr,
                    "name": contact["name"],
                    "company_name": contact["company_name"],
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                    "sender_account": provider["address"],
                    "status": f"failed: {e}",
                    "read_receipt": "False",
                    "reply_detected": "False",
                    "reply_at": "",
                })
                failed += 1
    except StopIteration:
        pass
    finally:
        for state in sender_states:
            if state["server"]:
                state["server"].quit()

    print("-" * 50)
    print(f"Done. Sent: {sent} | Skipped: {skipped} | Rate-limited: {limited} | Failed: {failed}")


if __name__ == "__main__":
    main()
