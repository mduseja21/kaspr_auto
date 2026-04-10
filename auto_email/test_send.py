#!/usr/bin/env python3
"""Send a single test email via Graph API using device code flow."""
import sys
import json
import urllib.request
import urllib.error
import msal

CLIENT_ID = "d3590ed6-52b3-4102-aeff-aad2292ab01c"
AUTHORITY = "https://login.microsoftonline.com/4130bd39-7c53-419c-b1e5-8758d6d63f21"
SCOPES = ["https://graph.microsoft.com/Mail.Send"]

app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)

# Try silent auth first (from cached token)
accounts = app.get_accounts()
token = None
if accounts:
    result = app.acquire_token_silent(SCOPES, account=accounts[0])
    if result and "access_token" in result:
        token = result["access_token"]
        print("Using cached token.")

if not token:
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        print("FAILED:", flow.get("error_description", flow))
        sys.exit(1)
    print(f"Go to: {flow['verification_uri']}")
    print(f"Enter: {flow['user_code']}")
    result = app.acquire_token_by_device_flow(flow)
    if "access_token" not in result:
        print("FAILED:", result.get("error_description", result))
        sys.exit(1)
    token = result["access_token"]

# Send test email
message = {
    "message": {
        "subject": "Exploring Quant Opportunities at Purdue",
        "body": {
            "contentType": "Text",
            "content": (
                "Dear Mohit,\n\n"
                "I hope you're doing well! I'm currently exploring quant researcher and "
                "quant developer opportunities and wanted to reach out to see if there are "
                "any relevant opportunities at Purdue.\n\n"
                "A bit about my background: I spent 4 years at Goldman Sachs as a "
                "Quantitative Strategist on the Equity Synthetics desk, where I developed "
                "pricing models for equity derivatives, built quantitative index calculation "
                "frameworks, optimized long-short portfolios using risk factor decomposition, "
                "and engineered low-latency pricing systems. Before that, I studied Mathematics "
                "and Scientific Computing at IIT Kanpur with a minor in Machine Learning. I'm "
                "currently finishing up my Master's in Computer Science and Statistics at Purdue "
                "and am STEM OPT eligible.\n\n"
                "No pressure at all if you're not able to help, I completely understand. Either way, "
                "thank you for considering it!\n\n"
                "Best regards, Mohit Duseja +1-765-476-3514"
            ),
        },
        "toRecipients": [
            {"emailAddress": {"address": "mduseja@purdue.edu"}}
        ],
    },
    "saveToSentItems": "true",
}

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
    print("Email sent successfully to mduseja@purdue.edu!")
except urllib.error.HTTPError as e:
    print(f"FAILED {e.code}: {e.read().decode()}")
