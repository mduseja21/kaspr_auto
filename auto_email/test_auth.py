#!/usr/bin/env python3
"""Test authentication and send a test email."""

import json
import os
import sys
import urllib.request

from outlook_auth import get_oauth2_token
from config import get_provider

provider = get_provider()
email = provider["address"]

token = get_oauth2_token(email)
print(f"Token acquired. Length: {len(token)}")

# Quick send test
msg = {
    "message": {
        "subject": "Auth test",
        "body": {"contentType": "Text", "content": "Token is working."},
        "toRecipients": [{"emailAddress": {"address": email}}],
    }
}
req = urllib.request.Request(
    "https://graph.microsoft.com/v1.0/me/sendMail",
    data=json.dumps(msg).encode(),
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    method="POST",
)
try:
    urllib.request.urlopen(req)
    print(f"Test email sent to {email}!")
except Exception as e:
    print(f"Send failed: {e}")
