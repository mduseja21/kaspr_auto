"""OAuth2 authentication for Outlook/Office 365 using device code flow.

Uses Microsoft Office's public client ID. Token is cached to disk so
you only need to sign in once (until the refresh token expires).
"""

import os
import sys
import msal

CLIENT_ID = "d3590ed6-52b3-4102-aeff-aad2292ab01c"
AUTHORITY = "https://login.microsoftonline.com/4130bd39-7c53-419c-b1e5-8758d6d63f21"
SCOPES = ["https://graph.microsoft.com/Mail.Send", "https://graph.microsoft.com/Mail.ReadWrite"]

TOKEN_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".outlook_token_cache.json")


def _build_app():
    cache = msal.SerializableTokenCache()
    if os.path.exists(TOKEN_CACHE_FILE):
        with open(TOKEN_CACHE_FILE, "r") as f:
            cache.deserialize(f.read())
    app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)
    return app, cache


def _save_cache(cache):
    if cache.has_state_changed:
        with open(TOKEN_CACHE_FILE, "w") as f:
            f.write(cache.serialize())


def get_oauth2_token(email_address):
    """Get an OAuth2 access token. Uses disk-cached refresh token if available."""
    app, cache = _build_app()

    # Try silent token acquisition from cache
    accounts = app.get_accounts(username=email_address)
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            _save_cache(cache)
            return result["access_token"]

    # Need interactive login via device code
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"Device flow failed: {flow.get('error_description', 'unknown error')}")

    print()
    print("=" * 55)
    print(f"  Go to:   {flow['verification_uri']}")
    print(f"  Enter:   {flow['user_code']}")
    print(f"  Sign in: {email_address}")
    print("=" * 55)
    print("Waiting for authentication...")
    sys.stdout.flush()

    result = app.acquire_token_by_device_flow(flow)
    if "access_token" not in result:
        raise RuntimeError(
            f"Auth failed: {result.get('error_description', result.get('error', 'unknown'))}"
        )

    _save_cache(cache)
    print("Authenticated!\n")
    return result["access_token"]
