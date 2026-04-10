import os
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

PROVIDERS = {
    "gmail": {
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 587,
        "imap_host": "imap.gmail.com",
        "imap_port": 993,
        "address": os.getenv("GMAIL_ADDRESS", ""),
        "password": os.getenv("GMAIL_APP_PASSWORD", ""),
        "auth_method": "gmail_api",
    },
    "outlook": {
        "smtp_host": "smtp.office365.com",
        "smtp_port": 587,
        "imap_host": "outlook.office365.com",
        "imap_port": 993,
        "address": os.getenv("OUTLOOK_ADDRESS", ""),
        "password": os.getenv("OUTLOOK_PASSWORD", ""),
        "auth_method": "oauth2",
    },
}

DEFAULT_SENDER = os.getenv("DEFAULT_SENDER", "gmail")


def get_provider(name=None):
    name = (name or DEFAULT_SENDER).lower()
    if name not in PROVIDERS:
        raise ValueError(f"Unknown provider '{name}'. Choose from: {list(PROVIDERS)}")
    cfg = PROVIDERS[name]
    if not cfg["address"]:
        raise ValueError(
            f"Credentials for '{name}' are not set. "
            f"Copy .env.example to .env and fill in your details."
        )
    return cfg
