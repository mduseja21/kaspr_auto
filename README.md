# kaspr_auto

Automated lead prospecting pipeline: discover contacts at target companies via Apollo, extract emails via Kaspr, and send templated outreach emails.

## How it works

```
Companies list ──> Apollo (find people) ──> Kaspr (extract emails) ──> Email (send outreach)
                        │                        │                        │
                        └── tracking.db ─────────┴────────────────────────┘
                           (shared SQLite state)
```

All three stages read and write the same SQLite database (`auto_email/tracking.db`). They can run in parallel — Apollo feeds new LinkedIn profiles into the DB, Kaspr picks them up and extracts emails, and the email sender picks up contacts with emails.

## Prerequisites

- **Node.js** 18+
- **Python** 3.10+
- **A Chromium-based browser** with the [Kaspr extension](https://chrome.google.com/webstore/detail/kaspr/kkfgenjfpmoegefcckjklfjieepogfhg) installed (Chrome, Helium, Brave, etc.)
- **An Apollo.io account** (free tier works)
- **Gmail and/or Outlook** email account for sending

## Quick Start

```bash
# 1. Install dependencies
npm install
python3 -m venv auto_email/venv
auto_email/venv/bin/pip install -r auto_email/requirements.txt

# 2. Download the Apollo browser (Camoufox)
npm run apollo:fetch-browser

# 3. Set up email credentials
cp auto_email/.env.example auto_email/.env
# Edit auto_email/.env with your Gmail/Outlook credentials

# 4. Set up browser sessions (see below)
# 5. Create your input files (see below)
# 6. Run the pipeline
```

## Setup

### A. Email Credentials

Copy the example env file and fill in your details:

```bash
cp auto_email/.env.example auto_email/.env
```

**Gmail** — requires an App Password (not your regular password):
1. Go to [Google Account > Security](https://myaccount.google.com/security)
2. Enable 2-Step Verification
3. Go to [App Passwords](https://myaccount.google.com/apppasswords)
4. Generate a password for "Mail"
5. Set `GMAIL_ADDRESS` and `GMAIL_APP_PASSWORD` in `.env`

**Gmail API** (for Graph API features):
1. Create a project in [Google Cloud Console](https://console.cloud.google.com/)
2. Enable the Gmail API
3. Create OAuth 2.0 credentials (Desktop app)
4. Download the credentials JSON and save as `auto_email/gmail_credentials.json`
5. On first run, a browser window opens for authorization

**Outlook** — uses OAuth2:
1. Set `OUTLOOK_ADDRESS` and `OUTLOOK_PASSWORD` in `.env`
2. On first run, the OAuth flow generates a token automatically

### B. Kaspr Browser Session

Kaspr runs as a Chrome extension. You need to log in once and the session is saved:

```bash
node setup_session.js
```

1. A browser opens to a LinkedIn profile page
2. The Kaspr widget appears on the right side
3. Click **"Log in"** inside the Kaspr widget (not the Kaspr website)
4. Complete the login
5. Press **Ctrl+C** to save the session

The session is stored in `runtime/chrome_profile/` and reused automatically.

### C. Apollo Browser Session

Apollo uses Camoufox (a privacy-focused Firefox). Log in once:

```bash
node -e "
const path = require('path');
(async () => {
  const { Camoufox } = await import('camoufox-js');
  const context = await Camoufox({
    headless: false,
    humanize: true,
    user_data_dir: path.join(process.cwd(), 'runtime/apollo/profile'),
  });
  const page = context.pages()[0] || await context.newPage();
  await page.goto('https://app.apollo.io/login');
  console.log('Log into Apollo, then press Ctrl+C.');
  process.on('SIGINT', async () => { await context.close(); process.exit(0); });
  await new Promise(() => {});
})();
"
```

1. A Firefox window opens to Apollo login
2. Log in with your Apollo credentials
3. Press **Ctrl+C** to save the session

The session is stored in `runtime/apollo/profile/` and reused automatically.

**Optional: Capsolver for Cloudflare** — if Apollo shows Cloudflare challenges:
1. Download the [Capsolver Firefox extension](https://addons.mozilla.org/en-US/firefox/addon/capsolver-captcha-solver/)
2. Extract the `.xpi` to `runtime/apollo/extensions/capsolver/`
3. Set your API key in `runtime/apollo/extensions/capsolver/assets/config.js`

### D. Input Files

**Companies list** — one company name per line:
```bash
# runtime/apollo/firms.csv
Two Sigma
Citadel Securities
Jane Street
Goldman Sachs
```

**Email template** — first line is the subject:
```
Subject: Exploring Opportunities at {company_name}

Dear {name},

I'm reaching out to see if there are opportunities at {company_name}...

Best regards,
Your Name
```

Template variables: `{name}`, `{company_name}`, `{title}`, `{email}`

**Excluded companies** (optional) — substring matching:
```bash
# auto_email/excluded_companies.txt
# Companies to skip (one per line, substring match)
voleon
goldman
```

## Running the Pipeline

### Apollo — Discover contacts at target companies

```bash
PIPELINE_MODE=apollo-only \
APOLLO_FIRM_INPUT_CSV=runtime/apollo/firms.csv \
APOLLO_COMBINED_URL="https://app.apollo.io/#/people?contactEmailStatusV2%5B%5D=verified&personTitles%5B%5D=quant&personTitles%5B%5D=recruiter&personLocations%5B%5D=United+States&prospectedByCurrentTeam%5B%5D=no" \
node index.js
```

The template URL controls which people to find. Key filters:
- `personTitles[]` — job titles to search (e.g., `quant`, `recruiter`, `portfolio manager`)
- `personLocations[]` — geographic filter
- `contactEmailStatusV2[]` — `verified` for confirmed emails only
- `prospectedByCurrentTeam[]` — `no` to skip already-contacted people

Apollo resolves company names to org IDs (cached), then calls the internal search API to find people. Results are upserted into `tracking.db` incrementally.

### Kaspr — Extract emails from LinkedIn profiles

```bash
PIPELINE_MODE=scrape node index.js
```

Kaspr runs in **watch mode** by default — it processes all pending profiles, then polls the DB every 30 seconds for new ones (added by Apollo). Set `KASPR_WATCH=false` to disable.

### Email — Send outreach

```bash
auto_email/venv/bin/python3 auto_email/prepare_contacts.py \
  --input auto_email/tracking.db \
  --output auto_email/contacts_from_scrape.csv

auto_email/venv/bin/python3 auto_email/send_emails.py \
  --tracking auto_email/tracking.db \
  --contacts auto_email/contacts_from_scrape.csv \
  --template auto_email/templates/sample.txt \
  --sender "outlook,gmail" \
  --pace 0.8 \
  --dry-run
```

Remove `--dry-run` to send for real. Options:
- `--sender "outlook,gmail"` — alternate between providers
- `--pace 0.8` — average minutes between sends (with 40% random jitter)
- `--max 50` — limit emails per run
- `--attach resume.pdf` — attach files
- `--no-watch` — disable watch mode (exit after queue is empty)

### Running all three in parallel

```bash
# Terminal 1: Apollo
PIPELINE_MODE=apollo-only \
APOLLO_FIRM_INPUT_CSV=runtime/apollo/firms.csv \
APOLLO_COMBINED_URL="..." \
node index.js

# Terminal 2: Kaspr (watch mode — auto-picks up Apollo results)
PIPELINE_MODE=scrape node index.js

# Terminal 3: Email (watch mode — auto-picks up Kaspr results)
auto_email/venv/bin/python3 auto_email/send_emails.py \
  --tracking auto_email/tracking.db \
  --contacts auto_email/contacts_from_scrape.csv \
  --template auto_email/templates/sample.txt \
  --sender "outlook,gmail" --pace 0.8
```

All three safely share `tracking.db` via SQLite WAL mode.

### Scheduling daily emails

```bash
# Add to crontab (crontab -e)
0 9 * * * cd /path/to/kaspr_auto && \
  auto_email/venv/bin/python3 auto_email/prepare_contacts.py \
    --input auto_email/tracking.db \
    --output auto_email/contacts_from_scrape.csv && \
  auto_email/venv/bin/python3 auto_email/send_emails.py \
    --tracking auto_email/tracking.db \
    --contacts auto_email/contacts_from_scrape.csv \
    --template auto_email/templates/sample.txt \
    --sender "outlook,gmail" --pace 0.8 \
    >> /tmp/kaspr_email.log 2>&1
```

## Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `PIPELINE_MODE` | `full` | `scrape`, `email`, `full`, `apollo-only`, `apollo-full` |
| `MAX_PROFILES` | `9999` | Max Kaspr profiles per run |
| `KASPR_WATCH` | `true` | Poll DB for new profiles after queue empty |
| `APOLLO_MAX_PAGES_PER_ORG` | `5` | Max result pages per org+title combo |
| `APOLLO_ACTION_MIN_WAIT_MS` | `4000` | Min delay between Apollo API calls |
| `APOLLO_ACTION_MAX_WAIT_MS` | `10000` | Max delay between Apollo API calls |
| `AUTO_EMAIL_TRACKING_DB` | `auto_email/tracking.db` | SQLite database path |
| `CHROME_USER_DATA_DIR` | `runtime/chrome_profile` | Kaspr browser profile |

## npm Scripts

```bash
npm start                    # Full pipeline
npm run mode:scrape-only     # Kaspr only
npm run mode:email-only      # Email only
npm run mode:apollo-only     # Apollo only
npm run mode:apollo-full     # Apollo + Kaspr + Email
npm run apollo:fetch-browser # Download Camoufox
npm run email:dry-run        # Preview emails without sending
```

## Monitoring

Check the database state:

```bash
node -e "
const db = require('better-sqlite3')('auto_email/tracking.db');
console.log('Total profiles:', db.prepare('SELECT COUNT(*) as c FROM tracking').get().c);
console.log('With email:', db.prepare(\"SELECT COUNT(*) as c FROM tracking WHERE kaspr_status = 'found'\").get().c);
console.log('Pending Kaspr:', db.prepare(\"SELECT COUNT(*) as c FROM tracking WHERE kaspr_status = ''\").get().c);
console.log('Emails sent:', db.prepare(\"SELECT COUNT(*) as c FROM tracking WHERE email_send_status = 'sent'\").get().c);
console.log('Ready to send:', db.prepare(\"SELECT COUNT(*) as c FROM tracking WHERE email != '' AND (email_send_status = '' OR email_send_status IS NULL)\").get().c);
db.close();
"
```

## Project Structure

```
kaspr_auto/
├── index.js                  # Main pipeline orchestrator
├── apollo_camoufox.js        # Apollo company search + people discovery
├── tracking_db.js            # SQLite tracking (Node.js)
├── tracking_state.js         # Tracking state helpers
├── setup_session.js          # One-time Kaspr login setup
├── package.json
├── auto_email/
│   ├── send_emails.py        # Email sender with watch mode
│   ├── prepare_contacts.py   # Build contacts from tracking DB
│   ├── tracking_db.py        # SQLite tracking (Python)
│   ├── master_tracking.py    # Tracking state helpers
│   ├── config.py             # Email provider config
│   ├── gmail_auth.py         # Gmail OAuth
│   ├── outlook_auth.py       # Outlook OAuth
│   ├── check_replies.py      # Check for email replies
│   ├── check_status.py       # Check read receipts
│   ├── cleanup_inbox.py      # Process bounces
│   ├── templates/sample.txt  # Email template
│   ├── excluded_companies.txt
│   ├── .env.example
│   └── requirements.txt
└── runtime/                  # Created at runtime (gitignored)
    ├── chrome_profile/       # Kaspr browser session
    └── apollo/
        ├── profile/          # Apollo browser session
        ├── extensions/       # Capsolver etc.
        ├── raw/              # Raw scrape output
        └── firms.csv         # Company input list
```
