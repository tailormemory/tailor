# Email Setup Guide

TAILOR can ingest your emails to build a searchable knowledge base of your communications.
Two methods are available: **IMAP** (recommended, works with any provider) and **Gmail API** (advanced).

---

## Option 1: IMAP (Recommended)

Works with Gmail, Outlook, Yahoo, iCloud, Fastmail, and any IMAP-compatible provider.
No Google Cloud project needed.

### Gmail via IMAP

Gmail requires an "App Password" instead of your regular password.

1. Go to [Google Account Security](https://myaccount.google.com/security)
2. Ensure **2-Step Verification** is enabled (required for app passwords)
3. Go to [App Passwords](https://myaccount.google.com/apppasswords)
4. Select **Mail** and your device, then click **Generate**
5. Copy the 16-character password

In `config/tailor.yaml`:

```yaml
email:
  provider: imap
  addresses:
    - yourname@gmail.com
  imap:
    host: imap.gmail.com
    port: 993
    username: yourname@gmail.com
    password: "abcd efgh ijkl mnop"   # the 16-char app password
```

### Outlook / Hotmail via IMAP

1. Go to [Microsoft Account Security](https://account.microsoft.com/security)
2. Under **App passwords**, generate one (requires 2FA enabled)

```yaml
email:
  provider: imap
  addresses:
    - yourname@outlook.com
  imap:
    host: outlook.office365.com
    port: 993
    username: yourname@outlook.com
    password: "your-app-password"
```

### Other IMAP providers

| Provider | Host | Port |
|---|---|---|
| iCloud | imap.mail.me.com | 993 |
| Yahoo | imap.mail.yahoo.com | 993 |
| Fastmail | imap.fastmail.com | 993 |
| ProtonMail | 127.0.0.1 (via Bridge) | 1143 |

```yaml
email:
  provider: imap
  addresses:
    - you@provider.com
  imap:
    host: imap.provider.com
    port: 993
    username: you@provider.com
    password: "your-password-or-app-password"
```

### Test your IMAP connection

```bash
.venv/bin/python3 scripts/gmail/export_imap.py --stats
```

---

## Option 2: Gmail API (Advanced)

The Gmail API offers more precise filtering (category exclusion, label access) but requires
a Google Cloud project with OAuth2 credentials. Use this if IMAP doesn't meet your needs.

### Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click **Select a project** → **New Project**
3. Name it (e.g. "TAILOR") and click **Create**

### Step 2: Enable the Gmail API

1. In your new project, go to **APIs & Services** → **Library**
2. Search for "Gmail API"
3. Click **Enable**

### Step 3: Create OAuth Credentials

1. Go to **APIs & Services** → **Credentials**
2. Click **Create Credentials** → **OAuth client ID**
3. If prompted, configure the **OAuth consent screen**:
   - User type: **External**
   - App name: "TAILOR"
   - Scopes: add `https://www.googleapis.com/auth/gmail.readonly`
   - Test users: add your Gmail address
4. Back in Credentials → **Create OAuth client ID**:
   - Application type: **Desktop app**
   - Name: "TAILOR"
5. Click **Download JSON** → save as `credentials/gmail_credentials.json`

### Step 4: Configure TAILOR

```yaml
email:
  provider: gmail
  addresses:
    - yourname@gmail.com
  gmail:
    credentials: ./credentials/gmail_credentials.json
    token: ./credentials/gmail_token.json
```

### Step 5: Authorize

```bash
.venv/bin/python3 scripts/gmail/export_gmail.py --stats
```

This opens a browser window for Google OAuth. Sign in, grant read-only access,
and the token is saved automatically. Subsequent runs don't require re-authorization.

> **Note**: While your app is in "Testing" mode, the token expires after 7 days.
> To avoid this, publish the app (Google review takes a few days) or simply
> re-run the export when the token expires.

---

## Email Pipeline

Once configured, TAILOR processes emails in 4 stages:

1. **Export** — downloads new emails (incremental)
2. **Triage** — LLM classifies which emails are worth keeping
3. **Chunk** — strips signatures/quotes, splits into chunks
4. **Ingest** — embeds and stores in the knowledge base

The nightly pipeline (`sync_email.sh`) runs all 4 stages automatically.
To run manually:

```bash
./sync_email.sh
```

### Customization

In `config/tailor.yaml`, you can extend the built-in filters:

```yaml
email:
  sender_blacklist_extra:      # skip these senders (merged with defaults)
    - "noreply@mybank.com"
  domain_blacklist_extra:      # skip these domains
    - "marketing.example.com"
  signature_patterns_extra:    # strip these from email bodies
    - "123 Main Street"
    - "My Company Ltd"
```

The built-in blacklists already cover common noise (social media notifications,
newsletters, automated alerts). Your extras are merged on top.
