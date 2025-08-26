# red-tide-automation

Automation to fetch FWC HAB data, process it, update WordPress custom post types, and write results to Google Sheets.

## GitHub Actions (CI) setup
The workflow at `.github/workflows/red-tide-update.yml` runs every 6 hours and can be triggered manually.

### Required repository Secrets
Create these in GitHub: Settings → Secrets and variables → Actions → New repository secret.
- WORDPRESS_SITE_URL
- WORDPRESS_USERNAME
- WORDPRESS_APP_PASSWORD (plain text or base64-encoded; both supported)
- GOOGLE_SERVICE_ACCOUNT (full JSON as a single line)
- GOOGLE_SHEET_ID

### Triggering the workflow
- Schedule: runs at `0 */6 * * *`.
- Manual: Actions → Red Tide Data Update → Run workflow.

## Local development
1) Copy the example env and fill values:
```bash
cp .env.example .env
```

2) Install dependencies:
```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

3) Run the processor:
```bash
python src/update_red_tide.py
```

Notes:
- `config/settings.py` uses `python-decouple` to read env vars; `src/update_red_tide.py` reads via `os.environ`.
- `GOOGLE_SERVICE_ACCOUNT` must be the JSON blob on a single line.

## Security
- `.env` is ignored by git. Never commit secrets.
- Rotate any credential that was previously leaked and consider revoking old keys.