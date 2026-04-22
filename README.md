# MedEx Scraper for GitHub Actions

This repository runs a MedEx scraper on GitHub Actions and saves the result as an Excel file.

## Files

- `scrape_medex.py` - the scraper script
- `.github/workflows/scrape-manual.yml` - run a custom page range manually
- `.github/workflows/scrape-parallel.yml` - run predefined ranges in parallel

## How to run

### Option A - Manual custom range
1. Push this repo to GitHub.
2. Go to **Actions**.
3. Open **Scrape MedEx Manual**.
4. Click **Run workflow**.
5. Enter a start page and end page.
6. Wait for the run to finish.
7. Download the Excel file from **Artifacts**.

### Option B - Parallel preset ranges
1. Go to **Actions**.
2. Open **Scrape MedEx Parallel**.
3. Click **Run workflow**.
4. It will run these ranges in parallel:
   - 1-30
   - 31-60
   - 61-90
5. Download each Excel file from **Artifacts**.

## Notes

- Start with a small test range like `1-2` first.
- Keep delays in place to avoid hitting the site too aggressively.
- Output files are written to the `output/` folder during the workflow run and uploaded as artifacts.
