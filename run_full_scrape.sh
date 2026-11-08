#!/bin/zsh
set -euo pipefail
cd "$(dirname "$0")"

TOTAL_PAGES="${TOTAL_PAGES:-856}"
START_PAGE="${START_PAGE:-1}"

mkdir -p output logs

export PYTHONUNBUFFERED=1
export FOLLOW_VARIANTS="${FOLLOW_VARIANTS:-0}"
# Keep this gentle — MedEx trips a security check under parallel load.
export SLEEP_BETWEEN_PRODUCTS="${SLEEP_BETWEEN_PRODUCTS:-1.0}"
export SLEEP_BETWEEN_PAGES="${SLEEP_BETWEEN_PAGES:-2.0}"
export RANDOM_JITTER_SECONDS="${RANDOM_JITTER_SECONDS:-0.4}"
export REQUEST_TIMEOUT_SECONDS="${REQUEST_TIMEOUT_SECONDS:-30}"
export MAX_RETRIES="${MAX_RETRIES:-8}"
export INITIAL_BACKOFF_SECONDS="${INITIAL_BACKOFF_SECONDS:-3.0}"
export SECURITY_CHALLENGE_WAIT_SECONDS="${SECURITY_CHALLENGE_WAIT_SECONDS:-240}"
export MEDEX_COOKIE_FILE="${MEDEX_COOKIE_FILE:-medex_cookies.json}"

echo "[$(date '+%F %T')] Starting full MedEx scrape: pages=${START_PAGE}..${TOTAL_PAGES} (single worker)"

python3 scrape_medex_full.py --start-page "$START_PAGE" --end-page "$TOTAL_PAGES" \
  2>&1 | tee "logs/full_scrape_${START_PAGE}_${TOTAL_PAGES}.log"

stamp=$(date '+%Y%m%d_%H%M%S')
src="output/medex_pos_mapped_pages_${START_PAGE}_to_${TOTAL_PAGES}.xlsx"
dst="output/medex_pos_mapped_FULL_${stamp}.xlsx"
if [[ -f "$src" ]]; then
  cp "$src" "$dst"
  echo "[$(date '+%F %T')] Copied final workbook to $dst"
fi

echo "[$(date '+%F %T')] ALL DONE"
ls -lah "$src" "$dst" 2>/dev/null || true
