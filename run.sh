#!/usr/bin/env bash
set -euo pipefail

# === editable ===
PROJECT_DIR="$HOME/lead-enricher"
VENV_DIR="$PROJECT_DIR/venv"
LOG_DIR="$PROJECT_DIR"
LOCK_FILE="$PROJECT_DIR/.enrich.lock"

export GOOGLE_APPLICATION_CREDENTIALS="$HOME/sheet-bot-key.json"
export SHEET_ID="1pwBp7c2ou5007RgMRc_wxQO9J9k9AnTat0_SGunTDdA"
export TAB_NAME="Anthony's Directory"   # or "Main"
# ================

# Simple lock to avoid overlapping runs
if [[ -e "$LOCK_FILE" ]]; then
  echo "$(date +"%F %T") — another run in progress; exiting." >> "$LOG_DIR/enrich.log"
  exit 0
fi
trap 'rm -f "$LOCK_FILE"' EXIT
: > "$LOCK_FILE"

cd "$PROJECT_DIR"

# Activate venv
source "$VENV_DIR/bin/activate"

# Optional: rotate log if too big (10 MB)
if [[ -f "$LOG_DIR/enrich.log" && $(wc -c < "$LOG_DIR/enrich.log") -gt 10485760 ]]; then
  mv "$LOG_DIR/enrich.log" "$LOG_DIR/enrich.log.$(date +%Y%m%d%H%M%S)"
fi

# Run and append logs
echo "=== $(date +"%F %T") start ===" >> "$LOG_DIR/enrich.log"
python main.py >> "$LOG_DIR/enrich.log" 2>&1
echo "=== $(date +"%F %T") end ===" >> "$LOG_DIR/enrich.log"
