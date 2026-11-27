#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Install the KPI status update as a daily cron job (run once, as the user that owns the repository).
#
#   bash status_update/install_cron.sh --daily-ist 02:00     once a day at 02:00 IST (converted to server time)
#   NO_PUSH=1 bash status_update/install_cron.sh ...         the job runs with --no-push (S3 output only)
#   bash status_update/install_cron.sh --remove              take the job out again
#
# The job:  cd <repo> && flock -w 14400 /tmp/wfm_trigger.lock .venv/bin/python -m status_update
#           >> logs/status_update_<date>.log
# flock on the WFM trigger's lock: when the 00:30 trigger is still running, the status update waits for it (up to
# 4 h) instead of running at the same time - the trigger's gold / push_gold and this job write the same tables.
# Pick a time after the daily DB export (DIC_Staging/year=/month=/day=) of the day.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-$REPO/.venv/bin/python}"
TAG="# kpi-status-update"

if [[ "${1:-}" == "--remove" ]]; then
  { crontab -l 2>/dev/null || true; } | { grep -v "$TAG" || true; } | crontab -
  echo "KPI status update removed from crontab"
  exit 0
fi

[[ "${1:-}" == "--daily-ist" ]] || { echo "usage: $0 --daily-ist HH:MM | --remove"; exit 2; }
IST="${2:-02:00}"
[[ "$IST" =~ ^([01]?[0-9]|2[0-3]):([0-5][0-9])$ ]] || { echo "ERROR: bad time '$IST' (use HH:MM, e.g. 02:00)"; exit 2; }
# IST is always UTC+05:30 -> server time = IST - 330 min + the server's own UTC offset
OFF=$(date +%z); SIGN=${OFF:0:1}; OFFMIN=$(( 10#${OFF:1:2} * 60 + 10#${OFF:3:2} )); [[ $SIGN == "-" ]] && OFFMIN=$(( -OFFMIN ))
T=$(( ( 10#${BASH_REMATCH[1]} * 60 + 10#${BASH_REMATCH[2]} - 330 + OFFMIN + 2880 ) % 1440 ))
SCHEDULE="$(( T % 60 )) $(( T / 60 )) * * *"
WHEN="daily at $IST IST = $(printf '%02d:%02d' $(( T / 60 )) $(( T % 60 ))) server time (UTC$OFF)"

[[ -x "$PYTHON" ]] || { echo "ERROR: $PYTHON not found - create the venv first"; exit 2; }
[[ -f "$REPO/.env" ]] || echo "WARNING: no $REPO/.env - cp .env.example .env and fill it in"
command -v crontab >/dev/null || { echo "ERROR: cron is not installed - run: apt install -y cron && systemctl enable --now cron"; exit 2; }

ARGS=""
[[ "${NO_PUSH:-}" == "1" ]] && ARGS="$ARGS --no-push"
mkdir -p "$REPO/logs"
LINE="$SCHEDULE cd '$REPO' && flock -w 14400 /tmp/wfm_trigger.lock '$PYTHON' -m status_update$ARGS >> '$REPO'/logs/status_update_\$(date +\\%Y\\%m\\%d).log 2>&1 $TAG"
{ { crontab -l 2>/dev/null || true; } | { grep -v "$TAG" || true; }; echo "$LINE"; } | crontab -
echo "installed ($WHEN):"
crontab -l | grep "$TAG" || { echo "ERROR: the job is not in the crontab"; exit 1; }
systemctl is-active --quiet cron 2>/dev/null || echo "WARNING: the cron service is not running - run: systemctl enable --now cron"
