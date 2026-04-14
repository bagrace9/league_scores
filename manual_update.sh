#!/usr/bin/env bash
set -euo pipefail

# Manual update script for the LXC deployment.
#
# Usage:
#   ./manual_update.sh
#   ./manual_update.sh --service league-scores.service
#   ./manual_update.sh --no-restart
#   ./manual_update.sh --branch main

BRANCH="main"
RESTART_SERVICE=true
SERVICE_NAME="league-scores.service"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --branch)
            BRANCH="$2"
            shift 2
            ;;
        --service)
            SERVICE_NAME="$2"
            shift 2
            ;;
        --no-restart)
            RESTART_SERVICE=false
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--branch <name>] [--service <name>] [--no-restart]"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run '$0 --help' for usage."
            exit 1
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$SCRIPT_DIR"

if [[ ! -d "$APP_DIR/.git" ]]; then
    echo "Error: $APP_DIR is not a git repository."
    exit 1
fi

if [[ ! -x "$APP_DIR/.venv/bin/python" ]]; then
    echo "Error: missing Python virtual environment at $APP_DIR/.venv"
    echo "Create it first with: python3 -m venv .venv"
    exit 1
fi

PYTHON="$APP_DIR/.venv/bin/python"
PIP="$APP_DIR/.venv/bin/pip"

cd "$APP_DIR"

echo "[1/5] Fetching latest code from origin..."
git fetch origin "$BRANCH"

echo "[2/5] Checking out branch '$BRANCH'..."
git checkout "$BRANCH"

echo "[3/5] Fast-forwarding local branch..."
git pull --ff-only origin "$BRANCH"

echo "[4/5] Installing/updating dependencies..."
"$PIP" install -r requirements.txt

echo "[5/5] Running syntax validation..."
PY_FILES=( $(find "$APP_DIR" -maxdepth 1 -type f -name "*.py" -printf "%f\n") )
if [[ ${#PY_FILES[@]} -gt 0 ]]; then
    "$PYTHON" -m py_compile "${PY_FILES[@]}"
fi

echo "Update complete."

if [[ "$RESTART_SERVICE" == true ]]; then
    if command -v systemctl >/dev/null 2>&1; then
        echo "Restarting service: $SERVICE_NAME"
        sudo systemctl restart "$SERVICE_NAME"
        sudo systemctl is-active "$SERVICE_NAME"
    else
        echo "systemctl not found; skipping service restart."
    fi
else
    echo "Skipping service restart (--no-restart)."
fi
