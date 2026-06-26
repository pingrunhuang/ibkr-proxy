#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_DIR="$ROOT_DIR/scripts/systemd"
UNIT_DIR="${SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
RUN_USER="${1:-${SUDO_USER:-${USER}}}"

if [[ ! -x "$ROOT_DIR/.venv/bin/python" ]]; then
    echo "Missing $ROOT_DIR/.venv/bin/python. Run 'uv sync' first."
    exit 1
fi

if [[ ! -f "$ROOT_DIR/.env" ]]; then
    echo "Missing $ROOT_DIR/.env. Copy .env.example to .env and configure it first."
    exit 1
fi

ESCAPED_ROOT="${ROOT_DIR//\\/\\\\}"
ESCAPED_ROOT="${ESCAPED_ROOT//\"/\\\"}"
install -d "$UNIT_DIR"
sed \
    -e "s|@PROJECT_ROOT@|$ESCAPED_ROOT|g" \
    -e "s|@RUN_USER@|$RUN_USER|g" \
    "$TEMPLATE_DIR/ibkr-proxy.service" \
    > "$UNIT_DIR/ibkr-proxy.service"
chmod 0644 "$UNIT_DIR/ibkr-proxy.service"

if [[ "${SKIP_SYSTEMD_RELOAD:-false}" != "true" ]]; then
    systemctl daemon-reload
fi

echo "Installed IBKR proxy systemd units for user: $RUN_USER"
echo "Installed units in: $UNIT_DIR"
echo
echo "Start manually with:"
echo "  systemctl start ibkr-proxy.service"
echo
echo "Inspect with:"
echo "  systemctl status ibkr-proxy.service"
