#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_DIR="$ROOT_DIR/scripts/systemd"
UNIT_DIR="${SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
BIN_DIR="${SYSTEMD_BIN_DIR:-/usr/local/bin}"
NOTIFY_CONFIG_DIR="${NOTIFY_EMAIL_CONFIG_DIR:-/etc/notify-email}"
NOTIFY_ASSET_DIR="${NOTIFY_EMAIL_ASSET_DIR:-$(cd "$ROOT_DIR/.." && pwd)}"
NOTIFY_SOURCE="${NOTIFY_EMAIL_SOURCE:-$NOTIFY_ASSET_DIR/notify-email.py}"
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
install -d "$BIN_DIR"
install -d "$NOTIFY_CONFIG_DIR/templates"
sed \
    -e "s|@PROJECT_ROOT@|$ESCAPED_ROOT|g" \
    -e "s|@RUN_USER@|$RUN_USER|g" \
    "$TEMPLATE_DIR/ibkr-proxy.service" \
    > "$UNIT_DIR/ibkr-proxy.service"

sed \
    -e "s|@NOTIFY_EMAIL_BIN@|$BIN_DIR/notify-email.py|g" \
    "$NOTIFY_ASSET_DIR/notify-email@.service" \
    > "$UNIT_DIR/notify-email@.service"

install -m 0644 \
    "$TEMPLATE_DIR/ibkr-proxy-start.timer" \
    "$TEMPLATE_DIR/ibkr-proxy-stop.service" \
    "$TEMPLATE_DIR/ibkr-proxy-stop.timer" \
    "$UNIT_DIR/"

install -m 0755 "$NOTIFY_SOURCE" "$BIN_DIR/notify-email.py"

if [[ ! -f "$NOTIFY_CONFIG_DIR/default.env" ]]; then
    install -m 0644 "$NOTIFY_ASSET_DIR/notify-email.default.env" "$NOTIFY_CONFIG_DIR/default.env"
fi

if [[ ! -f "$NOTIFY_CONFIG_DIR/templates/default.txt" ]]; then
    install -m 0644 "$NOTIFY_ASSET_DIR/notify-email.default.txt" "$NOTIFY_CONFIG_DIR/templates/default.txt"
fi

if [[ "${SKIP_SYSTEMD_RELOAD:-false}" != "true" ]]; then
    systemctl daemon-reload
fi

echo "Installed IBKR proxy systemd units for user: $RUN_USER"
echo "Installed units in: $UNIT_DIR"
echo "Installed notify-email helper: $BIN_DIR/notify-email.py"
echo "Notification config: $NOTIFY_CONFIG_DIR/default.env"
echo "Enable scheduled start/stop with:"
echo "  systemctl enable --now ibkr-proxy-start.timer ibkr-proxy-stop.timer"
echo
echo "Inspect timers with:"
echo "  systemctl list-timers ibkr-proxy-*.timer"
