#!/usr/bin/env bash
# Installs a binary already scp'd to /tmp by `make deploy`. Runs ON the target.
set -euo pipefail

BIN=claude-usage-mqtt
DEST="$HOME/.local/bin/$BIN"
UNIT="$HOME/.config/systemd/user/$BIN.service"

test -f "/tmp/$BIN.new" || { echo "no /tmp/$BIN.new — did scp run?"; exit 1; }

mkdir -p "$(dirname "$DEST")" "$(dirname "$UNIT")" "$HOME/.config/$BIN"
install -m644 "/tmp/$BIN.service" "$UNIT"

# Stop before replacing: swapping the file under a running process leaves the
# old image mapped, so a restart is required for the new build to take effect
# anyway — doing it explicitly makes that obvious rather than mysterious.
systemctl --user stop "$BIN" 2>/dev/null || true
install -m755 "/tmp/$BIN.new" "$DEST"
rm -f "/tmp/$BIN.new" "/tmp/$BIN.service"

systemctl --user daemon-reload
if [ -f "$HOME/.config/$BIN/config.toml" ]; then
    systemctl --user enable --now "$BIN"
    sleep 1
    systemctl --user --no-pager status "$BIN" | head -12
else
    echo "Installed, but $HOME/.config/$BIN/config.toml is missing — not starting."
    echo "Write the config, then: systemctl --user enable --now $BIN"
fi
