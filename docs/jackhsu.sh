#!/usr/bin/env bash
# jackhsu.sh — install jack-cli
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

pip install -r requirements.txt
pip install -e .

echo ""
JACK_PATH=$(which jack)
JACK_VERSION=$(pip show jackcli 2>/dev/null | grep ^Version | awk '{print $2}')

echo "Installed: ${JACK_PATH}"
echo "Version:   ${JACK_VERSION}"
echo ""
echo "Next: source .env && jack --help"
