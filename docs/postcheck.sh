#!/usr/bin/env bash
# postcheck.sh — jack-cli v0.2.0 post-deployment verification
# Run on the server after install. Any failure exits with code 1.
#
# Usage:
#   source .env
#   bash postcheck.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PASS=0
FAIL=0

_ok()   { echo "[PASS] $1"; ((PASS++)) || true; }
_fail() { echo "[FAIL] $1"; ((FAIL++)) || true; }

# ---------------------------------------------------------------------------
# 1. Static scan — forbidden SQL keywords
# ---------------------------------------------------------------------------
echo ""
echo "=== 1. Static scan ==="

# Extract only SQL block content (lines between *_SQL = """ and closing """)
# and scan those for forbidden keywords
SQL_TMP=$(mktemp)
python3 - <<'PYEOF' > "$SQL_TMP"
import re, os, glob

sql_block = re.compile(r'[A-Z_]+_SQL\s*=\s*r?"""(.*?)"""', re.DOTALL)
for path in glob.glob("jackcli/*.py"):
    src = open(path).read()
    for m in sql_block.finditer(src):
        print(m.group(1))
PYEOF

FORBIDDEN=(VACUUM ANALYZE pg_terminate_backend)
for keyword in "${FORBIDDEN[@]}"; do
    matches=$(grep -i "\b${keyword}\b" "$SQL_TMP" || true)
    if [ -n "$matches" ]; then
        _fail "Forbidden keyword in SQL block: $keyword"
        echo "$matches"
    else
        _ok "No '$keyword' in SQL blocks"
    fi
done
rm -f "$SQL_TMP"

# ---------------------------------------------------------------------------
# 2. Python syntax check
# ---------------------------------------------------------------------------
echo ""
echo "=== 2. Syntax check ==="

for f in jackcli/*.py; do
    if python3 -m py_compile "$f" 2>&1; then
        _ok "py_compile: $f"
    else
        _fail "py_compile: $f"
    fi
done

# ---------------------------------------------------------------------------
# 3. Import check
# ---------------------------------------------------------------------------
echo ""
echo "=== 3. Import check ==="

if python3 -c "import jackcli" 2>&1; then
    _ok "import jackcli"
else
    _fail "import jackcli"
fi

# ---------------------------------------------------------------------------
# 4. CLI --help smoke tests (no DB connection needed)
# ---------------------------------------------------------------------------
echo ""
echo "=== 4. CLI --help smoke tests ==="

HELP_CMDS=(
    "jack --help"
    "jack check --help"
    "jack incident --help"
    "jack maintain --help"
    "jack mcd --help"
    "jack config"
)

for cmd in "${HELP_CMDS[@]}"; do
    if $cmd > /dev/null 2>&1; then
        _ok "$cmd"
    else
        _fail "$cmd"
    fi
done

# ---------------------------------------------------------------------------
# 5. Live connection tests (requires .env sourced)
# ---------------------------------------------------------------------------
echo ""
echo "=== 5. Live connection tests ==="

if [ -z "${REDSHIFT_HOST:-}" ]; then
    echo "[SKIP] REDSHIFT_HOST not set — skipping live tests"
else
    LIVE_CMDS=(
        "jack check wlm --limit 1"
        "jack mcd etl-status"
        "jack mcd value-check"
    )

    for cmd in "${LIVE_CMDS[@]}"; do
        if $cmd > /dev/null 2>&1; then
            _ok "$cmd"
        else
            _fail "$cmd"
        fi
    done
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=============================="
echo "  PASS: $PASS  |  FAIL: $FAIL"
echo "=============================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
