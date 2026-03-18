#!/usr/bin/env bash
# test_live.sh — jack live integration tests against a real Redshift cluster
# 需要設定好環境變數（REDSHIFT_HOST / DATABASE / USER / PASSWORD）
# 不測試 Bedrock，不執行 terminate。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$SCRIPT_DIR/.venv"

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC}  $1"; }
fail() { echo -e "${RED}FAIL${NC}  $1"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "\n${BOLD}${YELLOW}---- $1 ----${NC}"; }

FAILURES=0
LOG_FILE="$SCRIPT_DIR/test_live_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "Log: $LOG_FILE"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
if [[ -f "$VENV/bin/activate" ]]; then
    source "$VENV/bin/activate"
fi

if ! command -v jack &>/dev/null; then
    echo -e "${RED}ERROR${NC}: jack not found. Run onboarding.sh first."
    exit 1
fi

# Verify env vars are set
for var in REDSHIFT_HOST REDSHIFT_DATABASE REDSHIFT_USER REDSHIFT_PASSWORD; do
    if [[ -z "${!var:-}" ]]; then
        echo -e "${RED}ERROR${NC}: $var is not set. Source your .env first."
        exit 1
    fi
done

echo -e "${BOLD}Target: ${REDSHIFT_USER}@${REDSHIFT_HOST}/${REDSHIFT_DATABASE}${NC}"

# ---------------------------------------------------------------------------
# Helper: run command, check exit 0, optionally check output contains string
# ---------------------------------------------------------------------------
run_ok() {
    local label="$1"; shift
    local out
    if out=$("$@" 2>&1); then
        pass "$label"
    else
        fail "$label"
        echo "       Output: $(echo "$out" | head -3)"
    fi
}

run_json_ok() {
    local label="$1"; shift
    local out
    if out=$("$@" 2>&1); then
        if echo "$out" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
            pass "$label (valid JSON)"
        else
            fail "$label (invalid JSON output)"
            echo "       Output: $(echo "$out" | head -3)"
        fi
    else
        fail "$label (non-zero exit)"
        echo "       Output: $(echo "$out" | head -3)"
    fi
}

# ---------------------------------------------------------------------------
# 1. check group
# ---------------------------------------------------------------------------
info "1. check — default params"

run_ok   "check disk"             jack check disk
run_ok   "check connections"      jack check connections
run_ok   "check long-queries"     jack check long-queries
run_ok   "check wlm"              jack check wlm
run_ok   "check etl-failures"     jack check etl-failures
run_ok   "check copy-status"      jack check copy-status
run_ok   "check table-health"     jack check table-health
run_ok   "check vacuum-progress"  jack check vacuum-progress
run_ok   "check skew"             jack check skew
run_ok   "check compression"      jack check compression
run_ok   "check datashares"       jack check datashares

# ---------------------------------------------------------------------------
# 2. check — custom params
# ---------------------------------------------------------------------------
info "2. check — custom params"

run_ok   "check disk --limit 5"                          jack check disk --limit 5
run_ok   "check long-queries --threshold 1"              jack check long-queries --threshold 1
run_ok   "check long-queries --threshold 60 --limit 5"  jack check long-queries --threshold 60 --limit 5
run_ok   "check etl-failures --hours 1"                  jack check etl-failures --hours 1
run_ok   "check etl-failures --hours 72 --limit 10"      jack check etl-failures --hours 72 --limit 10
run_ok   "check table-health --stats 20"                 jack check table-health --stats 20
run_ok   "check table-health --unsorted 5 --limit 10"   jack check table-health --unsorted 5 --limit 10
run_ok   "check skew --threshold 2"                      jack check skew --threshold 2
run_ok   "check skew --threshold 10 --limit 5"           jack check skew --threshold 10 --limit 5
run_ok   "check compression --min-size 50"               jack check compression --min-size 50
run_ok   "check compression --min-size 500 --limit 10"   jack check compression --min-size 500 --limit 10

# ---------------------------------------------------------------------------
# 3. incident group
# ---------------------------------------------------------------------------
info "3. incident — default params"

run_ok   "incident locks"    jack incident locks
run_ok   "incident spill"    jack incident spill
run_ok   "incident alerts"   jack incident alerts
run_ok   "incident scaling"  jack incident scaling

info "4. incident — custom params"

run_ok   "incident spill --hours 1"              jack incident spill --hours 1
run_ok   "incident spill --hours 48 --limit 5"  jack incident spill --hours 48 --limit 5
run_ok   "incident alerts --hours 6"             jack incident alerts --hours 6
run_ok   "incident alerts --hours 72 --limit 20" jack incident alerts --hours 72 --limit 20
run_ok   "incident scaling --limit 10"           jack incident scaling --limit 10

# ---------------------------------------------------------------------------
# 5. maintain group
# ---------------------------------------------------------------------------
info "5. maintain — default params"

run_ok   "maintain stale-tables"  jack maintain stale-tables
run_ok   "maintain audit"         jack maintain audit

info "6. maintain — custom params"

run_ok   "maintain audit --hours 1"               jack maintain audit --hours 1
run_ok   "maintain audit --hours 168 --limit 50"  jack maintain audit --hours 168 --limit 50

# ---------------------------------------------------------------------------
# 7. composite commands
# ---------------------------------------------------------------------------
info "7. composite commands"

run_ok   "morning"    jack morning
run_ok   "incident"   jack incident
run_ok   "maintain"   jack maintain

# ---------------------------------------------------------------------------
# 8. JSON output — validate parseable
# ---------------------------------------------------------------------------
info "8. JSON output validation"

run_json_ok   "check disk --json"            jack check disk --json
run_json_ok   "check connections --json"     jack check connections --json
run_json_ok   "check long-queries --json"    jack check long-queries --json
run_json_ok   "check wlm --json"             jack check wlm --json
run_json_ok   "check etl-failures --json"    jack check etl-failures --json
run_json_ok   "check table-health --json"    jack check table-health --json
run_json_ok   "incident locks --json"        jack incident locks --json
run_json_ok   "incident spill --json"        jack incident spill --json
run_json_ok   "incident alerts --json"       jack incident alerts --json
run_json_ok   "maintain stale-tables --json" jack maintain stale-tables --json
run_json_ok   "maintain audit --json"        jack maintain audit --json
run_json_ok   "morning --json"               jack morning --json

# ---------------------------------------------------------------------------
# 9. check deps (requires table argument)
# ---------------------------------------------------------------------------
info "9. check deps"

# Use a table name that almost certainly exists (information_schema.tables)
run_ok   "check deps pg_user"   jack check deps pg_user

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
if [[ $FAILURES -eq 0 ]]; then
    echo -e "${GREEN}${BOLD}All live tests passed.${NC}"
else
    echo -e "${RED}${BOLD}$FAILURES test(s) failed.${NC}"
    exit 1
fi
