#!/usr/bin/env bash
# test_cli.sh — jack CLI structure & error-handling smoke tests
# Does NOT require a real Redshift or Bedrock connection.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$SCRIPT_DIR/.venv"

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC}  $1"; }
fail() { echo -e "${RED}FAIL${NC}  $1"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}----${NC}  $1"; }

FAILURES=0

# ---------------------------------------------------------------------------
# Activate venv if available
# ---------------------------------------------------------------------------
if [[ -f "$VENV/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source "$VENV/bin/activate"
else
    info "No venv found at $VENV — using system Python"
fi

if ! command -v jack &>/dev/null; then
    echo -e "${RED}ERROR${NC}: jack not found. Run onboarding.sh first."
    exit 1
fi

# ---------------------------------------------------------------------------
# Helper: expect a command to succeed
# ---------------------------------------------------------------------------
expect_ok() {
    local label="$1"; shift
    if "$@" &>/dev/null; then
        pass "$label"
    else
        fail "$label (exit code $?)"
    fi
}

# Helper: expect a command to fail
expect_fail() {
    local label="$1"; shift
    if ! "$@" &>/dev/null; then
        pass "$label"
    else
        fail "$label (expected non-zero exit)"
    fi
}

# Helper: expect output to contain a string
expect_output() {
    local label="$1"
    local pattern="$2"
    shift 2
    local out
    out=$("$@" 2>&1 || true)
    if echo "$out" | grep -q "$pattern"; then
        pass "$label"
    else
        fail "$label (expected '$pattern' in output)"
        echo "       Got: $out" | head -5
    fi
}

# ---------------------------------------------------------------------------
# 1. CLI structure — all --help must succeed
# ---------------------------------------------------------------------------
info "1. CLI structure"

expect_ok   "jack --help"                         jack --help
expect_ok   "jack check --help"                   jack check --help
expect_ok   "jack incident --help"                jack incident --help
expect_ok   "jack maintain --help"                jack maintain --help
expect_ok   "jack mcd --help"                     jack mcd --help
expect_ok   "jack check disk --help"              jack check disk --help
expect_ok   "jack check connections --help"       jack check connections --help
expect_ok   "jack check long-queries --help"      jack check long-queries --help
expect_ok   "jack check wlm --help"               jack check wlm --help
expect_ok   "jack check etl-failures --help"      jack check etl-failures --help
expect_ok   "jack check copy-status --help"       jack check copy-status --help
expect_ok   "jack check table-health --help"      jack check table-health --help
expect_ok   "jack check vacuum-progress --help"   jack check vacuum-progress --help
expect_ok   "jack check skew --help"              jack check skew --help
expect_ok   "jack check compression --help"       jack check compression --help
expect_ok   "jack check deps --help"              jack check deps --help
expect_ok   "jack check datashares --help"        jack check datashares --help
expect_ok   "jack incident locks --help"          jack incident locks --help
expect_ok   "jack incident terminate --help"      jack incident terminate --help
expect_ok   "jack incident spill --help"          jack incident spill --help
expect_ok   "jack incident alerts --help"         jack incident alerts --help
expect_ok   "jack incident scaling --help"        jack incident scaling --help
expect_ok   "jack maintain stale-tables --help"   jack maintain stale-tables --help
expect_ok   "jack maintain audit --help"          jack maintain audit --help
expect_ok   "jack mcd etl-status --help"          jack mcd etl-status --help
expect_ok   "jack mcd etl-log --help"             jack mcd etl-log --help
expect_ok   "jack morning --help"                 jack morning --help
expect_ok   "jack explain --help"                 jack explain --help
expect_ok   "jack config --help"                  jack config --help

# ---------------------------------------------------------------------------
# 2. config — no DB needed
# ---------------------------------------------------------------------------
info "2. config command"

# With no env vars set, config should still run (just shows [not set])
unset REDSHIFT_HOST REDSHIFT_DATABASE REDSHIFT_USER REDSHIFT_PASSWORD \
      AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY 2>/dev/null || true

expect_ok      "jack config runs without env vars"           jack config
expect_output  "config shows [not set] for missing vars"  "not set"  jack config

# With env vars set
export REDSHIFT_HOST=test-cluster.us-east-1.redshift.amazonaws.com
export REDSHIFT_DATABASE=dev
export REDSHIFT_USER=admin
export REDSHIFT_PASSWORD=secret
export AWS_DEFAULT_REGION=ap-southeast-1

expect_output  "config shows REDSHIFT_HOST"       "test-cluster"    jack config
expect_output  "config masks REDSHIFT_PASSWORD"   "***"             jack config
expect_output  "config shows AWS_DEFAULT_REGION"  "ap-southeast-1"  jack config

unset REDSHIFT_HOST REDSHIFT_DATABASE REDSHIFT_USER REDSHIFT_PASSWORD

# ---------------------------------------------------------------------------
# 3. Missing env vars — should exit 1 with clear message
# ---------------------------------------------------------------------------
info "3. Missing env var error handling"

unset REDSHIFT_HOST REDSHIFT_DATABASE REDSHIFT_USER REDSHIFT_PASSWORD 2>/dev/null || true

expect_fail   "jack check disk exits 1 without env vars"  jack check disk
expect_output "error message mentions missing vars"  "REDSHIFT_HOST"  jack check disk

# ---------------------------------------------------------------------------
# 4. explain — stdin handling
# ---------------------------------------------------------------------------
info "4. explain stdin handling"

# No pipe (tty) — should exit 1
expect_fail "jack explain without pipe exits 1"  jack explain </dev/null

# Invalid JSON
expect_fail "jack explain with invalid JSON exits 1" \
    bash -c 'echo "not-json" | jack explain'

expect_output "jack explain invalid JSON shows error" "Invalid JSON" \
    bash -c 'echo "not-json" | jack explain 2>&1 || true'

# Valid JSON — will fail at Bedrock call (no creds), but JSON parsing should pass
# We check for Bedrock error, not JSON error
SAMPLE_JSON='{"command":"disk","data":{"cluster_summary":{"used_mb":1024,"total_mb":4096,"used_pct":25},"top_tables":[]}}'
expect_output "jack explain with valid JSON reaches Bedrock stage" \
    "Bedrock" \
    bash -c "echo '$SAMPLE_JSON' | jack explain 2>&1 || true"

# ---------------------------------------------------------------------------
# 5. terminate — cancel via stdin n
# ---------------------------------------------------------------------------
info "5. terminate confirmation"

export REDSHIFT_HOST=test-cluster.us-east-1.redshift.amazonaws.com
export REDSHIFT_DATABASE=dev
export REDSHIFT_USER=admin
export REDSHIFT_PASSWORD=secret

# Answering 'n' should abort without connecting
expect_output "jack incident terminate aborts on n" \
    "Aborted" \
    bash -c 'echo "n" | jack incident terminate 9999 2>&1 || true'

unset REDSHIFT_HOST REDSHIFT_DATABASE REDSHIFT_USER REDSHIFT_PASSWORD

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
if [[ $FAILURES -eq 0 ]]; then
    echo -e "${GREEN}All tests passed.${NC}"
else
    echo -e "${RED}$FAILURES test(s) failed.${NC}"
    exit 1
fi
