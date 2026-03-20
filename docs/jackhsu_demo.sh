#!/usr/bin/env bash
# jack_demo.sh — run all jack-cli commands and save results
#
# Usage:
#   source .env
#   bash jack_demo.sh

set -uo pipefail

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DEMO_DIR="demo_${TIMESTAMP}"
mkdir -p "$DEMO_DIR"

SUMMARY="$DEMO_DIR/00_demo_summary.md"

echo "# jack-cli Demo — ${TIMESTAMP}" > "$SUMMARY"
echo "" >> "$SUMMARY"
echo "Generated: $(date '+%Y-%m-%d %H:%M:%S')" >> "$SUMMARY"
echo "" >> "$SUMMARY"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

section() {
    echo "" >> "$SUMMARY"
    echo "---" >> "$SUMMARY"
    echo "" >> "$SUMMARY"
    echo "## $1" >> "$SUMMARY"
    echo ""
    echo "========================================"
    echo "  $1"
    echo "========================================"
}

run_cmd() {
    local label="$1"
    local cmd="$2"
    echo "" >> "$SUMMARY"
    echo "### \`$cmd\`" >> "$SUMMARY"
    echo '```' >> "$SUMMARY"
    eval "$cmd" >> "$SUMMARY" 2>&1 || true
    echo '```' >> "$SUMMARY"
    echo ""
    echo ">>> $label"
    eval "$cmd" 2>&1 || true
    echo ""
}

run_explain() {
    local label="$1"
    local cmd="$2"
    local outfile="${DEMO_DIR}/${label}.md"
    echo ""
    echo ">>> $label (saving to $outfile)"
    eval "$cmd" > "$outfile" 2>&1 || true
    echo "" >> "$SUMMARY"
    echo "### \`$cmd\`" >> "$SUMMARY"
    echo "_→ saved: \`${label}.md\`_" >> "$SUMMARY"
    echo ""
}

# ---------------------------------------------------------------------------
# 1. Config
# ---------------------------------------------------------------------------
section "Config"
run_cmd "config" "jack config"

# ---------------------------------------------------------------------------
# 2. Daily
# ---------------------------------------------------------------------------
section "Daily"
run_cmd "check wlm" "jack check wlm --limit 10"
run_cmd "check etl-failures" "jack check etl-failures"
run_cmd "mcd etl-status" "jack mcd etl-status"
run_cmd "mcd etl-log" "jack mcd etl-log --limit 20"
run_cmd "mcd value-check" "jack mcd value-check"
run_cmd "mcd value-check --all" "jack mcd value-check --all"

# ---------------------------------------------------------------------------
# 3. Weekly
# ---------------------------------------------------------------------------
section "Weekly"
run_cmd "check long-queries" "jack check long-queries"
run_cmd "maintain stale-tables" "jack maintain stale-tables"
run_cmd "maintain audit" "jack maintain audit"

# ---------------------------------------------------------------------------
# 4. Monthly
# ---------------------------------------------------------------------------
section "Monthly"
run_cmd "check disk" "jack check disk --limit 20"
run_cmd "check table-health" "jack check table-health"
run_cmd "check skew" "jack check skew"

# ---------------------------------------------------------------------------
# 5. Incident
# ---------------------------------------------------------------------------
section "Incident"
run_cmd "incident locks" "jack incident locks"
run_cmd "incident spill" "jack incident spill"

# ---------------------------------------------------------------------------
# 6. Ad-hoc
# ---------------------------------------------------------------------------
section "Ad-hoc"
run_cmd "mcd etl-missing" "jack mcd etl-missing"
# check deps requires a table name — edit below if needed
run_cmd "check deps" "jack check deps dwh.crm_mbr_prof"

# ---------------------------------------------------------------------------
# 7. Composite — with AI explain (auto-saved as jack_report_*.md)
# ---------------------------------------------------------------------------
section "Composite + AI Explain"

run_explain "daily_explain"   "jack daily   --json | jack explain"
run_explain "weekly_explain"  "jack weekly  --json | jack explain"
run_explain "monthly_explain" "jack monthly --json | jack explain"
run_explain "incident_explain" "jack incident --json | jack explain"
run_explain "maintain_explain" "jack maintain --json | jack explain"

# Move auto-saved jack_report_*.md into demo dir
mv jack_report_*.md "$DEMO_DIR/" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "========================================"
echo "  Demo complete"
echo "  Results saved to: $DEMO_DIR/"
echo "========================================"
echo ""
ls "$DEMO_DIR/"

echo "" >> "$SUMMARY"
echo "---" >> "$SUMMARY"
echo "" >> "$SUMMARY"
echo "## Files" >> "$SUMMARY"
ls "$DEMO_DIR/" | while read f; do echo "- \`$f\`" >> "$SUMMARY"; done
