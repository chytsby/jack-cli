#!/usr/bin/env bash
# onboarding.sh — jack 安裝與環境設定精靈
# 適用：新人上手、正式環境首次部署

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI_DIR="$SCRIPT_DIR"
VENV_DIR="$CLI_DIR/.venv"
ENV_FILE="$SCRIPT_DIR/.env"

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

step()  { echo -e "\n${BOLD}${YELLOW}▶ $1${NC}"; }
ok()    { echo -e "${GREEN}✓${NC} $1"; }
err()   { echo -e "${RED}✗ $1${NC}"; exit 1; }
ask()   { echo -e "${BOLD}$1${NC}"; }

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
echo -e "${BOLD}"
echo "╔══════════════════════════════════════╗"
echo "║   jack — Redshift Ops CLI Setup     ║"
echo "╚══════════════════════════════════════╝"
echo -e "${NC}"

# ---------------------------------------------------------------------------
# Step 1: Install system dependencies (Debian/Ubuntu)
# ---------------------------------------------------------------------------
step "1. 安裝系統套件"

if command -v apt-get &>/dev/null; then
    echo "  偵測到 apt，安裝 build-essential python3-dev virtualenv..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential python3-dev python3-venv virtualenv
    ok "系統套件安裝完成"
else
    echo "  非 Debian/Ubuntu 環境，跳過系統套件安裝（請自行確認已有 build-essential / python3-dev）"
fi

# ---------------------------------------------------------------------------
# Step 2: Check Python
# ---------------------------------------------------------------------------
step "2. 確認 Python 版本"

PYTHON=""
for py in python3 python; do
    if command -v "$py" &>/dev/null; then
        version=$("$py" -c 'import sys; print(sys.version_info[:2])')
        major=$("$py" -c 'import sys; print(sys.version_info.major)')
        minor=$("$py" -c 'import sys; print(sys.version_info.minor)')
        if [[ $major -eq 3 && $minor -ge 10 ]]; then
            PYTHON="$py"
            ok "找到 $py ($major.$minor)"
            break
        fi
    fi
done

[[ -z "$PYTHON" ]] && err "需要 Python 3.10+，請先安裝。"

# ---------------------------------------------------------------------------
# Step 2: Create venv
# ---------------------------------------------------------------------------
step "3. 建立虛擬環境"

if [[ -d "$VENV_DIR" ]]; then
    ok "虛擬環境已存在：$VENV_DIR"
else
    "$PYTHON" -m venv "$VENV_DIR"
    ok "建立虛擬環境：$VENV_DIR"
fi

# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"
ok "虛擬環境已啟用"

# ---------------------------------------------------------------------------
# Step 3: Install jack
# ---------------------------------------------------------------------------
step "4. 安裝 jack"

pip install --quiet --upgrade pip
pip install --quiet -e "$CLI_DIR"
ok "jack 安裝完成 ($(jack --version 2>/dev/null || echo 'v0.1.0'))"

# ---------------------------------------------------------------------------
# Step 4: Redshift 連線設定
# ---------------------------------------------------------------------------
step "5. Redshift 連線設定"

echo ""
echo "請輸入 Redshift 連線資訊（直接按 Enter 跳過，之後手動設定）："
echo ""

read_var() {
    local prompt="$1"
    local default="$2"
    local secret="${3:-false}"
    local value

    if [[ "$secret" == "true" ]]; then
        read -r -s -p "  $prompt: " value
        echo ""
    else
        read -r -p "  $prompt [$default]: " value
    fi

    echo "${value:-$default}"
}

REDSHIFT_HOST=$(read_var "REDSHIFT_HOST (cluster endpoint)" "")
REDSHIFT_PORT=$(read_var "REDSHIFT_PORT" "5439")
REDSHIFT_DATABASE=$(read_var "REDSHIFT_DATABASE" "dev")
REDSHIFT_USER=$(read_var "REDSHIFT_USER" "admin")
REDSHIFT_PASSWORD=$(read_var "REDSHIFT_PASSWORD" "" true)

# ---------------------------------------------------------------------------
# Step 5: AWS / Bedrock 設定
# ---------------------------------------------------------------------------
step "6. AWS / Bedrock 設定"

echo ""
echo "Bedrock 用於 jack explain（AI 報告），可跳過。"
echo ""

AWS_DEFAULT_REGION=$(read_var "AWS_DEFAULT_REGION" "ap-southeast-1")
AWS_ACCESS_KEY_ID=$(read_var "AWS_ACCESS_KEY_ID (留空使用 IAM role)" "")
AWS_SECRET_ACCESS_KEY=""
if [[ -n "$AWS_ACCESS_KEY_ID" ]]; then
    AWS_SECRET_ACCESS_KEY=$(read_var "AWS_SECRET_ACCESS_KEY" "" true)
fi

# ---------------------------------------------------------------------------
# Step 6: 寫入 .env
# ---------------------------------------------------------------------------
step "7. 儲存設定"

echo ""
ask "是否將設定寫入 $ENV_FILE？(y/N)"
read -r save_env

if [[ "$save_env" =~ ^[Yy]$ ]]; then
    {
        echo "# jack 環境設定"
        echo "# 產生時間：$(date)"
        echo ""
        echo "# Redshift"
        printf 'export REDSHIFT_HOST=%q\n'     "${REDSHIFT_HOST}"
        printf 'export REDSHIFT_PORT=%q\n'     "${REDSHIFT_PORT}"
        printf 'export REDSHIFT_DATABASE=%q\n' "${REDSHIFT_DATABASE}"
        printf 'export REDSHIFT_USER=%q\n'     "${REDSHIFT_USER}"
        printf 'export REDSHIFT_PASSWORD=%q\n' "${REDSHIFT_PASSWORD}"
        echo ""
        echo "# AWS Bedrock"
        printf 'export AWS_DEFAULT_REGION=%q\n' "${AWS_DEFAULT_REGION}"
    } > "$ENV_FILE"

    if [[ -n "$AWS_ACCESS_KEY_ID" ]]; then
        printf 'export AWS_ACCESS_KEY_ID=%q\n'     "${AWS_ACCESS_KEY_ID}"     >> "$ENV_FILE"
        printf 'export AWS_SECRET_ACCESS_KEY=%q\n' "${AWS_SECRET_ACCESS_KEY}" >> "$ENV_FILE"
    fi

    chmod 600 "$ENV_FILE"
    ok "設定已儲存至 $ENV_FILE（權限 600）"
    echo ""
    echo -e "${YELLOW}  重要：請確認 .env 已加入 .gitignore，不要 commit 到版本控制${NC}"
fi

# ---------------------------------------------------------------------------
# Step 7: 套用並驗證
# ---------------------------------------------------------------------------
step "8. 套用設定並驗證"

# Export for current shell
[[ -n "$REDSHIFT_HOST" ]]     && export REDSHIFT_HOST
[[ -n "$REDSHIFT_PORT" ]]     && export REDSHIFT_PORT
[[ -n "$REDSHIFT_DATABASE" ]] && export REDSHIFT_DATABASE
[[ -n "$REDSHIFT_USER" ]]     && export REDSHIFT_USER
[[ -n "$REDSHIFT_PASSWORD" ]] && export REDSHIFT_PASSWORD
[[ -n "$AWS_DEFAULT_REGION" ]] && export AWS_DEFAULT_REGION
[[ -n "$AWS_ACCESS_KEY_ID" ]] && export AWS_ACCESS_KEY_ID
[[ -n "$AWS_SECRET_ACCESS_KEY" ]] && export AWS_SECRET_ACCESS_KEY

echo ""
jack config

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo -e "${GREEN}${BOLD}設定完成！${NC}"
echo ""
echo -e "${YELLOW}${BOLD}下一步：每次新開 terminal 執行以下兩行${NC}"
echo ""
echo -e "  ${BOLD}source $ENV_FILE${NC}           # 載入環境變數"
echo -e "  ${BOLD}source $VENV_DIR/bin/activate${NC}   # 啟用虛擬環境"
echo ""
echo "  建議加入 ~/.bashrc 或 ~/.zshrc 自動套用："
echo ""
echo "  echo 'source $ENV_FILE' >> ~/.bashrc"
echo "  echo 'source $VENV_DIR/bin/activate' >> ~/.bashrc"
echo ""
echo "使用方式："
echo ""
echo "  # 常用指令"
echo "  jack morning                          # 早晨巡檢"
echo "  jack check disk                       # 磁碟用量"
echo "  jack incident                         # 事件診斷"
echo "  jack morning --json | jack explain   # AI 分析報告"
echo ""
echo "  # 完整指令清單"
echo "  jack --help"
echo "  jack check --help"
echo "  jack incident --help"
echo ""
