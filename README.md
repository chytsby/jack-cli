# jack — Redshift AI-Native Operations CLI

Redshift RA3 日常維運 CLI，將常用 DBA 查詢封裝為一條指令，並支援 `--json` 輸出供 AI Agent 解析。

---

## 安裝

**系統需求（Debian/Ubuntu）：**

```bash
sudo apt update
sudo apt install -y build-essential python3-dev python3-venv virtualenv
```

**互動式安裝精靈（推薦）：**

```bash
bash onboarding.sh
```

onboarding.sh 會自動安裝系統套件、建立 venv、安裝 jack、引導輸入連線設定並儲存為 `.env`。

**完成後，每次新開 terminal：**

```bash
source /path/to/redshift_ops_cli/.env          # 載入環境變數
source /path/to/redshift_ops_cli/.venv/bin/activate  # 啟用虛擬環境
```

或加入 `~/.bashrc` 自動套用：

```bash
echo 'source /path/to/redshift_ops_cli/.env' >> ~/.bashrc
echo 'source /path/to/redshift_ops_cli/.venv/bin/activate' >> ~/.bashrc
```

**手動安裝：**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

**需求：** Python 3.10+

---

## 環境變數

```bash
# Redshift（必填）
export REDSHIFT_HOST=my-cluster.xxxx.us-east-1.redshift.amazonaws.com
export REDSHIFT_DATABASE=dev
export REDSHIFT_USER=admin
export REDSHIFT_PASSWORD=secret
export REDSHIFT_PORT=5439        # 選填，預設 5439

# AWS Bedrock（使用 jack explain 時需要）
export AWS_DEFAULT_REGION=ap-southeast-1           # 選填，預設 ap-southeast-1
export AWS_ACCESS_KEY_ID=...                       # 或使用 IAM role
export AWS_SECRET_ACCESS_KEY=...
export BEDROCK_MODEL=apac.amazon.nova-pro-v1:0        # 選填，預設 apac.amazon.nova-pro-v1:0（inference profile）
```

---

## 指令

### 系統

```bash
jack config                                       # 顯示目前有效設定（不顯示密碼明文）
jack explain                                      # 讀 stdin JSON，透過 AWS Bedrock 產出 Markdown 分析報告
jack explain --format plain                       # 純文字輸出（預設 markdown）
```

### 早晨例行巡檢

```bash
jack morning                                      # 一次跑完所有巡檢（composite）
jack morning --json | jack explain               # 早晨巡檢 + AI 摘要

jack check disk                                   # 磁碟用量 + 大表排行（預設 top 30）
jack check disk --limit 10                        # 只顯示 top 10

jack check connections                            # 連線數與連線狀況

jack check long-queries                           # 長時間執行的 query（預設 >5 分鐘）
jack check long-queries --threshold 1             # 超過 1 分鐘就列出
jack check long-queries --threshold 30 --limit 5 # 超過 30 分鐘，只顯示 top 5

jack check wlm                                    # WLM queue 狀態

jack check etl-failures                           # ETL load error / query error（預設 24h）
jack check etl-failures --hours 1                 # 只看最近 1 小時
jack check etl-failures --hours 72 --limit 20    # 最近 3 天，最多 20 筆

jack check copy-status                            # 進行中的 COPY job

jack check table-health                           # 需要 VACUUM / ANALYZE 的表（預設 staleness/unsorted >10%）
jack check table-health --stats 20               # stats staleness 超過 20% 才列出
jack check table-health --unsorted 5 --limit 10  # unsorted 超過 5%，最多 10 筆

jack check vacuum-progress                        # VACUUM 執行進度
```

### 出事了，立刻查

```bash
jack incident                                     # 一次跑完所有事件診斷（composite）
jack incident --json | jack explain              # 事件診斷 + AI 摘要

jack incident locks                               # Lock 狀況與 block chain
jack incident terminate <pid>                     # Kill 指定 query（有確認提示）
jack incident terminate <pid> --force             # 跳過確認直接 kill

jack incident spill                               # Spill to disk 的 query（預設 24h）
jack incident spill --hours 1                     # 只看最近 1 小時
jack incident spill --hours 48 --limit 10         # 最近 48 小時，最多 10 筆

jack incident alerts                              # Optimizer alert 紀錄（預設 24h）
jack incident alerts --hours 6                    # 只看最近 6 小時
jack incident alerts --hours 72 --limit 30        # 最近 3 天，最多 30 筆

jack incident scaling                             # Concurrency scaling 使用紀錄
jack incident scaling --limit 10                  # 只顯示最近 10 筆
```

### 找根本原因

```bash
jack check skew                                   # 資料分佈 skew（預設 ratio >4）
jack check skew --threshold 2                     # ratio >2 就列出
jack check skew --threshold 10 --limit 5          # 嚴重 skew，top 5

jack check compression                            # 欄位 encoding 建議（預設 >100MB 的表）
jack check compression --min-size 50              # 50MB 以上的表都檢查
jack check compression --min-size 1000 --limit 10 # 只看大表前 10 筆

jack check deps <table>                           # 哪些 SP / view 引用了這張表
jack check deps dm.mkt_acty                       # 指定 schema.table

jack check datashares                             # DataShare 定義清單
```

### 維護 & 治理

```bash
jack maintain                                     # 一次跑完所有維護檢查（composite）

jack maintain stale-tables                        # 找廢棄備份表（_bck_、_tmp_、_old_ 等）

jack maintain audit                               # 近期 DDL 與權限異動紀錄（預設 24h）
jack maintain audit --hours 1                     # 只看最近 1 小時
jack maintain audit --hours 168 --limit 50        # 最近一週，最多 50 筆
```

### MCD 自建監控

```bash
jack mcd etl-status                               # ETL 處理狀態（sys.etl_entity_prof）

jack mcd etl-log                                  # ETL 執行日誌（sys.etl_audit_log，預設 24h）
jack mcd etl-log --hours 1                        # 只看最近 1 小時
jack mcd etl-log --hours 48 --limit 50            # 最近 48 小時，最多 50 筆
```

---

## AI 報告

任何指令加 `--json` 後 pipe 給 `jack explain`，透過 AWS Bedrock 產出對應的 Markdown 分析報告。每個指令都有專屬的分析 prompt，不使用通用模板。

```bash
# 單一指令
jack check disk --json | jack explain
jack incident locks --json | jack explain
jack check deps dm.mkt_acty --json | jack explain

# Composite（自動帶入全套分析 prompt）
jack morning --json | jack explain
jack incident --json | jack explain
jack maintain --json | jack explain

# 切換純文字輸出
jack check disk --json | jack explain --format plain
```

不使用 AI 時，所有指令預設輸出 Rich 格式化表格，`jack explain` 完全選配。

---

## 常用選項

| 選項 | 說明 |
|------|------|
| `--json` / `-j` | 輸出 JSON（供 pipe 給 explain 或 AI agent） |
| `--limit` / `-l` | 最大回傳筆數 |
| `--hours` / `-h` | 查詢時間範圍（小時） |
| `--threshold` / `-t` | 數值門檻（如最小執行分鐘數） |
| `--force` / `-f` | 跳過 terminate 的確認提示 |

---

## 測試

```bash
# Smoke test（不需要真實 DB 或 Bedrock）
bash test_cli.sh

# Live integration test（需要真實 Redshift 連線）
source .env
bash test_live.sh
```

`test_cli.sh` 涵蓋：CLI 結構驗證、config 輸出、缺少環境變數的錯誤處理、explain stdin 處理、terminate 確認流程。

`test_live.sh` 涵蓋：所有指令實際連線執行、各參數組合（`--limit`、`--hours`、`--threshold`、`--min-size`）、JSON 輸出驗證（是否為合法 JSON）、composite 指令（`morning`、`incident`、`maintain`）。

---

## Redshift 使用者權限需求

```sql
GRANT SELECT ON stv_recents TO <user>;
GRANT SELECT ON stv_sessions TO <user>;
GRANT SELECT ON stv_wlm_query_state TO <user>;
GRANT SELECT ON stv_wlm_service_class_state TO <user>;
GRANT SELECT ON stv_wlm_service_class_config TO <user>;
GRANT SELECT ON stv_partitions TO <user>;
GRANT SELECT ON stv_load_state TO <user>;
GRANT SELECT ON stv_locks TO <user>;
GRANT SELECT ON svv_table_info TO <user>;
GRANT SELECT ON svv_datashares TO <user>;
GRANT SELECT ON svv_vacuum_progress TO <user>;
GRANT SELECT ON svv_concurrency_scaling_status TO <user>;
GRANT SELECT ON svcs_concurrency_scaling_usage TO <user>;
GRANT SELECT ON stl_load_errors TO <user>;
GRANT SELECT ON stl_query TO <user>;
GRANT SELECT ON stl_query_metrics TO <user>;
GRANT SELECT ON stl_error TO <user>;
GRANT SELECT ON stl_ddltext TO <user>;
GRANT SELECT ON stl_alert_event_log TO <user>;
```

生產環境建議使用 `aws redshift get-cluster-credentials` 產生 IAM 臨時 token，取代靜態密碼。

---

## 錯誤處理

| 情境 | 行為 |
|------|------|
| 環境變數缺失 | 啟動即停止，列出所有缺少的變數 |
| 連線逾時 / 拒絕 | 自動重試最多 3 次（指數退避 1–8 秒） |
| 認證失敗 | 不重試，exit 1 |
| 權限不足 | 顯示缺少哪些 system view 的 SELECT 權限 |
| Bedrock 無憑證 | 顯示設定步驟，exit 1 |
| explain 無 stdin | 顯示用法提示，exit 1 |
| explain 非法 JSON | 顯示錯誤，exit 1 |
| 空結果 | 顯示綠色提示，exit 0 |
