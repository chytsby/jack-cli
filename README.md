# jack — Redshift AI-Native Operations CLI

Redshift RA3 日常維運 CLI，將常用 DBA 查詢封裝為一條指令，並支援 `--json` 輸出供 AI Agent 解析。

---

## 安裝

**需求：** Python 3.10+

```bash
pip install -r requirements.txt
pip install -e .
```

設定環境變數後即可使用：

```bash
export REDSHIFT_HOST="..."
export REDSHIFT_PORT="5439"
export REDSHIFT_DATABASE="dwa"
export REDSHIFT_USER="datalake_op"
export REDSHIFT_PASSWORD="..."

export AWS_DEFAULT_REGION="ap-southeast-1"
export BEDROCK_MODEL="apac.amazon.nova-pro-v1:0"
```

參考 `.env.example` 查看完整範本。

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
jack config                                       # 顯示目前有效設定（密碼不顯示明文）
jack explain                                      # 讀 stdin JSON，透過 AWS Bedrock 產出分析報告
jack explain --format plain                       # 純文字輸出（預設 markdown）
```

### Composite（一次跑完）

```bash
jack daily                                        # WLM + ETL failures + MCD etl-status/log/value-check
jack daily --json | jack explain

jack weekly                                       # Long-queries + stale-tables + audit
jack weekly --json | jack explain

jack monthly                                      # Disk + table-health + skew
jack monthly --json | jack explain
```

### Daily

```bash
jack check wlm                                    # WLM 執行紀錄（預設 24h）
jack check wlm --hours 48 --limit 100

jack check etl-failures                           # COPY job load errors（預設 24h）
jack check etl-failures --hours 72 --limit 20

jack mcd etl-status                               # ETL 處理狀態（sys.etl_entity_prof）

jack mcd etl-log                                  # ETL 執行日誌（sys.etl_audit_log，預設 24h）
jack mcd etl-log --hours 48 --limit 50

jack mcd value-check                              # 資料新鮮度檢查（落後 benchmark 的表）
jack mcd value-check --all                        # 顯示所有表（不只 stale）
```

### Weekly

```bash
jack check long-queries                           # 近 14 天慢查詢（預設 >10 min，非 ETL 帳號）
jack check long-queries --threshold 30 --limit 10

jack maintain stale-tables                        # 廢棄備份表（_bck_、_tmp_、_old_ 等）

jack maintain audit                               # DDL 異動紀錄（預設 7 天）
jack maintain audit --hours 48
```

### Monthly

```bash
jack check disk                                   # 磁碟用量 + 大表排行（預設 top 30）
jack check disk --limit 10

jack check table-health                           # 需要 VACUUM / ANALYZE 的表（report only）
jack check table-health --stats 20 --unsorted 5

jack check skew                                   # 資料分佈 skew（預設 ratio >4）
jack check skew --threshold 2 --limit 10
```

### Incident

```bash
jack incident                                     # Locks + spill（composite）
jack incident --json | jack explain

jack incident locks                               # Lock 狀況與 block chain
jack incident spill                               # Spill to disk（預設 24h）
jack incident spill --hours 1
```

### Ad-hoc

```bash
jack check deps <table>                           # 哪些 SP / view 引用了這張表
jack check deps dm.mkt_acty

jack mcd etl-missing                              # ETL config 裡有但今天未成功執行的表
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
| `--all` / `-a` | 顯示全部（如 mcd value-check 顯示所有表） |

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
