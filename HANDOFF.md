# jack-cli — Handoff Document

jack-cli 是一個 Redshift 日常維運 CLI 工具，將常用 DBA 查詢封裝為結構化指令，並支援 `--json` 輸出供 AI Agent（AWS Bedrock）產出分析報告。目標是讓沒有 DB admin 權限的 ops 人員也能快速掌握 Redshift 的健康狀態，同時嚴格限制任何會影響 DB 效能的操作。

---

## 環境

- **Cluster**: Redshift RA3，2 nodes，ap-southeast-1
- **Database**: `dwa`
- **Ops 帳號**: `datalake_op`（唯讀，無 admin 權限）
- **AI 後端**: AWS Bedrock（Amazon Nova Pro，inference profile `apac.amazon.nova-pro-v1:0`）

---

## 架構

```
jack-cli/
├── jackcli/
│   ├── main.py        # CLI entry-point，所有指令定義（typer）
│   ├── queries.py     # SQL 查詢 + Bedrock explain prompts
│   ├── connection.py  # psycopg2 連線層，retry logic
│   ├── config.py      # env var 讀取（RedshiftConfig）
│   ├── output.py      # Rich table / JSON 輸出
│   └── bedrock.py     # AWS Bedrock API call
├── .env.example       # 環境變數範本
├── requirements.txt   # pip 依賴
├── pyproject.toml     # 套件設定（version, entry-point）
├── postcheck.sh       # 部署後驗證腳本
└── HANDOFF.md         # 本文件
```

---

## 權限限制

`datalake_op` 有以下 system view 的讀取權限：`svv_table_info`, `stl_wlm_query`, `stl_wlm_rule_action`, `stl_querytext`, `stl_load_errors`, `stl_error`, `pg_locks`, `pg_class`, `stv_sessions`, `stv_recents`, `stl_query`, `stl_query_metrics`, `stl_ddltext`, `pg_proc`, `information_schema.views`, `sys.etl_entity_prof`, `sys.etl_audit_log`

**嚴格禁止**：
- 任何 VACUUM / ANALYZE 執行（可在 explain 報告中建議，但不可執行）
- 任何 DML / DDL（DROP / ALTER / INSERT / UPDATE / DELETE）
- `pg_terminate_backend`（需 admin 帳號）
- 讓使用者傳入 SQL 直接執行

---

## 指令對照

| 頻率 | 指令 | 資料來源 |
|------|------|----------|
| Composite | `jack daily` | wlm + etl-failures + mcd etl-status/log/value-check |
| Composite | `jack weekly` | long-queries + stale-tables + audit |
| Composite | `jack monthly` | disk + table-health + skew |
| Daily | `jack check wlm` | `stl_wlm_query` CTE（近 24h，排除 datalake_op/etl） |
| Daily | `jack check etl-failures` | `stl_load_errors`（COPY job errors） |
| Daily | `jack mcd etl-status` | `sys.etl_entity_prof`（status_id: 0=ok, 1=running, 9=failed） |
| Daily | `jack mcd etl-log` | `sys.etl_audit_log` |
| Daily | `jack mcd value-check` | `sys.vw_tbl_stat_max_column_value`（max_column_value_todate vs benchmark） |
| Weekly | `jack check long-queries` | `sys.vw_tbl_query_log`（14天, SELECT, 非 datalake_etl, >10min） |
| Weekly | `jack maintain stale-tables` | `information_schema.tables`（_bck_, _tmp_, _old_ 等） |
| Weekly | `jack maintain audit` | `stl_ddltext`（預設 7 天） |
| Monthly | `jack check disk` | `svv_table_info` SUM(size) |
| Monthly | `jack check table-health` | `svv_table_info`（report only，不執行 VACUUM/ANALYZE） |
| Monthly | `jack check skew` | `svv_table_info` |
| Incident | `jack incident locks` | `pg_locks` + `stv_sessions` |
| Incident | `jack incident spill` | `stl_query` + `stl_query_metrics` |
| Ad-hoc | `jack check deps <table>` | `pg_proc` + `information_schema.views` |
| Ad-hoc | `jack mcd etl-missing` | `etl_table_config` × `etl_entity_prof`（config 有但今天未成功執行） |

---

## MCD 自建監控表（sys schema）

| Table/View | 用途 |
|------------|------|
| `sys.etl_entity_prof` | ETL 各表最新執行狀態（4 欄：schema_name, table_name, last_exec_dtm, status_id） |
| `sys.etl_audit_log` | ETL 執行歷史（含 exec_start/end_dtm, status_id, msg） |
| `sys.vw_tbl_stat_max_column_value` | 各表 sort key 的 max 值與預期 benchmark，每天 08:30 更新 |
| `sys.etl_table_config` | ETL pipeline mapping（source → stg → ods），供 etl-missing 使用 |

---

## AI 報告

任何指令加 `--json` pipe 給 `jack explain` 即可透過 Bedrock 產出 Markdown 報告，並自動儲存為 `jack_report_{command}_{timestamp}.md`。

每個 query function 在 `queries.py` 中都有對應的 `*_EXPLAIN_PROMPT`，Bedrock 使用這個 prompt 而非通用模板。

```bash
jack daily --json | jack explain
jack monthly --json | jack explain
```

---

## 部署

```bash
pip install -r requirements.txt
pip install -e .
source .env
bash postcheck.sh   # 驗證靜態掃描 + syntax + live connection
```

---

## 已知限制

- `datalake_op` 無法建立 table，monitoring table 需由有 admin 權限的帳號建立
- VACUUM / ANALYZE 只能在報告中建議，不可由 jack 執行
- `vw_tbl_query_log` 欄位無法透過 `information_schema.columns` 查詢（view 特性），欄位定義見 `queries.py`
- Bedrock inference profile 需在 ap-southeast-1 啟用
