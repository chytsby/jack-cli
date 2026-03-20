# jackcli — Command / Function Map

| CLI 指令 | 說明 | `queries.py` function |
|----------|------|-----------------------|
| `jack morning` | 一次跑完所有巡檢（composite） | `get_disk_usage` `get_connections` `get_long_running_queries` `get_wlm_status` `get_etl_failures` `get_copy_status` `get_table_health` `get_vacuum_progress` |
| `jack check disk` | 磁碟用量 + 大表排行 | `get_disk_usage` |
| `jack check connections` | 連線數與連線狀況 | `get_connections` |
| `jack check long-queries` | 長時間執行的 query | `get_long_running_queries` |
| `jack check wlm` | WLM queue 狀態 | `get_wlm_status` |
| `jack check etl-failures` | ETL load error / query error | `get_etl_failures` |
| `jack check copy-status` | 進行中的 COPY job | `get_copy_status` |
| `jack check table-health` | 需要 VACUUM / ANALYZE 的表 | `get_table_health` |
| `jack check vacuum-progress` | VACUUM 執行進度 | `get_vacuum_progress` |
| `jack check skew` | 資料分佈 skew | `get_skew` |
| `jack check compression` | 欄位 encoding 建議 | `get_compression` |
| `jack check deps <table>` | 哪些 SP / view 引用了這張表 | `get_deps` |
| `jack check datashares` | DataShare 定義清單 | `get_datashare_status` |
| `jack incident` | 一次跑完所有事件診斷（composite） | `get_locks` `get_spill` `get_alerts` `get_long_running_queries` `get_scaling` |
| `jack incident locks` | Lock 狀況與 block chain | `get_locks` |
| `jack incident terminate <pid>` | Kill 指定 query | — (`pg_terminate_backend`) |
| `jack incident spill` | Spill to disk 的 query | `get_spill` |
| `jack incident alerts` | Optimizer alert 紀錄 | `get_alerts` |
| `jack incident scaling` | Concurrency scaling 使用紀錄 | `get_scaling` |
| `jack maintain` | 一次跑完所有維護檢查（composite） | `get_table_health` `get_vacuum_progress` `get_stale_tables` |
| `jack maintain stale-tables` | 找廢棄備份表 | `get_stale_tables` |
| `jack maintain audit` | 近期 DDL 與權限異動紀錄 | `get_audit` |
| `jack mcd etl-status` | ETL 處理狀態（自建表） | `get_mcd_etl_status` |
| `jack mcd etl-log` | ETL 執行日誌（自建表） | `get_mcd_etl_log` |
| `jack config` | 顯示目前有效設定 | — (讀環境變數) |
| `jack explain` | 讀 stdin JSON，透過 Bedrock 產出報告 | — (`call_bedrock`) |
