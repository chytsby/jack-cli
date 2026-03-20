"""Task logic layer: every function returns a list[dict] or dict ready for JSON serialisation."""

from __future__ import annotations

from typing import Any

import psycopg2.extensions


def _fetchall_as_dicts(
    conn: psycopg2.extensions.connection, sql: str, params: dict | None = None
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# 1. Long-running queries (weekly — historical, via vw_tbl_query_log)
# ---------------------------------------------------------------------------

LONG_RUNNING_SQL = """
SELECT
    user_name,
    query_text,
    start_time_tw,
    end_time_tw,
    ttl_duration_min,
    returned_rows,
    returned_mb
FROM "dwa"."sys"."vw_tbl_query_log"
WHERE start_time_tw >= DATEADD(day, -14, CURRENT_DATE)
  AND query_type IN ('SELECT')
  AND user_name <> 'datalake_etl'
  AND ttl_duration_min > %(threshold_minutes)s
ORDER BY ttl_duration_min DESC
LIMIT %(limit)s;
"""

LONG_RUNNING_EXPLAIN_PROMPT = """
分析以下近兩週長時間執行的 Redshift query 清單（非 ETL 帳號，SELECT only），重點：
- 哪些 query 執行時間最長（依 ttl_duration_min）
- 同一 user 是否有重複的慢查詢模式
- returned_mb 過大的 query 是否有改寫空間
- 建議需要追蹤或優化的項目
"""


def get_long_running_queries(
    conn: psycopg2.extensions.connection, threshold_minutes: float = 10.0, limit: int = 20
) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, LONG_RUNNING_SQL, {
        "threshold_minutes": threshold_minutes,
        "limit": limit,
    })


# ---------------------------------------------------------------------------
# 2. Disk usage (monthly — via svv_table_info)
# ---------------------------------------------------------------------------

DISK_SUMMARY_SQL = """
SELECT
    ROUND(SUM(size) / 1024.0, 2)         AS used_gb,
    ROUND(SUM(size) / 1024.0 / 1024.0, 3) AS used_tb
FROM svv_table_info
WHERE schema NOT IN ('pg_catalog', 'information_schema');
"""

DISK_TOP_TABLES_SQL = """
SELECT
    schema                  AS schema_name,
    "table"                 AS table_name,
    size                    AS size_mb,
    tbl_rows                AS row_count,
    unsorted                AS unsorted_pct,
    stats_off               AS stats_staleness_pct
FROM svv_table_info
WHERE schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY size DESC
LIMIT %(limit)s;
"""

DISK_EXPLAIN_PROMPT = """
分析以下 Redshift 磁碟用量資料，重點：
- 整體使用量（GB/TB）是否接近 provisioned 上限
- 哪些表異常大，大小是否合理
- unsorted_pct 或 stats_staleness_pct 偏高的表（可建議但不執行 VACUUM/ANALYZE）
- 建議優先關注的項目
"""


def get_disk_usage(
    conn: psycopg2.extensions.connection, limit: int = 30
) -> dict[str, Any]:
    summary_rows = _fetchall_as_dicts(conn, DISK_SUMMARY_SQL)
    tables = _fetchall_as_dicts(conn, DISK_TOP_TABLES_SQL, {"limit": limit})
    return {
        "cluster_summary": summary_rows[0] if summary_rows else {},
        "top_tables": tables,
    }


# ---------------------------------------------------------------------------
# 3. ETL failure log (daily — stl_load_errors for COPY jobs)
# ---------------------------------------------------------------------------

ETL_LOAD_ERRORS_SQL = """
SELECT
    le.starttime,
    TRIM(le.filename)   AS source_file,
    le.colname,
    TRIM(le.err_reason) AS error_reason,
    TRIM(u.usename)     AS username
FROM stl_load_errors le
LEFT JOIN stl_query q ON q.query = le.query
LEFT JOIN pg_user u ON u.usesysid = q.userid
WHERE le.starttime > GETDATE() - INTERVAL '%(hours)s hours'
ORDER BY le.starttime DESC
LIMIT %(limit)s;
"""

ETL_FAILURES_EXPLAIN_PROMPT = """
分析以下 Redshift COPY job 錯誤紀錄，重點：
- 主要錯誤類型（型別不符、檔案格式、欄位長度等）
- 哪些來源檔案或 user 最常出錯
- 建議修復方向
"""


def get_etl_failures(
    conn: psycopg2.extensions.connection, hours: int = 24, limit: int = 50
) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, ETL_LOAD_ERRORS_SQL, {"hours": hours, "limit": limit})


# ---------------------------------------------------------------------------
# 4. Table health (monthly — vacuum/analyze candidates)
# ---------------------------------------------------------------------------

TABLE_HEALTH_SQL = """
SELECT
    schema                  AS schema_name,
    "table"                 AS table_name,
    size                    AS size_mb,
    stats_off               AS stats_staleness_pct,
    unsorted                AS unsorted_pct,
    tbl_rows                AS row_count
FROM svv_table_info
WHERE schema NOT IN ('pg_catalog', 'information_schema')
  AND (stats_off > %(stats_threshold)s OR unsorted > %(unsorted_threshold)s)
ORDER BY size DESC
LIMIT %(limit)s;
"""

TABLE_HEALTH_EXPLAIN_PROMPT = """
分析以下需要維護的 Redshift 表清單（注意：VACUUM/ANALYZE 需由有權限的帳號執行，不在本工具範圍內），重點：
- 依 unsorted_pct 和 size 排出優先 VACUUM 的表
- 依 stats_staleness_pct 排出優先 ANALYZE 的表
- 建議回報給有 admin 權限的人員處理的順序
"""


def get_table_health(
    conn: psycopg2.extensions.connection,
    stats_threshold: float = 10.0,
    unsorted_threshold: float = 10.0,
    limit: int = 30,
) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, TABLE_HEALTH_SQL, {
        "stats_threshold": stats_threshold,
        "unsorted_threshold": unsorted_threshold,
        "limit": limit,
    })


# ---------------------------------------------------------------------------
# 5. WLM status (daily — full CTE with rule actions and query text)
# ---------------------------------------------------------------------------

WLM_STATUS_SQL = """
WITH q AS (
    SELECT w.query,
           w.userid,
           w.service_class,
           w.service_class_name,
           w.queue_start_time,
           w.exec_start_time,
           w.exec_end_time,
           w.total_queue_time,
           w.total_exec_time
    FROM stl_wlm_query w
    WHERE w.exec_start_time >= DATEADD(hour, -%(hours)s, GETDATE())
),
r AS (
    SELECT a.query,
           a.rule,
           a.action,
           a.action_value,
           a.recordtime
    FROM stl_wlm_rule_action a
    WHERE a.recordtime >= DATEADD(hour, -%(hours)s, GETDATE())
),
t AS (
    SELECT qt.query,
           LISTAGG(qt.text, '') WITHIN GROUP (ORDER BY qt.sequence) AS full_query_text
    FROM stl_querytext qt
    GROUP BY qt.query
)
SELECT
    q.query                         AS query_id,
    u.usename                       AS user_name,
    q.service_class_name            AS queue_name,
    q.queue_start_time,
    q.exec_start_time,
    q.exec_end_time,
    ROUND(q.total_queue_time / 1000000.0, 2) AS queue_seconds,
    ROUND(q.total_exec_time  / 1000000.0, 2) AS exec_seconds,
    r.rule                          AS hit_qmr_rule,
    r.action                        AS qmr_action,
    r.action_value,
    LEFT(t.full_query_text, 500)    AS query_text
FROM q
JOIN pg_user u ON u.usesysid = q.userid AND u.usesysid >= 100
LEFT JOIN r ON r.query = q.query
LEFT JOIN t ON t.query = q.query
WHERE u.usename NOT IN ('datalake_op', 'datalake_etl')
ORDER BY q.exec_start_time DESC
LIMIT %(limit)s;
"""

WLM_EXPLAIN_PROMPT = """
分析以下 WLM 執行紀錄，重點：
- 有沒有 queue_seconds 異常高的 query（積壓嚴重）
- 有沒有觸發 QMR rule 的 query（hit_qmr_rule 不為空）
- 哪些 user 佔用最多資源
- 建議是否需要調整 WLM 設定
"""


def get_wlm_status(
    conn: psycopg2.extensions.connection, hours: int = 24, limit: int = 50
) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, WLM_STATUS_SQL, {"hours": hours, "limit": limit})


# ---------------------------------------------------------------------------
# 6. Locks (incident)
# ---------------------------------------------------------------------------

LOCKS_SQL = """
SELECT
    l.pid,
    TRIM(c.relname)     AS table_name,
    l.mode,
    l.granted,
    TRIM(s.user_name)   AS username,
    ROUND(EXTRACT(EPOCH FROM (GETDATE() - r.starttime)) / 60.0, 2) AS running_minutes
FROM pg_locks l
JOIN pg_class c ON c.oid = l.relation
LEFT JOIN stv_sessions s ON s.process = l.pid
LEFT JOIN stv_recents r ON r.pid = l.pid AND r.status = 'Running'
WHERE l.relation IS NOT NULL
ORDER BY l.granted, running_minutes DESC;
"""

LOCKS_EXPLAIN_PROMPT = """
分析以下 Redshift lock 狀況，重點：
- 找出 blocker（granted = false 且有人在等待的 pid）
- 說明 block chain（誰 block 誰）
- 建議 kill 哪個 pid 最有效（需由有 admin 權限的帳號執行 pg_terminate_backend）
- 評估 kill 的風險（該 query 執行多久了）
"""


def get_locks(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, LOCKS_SQL)


# ---------------------------------------------------------------------------
# 7. Spill to disk (incident)
# ---------------------------------------------------------------------------

SPILL_SQL = """
SELECT
    q.query                     AS query_id,
    TRIM(u.usename)             AS username,
    q.starttime,
    SUM(m.blocks_to_disk)       AS spilled_blocks
FROM stl_query q
JOIN stl_query_metrics m ON m.query = q.query
JOIN pg_user u ON u.usesysid = q.userid
WHERE q.starttime > GETDATE() - INTERVAL '%(hours)s hours'
  AND m.blocks_to_disk > 0
GROUP BY q.query, u.usename, q.starttime
ORDER BY spilled_blocks DESC
LIMIT %(limit)s;
"""

SPILL_EXPLAIN_PROMPT = """
分析以下 Redshift spill to disk 資料，重點：
- 哪些 query spill 最嚴重
- 可能原因（sort key 設計不良、記憶體不足、大型 hash join、WLM memory 分配過低）
- 建議調優方向（改 sort key、調整 WLM memory、改寫 query）
"""


def get_spill(
    conn: psycopg2.extensions.connection, hours: int = 24, limit: int = 20
) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, SPILL_SQL, {"hours": hours, "limit": limit})


# ---------------------------------------------------------------------------
# 8. Data skew (monthly)
# ---------------------------------------------------------------------------

SKEW_SQL = """
SELECT
    schema              AS schema_name,
    "table"             AS table_name,
    size                AS size_mb,
    tbl_rows            AS row_count,
    skew_rows           AS skew_rows_pct,
    skew_sortkey1       AS skew_sortkey1_pct,
    diststyle,
    TRIM(sortkey1)      AS sortkey1
FROM svv_table_info
WHERE schema NOT IN ('pg_catalog', 'information_schema')
  AND (skew_rows > %(skew_threshold)s OR skew_sortkey1 > %(skew_threshold)s)
ORDER BY skew_rows DESC
LIMIT %(limit)s;
"""

SKEW_EXPLAIN_PROMPT = """
分析以下 Redshift 資料分佈 skew 狀況，重點：
- 哪些表 skew 最嚴重（skew_rows_pct 越高越差）
- 目前 diststyle 是否合適
- 建議改用哪種 distribution key（需由有 admin 權限的帳號執行 DDL）
"""


def get_skew(
    conn: psycopg2.extensions.connection, skew_threshold: float = 4.0, limit: int = 30
) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, SKEW_SQL, {"skew_threshold": skew_threshold, "limit": limit})


# ---------------------------------------------------------------------------
# 9. Dependencies (ad-hoc)
# ---------------------------------------------------------------------------

DEPS_SP_SQL = """
SELECT
    'procedure'         AS dep_type,
    TRIM(n.nspname)     AS schema_name,
    TRIM(p.proname)     AS name
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
  AND p.prosrc LIKE %(pattern)s
ORDER BY n.nspname, p.proname;
"""

DEPS_VIEW_SQL = """
SELECT
    'view'              AS dep_type,
    table_schema        AS schema_name,
    table_name          AS name
FROM information_schema.views
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
  AND view_definition LIKE %(pattern)s
ORDER BY table_schema, table_name;
"""

DEPS_EXPLAIN_PROMPT = """
分析以下依賴關係，重點：
- 列出所有引用此表的 stored procedure 和 view
- 如果要修改或刪除此表，影響範圍有多大
- 建議修改前需要通知的下游系統或團隊
"""


def get_deps(
    conn: psycopg2.extensions.connection, table_name: str
) -> dict[str, Any]:
    pattern = {"pattern": f"%{table_name}%"}
    return {
        "table": table_name,
        "procedures": _fetchall_as_dicts(conn, DEPS_SP_SQL, pattern),
        "views": _fetchall_as_dicts(conn, DEPS_VIEW_SQL, pattern),
    }


# ---------------------------------------------------------------------------
# 10. Stale tables (weekly)
# ---------------------------------------------------------------------------

STALE_TABLES_SQL = """
SELECT
    table_schema        AS schema_name,
    table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND table_type = 'BASE TABLE'
  AND (
       table_name LIKE '%_bck_%'
    OR table_name LIKE '%_bak_%'
    OR table_name LIKE '%_tmp_%'
    OR table_name LIKE '%_temp_%'
    OR table_name LIKE '%_old_%'
    OR table_name LIKE '%_backup_%'
    OR table_name LIKE '%_bck'
    OR table_name LIKE '%_bak'
    OR table_name LIKE '%_old'
  )
ORDER BY schema_name, table_name;
"""

STALE_TABLES_EXPLAIN_PROMPT = """
分析以下疑似廢棄的 Redshift 備份表，重點：
- 哪些表最確定可以刪除（依命名模式判斷）
- 哪些表需要先確認（可能仍在使用）
- 產出 DROP TABLE IF EXISTS SQL 清單供人工確認後執行
"""


def get_stale_tables(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, STALE_TABLES_SQL)


# ---------------------------------------------------------------------------
# 11. Audit log (weekly)
# ---------------------------------------------------------------------------

AUDIT_SQL = """
SELECT
    starttime           AS event_time,
    TRIM(u.usename)     AS username,
    TRIM(d.text)        AS ddl_text
FROM stl_ddltext d
JOIN pg_user u ON u.usesysid = d.userid
WHERE starttime > GETDATE() - INTERVAL '%(hours)s hours'
ORDER BY starttime DESC
LIMIT %(limit)s;
"""

AUDIT_EXPLAIN_PROMPT = """
分析以下 Redshift DDL 操作紀錄，重點：
- 有沒有可疑的異動（非預期時段、非預期帳號）
- 重要的結構變更摘要（DROP TABLE、ALTER TABLE、GRANT/REVOKE）
- 建議是否需要進一步稽核
"""


def get_audit(
    conn: psycopg2.extensions.connection, hours: int = 168, limit: int = 100
) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, AUDIT_SQL, {"hours": hours, "limit": limit})


# ---------------------------------------------------------------------------
# 12. MCD — ETL entity status (daily)
# ---------------------------------------------------------------------------

MCD_ETL_STATUS_SQL = """
SELECT
    schema_name,
    table_name,
    last_exec_dtm,
    status_id
FROM "dwa"."sys"."etl_entity_prof"
WHERE schema_name IN ('dwh', 'dm', 'ods', 'stg')
ORDER BY last_exec_dtm DESC;
"""

MCD_ETL_STATUS_EXPLAIN_PROMPT = """
分析以下 MCD ETL 處理狀態（status_id: 0=success, 1=running, 9=failed），重點：
- 有沒有 failed（status_id=9）的項目
- running 中的 job 是否超時
- 建議需要重跑的清單（使用 Lambda / Glue / Step Function 重觸發）
"""


def get_mcd_etl_status(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, MCD_ETL_STATUS_SQL)


# ---------------------------------------------------------------------------
# 13. MCD — ETL audit log (daily)
# ---------------------------------------------------------------------------

MCD_ETL_LOG_SQL = """
SELECT TOP %(limit)s
    schema_name,
    table_name,
    exec_start_dtm,
    exec_end_dtm,
    DATEDIFF('minute', exec_start_dtm, exec_end_dtm) AS duration_minutes,
    status_id,
    msg
FROM "dwa"."sys"."etl_audit_log"
WHERE exec_start_dtm >= GETDATE() - INTERVAL '%(hours)s hours'
  AND schema_name IN ('dwh', 'dm', 'ods')
ORDER BY id DESC;
"""

MCD_ETL_LOG_EXPLAIN_PROMPT = """
分析以下 MCD ETL 執行日誌，重點：
- 哪些表執行時間最長（duration_minutes 最高）
- 有沒有 failed（status_id=9）的紀錄及 msg 內容
- 整體執行趨勢是否有惡化跡象
"""


def get_mcd_etl_log(
    conn: psycopg2.extensions.connection, hours: int = 24, limit: int = 100
) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, MCD_ETL_LOG_SQL, {"hours": hours, "limit": limit})


# ---------------------------------------------------------------------------
# 14. MCD — Value check (daily — data freshness via vw_tbl_stat_max_column_value)
# ---------------------------------------------------------------------------

MCD_VALUE_CHECK_SQL = """
SELECT
    data_src_name,
    schema_name,
    table_name,
    column_name,
    max_column_value_todate,
    benchmark_value::date           AS expected_date,
    DATEDIFF('day', max_column_value_todate, benchmark_value::date) AS days_behind
FROM "dwa"."sys"."vw_tbl_stat_max_column_value"
WHERE max_column_value_todate < benchmark_value::date
ORDER BY days_behind DESC;
"""

MCD_VALUE_CHECK_ALL_SQL = """
SELECT
    data_src_name,
    schema_name,
    table_name,
    column_name,
    max_column_value_todate,
    benchmark_value::date           AS expected_date,
    DATEDIFF('day', max_column_value_todate, benchmark_value::date) AS days_behind
FROM "dwa"."sys"."vw_tbl_stat_max_column_value"
ORDER BY days_behind DESC;
"""

MCD_VALUE_CHECK_EXPLAIN_PROMPT = """
分析以下 MCD 資料新鮮度檢查結果，重點：
- 哪些表資料落後最嚴重（days_behind 最大）
- 落後超過 3 天的表是否需要緊急重跑
- 依 data_src_name 分組，哪個來源系統問題最多
"""


def get_mcd_value_check(
    conn: psycopg2.extensions.connection, all_tables: bool = False
) -> list[dict[str, Any]]:
    sql = MCD_VALUE_CHECK_ALL_SQL if all_tables else MCD_VALUE_CHECK_SQL
    return _fetchall_as_dicts(conn, sql)


# ---------------------------------------------------------------------------
# 15. MCD — ETL missing (ad-hoc — config vs actual execution)
# ---------------------------------------------------------------------------

MCD_ETL_MISSING_SQL = """
SELECT
    c.source_name,
    c.ods_schema_name   AS schema_name,
    c.ods_table_name    AS table_name,
    e.last_exec_dtm,
    e.status_id
FROM "dwa"."sys"."etl_table_config" c
LEFT JOIN "dwa"."sys"."etl_entity_prof" e
    ON e.schema_name = c.ods_schema_name
   AND e.table_name  = c.ods_table_name
WHERE e.table_name IS NULL
   OR e.status_id = 9
   OR e.last_exec_dtm < CURRENT_DATE
ORDER BY c.source_name, c.ods_table_name;
"""

MCD_ETL_MISSING_EXPLAIN_PROMPT = """
分析以下未成功執行的 ETL 表清單（有 config 但今天沒成功跑），重點：
- 分類：從未執行（last_exec_dtm IS NULL）、執行失敗（status_id=9）、今天尚未執行
- 哪些表優先處理（依 source_name 分組）
- 建議使用 Lambda / Glue / Step Function 重觸發的順序
"""


def get_mcd_etl_missing(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    return _fetchall_as_dicts(conn, MCD_ETL_MISSING_SQL)
