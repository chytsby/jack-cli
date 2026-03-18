"""Task logic layer: every function returns a list[dict] or dict ready for JSON serialisation."""

from __future__ import annotations

from typing import Any

import psycopg2.extensions


def _fetchall_as_dicts(
    conn: psycopg2.extensions.connection, sql: str, params: dict = {}
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# 1. Long-running queries
# ---------------------------------------------------------------------------

LONG_RUNNING_SQL = """
SELECT
    r.pid                                           AS pid,
    trim(r.user_name)                               AS username,
    trim(r.query)                                   AS sql_text,
    r.starttime                                     AS start_time,
    ROUND(r.duration / 1000000.0 / 60.0, 2)        AS running_minutes
FROM stv_recents r
WHERE r.status = 'Running'
ORDER BY r.duration DESC
LIMIT %(limit)s;
"""

LONG_RUNNING_EXPLAIN_PROMPT = """
分析以下長時間執行的 Redshift query 清單，重點：
- 哪些 query 最需要立刻關注（依執行時間）
- 可能的原因（lock 等待、資料量過大、missing stats、WLM queue 積壓）
- 建議下一步（是否需要 kill、是否需要調查 lock）
"""


def get_long_running_queries(
    conn: psycopg2.extensions.connection, threshold_minutes: float = 5.0, limit: int = 20
) -> list[dict[str, Any]]:
    """Return queries running longer than *threshold_minutes*."""
    rows = _fetchall_as_dicts(conn, LONG_RUNNING_SQL, {"limit": limit})
    return [r for r in rows if (r.get("running_minutes") or 0) >= threshold_minutes]


# ---------------------------------------------------------------------------
# 2. Disk usage
# ---------------------------------------------------------------------------

DISK_USAGE_SQL = """
SELECT
    trim(n.nspname)                         AS schema_name,
    trim(t."table")                         AS table_name,
    t.size                                  AS size_mb,
    t.tbl_rows                              AS row_count,
    t.stats_off                             AS stats_staleness_pct,
    t.unsorted                              AS unsorted_pct,
    t.skew_rows                             AS skew_rows_pct,
    t.pct_used                              AS pct_disk_used
FROM svv_table_info t
JOIN pg_namespace n ON n.nspname = t.schema
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY t.size DESC
LIMIT %(limit)s;
"""

CLUSTER_DISK_SQL = """
SELECT
    SUM(used)                                               AS used_mb,
    SUM(capacity)                                           AS total_mb,
    ROUND(100.0 * SUM(used) / NULLIF(SUM(capacity), 0), 2) AS used_pct
FROM stv_partitions
WHERE part_begin = 0;
"""

DISK_EXPLAIN_PROMPT = """
分析以下 Redshift 磁碟用量資料，重點：
- 整體磁碟使用率是否有風險（>80% 需警示，>90% 緊急）
- 哪些表異常大，大小是否合理
- 有沒有 stats_staleness 或 unsorted 需要順便處理
- 建議優先處理的項目
"""


def get_disk_usage(
    conn: psycopg2.extensions.connection, limit: int = 30
) -> dict[str, Any]:
    """Return cluster-level disk summary plus top tables by size."""
    cluster = _fetchall_as_dicts(conn, CLUSTER_DISK_SQL)
    tables = _fetchall_as_dicts(conn, DISK_USAGE_SQL, {"limit": limit})
    return {
        "cluster_summary": cluster[0] if cluster else {},
        "top_tables": tables,
    }


# ---------------------------------------------------------------------------
# 3. ETL failure log
# ---------------------------------------------------------------------------

ETL_LOAD_ERRORS_SQL = """
SELECT
    le.starttime,
    trim(le.filename)   AS source_file,
    le.colname,
    le.type             AS column_type,
    le.col_length,
    le.err_code,
    trim(le.err_reason) AS error_reason,
    trim(u.usename)     AS username
FROM stl_load_errors le
LEFT JOIN stl_query q ON q.query = le.query
LEFT JOIN pg_user u ON u.usesysid = q.userid
WHERE le.starttime > GETDATE() - INTERVAL '%(hours)s hours'
ORDER BY le.starttime DESC
LIMIT %(limit)s;
"""

ETL_QUERY_ERRORS_SQL = """
SELECT
    recordtime              AS event_time,
    trim(u.usename)         AS username,
    errcode                 AS error_code,
    trim(context)           AS error_context
FROM stl_error e
JOIN pg_user u ON u.usesysid = e.userid
WHERE recordtime > GETDATE() - INTERVAL '%(hours)s hours'
ORDER BY recordtime DESC
LIMIT %(limit)s;
"""

ETL_FAILURES_EXPLAIN_PROMPT = """
分析以下 Redshift ETL 錯誤紀錄，重點：
- 主要錯誤類型是什麼（load error vs query error）
- 哪些來源檔案或表最常出錯
- 錯誤原因分析（型別不符、檔案格式、權限等）
- 建議修復方向
"""


def get_etl_failures(
    conn: psycopg2.extensions.connection, hours: int = 24, limit: int = 50
) -> dict[str, Any]:
    """Return recent ETL load errors and query-level errors."""
    params = {"hours": hours, "limit": limit}
    return {
        "load_errors": _fetchall_as_dicts(conn, ETL_LOAD_ERRORS_SQL, params),
        "query_errors": _fetchall_as_dicts(conn, ETL_QUERY_ERRORS_SQL, params),
    }


# ---------------------------------------------------------------------------
# 4. Table health (vacuum / analyze status)
# ---------------------------------------------------------------------------

TABLE_HEALTH_SQL = """
SELECT
    trim(schema)                AS schema_name,
    trim("table")               AS table_name,
    size                        AS size_mb,
    stats_off                   AS stats_staleness_pct,
    unsorted                    AS unsorted_pct,
    vacuum_sort_benefit         AS vacuum_sort_benefit,
    tbl_rows                    AS row_count
FROM svv_table_info
WHERE schema NOT IN ('pg_catalog', 'information_schema')
  AND (stats_off > %(stats_threshold)s OR unsorted > %(unsorted_threshold)s)
ORDER BY size DESC
LIMIT %(limit)s;
"""

TABLE_HEALTH_EXPLAIN_PROMPT = """
分析以下需要維護的 Redshift 表清單，重點：
- 依 unsorted_pct 和 size 排出優先 VACUUM 的表
- 依 stats_staleness_pct 排出優先 ANALYZE 的表
- 估算維護影響（大表優先還是急用表優先）
- 建議執行順序與指令
"""


def get_table_health(
    conn: psycopg2.extensions.connection,
    stats_threshold: float = 10.0,
    unsorted_threshold: float = 10.0,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """Return tables whose statistics are stale or unsorted ratio is high."""
    return _fetchall_as_dicts(
        conn,
        TABLE_HEALTH_SQL,
        {
            "stats_threshold": stats_threshold,
            "unsorted_threshold": unsorted_threshold,
            "limit": limit,
        },
    )


# ---------------------------------------------------------------------------
# 5. WLM queue status
# ---------------------------------------------------------------------------

WLM_STATUS_SQL = """
SELECT
    s.service_class                             AS queue_id,
    c.name                                      AS queue_name,
    s.num_queued_queries                        AS queued,
    s.num_executing_queries                     AS executing,
    s.num_executed_queries                      AS completed_total,
    c.num_query_tasks                           AS max_concurrency,
    c.query_working_mem                         AS mem_per_slot_mb
FROM stv_wlm_service_class_state s
JOIN stv_wlm_service_class_config c ON c.service_class = s.service_class
WHERE s.service_class > 4
ORDER BY s.service_class;
"""

WLM_EXPLAIN_PROMPT = """
分析以下 WLM queue 狀態，重點：
- 有沒有 queue 積壓嚴重（queued 數量高）
- concurrency 使用率是否接近上限
- memory 分配是否合理
- 建議是否需要調整 WLM 設定
"""


def get_wlm_status(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    """Return WLM queue utilisation snapshot."""
    return _fetchall_as_dicts(conn, WLM_STATUS_SQL)


# ---------------------------------------------------------------------------
# 6. DataShare status
# ---------------------------------------------------------------------------

DATASHARE_SQL = """
SELECT
    share_name,
    share_type,
    is_publicaccessible,
    share_acl
FROM svv_datashares
ORDER BY share_name;
"""

DATASHARES_EXPLAIN_PROMPT = """
分析以下 Redshift DataShare 定義，重點：
- 有沒有 is_publicaccessible = true 的 DataShare（安全風險）
- ACL 設定是否合理
- 建議是否需要調整權限
"""


def get_datashare_status(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    """Return DataShare definitions visible to this cluster."""
    return _fetchall_as_dicts(conn, DATASHARE_SQL)


# ---------------------------------------------------------------------------
# 7. Connections
# ---------------------------------------------------------------------------

CONNECTIONS_SQL = """
SELECT
    process                     AS pid,
    trim(user_name)             AS username,
    trim(db_name)               AS database,
    starttime                   AS connected_since,
    ROUND(DATEDIFF('second', starttime, GETDATE()) / 60.0, 2) AS connected_minutes
FROM stv_sessions
ORDER BY connected_since;
"""

CONNECTIONS_EXPLAIN_PROMPT = """
分析以下 Redshift 連線狀況，重點：
- 目前連線數是否接近上限（Redshift 預設 500，RA3 可能更低）
- 有沒有連線時間異常長的 session（可能是應用程式連線洩漏）
- 有沒有異常帳號或異常時段的連線
- 建議是否需要關閉特定連線
"""


def get_connections(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    """Return current cluster sessions."""
    return _fetchall_as_dicts(conn, CONNECTIONS_SQL)


# ---------------------------------------------------------------------------
# 8. COPY job status
# ---------------------------------------------------------------------------

COPY_STATUS_SQL = """
SELECT
    query                       AS query_id,
    slice,
    pct_complete                AS pct_done,
    num_files_complete,
    num_files,
    bytes_loaded,
    bytes_to_load,
    trim(current_file)          AS current_file
FROM stv_load_state
ORDER BY query, slice;
"""

COPY_STATUS_EXPLAIN_PROMPT = """
分析以下 COPY job 執行狀態，重點：
- 有沒有卡住或進度停滯的 job
- 各 slice 進度是否均衡（差距大可能有 skew 問題）
- is_canceled 的 job 是否需要調查
- 建議是否需要介入
"""


def get_copy_status(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    """Return active COPY job progress."""
    return _fetchall_as_dicts(conn, COPY_STATUS_SQL)


# ---------------------------------------------------------------------------
# 9. Vacuum progress
# ---------------------------------------------------------------------------

VACUUM_PROGRESS_SQL = """
SELECT
    trim(table_name)            AS table_name,
    trim(status)                AS status,
    time_remaining_estimate
FROM svv_vacuum_progress;
"""

VACUUM_PROGRESS_EXPLAIN_PROMPT = """
分析以下 VACUUM 執行進度，重點：
- 目前有幾個 VACUUM 在跑
- 各 VACUUM 進展是否正常（estimated_time_remaining 是否合理）
- 有沒有 VACUUM 卡住或異常慢
- 是否需要手動介入或調整排程
"""


def get_vacuum_progress(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    """Return ongoing VACUUM progress."""
    return _fetchall_as_dicts(conn, VACUUM_PROGRESS_SQL)


# ---------------------------------------------------------------------------
# 10. Locks
# ---------------------------------------------------------------------------

LOCKS_SQL = """
SELECT
    l.pid,
    trim(c.relname)             AS table_name,
    l.mode,
    l.granted,
    trim(s.user_name)           AS username,
    ROUND(EXTRACT(EPOCH FROM (GETDATE() - r.starttime)) / 60.0, 2)
                                AS running_minutes
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
- 建議 kill 哪個 pid 最有效，kill 順序為何
- 評估 kill 的風險（該 query 執行多久了）
"""


def get_locks(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    """Return current lock state."""
    return _fetchall_as_dicts(conn, LOCKS_SQL)


# ---------------------------------------------------------------------------
# 11. Spill to disk
# ---------------------------------------------------------------------------

SPILL_SQL = """
SELECT
    q.query                                 AS query_id,
    trim(u.usename)                         AS username,
    q.starttime,
    SUM(m.blocks_to_disk)                   AS spilled_blocks
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
    """Return queries that spilled to disk recently."""
    return _fetchall_as_dicts(conn, SPILL_SQL, {"hours": hours, "limit": limit})


# ---------------------------------------------------------------------------
# 12. Optimizer alerts
# ---------------------------------------------------------------------------

ALERTS_SQL = """
SELECT
    event_time,
    trim(solution)              AS alert_type,
    trim(event)                 AS event_detail,
    query                       AS query_id
FROM stl_alert_event_log
WHERE event_time > GETDATE() - INTERVAL '%(hours)s hours'
ORDER BY event_time DESC
LIMIT %(limit)s;
"""

ALERTS_EXPLAIN_PROMPT = """
分析以下 Redshift optimizer alert 紀錄，重點：
- 最常見的 alert 類型是什麼（missing stats、nested loop、very selective filter 等）
- 哪些表或 query 反覆出現 alert
- 建議優先解決的項目（通常 missing stats 最容易修）
- 建議行動（ANALYZE 哪些表、哪些 query 需要改寫）
"""


def get_alerts(
    conn: psycopg2.extensions.connection, hours: int = 24, limit: int = 50
) -> list[dict[str, Any]]:
    """Return recent optimizer alert events."""
    return _fetchall_as_dicts(conn, ALERTS_SQL, {"hours": hours, "limit": limit})


# ---------------------------------------------------------------------------
# 13. Concurrency scaling
# ---------------------------------------------------------------------------

SCALING_SQL = """
SELECT
    start_time,
    end_time,
    ROUND(usage_in_seconds / 60.0, 2)   AS usage_minutes,
    queries
FROM svcs_concurrency_scaling_usage
ORDER BY start_time DESC
LIMIT %(limit)s;
"""

SCALING_EXPLAIN_PROMPT = """
分析以下 Redshift Concurrency Scaling 使用紀錄，重點：
- 觸發頻率是否正常（過於頻繁代表 WLM 設定需要調整）
- 每次 scaling 持續多久
- 費用風險評估（Concurrency Scaling 按分鐘計費）
- 建議是否需要調整 WLM 設定來減少觸發
"""


def get_scaling(
    conn: psycopg2.extensions.connection, limit: int = 50
) -> list[dict[str, Any]]:
    """Return concurrency scaling usage history."""
    return _fetchall_as_dicts(conn, SCALING_SQL, {"limit": limit})


# ---------------------------------------------------------------------------
# 14. Data skew
# ---------------------------------------------------------------------------

SKEW_SQL = """
SELECT
    trim(schema)                AS schema_name,
    trim("table")               AS table_name,
    size                        AS size_mb,
    tbl_rows                    AS row_count,
    skew_rows                   AS skew_rows_pct,
    skew_sortkey1               AS skew_sortkey1_pct,
    diststyle,
    trim(sortkey1)              AS sortkey1
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
- 建議改用哪種 distribution key
- 改善 skew 預期帶來的效能提升
"""


def get_skew(
    conn: psycopg2.extensions.connection, skew_threshold: float = 4.0, limit: int = 30
) -> list[dict[str, Any]]:
    """Return tables with significant data skew."""
    return _fetchall_as_dicts(
        conn, SKEW_SQL, {"skew_threshold": skew_threshold, "limit": limit}
    )


# ---------------------------------------------------------------------------
# 15. Compression (column encoding)
# ---------------------------------------------------------------------------

COMPRESSION_SQL = """
SELECT
    d.schemaname                AS schema_name,
    d.tablename                 AS table_name,
    d.column                    AS column_name,
    d.type                      AS data_type,
    d.encoding
FROM pg_table_def d
WHERE d.encoding IN ('none', 'raw')
  AND d.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_internal', 'pg_toast')
ORDER BY d.schemaname, d.tablename, d.column
LIMIT %(limit)s;
"""

COMPRESSION_EXPLAIN_PROMPT = """
分析以下 Redshift 欄位 encoding 狀況，重點：
- 哪些表的壓縮效率最差（encoding = none/raw 且表很大）
- 依資料型別建議應該用哪種 encoding（varchar → lzo/zstd，integer → az64，timestamp → az64）
- 預估改善壓縮後可節省的空間
- 建議優先處理的表（最大且未壓縮的）
"""


def get_compression(
    conn: psycopg2.extensions.connection, min_size_mb: int = 100, limit: int = 50
) -> list[dict[str, Any]]:
    """Return columns with no or raw encoding on large tables."""
    return _fetchall_as_dicts(
        conn, COMPRESSION_SQL, {"min_size_mb": min_size_mb, "limit": limit}
    )


# ---------------------------------------------------------------------------
# 16. Dependencies
# ---------------------------------------------------------------------------

DEPS_SP_SQL = """
SELECT
    'procedure'                 AS dep_type,
    trim(n.nspname)             AS schema_name,
    trim(p.proname)             AS name,
    NULL                        AS definition
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
  AND p.prosrc LIKE %(pattern)s
ORDER BY n.nspname, p.proname;
"""

DEPS_VIEW_SQL = """
SELECT
    'view'                      AS dep_type,
    table_schema                AS schema_name,
    table_name                  AS name,
    view_definition             AS definition
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
- 建議安全的修改順序
"""


def get_deps(
    conn: psycopg2.extensions.connection, table_name: str
) -> dict[str, Any]:
    """Return stored procedures and views that reference *table_name*."""
    pattern = {"pattern": f"%{table_name}%"}
    return {
        "table": table_name,
        "procedures": _fetchall_as_dicts(conn, DEPS_SP_SQL, pattern),
        "views": _fetchall_as_dicts(conn, DEPS_VIEW_SQL, pattern),
    }


# ---------------------------------------------------------------------------
# 17. Stale tables
# ---------------------------------------------------------------------------

STALE_TABLES_SQL = """
SELECT
    table_schema                AS schema_name,
    table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND table_type = 'BASE TABLE'
  AND (
       table_name LIKE '%%\_bck\_%%'     ESCAPE '\'
    OR table_name LIKE '%%\_bak\_%%'     ESCAPE '\'
    OR table_name LIKE '%%\_tmp\_%%'     ESCAPE '\'
    OR table_name LIKE '%%\_temp\_%%'    ESCAPE '\'
    OR table_name LIKE '%%\_old\_%%'     ESCAPE '\'
    OR table_name LIKE '%%\_backup\_%%'  ESCAPE '\'
    OR table_name LIKE '%%\_bck'         ESCAPE '\'
    OR table_name LIKE '%%\_bak'         ESCAPE '\'
    OR table_name LIKE '%%\_old'         ESCAPE '\'
  )
ORDER BY schema_name, table_name;
"""

STALE_TABLES_EXPLAIN_PROMPT = """
分析以下疑似廢棄的 Redshift 備份表，重點：
- 哪些表最確定可以刪除（依命名模式和大小判斷）
- 哪些表需要先確認（可能仍在使用）
- 刪除這些表可以釋放多少空間
- 建議刪除前的確認步驟
- 產出 DROP TABLE SQL 清單（加上 IF EXISTS）
"""


def get_stale_tables(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    """Return tables that appear to be stale backups."""
    return _fetchall_as_dicts(conn, STALE_TABLES_SQL)


# ---------------------------------------------------------------------------
# 18. Audit log (DDL & permission changes)
# ---------------------------------------------------------------------------

AUDIT_SQL = """
SELECT
    starttime                   AS event_time,
    trim(u.usename)             AS username,
    trim(d.text)                AS ddl_text
FROM stl_ddltext d
JOIN pg_user u ON u.usesysid = d.userid
WHERE starttime > GETDATE() - INTERVAL '%(hours)s hours'
ORDER BY starttime DESC
LIMIT %(limit)s;
"""

AUDIT_EXPLAIN_PROMPT = """
分析以下 Redshift DDL 操作紀錄，重點：
- 有沒有可疑的異動（非預期時段、非預期帳號執行的 DDL）
- 重要的結構變更摘要（DROP TABLE、ALTER TABLE、GRANT/REVOKE）
- 有沒有需要追蹤或回報的操作
- 建議是否需要進一步稽核
"""


def get_audit(
    conn: psycopg2.extensions.connection, hours: int = 24, limit: int = 100
) -> list[dict[str, Any]]:
    """Return recent DDL operations."""
    return _fetchall_as_dicts(conn, AUDIT_SQL, {"hours": hours, "limit": limit})


# ---------------------------------------------------------------------------
# 19. MCD — ETL entity status (custom table)
# ---------------------------------------------------------------------------

MCD_ETL_STATUS_SQL = """
SELECT *
FROM "sys"."etl_entity_prof"
WHERE schema_name IN ('dwh', 'dm', 'ods', 'stg')
ORDER BY last_exec_dtm DESC;
"""

MCD_ETL_STATUS_EXPLAIN_PROMPT = """
分析以下 MCD ETL 處理狀態（status_id: 0=success, 1=running, 9=failed），重點：
- 有沒有 failed（status_id=9）的項目
- running 中的 job 是否超時（執行時間異常長）
- 建議需要重跑的清單
- 建議使用 Lambda / Glue / Step Function 重觸發的優先順序
"""


def get_mcd_etl_status(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    """Return MCD custom ETL entity processing status."""
    return _fetchall_as_dicts(conn, MCD_ETL_STATUS_SQL)


# ---------------------------------------------------------------------------
# 20. MCD — ETL audit log (custom table)
# ---------------------------------------------------------------------------

MCD_ETL_LOG_SQL = """
SELECT TOP %(limit)s
    DATEDIFF('minute', exec_start_dtm, exec_end_dtm) AS duration_minutes,
    schema_name,
    table_name,
    exec_start_dtm,
    exec_end_dtm,
    status_id
FROM sys.etl_audit_log
WHERE exec_start_dtm >= GETDATE() - INTERVAL '%(hours)s hours'
  AND schema_name IN ('dwh', 'dm', 'ods')
ORDER BY id DESC;
"""

MCD_ETL_LOG_EXPLAIN_PROMPT = """
分析以下 MCD ETL 執行日誌，重點：
- 哪些表執行時間最長（duration_minutes 最高）
- 有沒有異常的 duration（比平常慢很多）
- 有沒有 failed（status_id=9）的紀錄
- 整體執行趨勢是否有惡化跡象
"""


def get_mcd_etl_log(
    conn: psycopg2.extensions.connection, hours: int = 24, limit: int = 100
) -> list[dict[str, Any]]:
    """Return MCD custom ETL audit log."""
    return _fetchall_as_dicts(conn, MCD_ETL_LOG_SQL, {"hours": hours, "limit": limit})
