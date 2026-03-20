#!/usr/bin/env python3
"""
db_survey.py — jack-cli v0.2.0 pre-build DB survey
使用與 jack-cli 相同的 redshift_connector 連線方式

Usage:
    source .env
    python3 db_survey.py 2>&1 | tee db_survey_result.txt
"""
import os
import sys
import redshift_connector

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_conn():
    required = ["REDSHIFT_HOST", "REDSHIFT_DATABASE", "REDSHIFT_USER", "REDSHIFT_PASSWORD"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"ERROR: missing env vars: {', '.join(missing)}")
        sys.exit(1)

    return redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        database=os.environ["REDSHIFT_DATABASE"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
    )


def run_sql(conn, label, sql):
    print(f"\n>>> {label}")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description:
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                print("  " + " | ".join(cols))
                print("  " + "-+-".join("-" * len(c) for c in cols))
                for row in rows:
                    print("  " + " | ".join(str(v) for v in row))
                if not rows:
                    print("  (0 rows)")
            else:
                print("  OK")
    except Exception as e:
        conn.rollback()
        print(f"  ERROR: {e}")


# ---------------------------------------------------------------------------
# Survey
# ---------------------------------------------------------------------------

def main():
    print("=" * 50)
    print(f"  jack-cli v0.2.0 DB Survey")
    print(f"  host: {os.environ.get('REDSHIFT_HOST', '?')}")
    print(f"  user: {os.environ.get('REDSHIFT_USER', '?')}")
    print("=" * 50)

    try:
        conn = get_conn()
        print("\nConnection: OK")
    except Exception as e:
        print(f"\nConnection FAILED: {e}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # A. System view permissions
    # ------------------------------------------------------------------
    print("\n=== A. System view permissions ===")

    checks = [
        ("A01 svv_table_info",      "SELECT 'svv_table_info' AS v, COUNT(*) FROM svv_table_info WHERE 1=0"),
        ("A02 vw_tbl_query_log",    'SELECT \'vw_tbl_query_log\' AS v, COUNT(*) FROM "dwa"."sys"."vw_tbl_query_log" WHERE 1=0'),
        ("A03 stl_wlm_query",       "SELECT 'stl_wlm_query' AS v, COUNT(*) FROM stl_wlm_query WHERE 1=0"),
        ("A04 stl_wlm_rule_action", "SELECT 'stl_wlm_rule_action' AS v, COUNT(*) FROM stl_wlm_rule_action WHERE 1=0"),
        ("A05 stl_querytext",       "SELECT 'stl_querytext' AS v, COUNT(*) FROM stl_querytext WHERE 1=0"),
        ("A06 stl_load_errors",     "SELECT 'stl_load_errors' AS v, COUNT(*) FROM stl_load_errors WHERE 1=0"),
        ("A07 stl_error",           "SELECT 'stl_error' AS v, COUNT(*) FROM stl_error WHERE 1=0"),
        ("A08 pg_locks",            "SELECT 'pg_locks' AS v, COUNT(*) FROM pg_locks WHERE 1=0"),
        ("A09 pg_class",            "SELECT 'pg_class' AS v, COUNT(*) FROM pg_class WHERE 1=0"),
        ("A10 stv_sessions",        "SELECT 'stv_sessions' AS v, COUNT(*) FROM stv_sessions WHERE 1=0"),
        ("A11 stv_recents",         "SELECT 'stv_recents' AS v, COUNT(*) FROM stv_recents WHERE 1=0"),
        ("A12 stl_query",           "SELECT 'stl_query' AS v, COUNT(*) FROM stl_query WHERE 1=0"),
        ("A13 stl_query_metrics",   "SELECT 'stl_query_metrics' AS v, COUNT(*) FROM stl_query_metrics WHERE 1=0"),
        ("A14 stl_ddltext",         "SELECT 'stl_ddltext' AS v, COUNT(*) FROM stl_ddltext WHERE 1=0"),
        ("A15 pg_proc",             "SELECT 'pg_proc' AS v, COUNT(*) FROM pg_catalog.pg_proc WHERE 1=0"),
        ("A16 information_schema.views", "SELECT 'information_schema.views' AS v, COUNT(*) FROM information_schema.views WHERE 1=0"),
        ("A17 etl_entity_prof",     'SELECT \'etl_entity_prof\' AS v, COUNT(*) FROM "sys"."etl_entity_prof" WHERE 1=0'),
        ("A18 etl_audit_log",       "SELECT 'etl_audit_log' AS v, COUNT(*) FROM sys.etl_audit_log WHERE 1=0"),
    ]
    for label, sql in checks:
        run_sql(conn, label, sql)

    # ------------------------------------------------------------------
    # B. Custom table columns
    # ------------------------------------------------------------------
    print("\n=== B. Custom table columns ===")

    col_checks = [
        ("B01 vw_tbl_query_log columns",
         "SELECT column_name, data_type FROM information_schema.columns "
         "WHERE table_schema='sys' AND table_name='vw_tbl_query_log' ORDER BY ordinal_position"),
        ("B02 etl_entity_prof columns",
         "SELECT column_name, data_type FROM information_schema.columns "
         "WHERE table_schema='sys' AND table_name='etl_entity_prof' ORDER BY ordinal_position"),
        ("B03 etl_audit_log columns",
         "SELECT column_name, data_type FROM information_schema.columns "
         "WHERE table_schema='sys' AND table_name='etl_audit_log' ORDER BY ordinal_position"),
    ]
    for label, sql in col_checks:
        run_sql(conn, label, sql)

    # ------------------------------------------------------------------
    # C. All objects in sys schema
    # ------------------------------------------------------------------
    print("\n=== C. All objects in sys schema ===")
    run_sql(conn, "C01 sys schema tables",
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema='sys' ORDER BY table_type, table_name")

    # ------------------------------------------------------------------
    # D. datalake_op schema privileges
    # ------------------------------------------------------------------
    print("\n=== D. Schema privileges for datalake_op ===")
    run_sql(conn, "D01 schema privileges",
            "SELECT namespace_name AS schema_name, privilege_type, identity_name "
            "FROM svv_schema_privileges WHERE identity_name='datalake_op' "
            "ORDER BY namespace_name, privilege_type")

    conn.close()
    print("\n=== Survey complete ===")


if __name__ == "__main__":
    main()
