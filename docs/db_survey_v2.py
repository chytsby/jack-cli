#!/usr/bin/env python3
"""
db_survey_v2.py — jack-cli v0.2.0 supplemental DB survey

Usage:
    source .env
    python3 db_survey_v2.py 2>&1 | tee db_survey_v2_result.txt
"""
import os
import sys
import redshift_connector


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


def main():
    print("=" * 50)
    print(f"  jack-cli v0.2.0 DB Survey v2")
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
    # A. vw_tbl_stat_max_column_value — view 欄位
    # ------------------------------------------------------------------
    print("\n=== A. vw_tbl_stat_max_column_value ===")

    run_sql(conn, "A01 view columns",
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'sys' AND table_name = 'vw_tbl_stat_max_column_value' "
            "ORDER BY ordinal_position")

    run_sql(conn, "A02 sample rows (top 5)",
            'SELECT * FROM "dwa"."sys"."vw_tbl_stat_max_column_value" LIMIT 5')

    # ------------------------------------------------------------------
    # B. vw_etl_entity_prof — view 欄位 vs base table
    # ------------------------------------------------------------------
    print("\n=== B. vw_etl_entity_prof ===")

    run_sql(conn, "B01 view columns",
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'sys' AND table_name = 'vw_etl_entity_prof' "
            "ORDER BY ordinal_position")

    run_sql(conn, "B02 sample rows (top 5, failed only)",
            'SELECT * FROM "dwa"."sys"."vw_etl_entity_prof" WHERE status_id = 9 LIMIT 5')

    run_sql(conn, "B03 sample rows (top 5, all)",
            'SELECT * FROM "dwa"."sys"."vw_etl_entity_prof" ORDER BY last_exec_dtm DESC LIMIT 5')

    # ------------------------------------------------------------------
    # C. etl_table_config — 預計執行清單
    # ------------------------------------------------------------------
    print("\n=== C. etl_table_config ===")

    run_sql(conn, "C01 columns",
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'sys' AND table_name = 'etl_table_config' "
            "ORDER BY ordinal_position")

    run_sql(conn, "C02 sample rows (top 5)",
            'SELECT * FROM "dwa"."sys"."etl_table_config" LIMIT 5')

    run_sql(conn, "C03 vw_etl_table_config columns",
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'sys' AND table_name = 'vw_etl_table_config' "
            "ORDER BY ordinal_position")

    conn.close()
    print("\n=== Survey v2 complete ===")


if __name__ == "__main__":
    main()
