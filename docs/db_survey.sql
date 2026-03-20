SELECT 'svv_table_info' AS view_name, COUNT(*) AS accessible_rows FROM svv_table_info WHERE 1=0;
SELECT 'vw_tbl_query_log' AS view_name, COUNT(*) AS accessible_rows FROM "dwa"."sys"."vw_tbl_query_log" WHERE 1=0;
SELECT 'stl_wlm_query' AS view_name, COUNT(*) AS accessible_rows FROM stl_wlm_query WHERE 1=0;
SELECT 'stl_wlm_rule_action' AS view_name, COUNT(*) AS accessible_rows FROM stl_wlm_rule_action WHERE 1=0;
SELECT 'stl_querytext' AS view_name, COUNT(*) AS accessible_rows FROM stl_querytext WHERE 1=0;
SELECT 'stl_load_errors' AS view_name, COUNT(*) AS accessible_rows FROM stl_load_errors WHERE 1=0;
SELECT 'stl_error' AS view_name, COUNT(*) AS accessible_rows FROM stl_error WHERE 1=0;
SELECT 'pg_locks' AS view_name, COUNT(*) AS accessible_rows FROM pg_locks WHERE 1=0;
SELECT 'pg_class' AS view_name, COUNT(*) AS accessible_rows FROM pg_class WHERE 1=0;
SELECT 'stv_sessions' AS view_name, COUNT(*) AS accessible_rows FROM stv_sessions WHERE 1=0;
SELECT 'stv_recents' AS view_name, COUNT(*) AS accessible_rows FROM stv_recents WHERE 1=0;
SELECT 'stl_query' AS view_name, COUNT(*) AS accessible_rows FROM stl_query WHERE 1=0;
SELECT 'stl_query_metrics' AS view_name, COUNT(*) AS accessible_rows FROM stl_query_metrics WHERE 1=0;
SELECT 'stl_ddltext' AS view_name, COUNT(*) AS accessible_rows FROM stl_ddltext WHERE 1=0;
SELECT 'pg_proc' AS view_name, COUNT(*) AS accessible_rows FROM pg_catalog.pg_proc WHERE 1=0;
SELECT 'information_schema.views' AS view_name, COUNT(*) AS accessible_rows FROM information_schema.views WHERE 1=0;
SELECT 'etl_entity_prof' AS view_name, COUNT(*) AS accessible_rows FROM "sys"."etl_entity_prof" WHERE 1=0;
SELECT 'etl_audit_log' AS view_name, COUNT(*) AS accessible_rows FROM sys.etl_audit_log WHERE 1=0;

SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='sys' AND table_name='vw_tbl_query_log' ORDER BY ordinal_position;
SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='sys' AND table_name='etl_entity_prof' ORDER BY ordinal_position;
SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='sys' AND table_name='etl_audit_log' ORDER BY ordinal_position;

SELECT table_name, table_type FROM information_schema.tables WHERE table_schema='sys' ORDER BY table_type, table_name;

SELECT namespace_name AS schema_name, privilege_type, identity_name FROM svv_schema_privileges WHERE identity_name='datalake_op' ORDER BY namespace_name, privilege_type;
