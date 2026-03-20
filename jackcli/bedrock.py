"""AWS Bedrock integration for jack explain command."""

from __future__ import annotations

import json
import os
from typing import Any

import boto3
from botocore.exceptions import NoCredentialsError

from .queries import (
    LONG_RUNNING_EXPLAIN_PROMPT,
    DISK_EXPLAIN_PROMPT,
    ETL_FAILURES_EXPLAIN_PROMPT,
    TABLE_HEALTH_EXPLAIN_PROMPT,
    WLM_EXPLAIN_PROMPT,
    LOCKS_EXPLAIN_PROMPT,
    SPILL_EXPLAIN_PROMPT,
    SKEW_EXPLAIN_PROMPT,
    DEPS_EXPLAIN_PROMPT,
    STALE_TABLES_EXPLAIN_PROMPT,
    AUDIT_EXPLAIN_PROMPT,
    MCD_ETL_STATUS_EXPLAIN_PROMPT,
    MCD_ETL_LOG_EXPLAIN_PROMPT,
    MCD_VALUE_CHECK_EXPLAIN_PROMPT,
    MCD_ETL_MISSING_EXPLAIN_PROMPT,
)

DEFAULT_MODEL = "apac.amazon.nova-pro-v1:0"
DEFAULT_REGION = "ap-southeast-1"

SYSTEM_PROMPT = """你是資深 Redshift DBA，專精於 RA3 叢集的日常維運與效能調優。
分析使用者提供的 jack 指令輸出，直接給出結論與建議，不要廢話。
格式：Markdown，條列重點，用繁體中文回答。

重要規則：
- 分析要具體：說明為什麼異常、可能的根本原因、預期的影響範圍
- drill-down 建議附上可直接在 DB 執行的 SQL（```sql ... ```），不要建議 CLI 指令
- 如果資料正常，明確說「無異常」，不要硬湊建議
- VACUUM / ANALYZE 只能建議，標注「需由有 admin 權限的帳號執行」，不要在 SQL 中給出執行語句"""

PROMPT_REGISTRY: dict[str, str] = {
    "long_queries":     LONG_RUNNING_EXPLAIN_PROMPT,
    "disk":             DISK_EXPLAIN_PROMPT,
    "etl_failures":     ETL_FAILURES_EXPLAIN_PROMPT,
    "table_health":     TABLE_HEALTH_EXPLAIN_PROMPT,
    "wlm":              WLM_EXPLAIN_PROMPT,
    "locks":            LOCKS_EXPLAIN_PROMPT,
    "spill":            SPILL_EXPLAIN_PROMPT,
    "skew":             SKEW_EXPLAIN_PROMPT,
    "deps":             DEPS_EXPLAIN_PROMPT,
    "stale_tables":     STALE_TABLES_EXPLAIN_PROMPT,
    "audit":            AUDIT_EXPLAIN_PROMPT,
    "mcd_etl_status":   MCD_ETL_STATUS_EXPLAIN_PROMPT,
    "mcd_etl_log":      MCD_ETL_LOG_EXPLAIN_PROMPT,
    "mcd_value_check":  MCD_VALUE_CHECK_EXPLAIN_PROMPT,
    "mcd_etl_missing":  MCD_ETL_MISSING_EXPLAIN_PROMPT,
    "daily": """
這是 Redshift daily 巡檢報告，涵蓋 WLM、ETL 錯誤、MCD ETL 狀態與日誌、資料新鮮度。
請產出：
1. 整體健康狀況（一句話摘要）
2. 今天需要立刻處理的事項（條列，沒有就寫「無」）
3. 今天需要留意但不緊急的事項（條列，沒有就寫「無」）
""",
    "weekly": """
這是 Redshift weekly 巡檢報告，涵蓋近兩週慢查詢、廢棄備份表、DDL 異動紀錄。
請產出：
1. 需要追蹤的慢查詢（user 與 query pattern）
2. 建議清理的廢棄表清單
3. 值得注意的 DDL 異動
""",
    "monthly": """
這是 Redshift monthly 巡檢報告，涵蓋磁碟用量、table health、data skew。
請產出：
1. 磁碟用量評估（是否需要關注）
2. 需要 DBA 處理的 table 維護清單（注意：本工具不執行 VACUUM/ANALYZE，僅提供報告）
3. Skew 問題表與建議的 distribution key 調整（需 DBA 執行 DDL）
""",
    "incident": """
這是 Redshift 事件診斷報告，涵蓋 locks 與 spill to disk。
請產出：
1. 問題根因判斷
2. 受影響範圍
3. 建議立即行動（kill 指令需由有 admin 權限的帳號執行）
4. 後續預防建議
""",
    "maintain": """
這是 Redshift 維護報告，涵蓋廢棄備份表與 DDL 異動紀錄。
請產出：
1. 建議清理的廢棄表清單（含 DROP TABLE IF EXISTS SQL）
2. 值得注意的 DDL 異動摘要
""",
}

FALLBACK_PROMPT = "分析以下 Redshift ops 資料，條列重點發現與建議行動。"


def call_bedrock(command: str, data: Any, output_format: str = "markdown") -> str:
    """Call AWS Bedrock via Converse API and return the model response as a string."""
    region = os.environ.get("AWS_DEFAULT_REGION", DEFAULT_REGION)
    model_id = os.environ.get("BEDROCK_MODEL", DEFAULT_MODEL)

    try:
        client = boto3.client("bedrock-runtime", region_name=region)
    except Exception as exc:
        raise RuntimeError(f"Failed to initialise Bedrock client: {exc}") from exc

    user_prompt = PROMPT_REGISTRY.get(command, FALLBACK_PROMPT)
    if command not in PROMPT_REGISTRY:
        user_prompt = f"[未知指令: {command}]\n{FALLBACK_PROMPT}"

    format_note = (
        "" if output_format == "markdown"
        else "\n\n請用純文字格式輸出，不要使用 Markdown 語法。"
    )

    user_content = (
        f"{user_prompt}{format_note}\n\n"
        f"```json\n{json.dumps(data, ensure_ascii=False, indent=2)}\n```"
    )

    try:
        response = client.converse(
            modelId=model_id,
            system=[{"text": SYSTEM_PROMPT}],
            messages=[{"role": "user", "content": [{"text": user_content}]}],
            inferenceConfig={"maxTokens": 2048},
        )
    except NoCredentialsError as exc:
        raise RuntimeError(
            "AWS credentials not found.\n"
            "Configure via environment variables or IAM role:\n"
            "  export AWS_ACCESS_KEY_ID=...\n"
            "  export AWS_SECRET_ACCESS_KEY=...\n"
            f"  export AWS_DEFAULT_REGION={DEFAULT_REGION}"
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"Bedrock API error: {exc}") from exc

    return response["output"]["message"]["content"][0]["text"]
