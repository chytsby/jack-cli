"""AWS Bedrock integration for rsops explain command."""

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
    DATASHARES_EXPLAIN_PROMPT,
    CONNECTIONS_EXPLAIN_PROMPT,
    COPY_STATUS_EXPLAIN_PROMPT,
    VACUUM_PROGRESS_EXPLAIN_PROMPT,
    LOCKS_EXPLAIN_PROMPT,
    SPILL_EXPLAIN_PROMPT,
    ALERTS_EXPLAIN_PROMPT,
    SCALING_EXPLAIN_PROMPT,
    SKEW_EXPLAIN_PROMPT,
    COMPRESSION_EXPLAIN_PROMPT,
    DEPS_EXPLAIN_PROMPT,
    STALE_TABLES_EXPLAIN_PROMPT,
    AUDIT_EXPLAIN_PROMPT,
    MCD_ETL_STATUS_EXPLAIN_PROMPT,
    MCD_ETL_LOG_EXPLAIN_PROMPT,
)

DEFAULT_MODEL = "apac.amazon.nova-pro-v1:0"
DEFAULT_REGION = "ap-southeast-1"

SYSTEM_PROMPT = """你是資深 Redshift DBA，專精於 RA3 叢集的日常維運與效能調優。
分析使用者提供的 rsops 指令輸出，直接給出結論與建議，不要廢話。
格式：Markdown，條列重點，用繁體中文回答。

重要規則：
- 每個建議行動必須同時附上兩個 code block：
  1. 可直接執行的 rsops 指令（```bash ... ```）
  2. 對應的原始 SQL（```sql ... ```）
- drill down 步驟要具體，說明查什麼、為什麼查、預期看到什麼
- 沒有 code block 的建議視為不完整"""

PROMPT_REGISTRY: dict[str, str] = {
    "long_queries": LONG_RUNNING_EXPLAIN_PROMPT,
    "disk": DISK_EXPLAIN_PROMPT,
    "etl_failures": ETL_FAILURES_EXPLAIN_PROMPT,
    "table_health": TABLE_HEALTH_EXPLAIN_PROMPT,
    "wlm": WLM_EXPLAIN_PROMPT,
    "datashares": DATASHARES_EXPLAIN_PROMPT,
    "connections": CONNECTIONS_EXPLAIN_PROMPT,
    "copy_status": COPY_STATUS_EXPLAIN_PROMPT,
    "vacuum_progress": VACUUM_PROGRESS_EXPLAIN_PROMPT,
    "locks": LOCKS_EXPLAIN_PROMPT,
    "spill": SPILL_EXPLAIN_PROMPT,
    "alerts": ALERTS_EXPLAIN_PROMPT,
    "scaling": SCALING_EXPLAIN_PROMPT,
    "skew": SKEW_EXPLAIN_PROMPT,
    "compression": COMPRESSION_EXPLAIN_PROMPT,
    "deps": DEPS_EXPLAIN_PROMPT,
    "stale_tables": STALE_TABLES_EXPLAIN_PROMPT,
    "audit": AUDIT_EXPLAIN_PROMPT,
    "mcd_etl_status": MCD_ETL_STATUS_EXPLAIN_PROMPT,
    "mcd_etl_log": MCD_ETL_LOG_EXPLAIN_PROMPT,
    "morning": """
這是 Redshift cluster 早晨例行巡檢報告，涵蓋磁碟、連線、長查詢、WLM、ETL 錯誤、COPY 狀態、表健康度、VACUUM 進度。
請產出：
1. 整體健康狀況（一句話摘要）
2. 今天需要立刻處理的事項（條列，沒有就寫「無」）
3. 今天需要留意但不緊急的事項（條列，沒有就寫「無」）
4. 各項目無異常確認清單
""",
    "incident": """
這是 Redshift 事件診斷報告，涵蓋 locks、spill、optimizer alerts、long queries、concurrency scaling。
請產出：
1. 問題根因判斷（最可能的原因）
2. 受影響範圍（哪些表、哪些 query、哪些使用者）
3. 建議立即行動（依優先順序，包含具體指令）
4. 後續預防建議
""",
    "maintain": """
這是 Redshift 維護報告，涵蓋 table health、vacuum progress、stale tables。
請產出：
1. 本次維護優先清單（VACUUM / ANALYZE / 刪表，依影響排序）
2. 預估維護時間
3. 建議執行的具體指令
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
