"""CLI entry-point: jack <group> <command> [options]"""

from __future__ import annotations

import json
import os
import sys
from typing import Optional

import typer
from rich.console import Console

from .config import RedshiftConfig
from .connection import get_connection, ConnectionError, PermissionError
from .queries import (
    get_long_running_queries,
    get_disk_usage,
    get_etl_failures,
    get_table_health,
    get_wlm_status,
    get_locks,
    get_spill,
    get_skew,
    get_deps,
    get_stale_tables,
    get_audit,
    get_mcd_etl_status,
    get_mcd_etl_log,
    get_mcd_value_check,
    get_mcd_etl_missing,
)
from .output import print_json, print_table, print_dict_as_table, console
from .bedrock import call_bedrock

# ---------------------------------------------------------------------------
# App + sub-apps
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="jack",
    help="Redshift AI-Native Operations CLI.",
    no_args_is_help=True,
)

check_app = typer.Typer(help="Health checks and routine monitoring.")
incident_app = typer.Typer(
    help="Incident response. Run without subcommand for a full report.",
    invoke_without_command=True,
)
maintain_app = typer.Typer(
    help="Maintenance and governance. Run without subcommand for a full report.",
    invoke_without_command=True,
)
mcd_app = typer.Typer(help="MCD custom table queries.")

app.add_typer(check_app, name="check")
app.add_typer(incident_app, name="incident")
app.add_typer(maintain_app, name="maintain")
app.add_typer(mcd_app, name="mcd")

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _get_cfg() -> RedshiftConfig:
    try:
        return RedshiftConfig.from_env()
    except EnvironmentError as exc:
        console.print(f"[bold red]Configuration error:[/bold red] {exc}")
        raise typer.Exit(code=1)


def _handle_errors(exc: Exception) -> None:
    if isinstance(exc, ConnectionError):
        console.print(f"[bold red]Connection failed:[/bold red] {exc}")
    elif isinstance(exc, PermissionError):
        console.print(
            f"[bold red]Permission denied:[/bold red] {exc}\n"
            "[dim]Ensure your Redshift user has SELECT on the required system views.[/dim]"
        )
    else:
        console.print(f"[bold red]Unexpected error:[/bold red] {exc}")
    raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# check commands
# ---------------------------------------------------------------------------


@check_app.command("long-queries")
def check_long_queries(
    threshold: float = typer.Option(10.0, "--threshold", "-t", help="Minimum duration in minutes."),
    limit: int = typer.Option(20, "--limit", "-l", help="Maximum rows to return."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show queries from the past 14 days running longer than THRESHOLD minutes (non-ETL, SELECT only)."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_long_running_queries(conn, threshold_minutes=threshold, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "long_queries", "threshold_minutes": threshold, "rows": rows})
    else:
        if not rows:
            console.print(f"[green]No queries longer than {threshold} minutes in the past 14 days.[/green]")
        else:
            print_table(rows, title=f"Long-Running Queries (>{threshold} min, past 14 days)")


@check_app.command("disk")
def check_disk(
    limit: int = typer.Option(30, "--limit", "-l", help="Top N tables by size."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show cluster disk usage and top tables by size."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            data = get_disk_usage(conn, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "disk", "data": data})
    else:
        print_dict_as_table(data["cluster_summary"], title="Cluster Disk Usage")
        print_table(data["top_tables"], title=f"Top {limit} Tables by Size")


@check_app.command("etl-failures")
def check_etl_failures(
    hours: int = typer.Option(24, "--hours", "-h", help="Look-back window in hours."),
    limit: int = typer.Option(50, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show COPY job load errors in the last HOURS hours."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_etl_failures(conn, hours=hours, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "etl_failures", "hours": hours, "rows": rows})
    else:
        if not rows:
            console.print(f"[green]No COPY load errors in the last {hours}h.[/green]")
        else:
            print_table(rows, title=f"COPY Load Errors (last {hours}h)")


@check_app.command("table-health")
def check_table_health(
    stats_threshold: float = typer.Option(10.0, "--stats", help="Flag tables with stats staleness > N%."),
    unsorted_threshold: float = typer.Option(10.0, "--unsorted", help="Flag tables with unsorted ratio > N%."),
    limit: int = typer.Option(30, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show tables that need VACUUM or ANALYZE attention (report only — no operations executed)."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_table_health(conn, stats_threshold, unsorted_threshold, limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "table_health", "rows": rows})
    else:
        if not rows:
            console.print("[green]All tables are within healthy thresholds.[/green]")
        else:
            print_table(rows, title="Tables Needing VACUUM / ANALYZE (report only)")


@check_app.command("wlm")
def check_wlm(
    hours: int = typer.Option(24, "--hours", "-h", help="Look-back window in hours."),
    limit: int = typer.Option(50, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show WLM execution history with queue times and QMR rule hits."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_wlm_status(conn, hours=hours, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "wlm", "hours": hours, "rows": rows})
    else:
        if not rows:
            console.print(f"[green]No WLM activity in the last {hours}h.[/green]")
        else:
            print_table(rows, title=f"WLM Execution (last {hours}h)")


@check_app.command("skew")
def check_skew(
    skew_threshold: float = typer.Option(4.0, "--threshold", "-t", help="Flag tables with skew ratio > N."),
    limit: int = typer.Option(30, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show tables with significant data skew across slices."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_skew(conn, skew_threshold=skew_threshold, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "skew", "rows": rows})
    else:
        if not rows:
            console.print(f"[green]No tables with skew ratio > {skew_threshold}.[/green]")
        else:
            print_table(rows, title=f"Tables with Data Skew (>{skew_threshold})")


@check_app.command("deps")
def check_deps(
    table: str = typer.Argument(..., help="Table name to search for (partial match, e.g. 'schema.table')."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show stored procedures and views that reference TABLE."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            data = get_deps(conn, table_name=table)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "deps", "table": table, "data": data})
    else:
        print_table(data["procedures"], title=f"Stored Procedures referencing '{table}'")
        print_table(data["views"], title=f"Views referencing '{table}'")


# ---------------------------------------------------------------------------
# incident commands
# ---------------------------------------------------------------------------


@incident_app.callback()
def incident_callback(ctx: typer.Context, json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON.")) -> None:
    """Incident response: run all incident checks when called without a subcommand."""
    if ctx.invoked_subcommand is not None:
        return

    cfg = _get_cfg()
    results: dict = {}
    try:
        with get_connection(cfg) as conn:
            results["locks"] = get_locks(conn)
            results["spill"] = get_spill(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "incident", "results": results})
    else:
        print_table(results["locks"], title="Locks")
        print_table(results["spill"], title="Spill to Disk")


@incident_app.command("locks")
def incident_locks(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show queries that are blocking each other."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_locks(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "locks", "rows": rows})
    else:
        if not rows:
            console.print("[green]No locks detected.[/green]")
        else:
            print_table(rows, title="Lock Status")


@incident_app.command("spill")
def incident_spill(
    hours: int = typer.Option(24, "--hours", "-h", help="Look-back window in hours."),
    limit: int = typer.Option(20, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show queries that spilled to disk recently."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_spill(conn, hours=hours, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "spill", "hours": hours, "rows": rows})
    else:
        if not rows:
            console.print(f"[green]No spill to disk in the last {hours}h.[/green]")
        else:
            print_table(rows, title=f"Queries Spilled to Disk (last {hours}h)")


# ---------------------------------------------------------------------------
# maintain commands
# ---------------------------------------------------------------------------


@maintain_app.callback()
def maintain_callback(ctx: typer.Context, json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON.")) -> None:
    """Maintenance: run all maintenance checks when called without a subcommand."""
    if ctx.invoked_subcommand is not None:
        return

    cfg = _get_cfg()
    results: dict = {}
    try:
        with get_connection(cfg) as conn:
            results["stale_tables"] = get_stale_tables(conn)
            results["audit"] = get_audit(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "maintain", "results": results})
    else:
        print_table(results["stale_tables"], title="Stale Backup Tables")
        print_table(results["audit"], title="DDL Audit Log (past 7 days)")


@maintain_app.command("stale-tables")
def maintain_stale_tables(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Find tables that appear to be stale backups (_bck_, _tmp_, _old_, etc.)."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_stale_tables(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "stale_tables", "rows": rows})
    else:
        if not rows:
            console.print("[green]No stale backup tables found.[/green]")
        else:
            print_table(rows, title="Stale Backup Tables")


@maintain_app.command("audit")
def maintain_audit(
    hours: int = typer.Option(168, "--hours", "-h", help="Look-back window in hours (default: 168 = 7 days)."),
    limit: int = typer.Option(100, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show recent DDL operations (CREATE, DROP, ALTER, GRANT, REVOKE)."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_audit(conn, hours=hours, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "audit", "hours": hours, "rows": rows})
    else:
        if not rows:
            console.print(f"[green]No DDL operations in the last {hours}h.[/green]")
        else:
            print_table(rows, title=f"DDL Audit Log (last {hours}h)")


# ---------------------------------------------------------------------------
# mcd commands
# ---------------------------------------------------------------------------


@mcd_app.command("etl-status")
def mcd_etl_status(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show MCD ETL entity processing status."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_mcd_etl_status(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "mcd_etl_status", "rows": rows})
    else:
        print_table(rows, title="MCD ETL Entity Status")


@mcd_app.command("etl-log")
def mcd_etl_log(
    hours: int = typer.Option(24, "--hours", "-h", help="Look-back window in hours."),
    limit: int = typer.Option(100, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show MCD ETL execution log with duration."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_mcd_etl_log(conn, hours=hours, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "mcd_etl_log", "hours": hours, "rows": rows})
    else:
        print_table(rows, title=f"MCD ETL Audit Log (last {hours}h)")


@mcd_app.command("value-check")
def mcd_value_check(
    all_tables: bool = typer.Option(False, "--all", "-a", help="Show all tables, not just stale ones."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show tables where max column value is behind the expected date (data freshness check)."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_mcd_value_check(conn, all_tables=all_tables)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "mcd_value_check", "all_tables": all_tables, "rows": rows})
    else:
        if not rows:
            console.print("[green]All tables are up to date.[/green]")
        else:
            title = "All Tables — Data Freshness" if all_tables else "Stale Tables (behind expected date)"
            print_table(rows, title=title)


@mcd_app.command("etl-missing")
def mcd_etl_missing(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show tables in ETL config that have not successfully run today."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_mcd_etl_missing(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "mcd_etl_missing", "rows": rows})
    else:
        if not rows:
            console.print("[green]All configured ETL tables have run successfully today.[/green]")
        else:
            print_table(rows, title="ETL Missing / Failed (not successfully run today)")


# ---------------------------------------------------------------------------
# Composite commands
# ---------------------------------------------------------------------------


@app.command()
def daily(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Run all daily checks: wlm, etl-failures, mcd etl-status, mcd etl-log, mcd value-check."""
    cfg = _get_cfg()
    results: dict = {}
    try:
        with get_connection(cfg) as conn:
            results["wlm"] = get_wlm_status(conn)
            results["etl_failures"] = get_etl_failures(conn)
            results["mcd_etl_status"] = get_mcd_etl_status(conn)
            results["mcd_etl_log"] = get_mcd_etl_log(conn)
            results["mcd_value_check"] = get_mcd_value_check(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "daily", "results": results})
    else:
        print_table(results["wlm"], title="WLM (last 24h)")
        print_table(results["etl_failures"], title="COPY Load Errors (last 24h)")
        print_table(results["mcd_etl_status"], title="MCD ETL Status")
        print_table(results["mcd_etl_log"], title="MCD ETL Log (last 24h)")
        if not results["mcd_value_check"]:
            console.print("[green]Value Check: all tables up to date.[/green]")
        else:
            print_table(results["mcd_value_check"], title="Value Check — Stale Tables")


@app.command()
def weekly(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Run all weekly checks: long-queries, stale-tables, audit."""
    cfg = _get_cfg()
    results: dict = {}
    try:
        with get_connection(cfg) as conn:
            results["long_queries"] = get_long_running_queries(conn)
            results["stale_tables"] = get_stale_tables(conn)
            results["audit"] = get_audit(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "weekly", "results": results})
    else:
        print_table(results["long_queries"], title="Long-Running Queries (past 14 days)")
        print_table(results["stale_tables"], title="Stale Backup Tables")
        print_table(results["audit"], title="DDL Audit Log (past 7 days)")


@app.command()
def monthly(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Run all monthly checks: disk, table-health, skew."""
    cfg = _get_cfg()
    results: dict = {}
    try:
        with get_connection(cfg) as conn:
            results["disk"] = get_disk_usage(conn)
            results["table_health"] = get_table_health(conn)
            results["skew"] = get_skew(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "monthly", "results": results})
    else:
        print_dict_as_table(results["disk"]["cluster_summary"], title="Cluster Disk Usage")
        print_table(results["disk"]["top_tables"], title="Top Tables by Size")
        print_table(results["table_health"], title="Tables Needing VACUUM / ANALYZE (report only)")
        print_table(results["skew"], title="Tables with Data Skew")


# ---------------------------------------------------------------------------
# explain + config
# ---------------------------------------------------------------------------

_COMMAND_TO_CLI: dict[str, str] = {
    "long_queries":     "jack check long-queries",
    "disk":             "jack check disk",
    "etl_failures":     "jack check etl-failures",
    "table_health":     "jack check table-health",
    "wlm":              "jack check wlm",
    "skew":             "jack check skew",
    "locks":            "jack incident locks",
    "spill":            "jack incident spill",
    "deps":             "jack check deps",
    "stale_tables":     "jack maintain stale-tables",
    "audit":            "jack maintain audit",
    "mcd_etl_status":   "jack mcd etl-status",
    "mcd_etl_log":      "jack mcd etl-log",
    "mcd_value_check":  "jack mcd value-check",
    "mcd_etl_missing":  "jack mcd etl-missing",
    "incident":         "jack incident",
    "maintain":         "jack maintain",
    "daily":            "jack daily",
    "weekly":           "jack weekly",
    "monthly":          "jack monthly",
}


def _data_to_markdown_tables(data: dict) -> str:
    import datetime as _dt
    from decimal import Decimal as _Dec

    def _cell(v: object) -> str:
        if v is None:
            return "-"
        if isinstance(v, (_dt.datetime, _dt.date)):
            return v.isoformat()
        if isinstance(v, _Dec):
            return str(float(v))
        return str(v)

    def _rows_to_md(rows: list, title: str = "") -> str:
        if not rows or not isinstance(rows, list) or not isinstance(rows[0], dict):
            return ""
        header = list(rows[0].keys())
        lines = []
        if title:
            lines.append(f"### {title}")
        lines.append("| " + " | ".join(header) + " |")
        lines.append("| " + " | ".join("---" for _ in header) + " |")
        for row in rows:
            lines.append("| " + " | ".join(_cell(row.get(h)) for h in header) + " |")
        return "\n".join(lines)

    sections = []
    results = data.get("results") or data.get("data")
    if isinstance(results, dict):
        for key, val in results.items():
            if isinstance(val, list):
                sections.append(_rows_to_md(val, title=key))
            elif isinstance(val, dict):
                for sub_key, sub_val in val.items():
                    if isinstance(sub_val, list):
                        sections.append(_rows_to_md(sub_val, title=f"{key} / {sub_key}"))
    elif isinstance(data.get("rows"), list):
        sections.append(_rows_to_md(data["rows"]))
    elif isinstance(data.get("data"), dict):
        d = data["data"]
        for key, val in d.items():
            if isinstance(val, list):
                sections.append(_rows_to_md(val, title=key))
            elif isinstance(val, dict):
                sections.append(_rows_to_md([val], title=key))

    return "\n\n".join(s for s in sections if s)


@app.command()
def explain(
    fmt: str = typer.Option("markdown", "--format", "-f", help="Output format: markdown or plain."),
) -> None:
    """Read JSON from stdin (jack --json output) and generate an AI report via AWS Bedrock."""
    import datetime as _dt

    if sys.stdin.isatty():
        console.print(
            "[bold red]Error:[/bold red] No input detected.\n"
            "[dim]Usage: jack check disk --json | jack explain[/dim]"
        )
        raise typer.Exit(code=1)

    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        console.print(f"[bold red]Invalid JSON input:[/bold red] {exc}")
        raise typer.Exit(code=1)

    command = data.get("command", "unknown")
    cli_cmd = _COMMAND_TO_CLI.get(command, f"jack {command}")
    generated_at = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        report = call_bedrock(command=command, data=data, output_format=fmt)
    except RuntimeError as exc:
        console.print(f"[bold red]Bedrock error:[/bold red] {exc}")
        raise typer.Exit(code=1)

    table_md = _data_to_markdown_tables(data)

    header = (
        f"# jack explain report\n\n"
        f"- **Command**: `{cli_cmd} --json | jack explain`\n"
        f"- **Generated**: {generated_at}\n\n"
        f"---\n\n"
    )
    data_section = f"\n\n---\n\n## 原始資料\n\n{table_md}" if table_md else ""
    full_report = header + report + data_section

    timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"jack_report_{command}_{timestamp}.md"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(full_report)

    print(full_report)
    console.print(f"\n[dim]Report saved: {filename}[/dim]")


@app.command()
def config() -> None:
    """Show current effective configuration (Redshift + Bedrock)."""
    redshift_vars = {
        "REDSHIFT_HOST":     os.environ.get("REDSHIFT_HOST", "[not set]"),
        "REDSHIFT_PORT":     os.environ.get("REDSHIFT_PORT", "5439 (default)"),
        "REDSHIFT_DATABASE": os.environ.get("REDSHIFT_DATABASE", "[not set]"),
        "REDSHIFT_USER":     os.environ.get("REDSHIFT_USER", "[not set]"),
        "REDSHIFT_PASSWORD": "***" if os.environ.get("REDSHIFT_PASSWORD") else "[not set]",
    }
    bedrock_vars = {
        "AWS_DEFAULT_REGION":    os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1 (default)"),
        "AWS_ACCESS_KEY_ID":     "***" if os.environ.get("AWS_ACCESS_KEY_ID") else "[not set]",
        "AWS_SECRET_ACCESS_KEY": "***" if os.environ.get("AWS_SECRET_ACCESS_KEY") else "[not set]",
        "BEDROCK_MODEL":         os.environ.get("BEDROCK_MODEL", "apac.amazon.nova-pro-v1:0 (default)"),
    }

    print_dict_as_table(redshift_vars, title="Redshift Configuration")
    print_dict_as_table(bedrock_vars, title="Bedrock Configuration")


if __name__ == "__main__":
    app()
