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
    get_datashare_status,
    get_connections,
    get_copy_status,
    get_vacuum_progress,
    get_locks,
    get_spill,
    get_alerts,
    get_scaling,
    get_skew,
    get_compression,
    get_deps,
    get_stale_tables,
    get_audit,
    get_mcd_etl_status,
    get_mcd_etl_log,
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
    help="Incident response and diagnosis. Run without subcommand for a full report.",
    invoke_without_command=True,
)
maintain_app = typer.Typer(
    help="Maintenance and governance. Run without subcommand for a full report.",
    invoke_without_command=True,
)
mcd_app = typer.Typer(help="MCD-specific custom table queries.")

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
    threshold: float = typer.Option(5.0, "--threshold", "-t", help="Minimum running time in minutes."),
    limit: int = typer.Option(20, "--limit", "-l", help="Maximum rows to return."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show queries running longer than THRESHOLD minutes."""
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
            console.print(f"[green]No queries running longer than {threshold} minutes.[/green]")
        else:
            print_table(rows, title=f"Long-Running Queries (>{threshold} min)")


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
        print_dict_as_table(data["cluster_summary"], title="Cluster Disk Summary")
        print_table(data["top_tables"], title=f"Top {limit} Tables by Size")


@check_app.command("etl-failures")
def check_etl_failures(
    hours: int = typer.Option(24, "--hours", "-h", help="Look-back window in hours."),
    limit: int = typer.Option(50, "--limit", "-l", help="Maximum rows per error type."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show ETL load errors and query failures in the last HOURS hours."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            data = get_etl_failures(conn, hours=hours, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "etl_failures", "hours": hours, "data": data})
    else:
        print_table(data["load_errors"], title=f"Load Errors (last {hours}h)")
        print_table(data["query_errors"], title=f"Query Errors (last {hours}h)")


@check_app.command("table-health")
def check_table_health(
    stats_threshold: float = typer.Option(10.0, "--stats", help="Flag tables with stats staleness > N%."),
    unsorted_threshold: float = typer.Option(10.0, "--unsorted", help="Flag tables with unsorted ratio > N%."),
    limit: int = typer.Option(30, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show tables that need VACUUM or ANALYZE attention."""
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
            console.print("[green]All tables are healthy (within thresholds).[/green]")
        else:
            print_table(rows, title="Tables Needing VACUUM / ANALYZE")


@check_app.command("wlm")
def check_wlm(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show WLM queue status."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_wlm_status(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "wlm", "rows": rows})
    else:
        print_table(rows, title="WLM Queue Status")


@check_app.command("datashares")
def check_datashares(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """List DataShare definitions visible on this cluster."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_datashare_status(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "datashares", "rows": rows})
    else:
        if not rows:
            console.print("[yellow]No DataShares found.[/yellow]")
        else:
            print_table(rows, title="DataShare Status")


@check_app.command("connections")
def check_connections(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show current cluster connections."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_connections(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "connections", "rows": rows})
    else:
        console.print(f"[bold]Total connections:[/bold] {len(rows)}")
        print_table(rows, title="Active Connections")


@check_app.command("copy-status")
def check_copy_status(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show active COPY job progress."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_copy_status(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "copy_status", "rows": rows})
    else:
        if not rows:
            console.print("[green]No active COPY jobs.[/green]")
        else:
            print_table(rows, title="Active COPY Jobs")


@check_app.command("vacuum-progress")
def check_vacuum_progress(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show ongoing VACUUM progress."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_vacuum_progress(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "vacuum_progress", "rows": rows})
    else:
        if not rows:
            console.print("[green]No VACUUM operations in progress.[/green]")
        else:
            print_table(rows, title="VACUUM Progress")


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


@check_app.command("compression")
def check_compression(
    min_size_mb: int = typer.Option(100, "--min-size", help="Only check tables larger than N MB."),
    limit: int = typer.Option(50, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show columns with no or raw encoding on large tables."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_compression(conn, min_size_mb=min_size_mb, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "compression", "rows": rows})
    else:
        if not rows:
            console.print("[green]No uncompressed columns found on large tables.[/green]")
        else:
            print_table(rows, title="Columns Without Compression")


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
            results["alerts"] = get_alerts(conn)
            results["long_queries"] = get_long_running_queries(conn)
            results["scaling"] = get_scaling(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "incident", "results": results})
    else:
        print_table(results["locks"], title="Locks")
        print_table(results["spill"], title="Spill to Disk")
        print_table(results["alerts"], title="Optimizer Alerts")
        print_table(results["long_queries"], title="Long-Running Queries")
        print_table(results["scaling"], title="Concurrency Scaling")


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


@incident_app.command("terminate")
def incident_terminate(
    pid: int = typer.Argument(..., help="Process ID to terminate."),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt."),
) -> None:
    """Terminate a query by process ID."""
    if not force:
        confirm = typer.confirm(f"Terminate process {pid}?")
        if not confirm:
            raise typer.Abort()

    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                result = cur.fetchone()
                success = result[0] if result else False
    except Exception as exc:
        _handle_errors(exc)
        return

    if success:
        console.print(f"[green]Process {pid} terminated successfully.[/green]")
    else:
        console.print(f"[yellow]Process {pid} not found or already finished.[/yellow]")


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


@incident_app.command("alerts")
def incident_alerts(
    hours: int = typer.Option(24, "--hours", "-h", help="Look-back window in hours."),
    limit: int = typer.Option(50, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show Redshift optimizer alert events."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_alerts(conn, hours=hours, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "alerts", "hours": hours, "rows": rows})
    else:
        if not rows:
            console.print(f"[green]No optimizer alerts in the last {hours}h.[/green]")
        else:
            print_table(rows, title=f"Optimizer Alerts (last {hours}h)")


@incident_app.command("scaling")
def incident_scaling(
    limit: int = typer.Option(50, "--limit", "-l", help="Maximum rows."),
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Show concurrency scaling usage history."""
    cfg = _get_cfg()
    try:
        with get_connection(cfg) as conn:
            rows = get_scaling(conn, limit=limit)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "scaling", "rows": rows})
    else:
        if not rows:
            console.print("[green]No concurrency scaling events found.[/green]")
        else:
            print_table(rows, title="Concurrency Scaling Usage")


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
            results["table_health"] = get_table_health(conn)
            results["vacuum_progress"] = get_vacuum_progress(conn)
            results["stale_tables"] = get_stale_tables(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "maintain", "results": results})
    else:
        print_table(results["table_health"], title="Tables Needing VACUUM / ANALYZE")
        print_table(results["vacuum_progress"], title="VACUUM Progress")
        print_table(results["stale_tables"], title="Stale Backup Tables")


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
    hours: int = typer.Option(24, "--hours", "-h", help="Look-back window in hours."),
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
    """Show MCD ETL entity processing status (custom table)."""
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
    """Show MCD ETL execution log with duration (custom table)."""
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


# ---------------------------------------------------------------------------
# Top-level commands
# ---------------------------------------------------------------------------


@app.command()
def morning(
    json_out: bool = typer.Option(False, "--json", "-j", help="Output as JSON."),
) -> None:
    """Run all morning routine checks in one shot."""
    cfg = _get_cfg()
    results: dict = {}
    try:
        with get_connection(cfg) as conn:
            results["disk"] = get_disk_usage(conn)
            results["connections"] = get_connections(conn)
            results["long_queries"] = get_long_running_queries(conn)
            results["wlm"] = get_wlm_status(conn)
            results["etl_failures"] = get_etl_failures(conn)
            results["copy_status"] = get_copy_status(conn)
            results["table_health"] = get_table_health(conn)
            results["vacuum_progress"] = get_vacuum_progress(conn)
    except Exception as exc:
        _handle_errors(exc)
        return

    if json_out:
        print_json({"command": "morning", "results": results})
    else:
        print_dict_as_table(results["disk"]["cluster_summary"], title="Disk Summary")
        console.print(f"[bold]Connections:[/bold] {len(results['connections'])}")
        print_table(results["long_queries"], title="Long-Running Queries")
        print_table(results["wlm"], title="WLM Status")
        print_table(results["etl_failures"]["load_errors"], title="Load Errors (24h)")
        print_table(results["copy_status"], title="Active COPY Jobs")
        print_table(results["table_health"], title="Tables Needing Maintenance")
        print_table(results["vacuum_progress"], title="VACUUM Progress")


_COMMAND_TO_CLI: dict[str, str] = {
    "long_queries":    "jack check long-queries",
    "disk":            "jack check disk",
    "etl_failures":    "jack check etl-failures",
    "table_health":    "jack check table-health",
    "wlm":             "jack check wlm",
    "datashares":      "jack check datashares",
    "connections":     "jack check connections",
    "copy_status":     "jack check copy-status",
    "vacuum_progress": "jack check vacuum-progress",
    "locks":           "jack incident locks",
    "spill":           "jack incident spill",
    "alerts":          "jack incident alerts",
    "scaling":         "jack incident scaling",
    "skew":            "jack check skew",
    "compression":     "jack check compression",
    "deps":            "jack check deps",
    "stale_tables":    "jack maintain stale-tables",
    "audit":           "jack maintain audit",
    "mcd_etl_status":  "jack mcd etl-status",
    "mcd_etl_log":     "jack mcd etl-log",
    "morning":         "jack morning",
    "incident":        "jack incident",
    "maintain":        "jack maintain",
}


def _data_to_markdown_tables(data: dict) -> str:
    """Convert JSON payload rows/data into Markdown tables."""
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
    # Handle nested structures (morning / incident / maintain)
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
    import os as _os

    redshift_vars = {
        "REDSHIFT_HOST": _os.environ.get("REDSHIFT_HOST", "[not set]"),
        "REDSHIFT_PORT": _os.environ.get("REDSHIFT_PORT", "5439 (default)"),
        "REDSHIFT_DATABASE": _os.environ.get("REDSHIFT_DATABASE", "[not set]"),
        "REDSHIFT_USER": _os.environ.get("REDSHIFT_USER", "[not set]"),
        "REDSHIFT_PASSWORD": "***" if _os.environ.get("REDSHIFT_PASSWORD") else "[not set]",
    }
    bedrock_vars = {
        "AWS_DEFAULT_REGION": _os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1 (default)"),
        "AWS_ACCESS_KEY_ID": "***" if _os.environ.get("AWS_ACCESS_KEY_ID") else "[not set]",
        "AWS_SECRET_ACCESS_KEY": "***" if _os.environ.get("AWS_SECRET_ACCESS_KEY") else "[not set]",
        "BEDROCK_MODEL": _os.environ.get("BEDROCK_MODEL", "amazon.nova-pro-v1:0"),
    }

    print_dict_as_table(redshift_vars, title="Redshift Configuration")
    print_dict_as_table(bedrock_vars, title="Bedrock Configuration")


if __name__ == "__main__":
    app()
