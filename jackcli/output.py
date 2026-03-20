"""Output formatting: human-readable Rich tables or machine-readable JSON."""

from __future__ import annotations

import json
import sys
from datetime import datetime, date
from decimal import Decimal
from typing import Any

from rich.console import Console
from rich.table import Table

console = Console(stderr=True)


# ---------------------------------------------------------------------------
# JSON serialisation helpers
# ---------------------------------------------------------------------------

class _Encoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def print_json(data: Any) -> None:
    print(json.dumps(data, cls=_Encoder, ensure_ascii=False, indent=2))


# ---------------------------------------------------------------------------
# Rich table helpers
# ---------------------------------------------------------------------------

def print_table(rows: list[dict[str, Any]], title: str = "") -> None:
    if not rows:
        console.print(f"[yellow]No data returned.[/yellow]")
        return
    tbl = Table(title=title, show_lines=True)
    for col in rows[0].keys():
        tbl.add_column(col, overflow="fold")
    for row in rows:
        tbl.add_row(*[str(v) if v is not None else "-" for v in row.values()])
    console.print(tbl)


def print_dict_as_table(d: dict[str, Any], title: str = "") -> None:
    tbl = Table(title=title, show_lines=True)
    tbl.add_column("Key")
    tbl.add_column("Value")
    for k, v in d.items():
        tbl.add_row(str(k), str(v) if v is not None else "-")
    console.print(tbl)
