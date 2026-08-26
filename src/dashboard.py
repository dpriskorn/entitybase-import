"""FastAPI dashboard for monitoring import progress."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

DB_PATH = "import_state.db"


def get_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def compute_run_stats(run: dict, db_path: str) -> dict:
    """Compute live stats for a run from the entities table."""
    conn = get_db(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT status, COUNT(*) as count
            FROM entities
            WHERE run_id = ?
            GROUP BY status
            """,
            (run["run_id"],),
        )
        status_counts = {row["status"]: row["count"] for row in cursor.fetchall()}
    finally:
        conn.close()

    processed = sum(
        status_counts.get(s, 0) for s in ("success", "failed", "skipped")
    )
    total = run["total_entities"]
    start_time = run["start_time"]

    rate_per_second = 0.0
    eta_seconds: Optional[float] = None
    eta_formatted = "N/A"
    elapsed_seconds = 0.0

    if start_time:
        try:
            start_dt = datetime.fromisoformat(start_time)
            elapsed_seconds = (datetime.now() - start_dt).total_seconds()
        except (ValueError, TypeError):
            elapsed_seconds = 0.0

    if elapsed_seconds > 0 and processed > 0:
        rate_per_second = processed / elapsed_seconds
        remaining = total - processed
        if rate_per_second > 0:
            eta_seconds = remaining / rate_per_second
            eta_formatted = _format_eta(eta_seconds)

    rate_per_minute = rate_per_second * 60
    rate_per_hour = rate_per_second * 3600

    is_active = run["end_time"] is None
    percent = (processed / total * 100) if total > 0 else 0

    return {
        "run_id": run["run_id"],
        "is_active": is_active,
        "jsonl_file": run["jsonl_file"],
        "api_url": run["api_url"],
        "concurrency": run["concurrency"],
        "start_time": start_time,
        "end_time": run["end_time"],
        "total": total,
        "processed": processed,
        "success": status_counts.get("success", 0),
        "failed": status_counts.get("failed", 0),
        "skipped": status_counts.get("skipped", 0),
        "pending": status_counts.get("pending", 0) + status_counts.get("processing", 0),
        "percent": round(percent, 2),
        "rate_per_second": round(rate_per_second, 2),
        "rate_per_minute": round(rate_per_minute, 2),
        "rate_per_hour": round(rate_per_hour, 0),
        "elapsed_seconds": round(elapsed_seconds, 1),
        "elapsed_formatted": _format_elapsed(elapsed_seconds),
        "eta_seconds": eta_seconds,
        "eta_formatted": eta_formatted,
    }


def _format_eta(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        hours = seconds / 3600
        mins = (seconds % 3600) / 60
        return f"{hours:.0f}h {mins:.0f}m"


def _format_elapsed(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def create_app(db_path: str = DB_PATH) -> FastAPI:
    app = FastAPI(title="EntityBase Import Dashboard")

    @app.get("/api/status")
    def api_status() -> JSONResponse:
        if not Path(db_path).exists():
            return JSONResponse(
                {"active": None, "last": None, "message": "No database found"}
            )

        conn = get_db(db_path)
        try:
            cursor = conn.execute(
                "SELECT * FROM import_runs ORDER BY run_id DESC LIMIT 1"
            )
            row = cursor.fetchone()
        finally:
            conn.close()

        if not row:
            return JSONResponse({"active": None, "last": None, "message": "No runs found"})

        run = dict(row)
        stats = compute_run_stats(run, db_path)

        if stats["is_active"]:
            return JSONResponse({"active": stats, "last": None})

        return JSONResponse({"active": None, "last": stats})

    @app.get("/api/runs")
    def api_runs() -> JSONResponse:
        if not Path(db_path).exists():
            return JSONResponse([])

        conn = get_db(db_path)
        try:
            cursor = conn.execute(
                "SELECT * FROM import_runs ORDER BY run_id DESC LIMIT 20"
            )
            runs = [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

        return JSONResponse(runs)

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        html_path = Path(__file__).parent / "templates" / "dashboard.html"
        return HTMLResponse(html_path.read_text())

    return app


def run_dashboard(db_path: str = DB_PATH, host: str = "0.0.0.0", port: int = 80) -> None:
    import uvicorn

    app = create_app(db_path)
    print(f"Dashboard running at http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")
