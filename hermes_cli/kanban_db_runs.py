"""Run history reads for the kanban database facade."""

from __future__ import annotations

from typing import Any, Iterable, Optional


def list_runs(
    conn, task_id: str, *, include_active: bool = True,
    state_type: Optional[str] = None, state_name: Optional[str] = None,
):
    """Return task runs in start order with optional state filtering."""
    from hermes_cli import kanban_db as kb

    if (state_type is None) ^ (state_name is None):
        raise ValueError("state_type and state_name must both be set or both omitted")
    if state_type is not None and state_type not in ("status", "outcome"):
        raise ValueError("state_type must be 'status' or 'outcome'")
    query = "SELECT * FROM task_runs WHERE task_id = ?"
    params: list[Any] = [task_id]
    if not include_active:
        query += " AND ended_at IS NOT NULL"
    if state_type is not None:
        query += f" AND {state_type} = ?"
        params.append(state_name)
    rows = conn.execute(query + " ORDER BY started_at ASC, id ASC", params).fetchall()
    return [kb.Run.from_row(row) for row in rows]


def get_run(conn, run_id: int):
    from hermes_cli import kanban_db as kb

    row = conn.execute("SELECT * FROM task_runs WHERE id = ?", (int(run_id),)).fetchone()
    return kb.Run.from_row(row) if row else None


def latest_run(conn, task_id: str):
    """Return the most recent run regardless of outcome."""
    from hermes_cli import kanban_db as kb

    row = conn.execute(
        "SELECT * FROM task_runs WHERE task_id = ? "
        "ORDER BY started_at DESC, id DESC LIMIT 1", (task_id,),
    ).fetchone()
    return kb.Run.from_row(row) if row else None


def latest_summary(conn, task_id: str) -> Optional[str]:
    row = conn.execute(
        "SELECT summary FROM task_runs WHERE task_id = ? "
        "AND summary IS NOT NULL AND summary != '' "
        "ORDER BY COALESCE(ended_at, started_at) DESC, id DESC LIMIT 1", (task_id,),
    ).fetchone()
    return row["summary"] if row else None


def latest_summaries(conn, task_ids: Iterable[str]) -> dict[str, str]:
    """Return each task's newest non-empty run summary in one query."""
    ids = list(task_ids)
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT task_id, summary FROM (
            SELECT task_id, summary,
                   ROW_NUMBER() OVER (
                       PARTITION BY task_id
                       ORDER BY COALESCE(ended_at, started_at) DESC, id DESC
                   ) AS rn
              FROM task_runs
             WHERE task_id IN ({placeholders})
               AND summary IS NOT NULL AND summary != ''
        ) WHERE rn = 1
        """,
        ids,
    ).fetchall()
    return {row["task_id"]: row["summary"] for row in rows}


def current_run_started_ats(conn, task_ids: Iterable[str]) -> dict[str, int]:
    """Return the start time for each task's current run."""
    ids = list(task_ids)
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        "SELECT t.id AS task_id, r.started_at AS started_at FROM tasks t "
        "JOIN task_runs r ON r.id = t.current_run_id "
        f"WHERE t.id IN ({placeholders})",
        ids,
    ).fetchall()
    return {row["task_id"]: row["started_at"] for row in rows}
