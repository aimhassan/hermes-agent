"""Outcome-aware task ordering for the kanban dispatcher."""

from __future__ import annotations

import sqlite3
import time


def lane_rows(conn: sqlite3.Connection, status: str) -> list[sqlite3.Row]:
    """Unclaimed rows ordered by deadline safety, priority, urgency, risk, and value."""
    now = int(time.time())
    return conn.execute(
        "SELECT t.id, t.assignee FROM tasks t "
        "LEFT JOIN work_specs s ON s.task_id=t.id "
        f"WHERE t.status = '{status}' AND t.claim_lock IS NULL "
        "ORDER BY "
        "CASE WHEN s.deadline_at IS NOT NULL "
        "          AND s.deadline_at-s.safety_margin_seconds<=? THEN 1 ELSE 0 END DESC, "
        "t.priority DESC, "
        "CASE s.urgency WHEN 'critical' THEN 4 WHEN 'high' THEN 3 "
        "     WHEN 'normal' THEN 2 WHEN 'low' THEN 1 ELSE 0 END DESC, "
        "CASE s.risk_class WHEN 'critical' THEN 6 WHEN 'legal' THEN 5 "
        "     WHEN 'financial' THEN 5 WHEN 'safety' THEN 5 WHEN 'high' THEN 4 "
        "     WHEN 'normal' THEN 2 WHEN 'low' THEN 1 ELSE 3 END DESC, "
        "COALESCE(s.value_score, 0) DESC, "
        "CASE WHEN s.deadline_at IS NULL THEN 1 ELSE 0 END, s.deadline_at ASC, t.created_at ASC",
        (now,),
    ).fetchall()
