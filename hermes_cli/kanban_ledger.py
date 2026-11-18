"""Canonical work identity, decisions, projections and acceptance evidence.

Kanban tasks remain the work object and ``task_events`` remain its immutable
history. This sibling supplies the missing cross-system contracts without a
second task store: source readers deduplicate by immutable event id, external
systems map to one task, decisions close with an action, and completion is
gated by independently verified evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional


LEDGER_SCHEMA_SQL = """
-- Canonical source identity for work entering the board. Readers insert by
-- immutable source event id, never by a moving recent-since window.
CREATE TABLE IF NOT EXISTS work_source_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source         TEXT NOT NULL,
    account        TEXT NOT NULL DEFAULT '',
    event_id       TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    task_id        TEXT,
    disposition    TEXT NOT NULL,
    occurred_at    INTEGER,
    raw_ref        TEXT,
    payload_sha256 TEXT,
    ignored_reason TEXT,
    ingested_at    INTEGER NOT NULL,
    UNIQUE (source, account, event_id)
);

CREATE TABLE IF NOT EXISTS work_source_cursors (
    source                TEXT NOT NULL,
    account               TEXT NOT NULL DEFAULT '',
    stream                TEXT NOT NULL DEFAULT 'default',
    cursor                TEXT NOT NULL,
    last_event_at         INTEGER,
    backlog               INTEGER NOT NULL DEFAULT 0,
    freshness_sla_seconds INTEGER,
    updated_at            INTEGER NOT NULL,
    PRIMARY KEY (source, account, stream)
);

-- Machine-readable outcome, deadline and risk information for scheduling and
-- acceptance. The task title/body remain the human-facing brief.
CREATE TABLE IF NOT EXISTS work_specs (
    task_id               TEXT PRIMARY KEY,
    objective             TEXT NOT NULL,
    expected_result       TEXT NOT NULL,
    deadline_at           INTEGER,
    safety_margin_seconds INTEGER NOT NULL DEFAULT 0,
    urgency               TEXT NOT NULL DEFAULT 'normal',
    risk_class            TEXT NOT NULL DEFAULT 'normal',
    value_score           REAL NOT NULL DEFAULT 0,
    revision              INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL
);

-- External systems are projections of one canonical task.
CREATE TABLE IF NOT EXISTS work_projections (
    system         TEXT NOT NULL,
    external_id    TEXT NOT NULL,
    task_id        TEXT NOT NULL,
    state_hash     TEXT,
    last_synced_at INTEGER NOT NULL,
    PRIMARY KEY (system, external_id)
);

-- Questions have one durable identity across channels.
CREATE TABLE IF NOT EXISTS work_decisions (
    id               TEXT PRIMARY KEY,
    task_id          TEXT NOT NULL,
    question         TEXT NOT NULL,
    status           TEXT NOT NULL,
    answer           TEXT,
    action_ref       TEXT,
    no_action_reason TEXT,
    source_ref       TEXT,
    created_at       INTEGER NOT NULL,
    answered_at      INTEGER,
    updated_at       INTEGER NOT NULL
);

-- Domain-specific acceptance checks and their evidence receipts.
CREATE TABLE IF NOT EXISTS work_acceptance_requirements (
    task_id           TEXT NOT NULL,
    requirement_key   TEXT NOT NULL,
    description       TEXT NOT NULL,
    required          INTEGER NOT NULL DEFAULT 1,
    expected_reviewer TEXT,
    status            TEXT NOT NULL DEFAULT 'pending',
    evidence          TEXT,
    artifact_sha256   TEXT,
    artifact_size     INTEGER,
    verified_by       TEXT,
    verified_at       INTEGER,
    created_at        INTEGER NOT NULL,
    PRIMARY KEY (task_id, requirement_key)
);

-- Active blockers carry the operational facts needed to resolve or escalate
-- them. Resolved rows remain as history.
CREATE TABLE IF NOT EXISTS work_blocks (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id             TEXT NOT NULL,
    kind                TEXT NOT NULL,
    reason              TEXT,
    responsible_party   TEXT NOT NULL,
    unblock_condition   TEXT NOT NULL,
    next_check_at       INTEGER,
    escalation_at       INTEGER,
    affected_ref        TEXT,
    opened_at           INTEGER NOT NULL,
    resolved_at         INTEGER,
    resolved_by         TEXT,
    resolution          TEXT
);

-- Scope ownership is a live lease, not a name in prose. Binding is opt-in so
-- existing boards retain their current routing until migrated deliberately.
CREATE TABLE IF NOT EXISTS work_owner_leases (
    scope            TEXT PRIMARY KEY,
    owner            TEXT NOT NULL,
    backup_owner     TEXT,
    policy           TEXT,
    lease_expires_at INTEGER NOT NULL,
    heartbeat_at     INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    generation       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS work_task_owner_scopes (
    task_id  TEXT PRIMARY KEY,
    scope    TEXT NOT NULL,
    bound_at INTEGER NOT NULL
);

-- User response to an outcome or interruption. Raw edited text stays in its
-- source; the ledger records only classification, safe metadata, and a digest.
CREATE TABLE IF NOT EXISTS work_feedback (
    feedback_id       TEXT PRIMARY KEY,
    task_id           TEXT,
    outcome           TEXT NOT NULL,
    channel           TEXT,
    category          TEXT,
    correction_sha256 TEXT,
    metadata          TEXT,
    created_at        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_source_events_task    ON work_source_events(task_id, ingested_at);
CREATE INDEX IF NOT EXISTS idx_source_events_time    ON work_source_events(source, ingested_at);
CREATE INDEX IF NOT EXISTS idx_source_cursors_health ON work_source_cursors(updated_at, backlog);
CREATE INDEX IF NOT EXISTS idx_work_specs_queue       ON work_specs(deadline_at, urgency, risk_class, value_score);
CREATE INDEX IF NOT EXISTS idx_projections_task      ON work_projections(task_id, system);
CREATE INDEX IF NOT EXISTS idx_decisions_task        ON work_decisions(task_id, status);
CREATE INDEX IF NOT EXISTS idx_acceptance_task       ON work_acceptance_requirements(task_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_blocks_one_active ON work_blocks(task_id) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_blocks_health         ON work_blocks(resolved_at, next_check_at, escalation_at);
CREATE INDEX IF NOT EXISTS idx_task_owner_scope      ON work_task_owner_scopes(scope, task_id);
CREATE INDEX IF NOT EXISTS idx_feedback_outcome      ON work_feedback(outcome, created_at);
CREATE INDEX IF NOT EXISTS idx_feedback_task         ON work_feedback(task_id, created_at);
"""


SOURCE_DISPOSITIONS = frozenset({"created", "linked", "ignored"})
DECISION_STATUSES = frozenset({"open", "answered", "cancelled"})
ACCEPTANCE_STATUSES = frozenset({"pending", "passed", "failed", "waived"})
URGENCY_LEVELS = frozenset({"low", "normal", "high", "critical"})

_BLOCK_DEFAULT_PARTY = {
    "dependency": "parent-task",
    "needs_input": "user",
    "external": "external-party",
    "authentication": "operator",
    "capability": "operator",
    "quota": "provider",
    "technical": "system-owner",
    "transient": "system-owner",
    "untyped": "operator",
}


class LedgerConflict(ValueError):
    """An immutable external identity was presented with different facts."""


@dataclass(frozen=True)
class SourceIngestResult:
    source_event_id: int
    task_id: Optional[str]
    disposition: str
    created: bool


def _required_text(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{label} is required")
    return text


def _json_digest(payload: Any) -> Optional[str]:
    if payload is None:
        return None
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _source_identity(source: str, account: Optional[str], event_id: str) -> tuple[str, str, str]:
    return _required_text(source, "source"), str(account or "").strip(), _required_text(event_id, "event_id")


def _existing_source_result(
    row,
    *,
    event_type: str,
    raw_ref: Optional[str],
    payload_sha256: Optional[str],
    expected_disposition: str,
    expected_task_id: Optional[str],
    ignored_reason: Optional[str],
) -> SourceIngestResult:
    immutable = {
        "event_type": event_type,
        "raw_ref": raw_ref,
        "payload_sha256": payload_sha256,
    }
    conflicts = {
        key: (row[key], value) for key, value in immutable.items()
        if row[key] is not None and value is not None and row[key] != value
    }
    if conflicts:
        names = ", ".join(sorted(conflicts))
        raise LedgerConflict(f"source event was already ingested with different {names}")
    if row["disposition"] != expected_disposition:
        raise LedgerConflict(
            "source event was already ingested with disposition "
            f"{row['disposition']!r}, not {expected_disposition!r}"
        )
    if expected_task_id is not None and row["task_id"] != expected_task_id:
        raise LedgerConflict(
            f"source event was already linked to task {row['task_id']!r}, not {expected_task_id!r}"
        )
    if expected_disposition == "ignored" and row["ignored_reason"] != ignored_reason:
        raise LedgerConflict("source event was already ignored for a different reason")
    return SourceIngestResult(
        source_event_id=int(row["id"]), task_id=row["task_id"],
        disposition=row["disposition"], created=False,
    )


def ingest_source_event(
    conn,
    *,
    source: str,
    event_id: str,
    event_type: str,
    account: Optional[str] = None,
    occurred_at: Optional[int] = None,
    raw_ref: Optional[str] = None,
    payload: Any = None,
    payload_digest: Optional[str] = None,
    task_id: Optional[str] = None,
    task: Optional[Mapping[str, Any]] = None,
    ignored_reason: Optional[str] = None,
) -> SourceIngestResult:
    """Record one immutable source event exactly once.

    Choose exactly one disposition: link ``task_id``, provide ``task`` kwargs
    for atomic task creation, or provide ``ignored_reason``. A retry returns the
    first result. Reusing the identity for conflicting content fails loudly.
    """
    source, account, event_id = _source_identity(source, account, event_id)
    event_type = _required_text(event_type, "event_type")
    raw_ref = str(raw_ref).strip() if raw_ref is not None else None
    if payload is not None and payload_digest is not None:
        raise ValueError("provide payload or payload_digest, not both")
    if payload_digest is not None:
        payload_digest = str(payload_digest).strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", payload_digest):
            raise ValueError("payload_digest must be a 64-character SHA256 hex digest")
    payload_sha256 = payload_digest or _json_digest(payload)
    choices = sum((task_id is not None, task is not None, ignored_reason is not None))
    if choices != 1:
        raise ValueError("provide exactly one of task_id, task, or ignored_reason")
    ignored_reason = str(ignored_reason).strip() if ignored_reason is not None else None
    if ignored_reason is not None and not ignored_reason:
        raise ValueError("ignored_reason cannot be blank")
    expected_disposition = (
        "ignored" if ignored_reason is not None else "linked" if task_id is not None else "created"
    )
    expected_task_id = _required_text(task_id, "task_id") if task_id is not None else None

    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn, allow_nested=True):
        row = conn.execute(
            "SELECT * FROM work_source_events WHERE source=? AND account=? AND event_id=?",
            (source, account, event_id),
        ).fetchone()
        if row is not None:
            return _existing_source_result(
                row, event_type=event_type, raw_ref=raw_ref, payload_sha256=payload_sha256,
                expected_disposition=expected_disposition, expected_task_id=expected_task_id,
                ignored_reason=ignored_reason,
            )

        if ignored_reason is not None:
            linked_task_id, disposition = None, "ignored"
        elif task_id is not None:
            linked_task_id = expected_task_id
            if kb.get_task(conn, linked_task_id) is None:
                raise ValueError(f"no such task: {linked_task_id}")
            disposition = "linked"
        else:
            task_kwargs = dict(task or {})
            task_kwargs.setdefault("idempotency_key", f"source:{source}:{account}:{event_id}")
            linked_task_id = kb.create_task(conn, **task_kwargs)
            disposition = "created"

        now = int(time.time())
        cur = conn.execute(
            """
            INSERT INTO work_source_events (
                source, account, event_id, event_type, task_id, disposition,
                occurred_at, raw_ref, payload_sha256, ignored_reason, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source, account, event_id, event_type, linked_task_id, disposition,
                occurred_at, raw_ref, payload_sha256, ignored_reason, now,
            ),
        )
        if linked_task_id is not None:
            kb._append_event(
                conn, linked_task_id, "source_ingested",
                {
                    "source": source, "account": account or None,
                    "event_id": event_id, "event_type": event_type,
                    "disposition": disposition, "occurred_at": occurred_at,
                    "raw_ref": raw_ref, "payload_sha256": payload_sha256,
                },
            )
        return SourceIngestResult(
            source_event_id=int(cur.lastrowid), task_id=linked_task_id,
            disposition=disposition, created=True,
        )


def advance_source_cursor(
    conn,
    *,
    source: str,
    cursor: str,
    previous_cursor: Optional[str],
    account: Optional[str] = None,
    stream: str = "default",
    last_event_at: Optional[int] = None,
    backlog: int = 0,
    freshness_sla_seconds: Optional[int] = None,
) -> bool:
    """Advance a source cursor with compare-and-set semantics.

    The first write requires ``previous_cursor=None``. Every later write must
    name the exact cursor observed by the reader. This turns a race or stale
    checkpoint into a visible conflict instead of a skipped event range.
    """
    source = _required_text(source, "source")
    account = str(account or "").strip()
    stream = _required_text(stream, "stream")
    cursor = _required_text(cursor, "cursor")
    previous_cursor = str(previous_cursor).strip() if previous_cursor is not None else None
    if backlog < 0:
        raise ValueError("backlog must be non-negative")
    if freshness_sla_seconds is not None and freshness_sla_seconds <= 0:
        raise ValueError("freshness_sla_seconds must be positive")

    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        row = conn.execute(
            "SELECT cursor, last_event_at, backlog, freshness_sla_seconds "
            "FROM work_source_cursors WHERE source=? AND account=? AND stream=?",
            (source, account, stream),
        ).fetchone()
        current = row["cursor"] if row is not None else None
        if current != previous_cursor:
            raise LedgerConflict(
                f"source cursor moved for {source}:{account or '*'}:{stream}; "
                f"expected {previous_cursor!r}, found {current!r}"
            )
        effective_last_event_at = (
            last_event_at if last_event_at is not None
            else row["last_event_at"] if row is not None else None
        )
        effective_freshness_sla = (
            freshness_sla_seconds if freshness_sla_seconds is not None
            else row["freshness_sla_seconds"] if row is not None else None
        )
        changed = row is None or (
            current != cursor
            or row["last_event_at"] != effective_last_event_at
            or int(row["backlog"]) != int(backlog)
            or row["freshness_sla_seconds"] != effective_freshness_sla
        )
        now = int(time.time())
        conn.execute(
            """
            INSERT INTO work_source_cursors (
                source, account, stream, cursor, last_event_at, backlog,
                freshness_sla_seconds, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, account, stream) DO UPDATE SET
                cursor=excluded.cursor,
                last_event_at=excluded.last_event_at,
                backlog=excluded.backlog,
                freshness_sla_seconds=excluded.freshness_sla_seconds,
                updated_at=excluded.updated_at
            """,
            (
                source, account, stream, cursor, effective_last_event_at, int(backlog),
                effective_freshness_sla, now,
            ),
        )
        return changed


def sync_projection(conn, *, system: str, external_id: str, task_id: str,
                    state: Any = None) -> bool:
    """Bind one external object to one task. Return True when state changed."""
    system = _required_text(system, "system")
    external_id = _required_text(external_id, "external_id")
    task_id = _required_text(task_id, "task_id")
    state_hash = _json_digest(state)

    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        if kb.get_task(conn, task_id) is None:
            raise ValueError(f"no such task: {task_id}")
        row = conn.execute(
            "SELECT task_id, state_hash FROM work_projections WHERE system=? AND external_id=?",
            (system, external_id),
        ).fetchone()
        if row is not None and row["task_id"] != task_id:
            raise LedgerConflict(
                f"projection {system}:{external_id} already belongs to {row['task_id']}"
            )
        if row is not None and row["state_hash"] == state_hash:
            return False
        now = int(time.time())
        conn.execute(
            """
            INSERT INTO work_projections (system, external_id, task_id, state_hash, last_synced_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(system, external_id) DO UPDATE SET
                state_hash=excluded.state_hash, last_synced_at=excluded.last_synced_at
            """,
            (system, external_id, task_id, state_hash, now),
        )
        kb._append_event(
            conn, task_id, "projection_synced",
            {"system": system, "external_id": external_id, "state_hash": state_hash},
        )
        return True


def set_work_spec(
    conn,
    *,
    task_id: str,
    objective: str,
    expected_result: str,
    deadline_at: Optional[int] = None,
    safety_margin_seconds: int = 0,
    urgency: str = "normal",
    risk_class: str = "normal",
    value_score: float = 0,
    expected_revision: Optional[int] = None,
) -> int:
    """Create or compare-and-set the machine-readable outcome contract."""
    task_id = _required_text(task_id, "task_id")
    objective = _required_text(objective, "objective")
    expected_result = _required_text(expected_result, "expected_result")
    urgency = _required_text(urgency, "urgency").lower()
    risk_class = _required_text(risk_class, "risk_class").lower()
    if urgency not in URGENCY_LEVELS:
        raise ValueError(f"urgency must be one of {sorted(URGENCY_LEVELS)}")
    if deadline_at is not None and int(deadline_at) <= 0:
        raise ValueError("deadline_at must be positive Unix seconds")
    if int(safety_margin_seconds) < 0:
        raise ValueError("safety_margin_seconds must be non-negative")
    if not 0 <= float(value_score) <= 100:
        raise ValueError("value_score must be between 0 and 100")

    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        if kb.get_task(conn, task_id) is None:
            raise ValueError(f"no such task: {task_id}")
        row = conn.execute("SELECT revision FROM work_specs WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            if expected_revision not in (None, 0):
                raise LedgerConflict(f"work spec for {task_id} does not exist")
            revision = 1
        else:
            current = int(row["revision"])
            if expected_revision != current:
                raise LedgerConflict(
                    f"work spec revision moved; expected {expected_revision!r}, found {current}"
                )
            revision = current + 1
        now = int(time.time())
        conn.execute(
            """
            INSERT INTO work_specs (
                task_id, objective, expected_result, deadline_at, safety_margin_seconds,
                urgency, risk_class, value_score, revision, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                objective=excluded.objective, expected_result=excluded.expected_result,
                deadline_at=excluded.deadline_at,
                safety_margin_seconds=excluded.safety_margin_seconds,
                urgency=excluded.urgency, risk_class=excluded.risk_class,
                value_score=excluded.value_score, revision=excluded.revision,
                updated_at=excluded.updated_at
            """,
            (
                task_id, objective, expected_result, deadline_at, int(safety_margin_seconds),
                urgency, risk_class, float(value_score), revision, now,
            ),
        )
        kb._append_event(
            conn, task_id, "work_spec_set",
            {
                "revision": revision, "objective": objective,
                "expected_result": expected_result, "deadline_at": deadline_at,
                "safety_margin_seconds": int(safety_margin_seconds), "urgency": urgency,
                "risk_class": risk_class, "value_score": float(value_score),
            },
        )
        return revision


def open_decision(conn, *, decision_id: str, task_id: str, question: str,
                  source_ref: Optional[str] = None) -> bool:
    """Create one open question across every delivery channel."""
    decision_id = _required_text(decision_id, "decision_id")
    task_id = _required_text(task_id, "task_id")
    question = _required_text(question, "question")
    source_ref = str(source_ref).strip() if source_ref is not None else None

    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        if kb.get_task(conn, task_id) is None:
            raise ValueError(f"no such task: {task_id}")
        row = conn.execute("SELECT * FROM work_decisions WHERE id=?", (decision_id,)).fetchone()
        if row is not None:
            if row["task_id"] != task_id or row["question"] != question:
                raise LedgerConflict(f"decision {decision_id} already exists with different facts")
            return False
        now = int(time.time())
        conn.execute(
            """
            INSERT INTO work_decisions (
                id, task_id, question, status, source_ref, created_at, updated_at
            ) VALUES (?, ?, ?, 'open', ?, ?, ?)
            """,
            (decision_id, task_id, question, source_ref, now, now),
        )
        kb._append_event(
            conn, task_id, "decision_opened",
            {"decision_id": decision_id, "question": question, "source_ref": source_ref},
        )
        return True


def answer_decision(
    conn,
    decision_id: str,
    *,
    answer: str,
    actor: str,
    action_ref: Optional[str] = None,
    no_action_reason: Optional[str] = None,
    source_ref: Optional[str] = None,
) -> bool:
    """Answer or revise a decision, always closing its action semantics."""
    decision_id = _required_text(decision_id, "decision_id")
    answer = _required_text(answer, "answer")
    actor = _required_text(actor, "actor")
    action_ref = str(action_ref).strip() if action_ref is not None else None
    no_action_reason = str(no_action_reason).strip() if no_action_reason is not None else None
    if bool(action_ref) == bool(no_action_reason):
        raise ValueError("provide exactly one of action_ref or no_action_reason")

    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        row = conn.execute("SELECT * FROM work_decisions WHERE id=?", (decision_id,)).fetchone()
        if row is None:
            raise ValueError(f"no such decision: {decision_id}")
        if row["status"] == "cancelled":
            raise ValueError(f"decision {decision_id} is cancelled")
        if action_ref and action_ref.startswith("t_") and kb.get_task(conn, action_ref) is None:
            raise ValueError(f"action_ref names no task: {action_ref}")
        unchanged = (
            row["status"] == "answered" and row["answer"] == answer
            and row["action_ref"] == action_ref
            and row["no_action_reason"] == no_action_reason
        )
        if unchanged:
            return False
        now = int(time.time())
        revision = row["status"] == "answered"
        conn.execute(
            """
            UPDATE work_decisions
               SET status='answered', answer=?, action_ref=?, no_action_reason=?,
                   source_ref=COALESCE(?, source_ref), answered_at=?, updated_at=?
             WHERE id=?
            """,
            (answer, action_ref, no_action_reason, source_ref, now, now, decision_id),
        )
        kb._append_event(
            conn, row["task_id"], "decision_revised" if revision else "decision_answered",
            {
                "decision_id": decision_id, "answer": answer, "actor": actor,
                "action_ref": action_ref, "no_action_reason": no_action_reason,
                "source_ref": source_ref,
            },
        )
        return True


def cancel_decision(conn, decision_id: str, *, reason: str, actor: str) -> bool:
    """Cancel a question with an auditable reason."""
    decision_id = _required_text(decision_id, "decision_id")
    reason = _required_text(reason, "reason")
    actor = _required_text(actor, "actor")

    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        row = conn.execute("SELECT * FROM work_decisions WHERE id=?", (decision_id,)).fetchone()
        if row is None:
            raise ValueError(f"no such decision: {decision_id}")
        if row["status"] == "cancelled":
            return False
        now = int(time.time())
        conn.execute(
            "UPDATE work_decisions SET status='cancelled', no_action_reason=?, updated_at=? WHERE id=?",
            (reason, now, decision_id),
        )
        kb._append_event(
            conn, row["task_id"], "decision_cancelled",
            {"decision_id": decision_id, "reason": reason, "actor": actor},
        )
        return True


def record_block_opened(
    conn,
    *,
    task_id: str,
    kind: Optional[str],
    reason: Optional[str],
    responsible_party: Optional[str] = None,
    unblock_condition: Optional[str] = None,
    next_check_at: Optional[int] = None,
    escalation_at: Optional[int] = None,
    affected_ref: Optional[str] = None,
) -> dict[str, Any]:
    """Record the operational contract for a block. Caller holds a write transaction."""
    normalized_kind = str(kind or "untyped").strip()
    party = str(responsible_party or _BLOCK_DEFAULT_PARTY.get(normalized_kind, "operator")).strip()
    condition = str(unblock_condition or reason or f"Resolve {normalized_kind} blocker").strip()
    if not party or not condition:
        raise ValueError("a block requires responsible_party and unblock_condition")
    if next_check_at is not None and int(next_check_at) <= 0:
        raise ValueError("next_check_at must be positive Unix seconds")
    if escalation_at is not None and int(escalation_at) <= 0:
        raise ValueError("escalation_at must be positive Unix seconds")
    if next_check_at and escalation_at and int(escalation_at) < int(next_check_at):
        raise ValueError("escalation_at cannot be earlier than next_check_at")
    now = int(time.time())
    prior = conn.execute(
        "SELECT id FROM work_blocks WHERE task_id=? AND resolved_at IS NULL", (task_id,),
    ).fetchone()
    if prior is not None:
        conn.execute(
            "UPDATE work_blocks SET resolved_at=?, resolved_by='lifecycle', "
            "resolution='superseded by a new block episode' WHERE id=?",
            (now, int(prior["id"])),
        )
    cur = conn.execute(
        """
        INSERT INTO work_blocks (
            task_id, kind, reason, responsible_party, unblock_condition,
            next_check_at, escalation_at, affected_ref, opened_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id, normalized_kind, reason, party, condition, next_check_at,
            escalation_at, affected_ref, now,
        ),
    )
    details = {
        "block_id": int(cur.lastrowid), "responsible_party": party,
        "unblock_condition": condition, "next_check_at": next_check_at,
        "escalation_at": escalation_at, "affected_ref": affected_ref,
    }
    if prior is not None:
        details["superseded_block_id"] = int(prior["id"])
    return details


def resolve_active_block(conn, task_id: str, *, actor: str, resolution: str) -> bool:
    """Resolve the task's current blocker. Caller holds a write transaction."""
    now = int(time.time())
    return conn.execute(
        "UPDATE work_blocks SET resolved_at=?, resolved_by=?, resolution=? "
        "WHERE task_id=? AND resolved_at IS NULL",
        (now, _required_text(actor, "actor"), _required_text(resolution, "resolution"), task_id),
    ).rowcount == 1


def set_owner_lease(
    conn,
    *,
    scope: str,
    owner: str,
    lease_seconds: int,
    backup_owner: Optional[str] = None,
    policy: Optional[Mapping[str, Any]] = None,
    expected_generation: Optional[int] = None,
    now: Optional[int] = None,
) -> int:
    """Create, renew, or explicitly transfer a machine-enforced owner lease."""
    scope = _required_text(scope, "scope")
    owner = _required_text(owner, "owner")
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    now = int(time.time()) if now is None else int(now)
    encoded_policy = (
        None if policy is None
        else json.dumps(dict(policy), sort_keys=True, ensure_ascii=False)
    )
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        row = conn.execute("SELECT * FROM work_owner_leases WHERE scope=?", (scope,)).fetchone()
        if row is None:
            if expected_generation is not None:
                raise LedgerConflict(f"owner scope {scope} does not exist")
            generation = 1
            conn.execute(
                "INSERT INTO work_owner_leases VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    scope, owner, backup_owner, encoded_policy or "{}", now + lease_seconds,
                    now, now, generation,
                ),
            )
            return generation
        generation = int(row["generation"])
        transferring = row["owner"] != owner
        if transferring and int(row["lease_expires_at"]) > now and expected_generation != generation:
            raise LedgerConflict(
                f"owner scope {scope} is leased to {row['owner']} through {row['lease_expires_at']}"
            )
        if expected_generation is not None and expected_generation != generation:
            raise LedgerConflict(
                f"owner scope {scope} generation moved; expected {expected_generation}, found {generation}"
            )
        new_generation = generation + 1 if transferring else generation
        next_backup_owner = (
            row["backup_owner"]
            if not transferring and backup_owner is None
            else backup_owner
        )
        next_policy = row["policy"] if encoded_policy is None else encoded_policy
        conn.execute(
            """
            UPDATE work_owner_leases
               SET owner=?, backup_owner=?, policy=?, lease_expires_at=?,
                   heartbeat_at=?, updated_at=?, generation=?
             WHERE scope=?
            """,
            (
                owner, next_backup_owner, next_policy, now + lease_seconds,
                now, now, new_generation, scope,
            ),
        )
        return new_generation


def heartbeat_owner(conn, *, scope: str, owner: str, lease_seconds: int,
                    now: Optional[int] = None) -> bool:
    """Renew a lease only when its current owner still matches."""
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    now = int(time.time()) if now is None else int(now)
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        return conn.execute(
            "UPDATE work_owner_leases SET heartbeat_at=?, updated_at=?, lease_expires_at=? "
            "WHERE scope=? AND owner=?",
            (now, now, now + lease_seconds, scope, owner),
        ).rowcount == 1


def bind_task_owner_scope(conn, *, task_id: str, scope: str, now: Optional[int] = None) -> bool:
    """Bind task admission to the live owner of a registered scope."""
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn

    now = int(time.time()) if now is None else int(now)
    with write_txn(conn):
        task = kb.get_task(conn, task_id)
        if task is None:
            raise ValueError(f"no such task: {task_id}")
        lease = conn.execute("SELECT * FROM work_owner_leases WHERE scope=?", (scope,)).fetchone()
        if lease is None or int(lease["lease_expires_at"]) <= now:
            raise LedgerConflict(f"owner scope {scope} has no live lease")
        if task.assignee != lease["owner"]:
            raise LedgerConflict(
                f"task assignee {task.assignee!r} does not match scope owner {lease['owner']!r}"
            )
        row = conn.execute(
            "SELECT scope FROM work_task_owner_scopes WHERE task_id=?", (task_id,),
        ).fetchone()
        if row is not None:
            if row["scope"] != scope:
                raise LedgerConflict(f"task {task_id} is already bound to owner scope {row['scope']}")
            return False
        conn.execute(
            "INSERT INTO work_task_owner_scopes (task_id, scope, bound_at) VALUES (?, ?, ?)",
            (task_id, scope, now),
        )
        kb._append_event(conn, task_id, "owner_scope_bound", {"scope": scope, "owner": lease["owner"]})
        return True


def owner_claim_blocker(conn, task_id: str, *, now: Optional[int] = None) -> Optional[dict[str, Any]]:
    """Return why an owner-scoped implementation claim must fail, if anything."""
    now = int(time.time()) if now is None else int(now)
    row = conn.execute(
        """
        SELECT s.scope, l.owner, l.lease_expires_at, t.assignee
          FROM work_task_owner_scopes s
          JOIN tasks t ON t.id=s.task_id
          LEFT JOIN work_owner_leases l ON l.scope=s.scope
         WHERE s.task_id=?
        """,
        (task_id,),
    ).fetchone()
    if row is None:
        return None
    if row["owner"] is None or int(row["lease_expires_at"] or 0) <= now:
        return {"reason": "owner_lease_expired", "scope": row["scope"]}
    if row["assignee"] != row["owner"]:
        return {
            "reason": "owner_mismatch", "scope": row["scope"],
            "owner": row["owner"], "assignee": row["assignee"],
        }
    return None


def owner_registry_snapshot(conn, *, now: Optional[int] = None) -> list[dict[str, Any]]:
    """Return owner leases with live state and bound-task counts."""
    now = int(time.time()) if now is None else int(now)
    records: list[dict[str, Any]] = []
    for row in conn.execute(
        """
        SELECT l.*, COUNT(s.task_id) AS bound_tasks
          FROM work_owner_leases l
          LEFT JOIN work_task_owner_scopes s ON s.scope=l.scope
         GROUP BY l.scope
         ORDER BY l.scope
        """
    ):
        record = dict(row)
        record["policy"] = json.loads(record["policy"] or "{}")
        record["live"] = int(record["lease_expires_at"]) > now
        records.append(record)
    return records


def record_feedback(
    conn,
    *,
    feedback_id: str,
    outcome: str,
    task_id: Optional[str] = None,
    channel: Optional[str] = None,
    category: Optional[str] = None,
    correction_sha256: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> bool:
    """Record user-visible outcome feedback exactly once."""
    feedback_id = _required_text(feedback_id, "feedback_id")
    outcome = _required_text(outcome, "outcome")
    if correction_sha256 is not None:
        correction_sha256 = str(correction_sha256).strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", correction_sha256):
            raise ValueError("correction_sha256 must be a 64-character SHA256 hex digest")
    encoded = json.dumps(dict(metadata or {}), sort_keys=True, ensure_ascii=False)
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        if task_id is not None and kb.get_task(conn, task_id) is None:
            raise ValueError(f"no such task: {task_id}")
        row = conn.execute(
            "SELECT * FROM work_feedback WHERE feedback_id=?", (feedback_id,),
        ).fetchone()
        facts = (task_id, outcome, channel, category, correction_sha256, encoded)
        if row is not None:
            existing = tuple(row[key] for key in (
                "task_id", "outcome", "channel", "category", "correction_sha256", "metadata",
            ))
            if existing != facts:
                raise LedgerConflict(f"feedback {feedback_id} already exists with different facts")
            return False
        now = int(time.time())
        conn.execute(
            "INSERT INTO work_feedback VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (feedback_id, task_id, outcome, channel, category, correction_sha256, encoded, now),
        )
        if task_id is not None:
            kb._append_event(
                conn, task_id, "feedback_recorded",
                {
                    "feedback_id": feedback_id, "outcome": outcome,
                    "channel": channel, "category": category,
                    "correction_sha256": correction_sha256,
                },
            )
        return True


def add_acceptance_requirement(
    conn,
    *,
    task_id: str,
    requirement_key: str,
    description: str,
    expected_reviewer: Optional[str] = None,
    required: bool = True,
) -> bool:
    """Declare one explicit completion requirement. Existing facts are immutable."""
    task_id = _required_text(task_id, "task_id")
    requirement_key = _required_text(requirement_key, "requirement_key")
    description = _required_text(description, "description")
    expected_reviewer = str(expected_reviewer).strip() if expected_reviewer is not None else None

    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        if kb.get_task(conn, task_id) is None:
            raise ValueError(f"no such task: {task_id}")
        row = conn.execute(
            "SELECT * FROM work_acceptance_requirements WHERE task_id=? AND requirement_key=?",
            (task_id, requirement_key),
        ).fetchone()
        if row is not None:
            facts = (row["description"], row["expected_reviewer"], bool(row["required"]))
            if facts != (description, expected_reviewer, bool(required)):
                raise LedgerConflict(f"acceptance requirement {requirement_key} already has different facts")
            return False
        now = int(time.time())
        conn.execute(
            """
            INSERT INTO work_acceptance_requirements (
                task_id, requirement_key, description, required,
                expected_reviewer, status, created_at
            ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
            """,
            (task_id, requirement_key, description, 1 if required else 0, expected_reviewer, now),
        )
        kb._append_event(
            conn, task_id, "acceptance_required",
            {
                "requirement_key": requirement_key, "description": description,
                "expected_reviewer": expected_reviewer, "required": bool(required),
            },
        )
        return True


def _latest_implementer(conn, task_id: str) -> Optional[str]:
    row = conn.execute(
        "SELECT payload FROM task_events WHERE task_id=? AND kind='review_requested' "
        "ORDER BY id DESC LIMIT 1", (task_id,),
    ).fetchone()
    if row and row["payload"]:
        try:
            value = json.loads(row["payload"]).get("implementer")
        except (TypeError, ValueError):
            value = None
        if isinstance(value, str) and value.strip():
            return value.strip()
    run = conn.execute(
        "SELECT profile FROM task_runs WHERE task_id=? AND profile IS NOT NULL "
        "ORDER BY id DESC LIMIT 1", (task_id,),
    ).fetchone()
    return str(run["profile"]).strip() if run and run["profile"] else None


def _artifact_receipt(path: Optional[str]) -> tuple[Optional[str], Optional[int]]:
    if path is None:
        return None, None
    artifact = Path(path).expanduser().resolve()
    if not artifact.is_file():
        raise ValueError(f"artifact does not exist: {artifact}")
    digest = hashlib.sha256()
    size = 0
    with artifact.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def record_acceptance(
    conn,
    *,
    task_id: str,
    requirement_key: str,
    status: str,
    verified_by: str,
    evidence: Mapping[str, Any],
    artifact_path: Optional[str] = None,
) -> bool:
    """Persist an independent pass, failure or explicit waiver receipt."""
    task_id = _required_text(task_id, "task_id")
    requirement_key = _required_text(requirement_key, "requirement_key")
    status = _required_text(status, "status").lower()
    verified_by = _required_text(verified_by, "verified_by")
    if status not in ACCEPTANCE_STATUSES - {"pending"}:
        raise ValueError("status must be passed, failed, or waived")
    if not isinstance(evidence, Mapping) or not evidence:
        raise ValueError("evidence must be a non-empty mapping")
    if status == "waived":
        if not str(evidence.get("waiver_reason") or "").strip():
            raise ValueError("waived acceptance requires evidence.waiver_reason")
        if not str(evidence.get("user_approval_ref") or "").strip():
            raise ValueError("waived acceptance requires evidence.user_approval_ref")
    artifact_sha256, artifact_size = _artifact_receipt(artifact_path)

    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn

    with write_txn(conn):
        row = conn.execute(
            "SELECT * FROM work_acceptance_requirements WHERE task_id=? AND requirement_key=?",
            (task_id, requirement_key),
        ).fetchone()
        if row is None:
            raise ValueError(f"no such acceptance requirement: {task_id}:{requirement_key}")
        expected = row["expected_reviewer"]
        if expected and expected != verified_by:
            raise ValueError(f"requirement must be verified by {expected}")
        implementer = _latest_implementer(conn, task_id)
        if status == "passed" and implementer and implementer == verified_by:
            raise ValueError("implementer cannot independently pass their own acceptance requirement")
        if (
            status == "passed" and row["status"] == "failed"
            and artifact_sha256 is not None
            and artifact_sha256 == row["artifact_sha256"]
        ):
            raise ValueError("artifact is unchanged since the failed acceptance check")
        encoded = json.dumps(dict(evidence), sort_keys=True, ensure_ascii=False)
        unchanged = (
            row["status"] == status and row["verified_by"] == verified_by
            and row["evidence"] == encoded and row["artifact_sha256"] == artifact_sha256
        )
        if unchanged:
            return False
        now = int(time.time())
        conn.execute(
            """
            UPDATE work_acceptance_requirements
               SET status=?, evidence=?, artifact_sha256=?, artifact_size=?,
                   verified_by=?, verified_at=?
             WHERE task_id=? AND requirement_key=?
            """,
            (
                status, encoded, artifact_sha256, artifact_size,
                verified_by, now, task_id, requirement_key,
            ),
        )
        kb._append_event(
            conn, task_id, "acceptance_recorded",
            {
                "requirement_key": requirement_key, "status": status,
                "verified_by": verified_by, "implementer": implementer,
                "evidence": dict(evidence), "artifact_sha256": artifact_sha256,
                "artifact_size": artifact_size,
            },
        )
        return True


def completion_blockers(conn, task_id: str) -> list[dict[str, str]]:
    """Return machine-readable reasons the task cannot enter ``done``."""
    blockers: list[dict[str, str]] = []
    for row in conn.execute(
        "SELECT id, question FROM work_decisions WHERE task_id=? AND status='open' ORDER BY created_at, id",
        (task_id,),
    ):
        blockers.append({"kind": "open_decision", "key": row["id"], "detail": row["question"]})
    for row in conn.execute(
        """
        SELECT requirement_key, description, status
          FROM work_acceptance_requirements
         WHERE task_id=? AND required=1 AND status NOT IN ('passed', 'waived')
         ORDER BY created_at, requirement_key
        """,
        (task_id,),
    ):
        blockers.append({
            "kind": "acceptance", "key": row["requirement_key"],
            "detail": f"{row['status']}: {row['description']}",
        })
    for row in conn.execute(
        "SELECT id, kind, unblock_condition FROM work_blocks "
        "WHERE task_id=? AND resolved_at IS NULL ORDER BY id",
        (task_id,),
    ):
        blockers.append({
            "kind": "active_block", "key": str(row["id"]),
            "detail": f"{row['kind']}: {row['unblock_condition']}",
        })
    return blockers


def record_completion_rejected(conn, task_id: str, blockers: list[dict[str, str]]) -> None:
    if not blockers:
        return
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import write_txn
    with write_txn(conn):
        kb._append_event(conn, task_id, "completion_rejected", {"blockers": blockers})


def ledger_metrics(conn, *, now: Optional[int] = None) -> dict[str, Any]:
    """Outcome-oriented counters for health checks and Radar projections."""
    now = int(time.time()) if now is None else int(now)

    def count(sql: str, params=()) -> int:
        return int(conn.execute(sql, params).fetchone()[0])

    stale_sources = [
        {
            "source": row["source"], "account": row["account"],
            "stream": row["stream"], "updated_at": int(row["updated_at"]),
            "freshness_sla_seconds": int(row["freshness_sla_seconds"]),
        }
        for row in conn.execute(
            "SELECT source, account, stream, updated_at, freshness_sla_seconds "
            "FROM work_source_cursors WHERE freshness_sla_seconds IS NOT NULL "
            "AND updated_at + freshness_sla_seconds < ? ORDER BY source, account, stream",
            (now,),
        )
    ]
    return {
        "source_events": {
            "total": count("SELECT COUNT(*) FROM work_source_events"),
            "ignored": count("SELECT COUNT(*) FROM work_source_events WHERE disposition='ignored'"),
        },
        "open_decisions": count("SELECT COUNT(*) FROM work_decisions WHERE status='open'"),
        "source_health": {
            "streams": count("SELECT COUNT(*) FROM work_source_cursors"),
            "backlog": count("SELECT COALESCE(SUM(backlog), 0) FROM work_source_cursors"),
            "stale": stale_sources,
        },
        "work_specs": {
            "total": count("SELECT COUNT(*) FROM work_specs"),
            "past_deadline": count(
                "SELECT COUNT(*) FROM work_specs s JOIN tasks t ON t.id=s.task_id "
                "WHERE t.status NOT IN ('done', 'archived') "
                "AND s.deadline_at IS NOT NULL AND s.deadline_at < ?", (now,),
            ),
            "inside_safety_margin": count(
                "SELECT COUNT(*) FROM work_specs s JOIN tasks t ON t.id=s.task_id "
                "WHERE t.status NOT IN ('done', 'archived') AND s.deadline_at IS NOT NULL "
                "AND s.deadline_at >= ? AND s.deadline_at-s.safety_margin_seconds <= ?",
                (now, now),
            ),
            "by_urgency": {
                row["urgency"]: int(row["n"])
                for row in conn.execute(
                    "SELECT urgency, COUNT(*) AS n FROM work_specs GROUP BY urgency ORDER BY urgency"
                )
            },
            "by_risk": {
                row["risk_class"]: int(row["n"])
                for row in conn.execute(
                    "SELECT risk_class, COUNT(*) AS n FROM work_specs "
                    "GROUP BY risk_class ORDER BY risk_class"
                )
            },
        },
        "acceptance": {
            "pending": count("SELECT COUNT(*) FROM work_acceptance_requirements WHERE required=1 AND status='pending'"),
            "failed": count("SELECT COUNT(*) FROM work_acceptance_requirements WHERE required=1 AND status='failed'"),
        },
        "blocks": {
            "active": count("SELECT COUNT(*) FROM work_blocks WHERE resolved_at IS NULL"),
            "past_next_check": count(
                "SELECT COUNT(*) FROM work_blocks WHERE resolved_at IS NULL "
                "AND next_check_at IS NOT NULL AND next_check_at < ?", (now,),
            ),
            "past_escalation": count(
                "SELECT COUNT(*) FROM work_blocks WHERE resolved_at IS NULL "
                "AND escalation_at IS NOT NULL AND escalation_at < ?", (now,),
            ),
        },
        "owners": {
            "scopes": count("SELECT COUNT(*) FROM work_owner_leases"),
            "expired": count("SELECT COUNT(*) FROM work_owner_leases WHERE lease_expires_at < ?", (now,)),
            "bound_tasks": count("SELECT COUNT(*) FROM work_task_owner_scopes"),
        },
        "feedback": {
            row["outcome"]: int(row["n"])
            for row in conn.execute(
                "SELECT outcome, COUNT(*) AS n FROM work_feedback GROUP BY outcome ORDER BY outcome"
            )
        },
        "stale_claims": count(
            "SELECT COUNT(*) FROM tasks WHERE status='running' AND claim_expires IS NOT NULL AND claim_expires < ?",
            (now,),
        ),
        "waiting_by_kind": {
            (row["block_kind"] or "untyped"): int(row["n"])
            for row in conn.execute(
                "SELECT block_kind, COUNT(*) AS n FROM tasks WHERE status='blocked' GROUP BY block_kind"
            )
        },
    }


def ledger_audit(conn, *, now: Optional[int] = None) -> dict[str, Any]:
    """Find cross-system integrity drift without changing board state."""
    now = int(time.time()) if now is None else int(now)
    findings: list[dict[str, Any]] = []

    def add(kind: str, severity: str, detail: str, *, task_id: Optional[str] = None,
            ref: Optional[str] = None) -> None:
        finding: dict[str, Any] = {"kind": kind, "severity": severity, "detail": detail}
        if task_id is not None:
            finding["task_id"] = task_id
        if ref is not None:
            finding["ref"] = ref
        findings.append(finding)

    for row in conn.execute(
        """
        SELECT e.source, e.account, e.event_id, e.task_id
          FROM work_source_events e
          LEFT JOIN tasks t ON t.id=e.task_id
         WHERE (e.disposition IN ('created', 'linked') AND e.task_id IS NULL)
            OR (e.task_id IS NOT NULL AND t.id IS NULL)
         ORDER BY e.source, e.account, e.event_id
        """
    ):
        add(
            "source_identity_orphan", "critical",
            "Source event has no live canonical task.", task_id=row["task_id"],
            ref=f"{row['source']}:{row['account']}:{row['event_id']}",
        )

    for row in conn.execute(
        """
        SELECT p.system, p.external_id, p.task_id
          FROM work_projections p LEFT JOIN tasks t ON t.id=p.task_id
         WHERE t.id IS NULL ORDER BY p.system, p.external_id
        """
    ):
        add(
            "projection_orphan", "critical", "Projection points to a missing canonical task.",
            task_id=row["task_id"], ref=f"{row['system']}:{row['external_id']}",
        )

    for row in conn.execute(
        """
        SELECT id, task_id FROM work_decisions
         WHERE status='answered'
           AND ((action_ref IS NULL AND no_action_reason IS NULL)
             OR (action_ref IS NOT NULL AND no_action_reason IS NOT NULL))
         ORDER BY created_at, id
        """
    ):
        add(
            "decision_without_action_semantics", "error",
            "Answered decision must name exactly one action or no-action reason.",
            task_id=row["task_id"], ref=row["id"],
        )

    for row in conn.execute(
        """
        SELECT t.id, t.status, t.block_kind
          FROM tasks t
         WHERE (t.status='blocked' AND t.block_kind IS NULL)
            OR (t.status='done' AND EXISTS (
                   SELECT 1 FROM work_decisions d
                    WHERE d.task_id=t.id AND d.status='open'
               ))
            OR (t.status='done' AND EXISTS (
                   SELECT 1 FROM work_acceptance_requirements a
                    WHERE a.task_id=t.id AND a.required=1
                      AND a.status NOT IN ('passed', 'waived')
               ))
            OR (t.status='done' AND EXISTS (
                   SELECT 1 FROM work_blocks b
                    WHERE b.task_id=t.id AND b.resolved_at IS NULL
               ))
         ORDER BY t.id
        """
    ):
        if row["status"] == "blocked" and row["block_kind"] is None:
            add(
                "untyped_waiting_state", "warning",
                "Blocked task has no exact waiting cause.", task_id=row["id"],
            )
        else:
            add(
                "completed_with_open_obligation", "critical",
                "Done task still has an open decision, active block, or required acceptance debt.",
                task_id=row["id"],
            )

    for row in conn.execute(
        """
        SELECT s.task_id, s.scope
          FROM work_task_owner_scopes s
          LEFT JOIN tasks t ON t.id=s.task_id
          LEFT JOIN work_owner_leases l ON l.scope=s.scope
         WHERE t.id IS NULL OR (
                   t.status NOT IN ('done', 'archived')
               AND (l.scope IS NULL OR l.lease_expires_at<=? OR t.assignee IS NOT l.owner)
         )
         ORDER BY s.scope, s.task_id
        """,
        (now,),
    ):
        add(
            "owner_scope_drift", "error",
            "Task binding has a missing, expired, or mismatched live owner lease.",
            task_id=row["task_id"], ref=row["scope"],
        )

    for row in conn.execute(
        """
        SELECT s.task_id, s.deadline_at, s.safety_margin_seconds
          FROM work_specs s JOIN tasks t ON t.id=s.task_id
         WHERE t.status NOT IN ('done', 'archived') AND s.deadline_at IS NOT NULL
           AND s.deadline_at-s.safety_margin_seconds<=?
         ORDER BY s.deadline_at, s.task_id
        """,
        (now,),
    ):
        overdue = int(row["deadline_at"]) < now
        add(
            "deadline_breached" if overdue else "deadline_safety_margin_reached",
            "error" if overdue else "warning",
            "Work passed its deadline." if overdue else "Work entered its deadline safety margin.",
            task_id=row["task_id"], ref=str(row["deadline_at"]),
        )

    by_severity = {severity: 0 for severity in ("warning", "error", "critical")}
    for finding in findings:
        by_severity[finding["severity"]] += 1
    return {
        "ok": not findings,
        "finding_count": len(findings),
        "by_severity": by_severity,
        "findings": findings,
    }


def task_ledger_snapshot(conn, task_id: str) -> dict[str, Any]:
    """Return the cross-system identity and closure state for one task."""
    task_id = _required_text(task_id, "task_id")
    from hermes_cli import kanban_db as kb

    if kb.get_task(conn, task_id) is None:
        raise ValueError(f"no such task: {task_id}")

    def rows(sql: str) -> list[dict[str, Any]]:
        return [dict(row) for row in conn.execute(sql, (task_id,))]

    decisions = rows(
        "SELECT id, question, status, answer, action_ref, no_action_reason, source_ref, "
        "created_at, answered_at, updated_at FROM work_decisions WHERE task_id=? "
        "ORDER BY created_at, id"
    )
    acceptance = rows(
        "SELECT requirement_key, description, required, expected_reviewer, status, evidence, "
        "artifact_sha256, artifact_size, verified_by, verified_at, created_at "
        "FROM work_acceptance_requirements WHERE task_id=? ORDER BY created_at, requirement_key"
    )
    for row in acceptance:
        row["required"] = bool(row["required"])
        row["evidence"] = json.loads(row["evidence"]) if row["evidence"] else None
    spec_row = conn.execute("SELECT * FROM work_specs WHERE task_id=?", (task_id,)).fetchone()
    return {
        "task_id": task_id,
        "spec": dict(spec_row) if spec_row is not None else None,
        "sources": rows(
            "SELECT source, account, event_id, event_type, disposition, occurred_at, raw_ref, "
            "payload_sha256, ignored_reason, ingested_at FROM work_source_events WHERE task_id=? "
            "ORDER BY ingested_at, id"
        ),
        "projections": rows(
            "SELECT system, external_id, state_hash, last_synced_at FROM work_projections "
            "WHERE task_id=? ORDER BY system, external_id"
        ),
        "decisions": decisions,
        "acceptance": acceptance,
        "blocks": rows(
            "SELECT id, kind, reason, responsible_party, unblock_condition, next_check_at, "
            "escalation_at, affected_ref, opened_at, resolved_at, resolved_by, resolution "
            "FROM work_blocks WHERE task_id=? ORDER BY opened_at, id"
        ),
        "completion_blockers": completion_blockers(conn, task_id),
    }


def has_external_identity(conn, task_id: str) -> bool:
    """True when hard deletion would make a source or projection point nowhere."""
    return bool(conn.execute(
        "SELECT EXISTS(SELECT 1 FROM work_source_events WHERE task_id=? LIMIT 1) "
        "OR EXISTS(SELECT 1 FROM work_projections WHERE task_id=? LIMIT 1)",
        (task_id, task_id),
    ).fetchone()[0])
