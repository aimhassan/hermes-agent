"""Canonical work ledger contracts across intake, decisions and acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
import time

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_dispatch as kbd
from hermes_cli import kanban_db_dispatch_order as dispatch_order
from hermes_cli import kanban_ledger as ledger
from hermes_cli import kanban as kanban_cli


@pytest.fixture
def conn(tmp_path):
    connection = kbc.connect(tmp_path / "kanban.db")
    try:
        yield connection
    finally:
        connection.close()


def test_source_event_creates_one_task_across_retries(conn):
    first = ledger.ingest_source_event(
        conn,
        source="whatsapp",
        account="business",
        event_id="wamid.123",
        event_type="message",
        raw_ref="archive://chat/7#message-123",
        payload={"text_sha": "abc"},
        task={"title": "Prepare the promised document", "assignee": "task-manager"},
    )
    second = ledger.ingest_source_event(
        conn,
        source="whatsapp",
        account="business",
        event_id="wamid.123",
        event_type="message",
        raw_ref="archive://chat/7#message-123",
        payload={"text_sha": "abc"},
        task={"title": "A retry must not create this title"},
    )

    assert first.created is True
    assert second.created is False
    assert second.task_id == first.task_id
    assert conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
    event = conn.execute(
        "SELECT payload FROM task_events WHERE task_id=? AND kind='source_ingested'",
        (first.task_id,),
    ).fetchone()
    assert "wamid.123" in event["payload"]


def test_every_created_task_gets_a_replaceable_baseline_spec_and_source(conn):
    task_id = kb.create_task(
        conn, title="Prepare the source-backed result", idempotency_key="crm:task-42",
    )

    spec = conn.execute(
        "SELECT objective, expected_result, revision FROM work_specs WHERE task_id=?", (task_id,),
    ).fetchone()
    assert tuple(spec) == (
        "Prepare the source-backed result", ledger.DEFAULT_EXPECTED_RESULT, 0,
    )
    source = conn.execute(
        "SELECT source, event_id, event_type, task_id FROM work_source_events"
    ).fetchone()
    assert tuple(source) == ("crm", "task-42", "kanban_idempotency", task_id)
    assert ledger.set_work_spec(
        conn, task_id=task_id, objective="Prepare the verified CRM result",
        expected_result="The CRM task is completed and read back",
    ) == 1


def test_legacy_adoption_is_idempotent_and_covers_tasks_without_keys(conn):
    now = int(time.time())
    with kbc.write_txn(conn):
        conn.execute(
            "INSERT INTO tasks (id, title, status, priority, created_at, workspace_kind) "
            "VALUES ('legacy-no-key', 'Legacy card', 'ready', 0, ?, 'scratch')",
            (now,),
        )
        conn.execute(
            "INSERT INTO tasks (id, title, status, priority, created_at, workspace_kind, idempotency_key) "
            "VALUES ('legacy-keyed', 'Legacy keyed card', 'ready', 0, ?, 'scratch', 'old-key')",
            (now,),
        )

    first = ledger.adopt_legacy_tasks(conn)
    second = ledger.adopt_legacy_tasks(conn)
    assert first == {"tasks_seen": 2, "specs_created": 2, "sources_created": 1}
    assert second == {"tasks_seen": 2, "specs_created": 0, "sources_created": 0}
    assert conn.execute("SELECT COUNT(*) FROM work_specs").fetchone()[0] == 2
    source = conn.execute(
        "SELECT source, event_id, task_id FROM work_source_events"
    ).fetchone()
    assert tuple(source) == ("legacy-idempotency", "old-key", "legacy-keyed")


def test_audit_exposes_tasks_that_bypass_canonical_creation(conn):
    with kbc.write_txn(conn):
        conn.execute(
            "INSERT INTO tasks (id, title, status, priority, created_at, workspace_kind, idempotency_key) "
            "VALUES ('bypass', 'Bypassed task', 'ready', 0, ?, 'scratch', 'crm:bypass')",
            (int(time.time()),),
        )

    report = ledger.ledger_audit(conn)
    assert {item["kind"] for item in report["findings"]} == {
        "missing_work_spec", "idempotency_without_source",
    }


def test_source_event_conflict_is_visible(conn):
    ledger.ingest_source_event(
        conn, source="crm", event_id="task-1", event_type="created",
        payload={"version": 1}, ignored_reason="test record",
    )
    with pytest.raises(ledger.LedgerConflict, match="different payload_sha256"):
        ledger.ingest_source_event(
            conn, source="crm", event_id="task-1", event_type="created",
            payload={"version": 2}, ignored_reason="test record",
        )


def test_source_event_cannot_silently_change_disposition(conn):
    task_id = kb.create_task(conn, title="Existing task")
    ledger.ingest_source_event(
        conn, source="mail", event_id="message-1", event_type="message",
        ignored_reason="newsletter",
    )
    with pytest.raises(ledger.LedgerConflict, match="disposition"):
        ledger.ingest_source_event(
            conn, source="mail", event_id="message-1", event_type="message",
            task_id=task_id,
        )


def test_source_cursor_requires_compare_and_set(conn):
    assert ledger.advance_source_cursor(
        conn, source="mail", account="work", cursor="page-1", previous_cursor=None,
        backlog=3, freshness_sla_seconds=60,
    )
    with pytest.raises(ledger.LedgerConflict, match="expected 'stale', found 'page-1'"):
        ledger.advance_source_cursor(
            conn, source="mail", account="work", cursor="page-2", previous_cursor="stale",
        )


def test_idle_source_poll_refreshes_health_without_erasing_state(conn, monkeypatch):
    monkeypatch.setattr(ledger.time, "time", lambda: 100)
    assert ledger.advance_source_cursor(
        conn, source="mail", account="work", cursor="page-1", previous_cursor=None,
        last_event_at=90, backlog=2, freshness_sla_seconds=60,
    )
    monkeypatch.setattr(ledger.time, "time", lambda: 140)
    assert ledger.advance_source_cursor(
        conn, source="mail", account="work", cursor="page-1", previous_cursor="page-1",
        backlog=0,
    )
    row = conn.execute(
        "SELECT last_event_at, backlog, freshness_sla_seconds, updated_at "
        "FROM work_source_cursors WHERE source='mail' AND account='work'"
    ).fetchone()
    assert tuple(row) == (90, 0, 60, 140)
    assert ledger.ledger_metrics(conn, now=150)["source_health"]["stale"] == []
    assert ledger.advance_source_cursor(
        conn, source="mail", account="work", cursor="page-2", previous_cursor="page-1",
        backlog=0, freshness_sla_seconds=60,
    )


def test_projection_cannot_drift_between_tasks(conn):
    a = kb.create_task(conn, title="A")
    b = kb.create_task(conn, title="B")
    assert ledger.sync_projection(
        conn, system="twenty", external_id="42", task_id=a, state={"status": "OPEN"},
    )
    assert not ledger.sync_projection(
        conn, system="twenty", external_id="42", task_id=a, state={"status": "OPEN"},
    )
    with pytest.raises(ledger.LedgerConflict, match=a):
        ledger.sync_projection(
            conn, system="twenty", external_id="42", task_id=b, state={"status": "DONE"},
        )


def test_work_spec_is_revisioned_and_drives_deadline_order(conn, monkeypatch):
    monkeypatch.setattr(ledger.time, "time", lambda: 100)
    monkeypatch.setattr(dispatch_order.time, "time", lambda: 100)
    high_priority = kb.create_task(conn, title="Important", priority=10)
    deadline = kb.create_task(conn, title="Deadline", priority=0)
    later = kb.create_task(conn, title="Later", priority=0)
    assert ledger.set_work_spec(
        conn, task_id=deadline, objective="Deliver before the safety margin closes",
        expected_result="A verified submission", deadline_at=120,
        safety_margin_seconds=30, urgency="critical", risk_class="legal", value_score=90,
    ) == 1
    assert ledger.set_work_spec(
        conn, task_id=later, objective="Prepare a useful follow-up",
        expected_result="A reviewed follow-up", deadline_at=500,
        urgency="high", risk_class="normal", value_score=80,
    ) == 1
    with pytest.raises(ledger.LedgerConflict, match="revision moved"):
        ledger.set_work_spec(
            conn, task_id=deadline, objective="Changed",
            expected_result="Changed", expected_revision=None,
        )
    assert ledger.set_work_spec(
        conn, task_id=deadline, objective="Deliver before the safety margin closes",
        expected_result="A verified submission", deadline_at=120,
        safety_margin_seconds=30, urgency="critical", risk_class="legal", value_score=95,
        expected_revision=1,
    ) == 2
    assert [row["id"] for row in kbd._lane_rows(conn, "ready")] == [
        deadline, high_priority, later,
    ]
    snapshot = ledger.task_ledger_snapshot(conn, deadline)
    assert snapshot["spec"]["revision"] == 2
    assert snapshot["spec"]["risk_class"] == "legal"
    metrics = ledger.ledger_metrics(conn, now=100)
    assert metrics["work_specs"]["inside_safety_margin"] == 1
    audit = ledger.ledger_audit(conn, now=100)
    assert any(item["kind"] == "deadline_safety_margin_reached" for item in audit["findings"])


def test_open_decision_blocks_completion_until_answer_has_action(conn):
    task_id = kb.create_task(conn, title="Decide and act")
    ledger.open_decision(
        conn, decision_id="decision:budget", task_id=task_id,
        question="Which budget option should be used?",
    )

    assert not kb.complete_task(conn, task_id, result="Work finished")
    rejected = conn.execute(
        "SELECT payload FROM task_events WHERE task_id=? AND kind='completion_rejected'",
        (task_id,),
    ).fetchone()
    assert "open_decision" in rejected["payload"]
    with pytest.raises(ValueError, match="exactly one"):
        ledger.answer_decision(
            conn, "decision:budget", answer="Use option B", actor="Aiman",
        )
    ledger.answer_decision(
        conn, "decision:budget", answer="Use option B", actor="Aiman",
        no_action_reason="The chosen option is already reflected in the task output.",
    )
    assert kb.complete_task(conn, task_id, result="Work finished")


def test_cancelled_decision_is_auditable_and_does_not_block(conn):
    task_id = kb.create_task(conn, title="Obsolete question")
    ledger.open_decision(
        conn, decision_id="decision:obsolete", task_id=task_id, question="Old question?",
    )
    assert ledger.cancel_decision(
        conn, "decision:obsolete", reason="Superseded by a newer request", actor="operator",
    )
    assert not ledger.cancel_decision(
        conn, "decision:obsolete", reason="Superseded by a newer request", actor="operator",
    )
    assert kb.complete_task(conn, task_id, result="No action remains")


def test_block_contract_must_resolve_before_completion(conn):
    task_id = kb.create_task(conn, title="Wait for partner")
    assert kb.block_task(
        conn, task_id, kind="external", reason="Partner must approve the budget",
        responsible_party="partner", unblock_condition="Written approval is received",
        next_check_at=int(time.time()) + 60, affected_ref="budget-v3",
    )
    snapshot = ledger.task_ledger_snapshot(conn, task_id)
    assert snapshot["blocks"][0]["responsible_party"] == "partner"
    assert snapshot["completion_blockers"][0]["kind"] == "active_block"
    assert not kb.complete_task(conn, task_id, result="Cannot close while waiting")
    assert kb.unblock_task(conn, task_id)
    assert kb.complete_task(conn, task_id, result="Approval received and applied")
    resolved = ledger.task_ledger_snapshot(conn, task_id)["blocks"][0]
    assert resolved["resolved_at"] is not None


def test_required_acceptance_needs_independent_evidence(conn, tmp_path):
    task_id = kb.create_task(conn, title="Produce checked artifact", assignee="writer")
    claimed = kb.claim_task(conn, task_id, claimer="writer:1")
    assert claimed is not None
    run_id = claimed.current_run_id
    artifact = tmp_path / "report.txt"
    artifact.write_text("verified result", encoding="utf-8")
    ledger.add_acceptance_requirement(
        conn, task_id=task_id, requirement_key="artifact_matches_request",
        description="The final artifact contains the requested result.",
        expected_reviewer="reviewer",
    )
    assert kb.request_review(
        conn, task_id, summary="Ready", reviewer="reviewer", expected_run_id=run_id,
    )
    review = kb.claim_review_task(conn, task_id, claimer="reviewer:1")
    assert review is not None

    with pytest.raises(ValueError, match="must be verified by reviewer"):
        ledger.record_acceptance(
            conn, task_id=task_id, requirement_key="artifact_matches_request",
            status="passed", verified_by="writer", evidence={"check": "read back"},
            artifact_path=str(artifact),
        )
    ledger.record_acceptance(
        conn, task_id=task_id, requirement_key="artifact_matches_request",
        status="passed", verified_by="reviewer", evidence={"check": "read back"},
        artifact_path=str(artifact),
    )
    assert kb.complete_task(
        conn, task_id, result="Approved", expected_run_id=review.current_run_id,
    )
    receipt = conn.execute(
        "SELECT status, artifact_sha256, artifact_size, verified_by "
        "FROM work_acceptance_requirements WHERE task_id=?",
        (task_id,),
    ).fetchone()
    assert tuple(receipt) == (
        "passed", hashlib.sha256(artifact.read_bytes()).hexdigest(),
        len(artifact.read_bytes()), "reviewer",
    )


def test_worker_and_reviewer_context_contains_source_and_acceptance_contract(conn):
    task_id = kb.create_task(conn, title="Review source-backed result", assignee="writer")
    ledger.ingest_source_event(
        conn, source="mail", account="work", event_id="message-9", event_type="message",
        raw_ref="gmail://thread/9", payload={"subject": "Request"}, task_id=task_id,
    )
    ledger.add_acceptance_requirement(
        conn, task_id=task_id, requirement_key="external-readback",
        description="The external system shows the intended result.",
        expected_reviewer="reviewer",
    )
    context = kb.build_worker_context(conn, task_id)
    assert "## Work ledger and acceptance contract" in context
    assert "`mail/work:message-9`" in context
    assert "gmail://thread/9" in context
    assert "`external-readback` [required, pending]" in context
    assert "expected_reviewer=reviewer" in context


def test_failed_artifact_must_change_before_it_can_pass(conn, tmp_path):
    task_id = kb.create_task(conn, title="Revise artifact")
    artifact = tmp_path / "artifact.txt"
    artifact.write_text("unchanged", encoding="utf-8")
    ledger.add_acceptance_requirement(
        conn, task_id=task_id, requirement_key="quality", description="Quality check",
    )
    ledger.record_acceptance(
        conn, task_id=task_id, requirement_key="quality", status="failed",
        verified_by="reviewer", evidence={"reason": "missing section"},
        artifact_path=str(artifact),
    )
    with pytest.raises(ValueError, match="unchanged"):
        ledger.record_acceptance(
            conn, task_id=task_id, requirement_key="quality", status="passed",
            verified_by="reviewer", evidence={"check": "re-read"}, artifact_path=str(artifact),
        )
    artifact.write_text("changed", encoding="utf-8")
    assert ledger.record_acceptance(
        conn, task_id=task_id, requirement_key="quality", status="passed",
        verified_by="reviewer", evidence={"check": "re-read"}, artifact_path=str(artifact),
    )


def test_acceptance_waiver_requires_user_approval_reference(conn):
    task_id = kb.create_task(conn, title="Waived requirement")
    ledger.add_acceptance_requirement(
        conn, task_id=task_id, requirement_key="readback", description="Read back the result",
    )
    with pytest.raises(ValueError, match="user_approval_ref"):
        ledger.record_acceptance(
            conn, task_id=task_id, requirement_key="readback", status="waived",
            verified_by="operator", evidence={"waiver_reason": "User accepted the residual risk"},
        )
    assert ledger.record_acceptance(
        conn, task_id=task_id, requirement_key="readback", status="waived",
        verified_by="operator",
        evidence={
            "waiver_reason": "User accepted the residual risk",
            "user_approval_ref": "decision:accept-risk",
        },
    )


def test_metrics_report_outcome_debt(conn):
    task_id = kb.create_task(conn, title="Waiting")
    ledger.open_decision(
        conn, decision_id="decision:one", task_id=task_id, question="Choose one",
    )
    ledger.add_acceptance_requirement(
        conn, task_id=task_id, requirement_key="readback", description="External readback passes",
    )
    ledger.ingest_source_event(
        conn, source="mail", event_id="m1", event_type="message",
        ignored_reason="newsletter",
    )

    metrics = ledger.ledger_metrics(conn)
    assert metrics["source_events"] == {"total": 1, "ignored": 1}
    assert metrics["open_decisions"] == 1
    assert metrics["acceptance"] == {"pending": 1, "failed": 0}


def test_snapshot_exposes_one_cross_system_view(conn):
    task_id = kb.create_task(conn, title="One view")
    ledger.ingest_source_event(
        conn, source="calendar", event_id="e1", event_type="event", task_id=task_id,
    )
    ledger.sync_projection(conn, system="radar", external_id="r1", task_id=task_id)
    snapshot = ledger.task_ledger_snapshot(conn, task_id)
    assert snapshot["task_id"] == task_id
    assert snapshot["sources"][0]["event_id"] == "e1"
    assert snapshot["projections"][0]["external_id"] == "r1"
    assert not kb.delete_task(conn, task_id)
    assert kb.get_task(conn, task_id) is not None


def test_owner_scope_lease_fences_task_claims(conn):
    generation = ledger.set_owner_lease(
        conn, scope="inbox", owner="inbox-owner", lease_seconds=600,
        backup_owner="inbox-backup", policy={"risk": "normal"},
    )
    task_id = kb.create_task(conn, title="Handle inbox item", assignee="inbox-owner")
    assert ledger.bind_task_owner_scope(conn, task_id=task_id, scope="inbox")

    with pytest.raises(ledger.LedgerConflict, match="leased to inbox-owner"):
        ledger.set_owner_lease(
            conn, scope="inbox", owner="different-owner", lease_seconds=600,
        )
    assert ledger.set_owner_lease(
        conn, scope="inbox", owner="different-owner", lease_seconds=600,
        expected_generation=generation,
    ) == generation + 1
    assert kb.claim_task(conn, task_id, claimer="worker") is None
    assert kb.assign_task(conn, task_id, "different-owner")
    assert kb.claim_task(conn, task_id, claimer="worker") is not None


def test_owner_renewal_preserves_backup_and_policy(conn):
    ledger.set_owner_lease(
        conn, scope="contracts", owner="contract-owner", lease_seconds=60,
        backup_owner="contract-backup", policy={"risk": "high"}, now=100,
    )
    ledger.set_owner_lease(
        conn, scope="contracts", owner="contract-owner", lease_seconds=60, now=110,
    )
    row = conn.execute(
        "SELECT backup_owner, policy, lease_expires_at FROM work_owner_leases WHERE scope='contracts'"
    ).fetchone()
    assert tuple(row) == ("contract-backup", '{"risk": "high"}', 170)


def test_feedback_is_idempotent_and_visible_in_metrics(conn):
    task_id = kb.create_task(conn, title="Draft outcome")
    assert ledger.record_feedback(
        conn, feedback_id="draft:42", task_id=task_id, outcome="rewritten",
        channel="gmail", category="business", correction_sha256="a" * 64,
        metadata={"voice_guide": "business-v3"},
    )
    assert not ledger.record_feedback(
        conn, feedback_id="draft:42", task_id=task_id, outcome="rewritten",
        channel="gmail", category="business", correction_sha256="a" * 64,
        metadata={"voice_guide": "business-v3"},
    )
    with pytest.raises(ledger.LedgerConflict, match="different facts"):
        ledger.record_feedback(
            conn, feedback_id="draft:42", task_id=task_id, outcome="sent_as_written",
        )
    assert ledger.ledger_metrics(conn)["feedback"] == {"rewritten": 1}


def test_ledger_cli_is_a_stable_json_adapter(tmp_path, monkeypatch, capsys):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    parser = argparse.ArgumentParser()
    kanban_cli.build_parser(parser.add_subparsers(dest="command"))

    create_args = parser.parse_args([
        "kanban", "ledger", "ingest-source", "mail", "m-42", "message",
        "--create-title", "Reply to the request", "--assignee", "inbox",
    ])
    assert kanban_cli.kanban_command(create_args) == 0
    created = json.loads(capsys.readouterr().out)
    assert created["disposition"] == "created"

    spec_args = parser.parse_args([
        "kanban", "ledger", "set-spec", created["task_id"],
        "Answer the request", "A source-verified response",
        "--urgency", "high", "--risk-class", "business", "--value-score", "80",
    ])
    assert kanban_cli.kanban_command(spec_args) == 0
    assert json.loads(capsys.readouterr().out) == {"revision": 1}

    show_args = parser.parse_args([
        "kanban", "ledger", "show", created["task_id"],
    ])
    assert kanban_cli.kanban_command(show_args) == 0
    snapshot = json.loads(capsys.readouterr().out)
    assert snapshot["sources"][0]["event_id"] == "m-42"
    assert snapshot["spec"]["expected_result"] == "A source-verified response"

    audit_args = parser.parse_args(["kanban", "ledger", "audit"])
    assert kanban_cli.kanban_command(audit_args) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_ledger_audit_finds_closure_and_owner_drift(conn):
    waiting = kb.create_task(conn, title="Legacy waiting card")
    assert kb.block_task(conn, waiting, reason="legacy untyped block")

    owned = kb.create_task(conn, title="Bound task", assignee="owner")
    ledger.set_owner_lease(
        conn, scope="inbox", owner="owner", lease_seconds=10, now=100,
    )
    assert ledger.bind_task_owner_scope(conn, task_id=owned, scope="inbox", now=100)

    report = ledger.ledger_audit(conn, now=111)
    assert report["ok"] is False
    assert report["by_severity"] == {"warning": 1, "error": 1, "critical": 0}
    assert {item["kind"] for item in report["findings"]} == {
        "untyped_waiting_state", "owner_scope_drift",
    }
