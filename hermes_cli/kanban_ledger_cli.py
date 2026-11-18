"""Stable CLI adapter for the canonical work ledger."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any, Optional

from hermes_cli import kanban_ledger as ledger
from hermes_cli import kanban_db_connect as kbc


def _json_value(raw: Optional[str], label: str, *, object_only: bool = False) -> Any:
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON: {exc}") from exc
    if object_only and not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _emit(value: Any) -> int:
    print(json.dumps(value, sort_keys=True, ensure_ascii=False))
    return 0


def _ingest(conn, args) -> dict[str, Any]:
    task = None
    if args.create_title is not None:
        task = {
            "title": args.create_title,
            "body": args.create_body,
            "assignee": args.assignee,
            "created_by": args.created_by or "source-ingest",
        }
    result = ledger.ingest_source_event(
        conn,
        source=args.source,
        account=args.account,
        event_id=args.event_id,
        event_type=args.event_type,
        occurred_at=args.occurred_at,
        raw_ref=args.raw_ref,
        payload_digest=args.payload_sha256,
        task_id=args.task_id,
        task=task,
        ignored_reason=args.ignored_reason,
    )
    return asdict(result)


def dispatch(args) -> int:
    """Execute ``hermes kanban ledger <action>`` and emit JSON."""
    action = getattr(args, "ledger_action", None)
    if not action:
        raise ValueError("ledger action is required")

    with kbc.connect_closing() as conn:
        if action == "ingest-source":
            return _emit(_ingest(conn, args))
        if action == "advance-cursor":
            changed = ledger.advance_source_cursor(
                conn, source=args.source, account=args.account, stream=args.stream,
                cursor=args.cursor, previous_cursor=args.previous_cursor,
                last_event_at=args.last_event_at, backlog=args.backlog,
                freshness_sla_seconds=args.freshness_sla,
            )
            return _emit({"changed": changed})
        if action == "sync-projection":
            changed = ledger.sync_projection(
                conn, system=args.system, external_id=args.external_id,
                task_id=args.task_id, state=_json_value(args.state_json, "--state-json"),
            )
            return _emit({"changed": changed})
        if action == "set-spec":
            revision = ledger.set_work_spec(
                conn, task_id=args.task_id, objective=args.objective,
                expected_result=args.expected_result, deadline_at=args.deadline_at,
                safety_margin_seconds=args.safety_margin, urgency=args.urgency,
                risk_class=args.risk_class, value_score=args.value_score,
                expected_revision=args.expected_revision,
            )
            return _emit({"revision": revision})
        if action == "open-decision":
            changed = ledger.open_decision(
                conn, decision_id=args.decision_id, task_id=args.task_id,
                question=args.question, source_ref=args.source_ref,
            )
            return _emit({"changed": changed})
        if action == "answer-decision":
            changed = ledger.answer_decision(
                conn, args.decision_id, answer=args.answer, actor=args.actor,
                action_ref=args.action_ref, no_action_reason=args.no_action_reason,
                source_ref=args.source_ref,
            )
            return _emit({"changed": changed})
        if action == "cancel-decision":
            changed = ledger.cancel_decision(
                conn, args.decision_id, reason=args.reason, actor=args.actor,
            )
            return _emit({"changed": changed})
        if action == "require-acceptance":
            changed = ledger.add_acceptance_requirement(
                conn, task_id=args.task_id, requirement_key=args.requirement_key,
                description=args.description, expected_reviewer=args.expected_reviewer,
                required=not args.optional,
            )
            return _emit({"changed": changed})
        if action == "record-acceptance":
            changed = ledger.record_acceptance(
                conn, task_id=args.task_id, requirement_key=args.requirement_key,
                status=args.status, verified_by=args.verified_by,
                evidence=_json_value(args.evidence_json, "--evidence-json", object_only=True),
                artifact_path=args.artifact,
            )
            return _emit({"changed": changed})
        if action == "set-owner":
            generation = ledger.set_owner_lease(
                conn, scope=args.scope, owner=args.owner, lease_seconds=args.lease_seconds,
                backup_owner=args.backup_owner,
                policy=_json_value(args.policy_json, "--policy-json", object_only=True),
                expected_generation=args.expected_generation,
            )
            return _emit({"generation": generation})
        if action == "heartbeat-owner":
            changed = ledger.heartbeat_owner(
                conn, scope=args.scope, owner=args.owner, lease_seconds=args.lease_seconds,
            )
            return _emit({"changed": changed})
        if action == "bind-owner":
            changed = ledger.bind_task_owner_scope(
                conn, task_id=args.task_id, scope=args.scope,
            )
            return _emit({"changed": changed})
        if action == "owners":
            return _emit(ledger.owner_registry_snapshot(conn))
        if action == "record-feedback":
            changed = ledger.record_feedback(
                conn, feedback_id=args.feedback_id, outcome=args.outcome,
                task_id=args.task_id, channel=args.channel, category=args.category,
                correction_sha256=args.correction_sha256,
                metadata=_json_value(args.metadata_json, "--metadata-json", object_only=True),
            )
            return _emit({"changed": changed})
        if action == "show":
            return _emit(ledger.task_ledger_snapshot(conn, args.task_id))
        if action == "metrics":
            return _emit(ledger.ledger_metrics(conn))
        if action == "audit":
            report = ledger.ledger_audit(conn)
            _emit(report)
            return 0 if report["ok"] else 1
    raise ValueError(f"unknown ledger action: {action}")
