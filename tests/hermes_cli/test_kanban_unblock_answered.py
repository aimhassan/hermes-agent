"""A human answer starts a new block episode, while an automatic retry does not."""

from __future__ import annotations

import argparse

from hermes_cli import kanban as kanban_cli
from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from tests.hermes_cli.test_kanban_block_kinds import (
    _make_running_again,
    _running_task,
    kanban_home,
)


def _ask_answer_ask(conn, *, answered):
    tid = _running_task(conn)
    kb.block_task(conn, tid, reason="question one", kind="needs_input")
    assert kb.unblock_task(conn, tid, answered=answered)
    _make_running_again(conn, tid)
    kb.block_task(conn, tid, reason="question two", kind="needs_input")
    return kb.get_task(conn, tid), tid


def test_answered_unblock_starts_a_new_block_episode(kanban_home):
    with kbc.connect_closing() as conn:
        task, tid = _ask_answer_ask(conn, answered=True)
        assert (task.status, task.block_kind, task.block_recurrences) == (
            "blocked", "needs_input", 1,
        )
        unblocked = [event for event in kb.list_events(conn, tid) if event.kind == "unblocked"][-1]
        assert (unblocked.payload or {}).get("answered") is True


def test_unanswered_unblock_still_trips_the_loop_breaker(kanban_home):
    with kbc.connect_closing() as conn:
        task, _ = _ask_answer_ask(conn, answered=False)
        assert (task.status, task.block_recurrences) == (
            "triage", kb.BLOCK_RECURRENCE_LIMIT,
        )


def _cli_unblock(tid, *, reason=None, answered=False):
    args = argparse.Namespace(
        task_id=tid, task_ids=[tid], ids=[tid], reason=reason, answered=answered,
    )
    return kanban_cli._cmd_unblock(args)


def test_cli_distinguishes_answers_from_retry_notes(kanban_home, monkeypatch):
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    cases = (
        ({"answered": True}, "blocked"),
        ({"reason": "AIMAN: Arnhem"}, "blocked"),
        ({"reason": "answered: yes"}, "blocked"),
        ({"reason": "cron retry"}, "triage"),
    )
    for kwargs, expected in cases:
        with kbc.connect_closing() as conn:
            tid = _running_task(conn, title=str(kwargs))
            kb.block_task(conn, tid, reason="question one", kind="needs_input")
        assert _cli_unblock(tid, **kwargs) == 0
        with kbc.connect_closing() as conn:
            _make_running_again(conn, tid)
            kb.block_task(conn, tid, reason="question two", kind="needs_input")
            assert kb.get_task(conn, tid).status == expected


def test_two_step_answer_relay_records_the_comment_text(kanban_home, monkeypatch):
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    with kbc.connect_closing() as conn:
        tid = _running_task(conn, title="relay")
        kb.block_task(conn, tid, reason="Which city?", kind="needs_input")
        kb.add_comment(conn, tid, "t3", "AIMAN via T3: Arnhem")
    assert _cli_unblock(tid) == 0
    with kbc.connect_closing() as conn:
        decision = conn.execute(
            "SELECT status, answer FROM work_decisions WHERE task_id=?", (tid,),
        ).fetchone()
        assert tuple(decision) == ("answered", "AIMAN via T3: Arnhem")
        assert kb.recent_answer_comment(conn, tid) is True


def test_old_or_non_answer_comment_is_not_an_answer(kanban_home):
    with kbc.connect_closing() as conn:
        tid = _running_task(conn, title="old")
        kb.add_comment(conn, tid, "t3", "AIMAN: yes")
        later = int(__import__("time").time()) + kb.ANSWER_COMMENT_WINDOW_S + 5
        assert kb.recent_answer_comment(conn, tid) is True
        assert kb.recent_answer_comment(conn, tid, now=later) is False
        kb.add_comment(conn, tid, "worker", "status update")
        assert kb.recent_answer_comment(conn, tid) is False
