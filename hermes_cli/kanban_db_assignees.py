"""Assignee discovery for the kanban database facade."""

from __future__ import annotations


def list_profiles_on_disk() -> list[str]:
    """Return configured profiles plus the implicit default profile."""
    try:
        from hermes_cli.config_backend import config_exists
        from hermes_constants import get_default_hermes_root

        default_root = get_default_hermes_root()
        profiles_dir = default_root / "profiles"
    except Exception:
        return []

    names: set[str] = set()
    if default_root.exists():
        names.add("default")
    if profiles_dir.is_dir():
        try:
            names.update(
                entry.name
                for entry in profiles_dir.iterdir()
                if entry.is_dir() and config_exists(entry / "config.yaml")
            )
        except OSError:
            pass
    return sorted(names)


def known_assignees(conn) -> list[dict]:
    """Return on-disk profiles and task assignees with per-status counts."""
    from hermes_cli import kanban_db as kb

    on_disk = set(list_profiles_on_disk())
    counts = kb._counts_by_assignee(conn)
    return [
        {"name": name, "on_disk": name in on_disk, "counts": counts.get(name, {})}
        for name in sorted(on_disk | set(counts))
    ]
