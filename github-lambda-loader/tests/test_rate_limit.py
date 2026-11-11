"""
Offline tests for the rate-limit guard: long cartography sleeps become a
RATE_LIMITED result instead of a Lambda timeout. No network, no Neo4j.

  <cartography-venv>/bin/python -m pytest tests -q
"""

import os
import sys
from datetime import datetime
from datetime import timezone
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
os.environ.setdefault("NEO4J_URI", "bolt://127.0.0.1:1")

import handler as h  # noqa: E402


class FakeContext:
    def __init__(self, remaining_s: float) -> None:
        self.remaining_s = remaining_s

    def get_remaining_time_in_millis(self) -> int:
        return int(self.remaining_s * 1000)


@pytest.fixture
def guard():
    yield h._guarded_time
    h._guarded_time.context = None


def test_cartography_util_uses_guarded_time():
    assert h.github_util.time is h._guarded_time
    # Everything else on `time` still resolves to the real module.
    assert h.github_util.time.monotonic is __import__("time").monotonic


def test_short_sleep_runs_in_process(guard):
    guard.context = FakeContext(remaining_s=800)
    with mock.patch.object(h.time, "sleep") as real_sleep:
        guard.sleep(30)
    real_sleep.assert_called_once_with(30)


def test_sleep_beyond_remaining_time_raises_with_resume_at(guard):
    guard.context = FakeContext(remaining_s=800)  # 740s usable after the margin
    before = datetime.now(timezone.utc)
    with mock.patch.object(h.time, "sleep") as real_sleep:
        with pytest.raises(h.GitHubRateLimited) as ei:
            guard.sleep(1593)  # last night's 26.5 min
    real_sleep.assert_not_called()
    delta = (ei.value.resume_at - before).total_seconds()
    assert 1593 <= delta < 1600


def test_margin_is_respected(guard):
    guard.context = FakeContext(remaining_s=100)  # 40s usable
    with mock.patch.object(h.time, "sleep"):
        guard.sleep(40)
        with pytest.raises(h.GitHubRateLimited):
            guard.sleep(41)


def test_no_context_means_plain_sleep(guard):
    guard.context = None  # e.g. local scripts importing handler
    with mock.patch.object(h.time, "sleep") as real_sleep:
        guard.sleep(5000)
    real_sleep.assert_called_once_with(5000)


def test_not_swallowed_by_except_exception(guard):
    guard.context = FakeContext(remaining_s=100)

    def cartography_style():
        try:
            h.github_util.time.sleep(3600)
        except Exception:  # noqa: BLE001 — what cartography does
            return "swallowed"
        return "continued"

    with pytest.raises(h.GitHubRateLimited):
        cartography_style()


def test_handler_returns_rate_limited(monkeypatch):
    class FakeSessionCM:
        def __enter__(self):
            return object()

        def __exit__(self, *a):
            return False

    class FakeDriver:
        closed = False

        def session(self, **k):
            return FakeSessionCM()

        def close(self):
            FakeDriver.closed = True

    def ghcr(*a, **k):
        h.github_util.time.sleep(1593)

    monkeypatch.setattr(h, "_get_neo4j_driver", lambda: FakeDriver())
    monkeypatch.setattr(h, "_ghcr", ghcr)
    with mock.patch.object(h.time, "sleep") as real_sleep:
        r = h.handler(
            {"step": "ghcr", "org": "example-org", "part": 6, "update_tag": 1},
            FakeContext(remaining_s=800),
        )
    real_sleep.assert_not_called()
    assert r["status"] == "RATE_LIMITED"
    assert r["part"] == 6
    datetime.strptime(r["resumeAt"], "%Y-%m-%dT%H:%M:%SZ")
    assert FakeDriver.closed
    assert h._guarded_time.context is None


# ── GHCR cleanup-part guard ────────────────────────────────────────────────
# Lives here (not test_splits.py) to keep today's GHCR safety tests together.


class TaggedPackages:
    """execute_read stand-in: returns the requested ids stamped with the tag."""

    def __init__(self, stamped):
        self.stamped = set(stamped)
        self.calls = []

    def execute_read(self, fn, query, **params):
        self.calls.append(params)
        return [{"id": i} for i in params["ids"] if i in self.stamped]


def _ids(n):
    return [f"https://github.com/orgs/o/packages/container/package/p{i}" for i in range(n)]


def test_ghcr_guard_passes_when_earlier_parts_loaded():
    s = TaggedPackages(_ids(300))
    h._check_earlier_ghcr_parts_loaded(s, "o", "https://github.com/o", 6, _ids(300), 7)
    assert s.calls[0]["update_tag"] == 7


def test_ghcr_guard_refuses_lone_last_part():
    # Last part alone under a fresh tag; nothing from earlier parts stamped.
    s = TaggedPackages(set())
    with pytest.raises(RuntimeError, match="refusing"):
        h._check_earlier_ghcr_parts_loaded(s, "o", "https://github.com/o", 6, _ids(300), 7)


def test_ghcr_guard_refuses_partial():
    s = TaggedPackages(_ids(299))  # one package from an earlier part missing
    with pytest.raises(RuntimeError, match="1 of 300"):
        h._check_earlier_ghcr_parts_loaded(s, "o", "https://github.com/o", 6, _ids(300), 7)


def test_ghcr_guard_unsplit_org_skips_query():
    s = TaggedPackages(set())
    h._check_earlier_ghcr_parts_loaded(s, "o", "https://github.com/o", 0, [], 7)
    assert s.calls == []


def test_ghcr_last_part_refuses_before_loading(monkeypatch):
    class Fetch:
        cleanup_safe = True
        packages = [{"name": f"p{i}", "html_url": u} for i, u in enumerate(_ids(4))]

    loaded = []
    monkeypatch.setattr(h, "_get_domains_credential", lambda org: "tok")
    monkeypatch.setattr(h, "_ghcr_range_for_part", lambda org, part: (2, None))
    monkeypatch.setattr(h.packages, "get_container_packages", lambda *a: Fetch())
    monkeypatch.setattr(h.packages, "load_packages", lambda *a, **k: loaded.append(a))
    monkeypatch.setattr(h.packages, "cleanup_packages", lambda *a, **k: loaded.append("cleanup"))
    with pytest.raises(RuntimeError, match="refusing"):
        h._ghcr(TaggedPackages(set()), "o", 7, part=1)
    assert loaded == []
