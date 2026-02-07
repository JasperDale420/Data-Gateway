from __future__ import annotations

from types import SimpleNamespace

import pytest

from gateway.api import bulk, replay


class _FakeStatus:
    def __init__(self, value: str) -> None:
        self.value = value


class _FakeJob:
    def __init__(self, job_id: str, status: str) -> None:
        self.job_id = job_id
        self.status = _FakeStatus(status)

    def to_dict(self) -> dict[str, str]:
        return {"job_id": self.job_id, "status": self.status.value}


class _FakeSession:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id

    def to_dict(self) -> dict[str, str]:
        return {"session_id": self.session_id}


class _FakeBulkManager:
    def __init__(self, jobs: list[_FakeJob]) -> None:
        self._jobs = jobs

    async def list_jobs(self, _client_id: str) -> list[_FakeJob]:
        return list(self._jobs)


class _FakeReplayManager:
    def __init__(self, sessions: list[_FakeSession]) -> None:
        self._sessions = sessions

    async def list_sessions(self, _client_id: str) -> list[_FakeSession]:
        return list(self._sessions)


@pytest.mark.asyncio
async def test_bulk_list_jobs_applies_status_limit_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = _FakeBulkManager(
        jobs=[
            _FakeJob("job-1", "completed"),
            _FakeJob("job-2", "running"),
            _FakeJob("job-3", "completed"),
        ]
    )
    monkeypatch.setattr(bulk, "get_bulk_manager", lambda: manager)

    response = await bulk.list_jobs(
        status="completed",
        limit=1,
        offset=1,
        client=SimpleNamespace(id="client-1"),
    )

    assert response == {
        "jobs": [{"job_id": "job-3", "status": "completed"}],
        "count": 1,
    }


@pytest.mark.asyncio
async def test_replay_list_sessions_applies_limit_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = _FakeReplayManager(
        sessions=[
            _FakeSession("session-1"),
            _FakeSession("session-2"),
            _FakeSession("session-3"),
        ]
    )
    monkeypatch.setattr(replay, "get_replay_manager", lambda: manager)

    response = await replay.list_sessions(
        limit=2,
        offset=1,
        client=SimpleNamespace(id="client-1"),
    )

    assert response == {
        "sessions": [
            {"session_id": "session-2"},
            {"session_id": "session-3"},
        ],
        "count": 2,
    }
