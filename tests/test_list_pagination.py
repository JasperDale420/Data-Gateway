from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest
from fastapi.responses import StreamingResponse

from gateway.api import bulk, replay


class _FakeStatus:
    def __init__(self, value: str) -> None:
        self.value = value


class _FakeJob:
    def __init__(self, job_id: str, status: str, client_id: str = "client-1") -> None:
        self.job_id = job_id
        self.status = _FakeStatus(status)
        self.client_id = client_id

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

    async def list_jobs_page(
        self,
        *,
        client_id: str | None = None,
        status: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[list[_FakeJob], int, bool, int | None]:
        jobs = list(self._jobs)
        if client_id is not None:
            jobs = [job for job in jobs if job.client_id == client_id]
        if status:
            jobs = [job for job in jobs if job.status.value == status]
        total = len(jobs)
        if offset:
            jobs = jobs[offset:]
        if limit is not None:
            jobs = jobs[:limit]
        consumed = offset + len(jobs)
        has_more = consumed < total
        return jobs, total, has_more, (consumed if has_more else None)

    async def get_job(self, job_id: str) -> _FakeJob | None:
        for job in self._jobs:
            if job.job_id == job_id:
                return job
        return None

    def get_results_page(
        self,
        _job_id: str,
        *,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, int]], int, bool, int | None]:
        return (
            [{"row": offset + 1}],
            3,
            offset + limit < 3,
            (offset + limit) if offset + limit < 3 else None,
        )

    def iter_results_jsonl_chunks(
        self,
        _job_id: str,
        *,
        offset: int = 0,
        limit: int | None = None,
    ):
        yield f"jsonl:{offset}:{limit}".encode()

    def iter_results_json_chunks(
        self,
        _job_id: str,
        *,
        offset: int = 0,
        limit: int | None = None,
    ):
        yield f'{{"window":"{offset}:{limit}"}}'.encode()


class _FakeReplayManager:
    def __init__(self, sessions: list[_FakeSession]) -> None:
        self._sessions = sessions

    async def list_sessions(self, _client_id: str) -> list[_FakeSession]:
        return list(self._sessions)

    async def list_sessions_page(
        self,
        *,
        client_id: str | None = None,
        state: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[list[_FakeSession], int, bool, int | None]:
        sessions = list(self._sessions)
        _ = client_id
        _ = state
        total = len(sessions)
        if offset:
            sessions = sessions[offset:]
        if limit is not None:
            sessions = sessions[:limit]
        consumed = offset + len(sessions)
        has_more = consumed < total
        return sessions, total, has_more, (consumed if has_more else None)


async def _read_streaming_text(response: StreamingResponse) -> str:
    parts: list[bytes] = []
    async for chunk in response.body_iterator:
        if isinstance(chunk, bytes):
            parts.append(chunk)
        elif isinstance(chunk, str):
            parts.append(chunk.encode("utf-8"))
        else:
            parts.append(bytes(chunk))
    return b"".join(parts).decode("utf-8")


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
        "total": 2,
        "offset": 1,
        "limit": 1,
        "has_more": False,
        "next_offset": None,
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
        "total": 3,
        "offset": 1,
        "limit": 2,
        "has_more": False,
        "next_offset": None,
    }


@pytest.mark.asyncio
async def test_bulk_get_job_results_page_applies_limit_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _FakeBulkManager(jobs=[_FakeJob("job-1", "complete")])
    monkeypatch.setattr(bulk, "get_bulk_manager", lambda: manager)

    response = await bulk.get_job_results_page(
        job_id="job-1",
        limit=2,
        offset=1,
        client=SimpleNamespace(id="client-1"),
    )

    assert response == {
        "results": [{"row": 2}],
        "count": 1,
        "total_records": 3,
        "offset": 1,
        "limit": 2,
        "has_more": False,
        "next_offset": None,
    }


@pytest.mark.asyncio
async def test_bulk_download_job_results_forwards_offset_and_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _FakeBulkManager(jobs=[_FakeJob("job-1", "complete")])
    monkeypatch.setattr(bulk, "get_bulk_manager", lambda: manager)

    jsonl_response = cast(
        StreamingResponse,
        await bulk.download_job_results(
            job_id="job-1",
            format="jsonl",
            limit=5,
            offset=2,
            client=SimpleNamespace(id="client-1"),
        ),
    )
    jsonl_body = await _read_streaming_text(jsonl_response)
    assert jsonl_body == "jsonl:2:5"

    json_response = cast(
        StreamingResponse,
        await bulk.download_job_results(
            job_id="job-1",
            format="json",
            limit=4,
            offset=1,
            client=SimpleNamespace(id="client-1"),
        ),
    )
    json_body = await _read_streaming_text(json_response)
    assert json_body == '{"window":"1:4"}'
