"""Behavior tests: UW provider should log transient upstream errors at WARN, persistent at ERROR."""

from __future__ import annotations

import json
from typing import Any

import pytest

from gateway.providers.uw import UnusualWhalesProvider


class _RecordingLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def warning(self, event: str, **kwargs: Any) -> None:
        self.calls.append(("warning", event, kwargs))

    def error(self, event: str, **kwargs: Any) -> None:
        self.calls.append(("error", event, kwargs))

    def info(self, event: str, **kwargs: Any) -> None:
        self.calls.append(("info", event, kwargs))

    def debug(self, event: str, **kwargs: Any) -> None:
        self.calls.append(("debug", event, kwargs))


@pytest.fixture
def recording_flow_logger(monkeypatch):
    rec = _RecordingLogger()
    monkeypatch.setattr("gateway.providers.uw.flow.logger", rec)
    return rec


@pytest.fixture
def recording_options_logger(monkeypatch):
    rec = _RecordingLogger()
    monkeypatch.setattr("gateway.providers.uw.options.logger", rec)
    return rec


@pytest.mark.asyncio
async def test_market_tide_transient_jsondecode_logs_at_warning(monkeypatch, recording_flow_logger):
    provider = UnusualWhalesProvider()
    provider._client = object()

    async def _raise_json_decode(*_args: Any, **_kwargs: Any) -> Any:
        raise json.JSONDecodeError("Expecting value", "", 0)

    monkeypatch.setattr(provider, "_call_sync", _raise_json_decode)

    with pytest.raises(json.JSONDecodeError):
        await provider.get_market_tide()

    levels = [(level, event) for level, event, _ in recording_flow_logger.calls if event == "uw_market_tide_failed"]
    assert levels == [("warning", "uw_market_tide_failed")], (
        f"Expected one WARNING for transient JSONDecodeError, got: {levels}"
    )


@pytest.mark.asyncio
async def test_market_tide_persistent_error_logs_at_error(monkeypatch, recording_flow_logger):
    provider = UnusualWhalesProvider()
    provider._client = object()

    async def _raise_value_error(*_args: Any, **_kwargs: Any) -> Any:
        raise ValueError("genuinely broken response shape")

    monkeypatch.setattr(provider, "_call_sync", _raise_value_error)

    with pytest.raises(ValueError):
        await provider.get_market_tide()

    levels = [(level, event) for level, event, _ in recording_flow_logger.calls if event == "uw_market_tide_failed"]
    assert levels == [("error", "uw_market_tide_failed")], (
        f"Expected one ERROR for persistent ValueError, got: {levels}"
    )


@pytest.mark.asyncio
async def test_greek_exposure_transient_jsondecode_logs_at_warning(monkeypatch, recording_options_logger):
    provider = UnusualWhalesProvider()
    provider._client = object()

    async def _raise_json_decode(*_args: Any, **_kwargs: Any) -> Any:
        raise json.JSONDecodeError("Expecting value", "", 0)

    monkeypatch.setattr(provider, "_call_sync", _raise_json_decode)

    with pytest.raises(json.JSONDecodeError):
        await provider.get_greek_exposure("AAPL")

    levels = [
        (level, event) for level, event, _ in recording_options_logger.calls if event == "uw_greek_exposure_failed"
    ]
    assert levels == [("warning", "uw_greek_exposure_failed")], (
        f"Expected one WARNING for transient JSONDecodeError on greek exposure, got: {levels}"
    )
