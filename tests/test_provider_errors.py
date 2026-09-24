"""Tests for the shared provider HTTP error-severity logger."""

import httpx

import gateway.providers._errors as errors_module
from gateway.providers._errors import log_provider_http_error


class _CapturingLogger:
    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict]] = []
        self.errors: list[tuple[str, dict]] = []

    def warning(self, event: str, **kw) -> None:
        self.warnings.append((event, kw))

    def error(self, event: str, **kw) -> None:
        self.errors.append((event, kw))


def _http_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(str(status), request=request, response=response)


def test_4xx_logs_warning(monkeypatch) -> None:
    """A client-caused 4xx is a warning, not an error (the flood fix)."""
    log = _CapturingLogger()
    monkeypatch.setattr(errors_module, "logger", log)

    log_provider_http_error("finnhub_quote_failed", _http_error(404), symbol="AAPL")

    assert len(log.warnings) == 1 and not log.errors
    event, kw = log.warnings[0]
    assert event == "finnhub_quote_failed"
    assert kw["symbol"] == "AAPL"
    assert kw["status_code"] == 404


def test_5xx_logs_error(monkeypatch) -> None:
    """A server 5xx stays an error."""
    log = _CapturingLogger()
    monkeypatch.setattr(errors_module, "logger", log)

    log_provider_http_error("finnhub_quote_failed", _http_error(503), symbol="AAPL")

    assert len(log.errors) == 1 and not log.warnings
    assert log.errors[0][1]["status_code"] == 503


def test_non_http_logs_error(monkeypatch) -> None:
    """A non-HTTP failure (timeout, parse error) stays an error with no status."""
    log = _CapturingLogger()
    monkeypatch.setattr(errors_module, "logger", log)

    log_provider_http_error("finnhub_quote_failed", ValueError("boom"))

    assert len(log.errors) == 1 and not log.warnings
    assert "status_code" not in log.errors[0][1]


def test_status_and_status_code_aliases_both_present(monkeypatch) -> None:
    """Both the legacy `status` field and the new `status_code` field must
    coexist on a migrated call site's logged event, per the repo's documented
    severity convention (see AGENTS.md "Error-log severity convention")."""
    log = _CapturingLogger()
    monkeypatch.setattr(errors_module, "logger", log)

    log_provider_http_error("finnhub_quote_failed", _http_error(404), symbol="AAPL")

    event, kw = log.warnings[0]
    assert kw["status"] == 404
    assert kw["status_code"] == 404
    assert kw["status"] == kw["status_code"]


# Events that, before the migration to log_provider_http_error, did not log an
# `error` field at all. The helper now unconditionally adds one (Codex review
# finding 1 on PR #83) — this is a field-parity change worth pinning with a
# test even though whether to preserve the old shape is left to the author.
_PREVIOUSLY_ERROR_LESS_EVENTS = (
    "alpaca_crypto_latest_bars_error",
    "alpaca_crypto_latest_trades_error",
    "alpaca_option_trades_error",
    "alpaca_option_latest_trades_error",
    "alpaca_option_snapshots_error",
    "news_sentiment_error",
)


def test_previously_error_less_events_now_include_error_field(monkeypatch) -> None:
    """The six events that used to omit `error` entirely now get it from the
    shared helper, for both the warning (4xx) and error (5xx) severity paths."""
    log = _CapturingLogger()
    monkeypatch.setattr(errors_module, "logger", log)

    for event in _PREVIOUSLY_ERROR_LESS_EVENTS:
        log_provider_http_error(event, _http_error(404), symbol="AAPL")
        log_provider_http_error(event, _http_error(503))

    for logged_event, kw in log.warnings:
        assert logged_event in _PREVIOUSLY_ERROR_LESS_EVENTS
        assert "error" in kw

    for logged_event, kw in log.errors:
        assert logged_event in _PREVIOUSLY_ERROR_LESS_EVENTS
        assert "error" in kw

    assert len(log.warnings) == len(_PREVIOUSLY_ERROR_LESS_EVENTS)
    assert len(log.errors) == len(_PREVIOUSLY_ERROR_LESS_EVENTS)
