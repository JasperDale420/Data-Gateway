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
