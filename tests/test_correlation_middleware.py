"""CorrelationIdMiddleware — per-request x-trace-id binding and echo."""

from __future__ import annotations


def test_response_includes_generated_trace_id(client) -> None:
    # No inbound header → middleware mints one and echoes it.
    resp = client.get("/health")
    trace_id = resp.headers.get("x-trace-id")
    assert trace_id
    assert len(trace_id) >= 16


def test_response_echoes_supplied_trace_id(client) -> None:
    resp = client.get("/health", headers={"x-trace-id": "trace-abc-123"})
    assert resp.headers.get("x-trace-id") == "trace-abc-123"


def test_blank_inbound_trace_id_is_replaced(client) -> None:
    resp = client.get("/health", headers={"x-trace-id": "   "})
    trace_id = resp.headers.get("x-trace-id")
    assert trace_id and trace_id.strip()


def test_each_request_gets_a_distinct_trace_id(client) -> None:
    first = client.get("/health").headers.get("x-trace-id")
    second = client.get("/health").headers.get("x-trace-id")
    assert first and second and first != second
