"""Flow tests run against Prefect's in-process test harness — no server needed."""

import httpx
import pytest
from prefect.testing.utilities import prefect_test_harness

from flows.url_check import CheckResult, check_url, url_check


@pytest.fixture(autouse=True, scope="session")
def _prefect_harness():
    with prefect_test_harness():
        yield


class _FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


def test_flow_passes_when_all_urls_ok(monkeypatch):
    monkeypatch.setattr(httpx, "get", lambda url, **_: _FakeResponse(200))
    results = url_check(urls=["https://a.example", "https://b.example"], timeout=1)
    assert [r.ok for r in results] == [True, True]
    assert all(isinstance(r, CheckResult) for r in results)


def test_flow_fails_when_a_url_is_down(monkeypatch):
    monkeypatch.setattr(httpx, "get", lambda url, **_: _FakeResponse(503 if "b" in url else 200))
    with pytest.raises(RuntimeError, match="1 of 2 urls failed"):
        url_check(urls=["https://a.example", "https://b.example"], timeout=1)


def test_flow_can_report_instead_of_fail(monkeypatch):
    monkeypatch.setattr(httpx, "get", lambda url, **_: _FakeResponse(500))
    results = url_check(urls=["https://a.example"], timeout=1, fail_on_error=False)
    assert results[0].ok is False


def test_task_retries_then_raises(monkeypatch):
    calls = {"n": 0}

    def boom(url, **_):
        calls["n"] += 1
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(httpx, "get", boom)
    with pytest.raises(httpx.ConnectError):
        check_url.with_options(retry_delay_seconds=0)("https://a.example", 1)
    assert calls["n"] == 3  # 1 attempt + 2 retries
