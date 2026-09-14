"""URL availability check — a small but complete Prefect 3 flow.

Demonstrates the pieces a production flow should have: typed parameters,
tasks with retries, concurrent task submission, structured logging, a
result summary that fails the run when something is wrong, and a
``__main__`` block so the file runs locally without a server.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import httpx
from prefect import flow, get_run_logger, task

DEFAULT_URLS = [
    "https://docs.prefect.io",
    "https://kubernetes.io",
    "https://www.python.org",
]


@dataclass(frozen=True)
class CheckResult:
    url: str
    status: int | None
    elapsed_ms: int
    ok: bool


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def check_url(url: str, timeout: float) -> CheckResult:
    """GET one URL and report status and latency. Retries on network errors."""
    start = time.perf_counter()
    try:
        response = httpx.get(url, timeout=timeout, follow_redirects=True)
        status: int | None = response.status_code
    except httpx.HTTPError as exc:
        print(f"{url}: {exc.__class__.__name__}: {exc}")
        raise
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    ok = 200 <= status < 400
    print(f"{url}: {status} in {elapsed_ms} ms")
    return CheckResult(url=url, status=status, elapsed_ms=elapsed_ms, ok=ok)


@flow(name="url-check")
def url_check(
    urls: list[str] | None = None,
    timeout: float = 10.0,
    fail_on_error: bool = True,
) -> list[CheckResult]:
    """Check a list of URLs concurrently and summarise the outcome.

    Args:
        urls: URLs to probe; defaults to a small public set.
        timeout: per-request timeout in seconds.
        fail_on_error: when true the flow run fails if any URL is not OK,
            which is what you want for a scheduled monitor.
    """
    logger = get_run_logger()
    targets = urls or DEFAULT_URLS
    futures = [check_url.submit(url, timeout) for url in targets]
    results = [future.result() for future in futures]

    failed = [r for r in results if not r.ok]
    logger.info("checked %d urls, %d failed", len(results), len(failed))
    for r in failed:
        logger.warning("FAILED %s -> %s", r.url, r.status)
    if failed and fail_on_error:
        raise RuntimeError(f"{len(failed)} of {len(results)} urls failed")
    return results


if __name__ == "__main__":
    url_check()
