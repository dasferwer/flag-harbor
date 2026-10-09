import asyncio
import json
import random
import time

import httpx
import pytest

from flagharbor.sdk import FlagClient
from scripts.sdk_reconnect_proof import Target, validate_target, window


async def test_concurrent_start_does_not_leak_background_tasks():
    async def handler(request):
        await asyncio.sleep(0)
        return httpx.Response(503)

    sdk = FlagClient("http://test", "key", transport=httpx.MockTransport(handler))
    before = asyncio.all_tasks()
    spawned = set()
    try:
        await asyncio.gather(sdk.start(), sdk.start())
        spawned = asyncio.all_tasks() - before
        assert len(spawned) == 2
        assert spawned == set(sdk._tasks)
    finally:
        await sdk.close()
        for task in spawned:
            task.cancel()
        await asyncio.gather(*spawned, return_exceptions=True)


async def test_close_waits_for_inflight_start_and_cleans_all_tasks():
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(request):
        entered.set()
        await release.wait()
        return httpx.Response(503)

    sdk = FlagClient("http://test", "key", transport=httpx.MockTransport(handler))
    started = asyncio.create_task(sdk.start())
    await entered.wait()
    closed = asyncio.create_task(sdk.close())
    try:
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(started, closed)
        assert sdk._tasks == []
        assert sdk.http.is_closed
    finally:
        release.set()
        await asyncio.gather(started, closed, return_exceptions=True)
        await sdk.close()


async def test_retry_jitter_is_bounded_and_spreads_clients():
    delays = []
    for seed in range(32):
        sdk = FlagClient("http://test", "key", refresh_interval=2)
        sdk._random = random.Random(seed)
        try:
            assert 1.6 <= sdk._delay(0) <= 2.4
            assert 2 <= sdk._delay(1) <= 4
            assert 15 <= sdk._delay(10000) <= 30
            delays.append(sdk._delay(3))
        finally:
            await sdk.close()
    assert len(set(delays)) == 32


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True, -1, 0])
def test_invalid_timeouts_rejected(value):
    with pytest.raises(ValueError):
        FlagClient("http://test", "key", refresh_interval=value)


async def test_old_revision_does_not_replace_or_renew_cache():
    snapshot = {
        "environment_id": "19000000-0000-0000-0000-000000000001",
        "revision": 5,
        "enabled": True,
        "flags": [],
    }

    async def handler(request):
        return httpx.Response(200, json=snapshot)

    sdk = FlagClient("http://test", "key", transport=httpx.MockTransport(handler), max_stale=1)
    try:
        assert await sdk.refresh()
        accepted, _ = sdk._state
        old_time = time.monotonic() - 2
        sdk._state = (accepted, old_time)
        snapshot["revision"] = 4
        assert not await sdk.refresh()
        assert sdk._state == (accepted, old_time)
        assert sdk.evaluate("missing", {"user_id": "x"}).reason == "unavailable"
    finally:
        await sdk.close()


@pytest.mark.parametrize(
    "project,base",
    [
        ("19-flag-harbor", "http://localhost:8190"),
        ("codex-p2-19-1009", "https://localhost:8190"),
        ("codex-p2-19-1009", "http://example.com"),
        ("codex-p2-19-1009", "http://user:pass@localhost:8190"),
        ("codex-p2-19-1009", "http://localhost:8190/?target=other"),
    ],
)
def test_proof_refuses_user_services_and_external_hosts(project, base):
    with pytest.raises(ValueError):
        validate_target(project, base)


def test_attempt_bucket_boundaries_and_excluded_future():
    rows = [
        {"at": 1.001, "path": "/sdk/events"},
        {"at": 1.01, "path": "/sdk/snapshot"},
        {"at": 1.2, "path": "/sdk/snapshot"},
        {"at": 2, "path": "/sdk/snapshot"},
    ]
    result = window(rows, 1, 2)
    assert result["attempts"] == 3
    assert result["max_attempts_per_50ms"] == 2
    assert result["sse_attempts"] == 1


@pytest.mark.parametrize("port,allowed", [(54919, True), (8190, False)])
def test_proof_url_matches_owned_api_binding(monkeypatch, tmp_path, port, allowed):
    records = []
    for service in ("api", "database"):
        records.append(
            {
                "Config": {
                    "Labels": {
                        "com.docker.compose.project": "codex-p2-19-1009",
                        "com.docker.compose.service": service,
                    }
                },
                "Mounts": [],
                "NetworkSettings": {
                    "Ports": {"8000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "54919"}]}
                },
            }
        )
    monkeypatch.setattr(
        "scripts.sdk_reconnect_proof.subprocess.check_output",
        lambda command: (
            json.dumps(records).encode() if command[1] == "inspect" else b"api-id db-id"
        ),
    )
    if allowed:
        Target("codex-p2-19-1009", f"http://127.0.0.1:{port}", tmp_path / "compose.json")
    else:
        with pytest.raises(ValueError):
            Target("codex-p2-19-1009", f"http://127.0.0.1:{port}", tmp_path / "compose.json")
