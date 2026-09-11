"""Проверяем живой API и доставку изменений через SSE, включая отзыв SDK-ключа."""

import asyncio
import json
import os
import statistics
import time
from uuid import uuid4

import httpx

from flagharbor.sdk import FlagClient


async def create_demo(client):
    credentials = {"email": f"smoke-{uuid4().hex}@example.com", "password": "FlagHarborDemo123!"}
    response = await client.post("/auth/register", json=credentials)
    response.raise_for_status()
    token = (await client.post("/auth/login", json=credentials)).json()["access_token"]
    headers = {"Authorization": "Bearer " + token}
    response = await client.post("/environments", json={"name": "Smoke"}, headers=headers)
    response.raise_for_status()
    return response.json(), headers


async def set_flag(client, env, headers, *, rollout=10000, enabled=True):
    current = await client.get("/environments/" + env["id"], headers=headers)
    current.raise_for_status()
    response = await client.put(
        f"/environments/{env['id']}/flags/search",
        json={"rollout_bps": rollout, "enabled": enabled},
        headers={**headers, "If-Match": current.headers["ETag"]},
    )
    response.raise_for_status()
    return response.json()["revision"]


async def wait_until(predicate, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("SDK state did not change before deadline")


async def main():
    base = os.environ.get("API_URL", "http://localhost:8000")
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        env, headers = await create_demo(client)
        await set_flag(client, env, headers, rollout=100)
        async with FlagClient(base, env["sdk_key"], refresh_interval=30) as sdk:
            cohorts = []
            for percent in [100, 1000, 10000]:
                await set_flag(client, env, headers, rollout=percent)
                await sdk.refresh()
                cohorts.append(
                    {i for i in range(10000) if sdk.evaluate("search", {"user_id": str(i)}).value}
                )
            assert cohorts[0] <= cohorts[1] <= cohorts[2] and len(cohorts[2]) == 10000
            # Опрос настроен на 30 секунд. Более быстрое обновление здесь приходит по SSE.
            started = time.monotonic()
            revision = await set_flag(client, env, headers, enabled=False)
            await wait_until(lambda: sdk.evaluate("search", {"user_id": "42"}).revision == revision)
            push_ms = (time.monotonic() - started) * 1000
            assert not sdk.evaluate("search", {"user_id": "42"}).value
            await set_flag(client, env, headers)
            await sdk.refresh()
            durations = []
            for i in range(10000):
                before = time.perf_counter_ns()
                assert sdk.evaluate("search", {"user_id": str(i)}).value
                durations.append((time.perf_counter_ns() - before) / 1000)
            server = await client.post(
                "/sdk/evaluate",
                json={"key": "search", "context": {"user_id": "42"}},
                headers={"Authorization": "Bearer " + env["sdk_key"]},
            )
            assert server.json() == sdk.evaluate("search", {"user_id": "42"}).model_dump()
            current = await client.get("/environments/" + env["id"], headers=headers)
            rotated = await client.post(
                f"/environments/{env['id']}/rotate-key",
                headers={**headers, "If-Match": current.headers["ETag"]},
            )
            rotated.raise_for_status()
            await wait_until(lambda: sdk.last_error == "revoked")
            assert not sdk.evaluate("search", {"user_id": "42"}).value
            print(
                json.dumps(
                    {
                        "ok": True,
                        "cohorts_1_10_100_percent": [len(c) for c in cohorts],
                        "nested_cohorts": True,
                        "sse_kill_ms": round(push_ms, 2),
                        "sdk_evaluations": 10000,
                        "sdk_p50_us": round(statistics.median(durations), 2),
                        "sdk_p95_us": round(sorted(durations)[9499], 2),
                        "revoked_key_clears_cache": True,
                    },
                    indent=2,
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
