"""Останавливаем API и проверяем срок жизни кеша, затем возвращаем сервис."""

import asyncio
import json
import subprocess
import time
from pathlib import Path

import httpx
from smoke import create_demo, set_flag, wait_until

from flagharbor.sdk import FlagClient

ROOT = Path(__file__).resolve().parents[1]


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True)


async def main():
    base = "http://localhost:8190"
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        env, headers = await create_demo(client)
        await set_flag(client, env, headers)
        async with FlagClient(
            base, env["sdk_key"], max_stale=2, refresh_interval=0.2, watch=False
        ) as sdk:
            context = {"user_id": "outage-user"}
            assert sdk.evaluate("search", context).value
            try:
                await asyncio.to_thread(compose, "kill", "--signal", "SIGKILL", "api")
                await sdk.refresh()
                assert sdk.evaluate("search", context).value and sdk.last_error
                started = time.monotonic()
                await wait_until(lambda: not sdk.evaluate("search", context).value, timeout=4)
                expired_after = round(time.monotonic() - started, 2)
                assert sdk.evaluate("search", context).reason == "unavailable"
            finally:
                await asyncio.to_thread(
                    compose, "up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "api"
                )
            await wait_until(lambda: sdk.evaluate("search", context).value, timeout=5)
            await set_flag(client, env, headers, enabled=False)
            await wait_until(lambda: not sdk.evaluate("search", context).value)
            print(
                json.dumps(
                    {
                        "ok": True,
                        "fault": "SIGKILL flags API",
                        "short_outage_uses_cache": True,
                        "expired_cache_value": False,
                        "expiry_wait_seconds": expired_after,
                        "recovered_configuration": True,
                        "kill_switch_after_recovery": True,
                    },
                    indent=2,
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
