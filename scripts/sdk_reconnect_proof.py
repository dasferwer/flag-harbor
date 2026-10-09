"""Изолированный real-HTTP сценарий множества SDK; останавливает только proof API."""

import argparse
import asyncio
import json
import platform
import random
import re
import subprocess
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from flagharbor.sdk import FlagClient
from scripts.smoke import create_demo, set_flag, wait_until

ROOT = Path(__file__).resolve().parents[1]


def validate_target(project, base):
    if not re.fullmatch(r"(?:codex-p2-19-1009|proof-flagharbor-[a-z0-9-]{1,40})", project):
        raise ValueError("Разрешён только собственный proof namespace")
    url = urlsplit(base)
    if (
        url.scheme != "http"
        or url.hostname not in {"127.0.0.1", "localhost"}
        or url.username
        or url.password
        or url.path not in {"", "/"}
        or url.query
        or url.fragment
    ):
        raise ValueError("Разрешён только локальный HTTP API")


class Target:
    def __init__(self, project, base, compose_file):
        validate_target(project, base)
        self.command = ["docker", "compose", "-p", project, "-f", str(compose_file)]
        ids = subprocess.check_output(self.command + ["ps", "-q", "api", "database"])
        if len(ids.split()) != 2:
            raise ValueError("Ожидаются собственные api/database")
        containers = json.loads(
            subprocess.check_output(["docker", "inspect", *ids.decode().split()])
        )
        for container in containers:
            labels = container["Config"]["Labels"]
            if labels.get("com.docker.compose.project") != project:
                raise ValueError("Namespace контейнера не совпадает")
            for mount in container["Mounts"]:
                if mount["Type"] == "bind" or (
                    mount["Type"] == "volume" and not mount["Name"].startswith(project + "_")
                ):
                    raise ValueError("Proof не должен использовать внешнее хранилище")
            if labels.get("com.docker.compose.service") == "api":
                port = urlsplit(base).port or 80
                bindings = container["NetworkSettings"]["Ports"].get("8000/tcp") or []
                if not any(
                    binding["HostIp"] in {"127.0.0.1", "::1"} and int(binding["HostPort"]) == port
                    for binding in bindings
                ):
                    raise ValueError("HTTP-адрес не соответствует порту собственной api")

    def compose(self, *args):
        subprocess.run(self.command + list(args), check=True, capture_output=True, timeout=120)


class ObservedStream(httpx.AsyncByteStream):
    def __init__(self, stream, done):
        self.stream, self.done = stream, done
        self.closed = False

    async def __aiter__(self):
        async for chunk in self.stream:
            yield chunk

    async def aclose(self):
        try:
            await self.stream.aclose()
        finally:
            if not self.closed:
                self.closed = True
                self.done()


class ObservedTransport(httpx.AsyncBaseTransport):
    def __init__(self, metrics, index):
        self.inner = httpx.AsyncHTTPTransport(retries=0)
        self.metrics, self.index = metrics, index
        self.active = 0

    async def handle_async_request(self, request):
        row = {"at": time.monotonic(), "client": self.index, "path": request.url.path}
        self.metrics["requests"].append(row)
        self.active += 1
        self.metrics["active"] += 1
        self.metrics["peak"] = max(self.metrics["peak"], self.metrics["active"])
        self.metrics["per_client_peak"] = max(self.metrics["per_client_peak"], self.active)

        def done():
            self.active -= 1
            self.metrics["active"] -= 1

        try:
            response = await self.inner.handle_async_request(request)
            row["status"] = response.status_code
            response.stream = ObservedStream(response.stream, done)
            return response
        except BaseException as error:
            row["error"] = type(error).__name__
            done()
            raise

    async def aclose(self):
        await self.inner.aclose()


def window(rows, start, end):
    selected = [row for row in rows if start <= row["at"] < end]
    buckets = Counter(int((row["at"] - start) / 0.05) for row in selected)
    return {
        "duration_seconds": round(end - start, 3),
        "attempts": len(selected),
        "snapshot_attempts": sum(row["path"] == "/sdk/snapshot" for row in selected),
        "sse_attempts": sum(row["path"] == "/sdk/events" for row in selected),
        "max_attempts_per_50ms": max(buckets.values(), default=0),
        "buckets_50ms": dict(sorted(buckets.items())),
    }


async def run(args):
    target = Target(args.project, args.base_url, args.compose_file)
    metrics = {"requests": [], "active": 0, "peak": 0, "per_client_peak": 0}
    clients = []
    context = {"user_id": "proof-user"}
    async with httpx.AsyncClient(base_url=args.base_url, timeout=10, trust_env=False) as admin:
        env, headers = await create_demo(admin)
        await set_flag(admin, env, headers)
        for index in range(args.clients):
            sdk = FlagClient(
                args.base_url,
                env["sdk_key"],
                max_stale=1,
                refresh_interval=0.2,
                transport=ObservedTransport(metrics, index),
                watch=index % 2 == 0,
            )
            sdk._random = random.Random(args.seed + index)
            clients.append(sdk)
        try:
            await asyncio.gather(*(sdk.start() for sdk in clients))
            await asyncio.sleep(1)
            assert all(sdk.evaluate("search", context).value for sdk in clients)
            outage_start = time.monotonic()
            try:
                await asyncio.to_thread(target.compose, "kill", "--signal", "SIGKILL", "api")
                await asyncio.sleep(0.2)
                cached_before_ttl = sum(sdk.evaluate("search", context).value for sdk in clients)
                await asyncio.sleep(1.3)
                assert all(
                    sdk.evaluate("search", context).reason == "unavailable" for sdk in clients
                )
                expired_at = time.monotonic()
                await asyncio.sleep(0.5)
            finally:
                recovery_start = time.monotonic()
                await asyncio.to_thread(
                    target.compose, "up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "api"
                )
            await wait_until(
                lambda: all(sdk.evaluate("search", context).value for sdk in clients), timeout=20
            )
            recovered_at = time.monotonic()
            revision = await set_flag(admin, env, headers, enabled=False)
            await wait_until(
                lambda: all(
                    sdk.evaluate("search", context).revision == revision for sdk in clients
                ),
                timeout=10,
            )
            assert all(not sdk.evaluate("search", context).value for sdk in clients)
            current = await admin.get(f"/environments/{env['id']}", headers=headers)
            rotated = await admin.post(
                f"/environments/{env['id']}/rotate-key",
                headers={**headers, "If-Match": current.headers["ETag"]},
            )
            rotated.raise_for_status()
            await wait_until(
                lambda: all(sdk.last_error == "revoked" for sdk in clients), timeout=10
            )
            assert all(sdk._state[0] is None for sdk in clients)
            async with FlagClient(args.base_url, rotated.json()["sdk_key"], watch=False) as fresh:
                assert fresh.evaluate("search", context).revision == rotated.json()["revision"]
            task_counts = [len(sdk._tasks) for sdk in clients]
            assert task_counts == [2 if index % 2 == 0 else 1 for index in range(args.clients)]
            result = {
                "clients": args.clients,
                "sse_clients": args.clients // 2,
                "seed": args.seed,
                "max_stale_seconds": 1,
                "refresh_interval_seconds": 0.2,
                "cached_before_ttl": cached_before_ttl,
                "all_expired_at_observation_seconds": round(expired_at - outage_start, 3),
                "outage": window(metrics["requests"], outage_start, recovery_start),
                "recovery": window(metrics["requests"], recovery_start, recovered_at),
                "peak_outstanding_http_requests_or_streams": metrics["peak"],
                "per_client_peak_outstanding": metrics["per_client_peak"],
                "tasks": sum(task_counts),
                "all_recovered": True,
                "all_received_new_revision": True,
                "old_key_cleared_all_caches": True,
                "new_key_received_current_revision": True,
                "environment": {
                    "python": platform.python_version(),
                    "platform": platform.platform(),
                },
            }
        finally:
            await asyncio.gather(*(sdk.close() for sdk in clients))
        assert metrics["active"] == 0
        assert metrics["per_client_peak"] <= 2
        result["outstanding_after_close"] = metrics["active"]
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--compose-file", type=Path, default=ROOT / "docker-compose.yml")
    parser.add_argument("--clients", type=int, choices=range(2, 65, 2), default=32)
    parser.add_argument("--seed", type=int, default=1909)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/sdk-reconnect.json")
    asyncio.run(run(parser.parse_args()))
