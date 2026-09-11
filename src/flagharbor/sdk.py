"""SDK хранит проверенный снимок локально; evaluate не делает сетевых запросов."""

import asyncio
import json
import time
from contextlib import suppress

import httpx

from .evaluate import evaluate
from .schemas import Context, Evaluation, Snapshot


class FlagClient:
    def __init__(
        self, base_url, sdk_key, *, max_stale=30, refresh_interval=2, transport=None, watch=True
    ):
        if max_stale <= 0 or refresh_interval <= 0:
            raise ValueError("Cache timeouts must be positive")
        self.http = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": "Bearer " + sdk_key},
            timeout=2,
            trust_env=False,
            follow_redirects=False,
            transport=transport,
        )
        self.max_stale = max_stale
        self.refresh_interval = refresh_interval
        self.watch = watch
        self._state = (None, 0.0)
        self._lock = asyncio.Lock()
        self._tasks = []
        self.last_error = None

    async def refresh(self):
        async with self._lock:
            snapshot, _ = self._state
            headers = {"If-None-Match": f'"{snapshot.revision}"'} if snapshot else {}
            try:
                response = await self.http.get("/sdk/snapshot", headers=headers)
                if response.status_code in {401, 403}:
                    self._state = (None, 0.0)
                    self.last_error = "revoked"
                    return False
                if response.status_code == 304 and snapshot:
                    self._state = (snapshot, time.monotonic())
                else:
                    response.raise_for_status()
                    new = Snapshot.model_validate(response.json())
                    if snapshot and (
                        new.environment_id != snapshot.environment_id
                        or new.revision < snapshot.revision
                    ):
                        raise ValueError("Snapshot identity or revision regressed")
                    self._state = (new, time.monotonic())
                self.last_error = None
                return True
            except (httpx.HTTPError, ValueError) as error:
                self.last_error = type(error).__name__
                return False

    def evaluate(self, key, context):
        context = Context.model_validate(context)
        snapshot, updated = self._state
        if snapshot is None or time.monotonic() - updated > self.max_stale:
            return Evaluation(value=False, reason="unavailable", stale=True)
        result = evaluate(snapshot, key, context)
        result.stale = self.last_error is not None
        return result

    async def _poll(self):
        while True:
            await asyncio.sleep(self.refresh_interval)
            await self.refresh()

    async def _watch(self):
        while True:
            try:
                timeout = httpx.Timeout(2, read=15)
                async with self.http.stream("GET", "/sdk/events", timeout=timeout) as response:
                    if response.status_code in {401, 403}:
                        self._state = (None, 0.0)
                        self.last_error = "revoked"
                    else:
                        response.raise_for_status()
                        kind = ""
                        async for line in response.aiter_lines():
                            if line.startswith("event:"):
                                kind = line[6:].strip()
                            elif line.startswith("data:"):
                                if kind == "revoked":
                                    self._state = (None, 0.0)
                                    self.last_error = "revoked"
                                elif kind == "revision":
                                    revision = json.loads(line[5:])["revision"]
                                    snapshot, _ = self._state
                                    if snapshot is None or snapshot.revision != revision:
                                        await self.refresh()
            except (httpx.HTTPError, ValueError, KeyError):
                pass
            await asyncio.sleep(min(self.refresh_interval, 5))

    async def start(self):
        if self._tasks:
            return self
        await self.refresh()
        self._tasks = [asyncio.create_task(self._poll())]
        if self.watch:
            self._tasks.append(asyncio.create_task(self._watch()))
        return self

    async def close(self):
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError):
                await task
        self._tasks = []
        await self.http.aclose()

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *args):
        await self.close()
