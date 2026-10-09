"""Контролируемая общая ошибка503; измеряет попытки, не throughput сервера."""

import argparse
import asyncio
import json
import random
import time

import httpx

from flagharbor.sdk import FlagClient
from scripts.sdk_reconnect_proof import window


async def run(seed):
    requests = []

    async def unavailable(request):
        requests.append({"at": time.monotonic(), "path": request.url.path})
        return httpx.Response(503)

    clients = [
        FlagClient(
            "http://test",
            "fake-key",
            refresh_interval=0.2,
            watch=False,
            transport=httpx.MockTransport(unavailable),
        )
        for _ in range(32)
    ]
    try:
        for index, sdk in enumerate(clients):
            sdk._random = random.Random(seed + index)
        await asyncio.gather(*(sdk.start() for sdk in clients))
        started = time.monotonic()
        await asyncio.sleep(2)
        result = window(requests, started, time.monotonic())
        result.update(clients=32, seed=seed, interval_seconds=0.2, transport="MockTransport503")
        print(json.dumps(result, indent=2))
    finally:
        await asyncio.gather(*(sdk.close() for sdk in clients))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=1909)
    asyncio.run(run(parser.parse_args().seed))
