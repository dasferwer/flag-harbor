"""Демоключ нужен только для локального запуска; в базе хранится его хеш."""

import asyncio
import json
from uuid import UUID

from sqlalchemy import text

from flagharbor.auth import hasher
from flagharbor.db import engine
from flagharbor.service import key_hash

DEMO = UUID("19000000-0000-0000-0000-000000000001")
ENV = UUID("19000000-0000-0000-0000-000000000010")
SDK_KEY = "fh_local_demo_server_key_replace_before_deployment"


async def seed():
    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_xact_lock(190001)"))
        await conn.execute(
            text(
                "INSERT INTO users VALUES(:id,'demo@example.com',:hash,'admin') ON CONFLICT DO NOTHING"
            ),
            {"id": DEMO, "hash": hasher.hash("FlagHarborDemo123!")},
        )
        inserted = await conn.execute(
            text(
                "INSERT INTO environments(id,owner_id,name,sdk_key_hash) VALUES(:id,:owner,'Demo',:hash) ON CONFLICT DO NOTHING RETURNING id"
            ),
            {"id": ENV, "owner": DEMO, "hash": key_hash(SDK_KEY)},
        )
        if inserted.first():
            definition = {
                "key": "new-search",
                "enabled": True,
                "rollout_bps": 100,
                "organizations": ["pilot-org"],
                "groups": ["beta"],
                "excluded_users": [],
                "stickiness": "user",
                "salt": "19000000-0000-0000-0000-000000000099",
            }
            await conn.execute(
                text("INSERT INTO flags VALUES(:env,'new-search',CAST(:definition AS jsonb))"),
                {"env": ENV, "definition": json.dumps(definition)},
            )


if __name__ == "__main__":
    asyncio.run(seed())
