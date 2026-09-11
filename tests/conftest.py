import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import httpx
import jwt
import pytest_asyncio
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from flagharbor.config import settings  # noqa: E402
from flagharbor.db import engine  # noqa: E402
from flagharbor.main import app  # noqa: E402

USER = UUID("19000000-0000-0000-0000-000000000101")
OTHER = UUID("19000000-0000-0000-0000-000000000102")


def headers(user=USER):
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": str(user),
            "iat": now,
            "exp": now + timedelta(minutes=5),
            "iss": "flagharbor",
            "aud": "flagharbor",
        },
        settings.jwt_secret,
        algorithm="HS256",
    )
    return {"Authorization": "Bearer " + token}


@pytest_asyncio.fixture(autouse=True)
async def database():
    if os.environ.get("TESTING") != "true" or not settings.database_url.endswith(
        "/flagharbor_test"
    ):
        raise RuntimeError("Isolated test database required")
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE users,environments,flags,audit RESTART IDENTITY CASCADE"))
        for user in [USER, OTHER]:
            await conn.execute(
                text("INSERT INTO users VALUES(:id,:email,'unused','user')"),
                {"id": user, "email": str(user) + "@example.com"},
            )


@pytest_asyncio.fixture
async def client():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", headers=headers()
    ) as c:
        yield c


@pytest_asyncio.fixture
async def environment(client):
    return (await client.post("/environments", json={"name": "Test"})).json()
