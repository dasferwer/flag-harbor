import hashlib
import json

from fastapi import HTTPException
from sqlalchemy import text

from .db import engine
from .schemas import Snapshot


def key_hash(value):
    return hashlib.sha256(value.encode()).hexdigest()


async def owned(conn, env_id, user_id, lock=False):
    row = (
        (
            await conn.execute(
                text(
                    "SELECT * FROM environments WHERE id=:id AND owner_id=:owner"
                    + (" FOR UPDATE" if lock else "")
                ),
                {"id": env_id, "owner": user_id},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise HTTPException(404, "Environment not found")
    return row


def check_version(header, revision):
    if header is None:
        raise HTTPException(428, "If-Match is required")
    if header != f'"{revision}"':
        raise HTTPException(412, "Environment changed; fetch the current ETag")


async def changed(conn, environment, user_id, action, key=None, previous=None, next_value=None):
    revision = environment["revision"] + 1
    await conn.execute(
        text("UPDATE environments SET revision=:revision WHERE id=:id"),
        {"revision": revision, "id": environment["id"]},
    )
    await conn.execute(
        text(
            "INSERT INTO audit(environment_id,revision,actor_id,action,flag_key,previous,next) VALUES(:env,:rev,:actor,:action,:key,CAST(:before AS jsonb),CAST(:after AS jsonb))"
        ),
        {
            "env": environment["id"],
            "rev": revision,
            "actor": user_id,
            "action": action,
            "key": key,
            "before": json.dumps(previous),
            "after": json.dumps(next_value),
        },
    )
    # PostgreSQL доставляет уведомление после commit. SDK не увидит конфигурацию из откатившейся транзакции.
    await conn.execute(
        text("SELECT pg_notify('flags_changed',:id)"), {"id": str(environment["id"])}
    )
    return revision


async def snapshot_for_key(token):
    async with engine.connect() as base:
        conn = await base.execution_options(isolation_level="REPEATABLE READ")
        async with conn.begin():
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM environments WHERE sdk_key_hash=:hash"),
                        {"hash": key_hash(token)},
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise HTTPException(401, "Invalid or revoked SDK key")
            flags = [
                r[0]
                for r in (
                    await conn.execute(
                        text("SELECT definition FROM flags WHERE environment_id=:id ORDER BY key"),
                        {"id": row["id"]},
                    )
                )
            ]
    return Snapshot(
        environment_id=row["id"], revision=row["revision"], enabled=row["enabled"], flags=flags
    )
