import asyncio
import json
import secrets
from contextlib import asynccontextmanager, suppress
from uuid import UUID, uuid4

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Path, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

from .auth import User, current_user, router
from .config import settings
from .db import engine
from .evaluate import evaluate
from .observability import instrument
from .schemas import Context, Rules
from .service import changed, check_version, key_hash, owned, snapshot_for_key

subscribers = {}


async def listen():
    while True:
        connection = None
        try:
            connection = await asyncpg.connect(
                settings.database_url.replace("postgresql+asyncpg:", "postgresql:")
            )

            def notify(conn, pid, channel, payload):
                for queue in tuple(subscribers.get(payload, set())):
                    if not queue.full():
                        queue.put_nowait(True)

            await connection.add_listener("flags_changed", notify)
            while not connection.is_closed():
                await asyncio.sleep(1)
        except Exception:
            await asyncio.sleep(1)
        finally:
            if connection:
                await connection.close()


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(listen())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="FlagHarbor", version="0.1.0", lifespan=lifespan)
app.include_router(router)
instrument(app)


class EnvironmentInput(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class EnabledInput(BaseModel):
    enabled: bool


class EvaluateInput(BaseModel):
    key: str = Field(min_length=1, max_length=80)
    context: Context


def sdk_token(authorization: str = Header(default="")):
    if not authorization.startswith("Bearer ") or len(authorization) > 200:
        raise HTTPException(401, "SDK bearer key required")
    return authorization[7:]


@app.get("/health")
async def health():
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.post("/environments", status_code=201)
async def create_environment(data: EnvironmentInput, user: User = Depends(current_user)):
    token = "fh_" + secrets.token_urlsafe(32)
    async with engine.begin() as conn:
        await conn.execute(text("SELECT id FROM users WHERE id=:id FOR UPDATE"), {"id": user.id})
        if (
            await conn.execute(
                text("SELECT count(*) FROM environments WHERE owner_id=:id"), {"id": user.id}
            )
        ).scalar_one() >= 10:
            raise HTTPException(409, "Maximum 10 environments per user")
        row = (
            (
                await conn.execute(
                    text(
                        "INSERT INTO environments(id,owner_id,name,sdk_key_hash) VALUES(:id,:owner,:name,:hash) RETURNING id,name,revision,enabled"
                    ),
                    {"id": uuid4(), "owner": user.id, "name": data.name, "hash": key_hash(token)},
                )
            )
            .mappings()
            .one()
        )
    return {**dict(row), "sdk_key": token}


@app.get("/environments")
async def list_environments(user: User = Depends(current_user)):
    async with engine.connect() as conn:
        return [
            dict(r)
            for r in (
                await conn.execute(
                    text(
                        "SELECT id,name,revision,enabled FROM environments WHERE owner_id=:id ORDER BY created_at"
                    ),
                    {"id": user.id},
                )
            ).mappings()
        ]


@app.get("/environments/{env_id}")
async def get_environment(env_id: UUID, response: Response, user: User = Depends(current_user)):
    async with engine.connect() as conn:
        row = await owned(conn, env_id, user.id)
    response.headers["ETag"] = f'"{row["revision"]}"'
    return {key: row[key] for key in ["id", "name", "revision", "enabled"]}


@app.put("/environments/{env_id}/flags/{key}")
async def put_flag(
    env_id: UUID,
    data: Rules,
    response: Response,
    key: str = Path(pattern=r"^[a-z][a-z0-9_.-]{0,79}$"),
    if_match: str | None = Header(default=None),
    user: User = Depends(current_user),
):
    async with engine.begin() as conn:
        env = await owned(conn, env_id, user.id, True)
        check_version(if_match, env["revision"])
        old = (
            await conn.execute(
                text("SELECT definition FROM flags WHERE environment_id=:env AND key=:key"),
                {"env": env_id, "key": key},
            )
        ).scalar_one_or_none()
        if (
            old is None
            and (
                await conn.execute(
                    text("SELECT count(*) FROM flags WHERE environment_id=:id"), {"id": env_id}
                )
            ).scalar_one()
            >= 100
        ):
            raise HTTPException(409, "Maximum 100 flags per environment")
        definition = {**data.model_dump(), "key": key, "salt": old["salt"] if old else str(uuid4())}
        await conn.execute(
            text(
                "INSERT INTO flags VALUES(:env,:key,CAST(:definition AS jsonb)) ON CONFLICT(environment_id,key) DO UPDATE SET definition=EXCLUDED.definition"
            ),
            {"env": env_id, "key": key, "definition": json.dumps(definition)},
        )
        revision = await changed(conn, env, user.id, "flag_updated", key, old, definition)
    response.headers["ETag"] = f'"{revision}"'
    return {"revision": revision, "flag": definition}


@app.patch("/environments/{env_id}")
async def set_enabled(
    env_id: UUID,
    data: EnabledInput,
    response: Response,
    if_match: str | None = Header(default=None),
    user: User = Depends(current_user),
):
    async with engine.begin() as conn:
        env = await owned(conn, env_id, user.id, True)
        check_version(if_match, env["revision"])
        await conn.execute(
            text("UPDATE environments SET enabled=:enabled WHERE id=:id"),
            {"enabled": data.enabled, "id": env_id},
        )
        revision = await changed(
            conn,
            env,
            user.id,
            "environment_enabled",
            previous={"enabled": env["enabled"]},
            next_value=data.model_dump(),
        )
    response.headers["ETag"] = f'"{revision}"'
    return {"revision": revision, "enabled": data.enabled}


@app.post("/environments/{env_id}/rotate-key")
async def rotate(
    env_id: UUID, if_match: str | None = Header(default=None), user: User = Depends(current_user)
):
    token = "fh_" + secrets.token_urlsafe(32)
    async with engine.begin() as conn:
        env = await owned(conn, env_id, user.id, True)
        check_version(if_match, env["revision"])
        await conn.execute(
            text("UPDATE environments SET sdk_key_hash=:hash WHERE id=:id"),
            {"hash": key_hash(token), "id": env_id},
        )
        revision = await changed(conn, env, user.id, "sdk_key_rotated")
    return {"sdk_key": token, "revision": revision}


@app.get("/environments/{env_id}/audit")
async def audit(
    env_id: UUID,
    after: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=100),
    user: User = Depends(current_user),
):
    async with engine.connect() as conn:
        await owned(conn, env_id, user.id)
        return [
            dict(r)
            for r in (
                await conn.execute(
                    text(
                        "SELECT * FROM audit WHERE environment_id=:id AND id>:after ORDER BY id LIMIT :limit"
                    ),
                    {"id": env_id, "after": after, "limit": limit},
                )
            ).mappings()
        ]


@app.get("/sdk/snapshot")
async def snapshot(
    response: Response,
    if_none_match: str | None = Header(default=None),
    token: str = Depends(sdk_token),
):
    result = await snapshot_for_key(token)
    etag = f'"{result.revision}"'
    if if_none_match == etag:
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "no-store"})
    response.headers.update({"ETag": etag, "Cache-Control": "no-store"})
    return result


@app.post("/sdk/evaluate")
async def server_evaluate(data: EvaluateInput, token: str = Depends(sdk_token)):
    return evaluate(await snapshot_for_key(token), data.key, data.context)


@app.get("/sdk/events")
async def events(token: str = Depends(sdk_token)):
    initial = await snapshot_for_key(token)
    key = str(initial.environment_id)
    if sum(len(s) for s in subscribers.values()) >= 100:
        raise HTTPException(429, "Maximum 100 SDK streams per process")
    queue = asyncio.Queue(maxsize=1)

    async def stream():
        # Подписка живёт ровно столько же, сколько итератор ответа, даже при раннем отключении клиента.
        if sum(len(s) for s in subscribers.values()) >= 100:
            yield "event: unavailable\ndata: {}\n\n"
            return
        subscribers.setdefault(key, set()).add(queue)
        try:
            while True:
                try:
                    fresh = await snapshot_for_key(token)
                except HTTPException:
                    yield "event: revoked\ndata: {}\n\n"
                    return
                yield "event: revision\ndata: " + json.dumps({"revision": fresh.revision}) + "\n\n"
                with suppress(TimeoutError):
                    await asyncio.wait_for(queue.get(), timeout=5)
        finally:
            subscribers[key].discard(queue)
            if not subscribers[key]:
                subscribers.pop(key, None)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )
