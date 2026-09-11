import asyncio
import os
import subprocess
import sys
import time

import httpx
import pytest
from conftest import OTHER, headers
from sqlalchemy import text

from flagharbor.db import engine
from flagharbor.evaluate import bucket, evaluate
from flagharbor.schemas import Context, Snapshot
from flagharbor.sdk import FlagClient


def sdk_headers(environment):
    return {"Authorization": "Bearer " + environment["sdk_key"]}


async def put(client, environment, body, version=1, key="search"):
    return await client.put(
        f"/environments/{environment['id']}/flags/{key}",
        json=body,
        headers={"If-Match": f'"{version}"'},
    )


async def get_snapshot(client, environment):
    return Snapshot.model_validate(
        (await client.get("/sdk/snapshot", headers=sdk_headers(environment))).json()
    )


async def test_monotonic_rollout_and_uniform_cohorts(client, environment):
    await put(client, environment, {"rollout_bps": 100})
    snapshot = await get_snapshot(client, environment)
    cohorts = []
    for threshold in [100, 1000, 10000]:
        snapshot.flags[0].rollout_bps = threshold
        cohorts.append(
            {i for i in range(10000) if evaluate(snapshot, "search", Context(user_id=str(i))).value}
        )
    assert cohorts[0] <= cohorts[1] <= cohorts[2]
    assert 60 <= len(cohorts[0]) <= 140 and 850 <= len(cohorts[1]) <= 1150
    assert len(cohorts[2]) == 10000


def test_hash_is_stable_across_python_hash_seeds():
    code = "from flagharbor.evaluate import bucket; print(bucket('env','salt','user','user-42'))"
    values = [
        subprocess.check_output(
            [sys.executable, "-c", code], env={**os.environ, "PYTHONHASHSEED": str(seed)}, text=True
        ).strip()
        for seed in [1, 234]
    ]
    assert values[0] == values[1]
    assert bucket("env", "salt", "user", "a:b") != bucket("env", "salt:user", "a", "b")


async def test_organization_stickiness_and_target_groups(client, environment):
    await put(
        client, environment, {"rollout_bps": 5000, "stickiness": "organization", "groups": ["beta"]}
    )
    snap = await get_snapshot(client, environment)
    values = [
        evaluate(snap, "search", Context(user_id=str(i), organization_id="org")).value
        for i in range(100)
    ]
    assert len(set(values)) == 1
    assert evaluate(snap, "search", Context(user_id="a", groups=["beta"])).value
    assert evaluate(snap, "search", Context(user_id="a")).reason == "missing_organization"


async def test_kill_switch_overrides_all_targeting(client, environment):
    await put(
        client,
        environment,
        {"enabled": False, "rollout_bps": 10000, "organizations": ["org"], "groups": ["beta"]},
    )
    snap = await get_snapshot(client, environment)
    assert not evaluate(
        snap, "search", Context(user_id="a", organization_id="org", groups=["beta"])
    ).value
    await client.patch(
        f"/environments/{environment['id']}", json={"enabled": False}, headers={"If-Match": '"2"'}
    )
    snap = await get_snapshot(client, environment)
    assert evaluate(snap, "missing", Context(user_id="a")).reason == "environment_disabled"


async def test_exclusion_precedes_allowlists(client, environment):
    await put(
        client,
        environment,
        {"organizations": ["org"], "excluded_users": ["a"], "rollout_bps": 10000},
    )
    snap = await get_snapshot(client, environment)
    assert (
        evaluate(snap, "search", Context(user_id="a", organization_id="org")).reason == "excluded"
    )


async def test_percentage_update_preserves_salt(client, environment):
    first = (await put(client, environment, {"rollout_bps": 100})).json()
    second = (await put(client, environment, {"rollout_bps": 1000}, version=2)).json()
    assert first["flag"]["salt"] == second["flag"]["salt"]


async def test_concurrent_updates_require_fresh_etag(client, environment):
    results = await asyncio.gather(
        *(put(client, environment, {"rollout_bps": i * 100}) for i in range(10))
    )
    assert sorted(r.status_code for r in results) == [200] + [412] * 9
    assert len((await client.get(f"/environments/{environment['id']}/audit")).json()) == 1


async def test_sdk_key_has_no_write_permissions_and_owner_isolation(client, environment):
    path = f"/environments/{environment['id']}"
    assert (await client.get(path, headers=headers(OTHER))).status_code == 404
    assert (await client.get(path + "/audit", headers=headers(OTHER))).status_code == 404
    assert (
        await client.put(
            path + "/flags/search", json={}, headers={**sdk_headers(environment), "If-Match": '"1"'}
        )
    ).status_code == 401
    assert (await client.get("/sdk/snapshot", headers=headers())).status_code == 401


async def test_snapshot_etag_and_key_rotation(client, environment):
    first = await client.get("/sdk/snapshot", headers=sdk_headers(environment))
    assert (
        await client.get(
            "/sdk/snapshot",
            headers={**sdk_headers(environment), "If-None-Match": first.headers["ETag"]},
        )
    ).status_code == 304
    result = (
        await client.post(
            f"/environments/{environment['id']}/rotate-key", headers={"If-Match": '"1"'}
        )
    ).json()
    assert (await client.get("/sdk/snapshot", headers=sdk_headers(environment))).status_code == 401
    assert (
        await client.get("/sdk/snapshot", headers={"Authorization": "Bearer " + result["sdk_key"]})
    ).status_code == 200
    audit = (await client.get(f"/environments/{environment['id']}/audit")).text
    assert result["sdk_key"] not in audit and environment["sdk_key"] not in audit


async def test_change_and_audit_roll_back_together(client, environment):
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "CREATE FUNCTION break_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit unavailable'; END $$"
            )
        )
        await conn.execute(
            text(
                "CREATE TRIGGER break_audit BEFORE INSERT ON audit FOR EACH ROW EXECUTE FUNCTION break_audit()"
            )
        )
    try:
        with pytest.raises(Exception, match="audit unavailable"):
            await put(client, environment, {"rollout_bps": 10000})
        snap = await get_snapshot(client, environment)
        assert snap.revision == 1 and snap.flags == []
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TRIGGER break_audit ON audit"))
            await conn.execute(text("DROP FUNCTION break_audit()"))


async def test_api_evaluation_matches_sdk(client, environment):
    await put(client, environment, {"rollout_bps": 5000})
    snap = await get_snapshot(client, environment)
    context = Context(user_id="sample")
    remote = (
        await client.post(
            "/sdk/evaluate",
            json={"key": "search", "context": context.model_dump()},
            headers=sdk_headers(environment),
        )
    ).json()
    assert remote == evaluate(snap, "search", context).model_dump()


async def test_invalid_rules_and_missing_etag(client, environment):
    assert (await put(client, environment, {"rollout_bps": 10001})).status_code == 422
    assert (await put(client, environment, {"organizations": ["x" * 129]})).status_code == 422
    assert (
        await client.put(f"/environments/{environment['id']}/flags/search", json={})
    ).status_code == 428


async def test_sdk_reads_cache_without_network_and_fails_closed_after_expiry(client, environment):
    await put(client, environment, {"rollout_bps": 10000})
    snap = (await get_snapshot(client, environment)).model_dump(mode="json")
    calls = []

    async def handler(request):
        calls.append(request)
        return httpx.Response(200, json=snap)

    sdk = FlagClient(
        "http://test", "key", transport=httpx.MockTransport(handler), watch=False, max_stale=1
    )
    try:
        assert await sdk.refresh()
        for _ in range(1000):
            assert sdk.evaluate("search", {"user_id": "x"}).value
        assert len(calls) == 1
        snapshot, _ = sdk._state
        sdk._state = (snapshot, time.monotonic() - 2)
        assert sdk.evaluate("search", {"user_id": "x"}).reason == "unavailable"
    finally:
        await sdk.close()


async def test_sdk_keeps_last_good_snapshot_during_short_outage(client, environment):
    await put(client, environment, {"rollout_bps": 10000})
    snap = (await get_snapshot(client, environment)).model_dump(mode="json")
    mode = [200]

    async def handler(request):
        return httpx.Response(mode[0], json=snap)

    sdk = FlagClient("http://test", "key", transport=httpx.MockTransport(handler), watch=False)
    try:
        await sdk.refresh()
        mode[0] = 503
        assert not await sdk.refresh()
        result = sdk.evaluate("search", {"user_id": "x"})
        assert result.value and result.stale
        mode[0] = 401
        await sdk.refresh()
        assert not sdk.evaluate("search", {"user_id": "x"}).value
    finally:
        await sdk.close()


async def test_sdk_rejects_malformed_and_regressing_snapshots(client, environment):
    await put(client, environment, {"rollout_bps": 10000})
    snap = (await get_snapshot(client, environment)).model_dump(mode="json")

    async def handler(request):
        return httpx.Response(200, json=snap)

    sdk = FlagClient("http://test", "key", transport=httpx.MockTransport(handler), watch=False)
    try:
        await sdk.refresh()
        snap["revision"] = 1
        assert not await sdk.refresh()
        snap["revision"] = 3
        snap["flags"][0]["rollout_bps"] = 20000
        assert not await sdk.refresh()
        assert sdk.evaluate("search", {"user_id": "x"}).value
    finally:
        await sdk.close()


async def test_etag_304_renews_cache_age(client, environment):
    snap = (await get_snapshot(client, environment)).model_dump(mode="json")
    code = [200]

    async def handler(request):
        return httpx.Response(code[0], json=snap)

    sdk = FlagClient("http://test", "key", transport=httpx.MockTransport(handler), watch=False)
    try:
        await sdk.refresh()
        snapshot, _ = sdk._state
        sdk._state = (snapshot, 0)
        code[0] = 304
        assert await sdk.refresh()
        assert sdk.evaluate("missing", {"user_id": "x"}).reason == "missing_flag"
    finally:
        await sdk.close()


async def test_environment_keys_are_independent(client, environment):
    second = (await client.post("/environments", json={"name": "Second"})).json()
    await put(client, environment, {"rollout_bps": 10000})
    assert (await get_snapshot(client, second)).flags == []


async def test_seed_preserves_later_changes():
    from scripts.seed import ENV, seed

    await seed()
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE environments SET enabled=false WHERE id=:id"), {"id": ENV})
    await seed()
    async with engine.connect() as conn:
        assert not (
            await conn.execute(text("SELECT enabled FROM environments WHERE id=:id"), {"id": ENV})
        ).scalar_one()
