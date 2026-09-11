import hashlib
import json

from .schemas import Context, Evaluation, Snapshot


def bucket(environment_id, salt, kind, identity):
    # Соль флага не меняется при редактировании процента: новая группа расширяет прежнюю.
    payload = json.dumps(
        [str(environment_id), str(salt), kind, identity], ensure_ascii=False, separators=(",", ":")
    ).encode()
    number = int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")
    return number * 10000 // (1 << 64)


def evaluate(snapshot: Snapshot, key: str, context: Context):
    def result(value, reason, cohort=None):
        return Evaluation(value=value, reason=reason, revision=snapshot.revision, bucket=cohort)

    if not snapshot.enabled:
        return result(False, "environment_disabled")
    flag = next((f for f in snapshot.flags if f.key == key), None)
    if flag is None:
        return result(False, "missing_flag")
    if not flag.enabled:
        return result(False, "disabled")
    if context.user_id in flag.excluded_users:
        return result(False, "excluded")
    if context.organization_id and context.organization_id in flag.organizations:
        return result(True, "organization")
    if set(context.groups) & set(flag.groups):
        return result(True, "group")
    identity = context.user_id if flag.stickiness == "user" else context.organization_id
    if identity is None:
        return result(False, "missing_organization")
    cohort = bucket(snapshot.environment_id, flag.salt, flag.stickiness, identity)
    return result(cohort < flag.rollout_bps, "percentage", cohort)
