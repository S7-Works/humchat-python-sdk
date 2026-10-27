from __future__ import annotations

from typing import Any

from .._context import ResourceContext
from .._signal import AbortSignal
from .._transport import HttpOptions
from .._wire import Invalid, array, identity, obj, required, string
from ..errors import ProtocolError
from ..types import OrgMember


class MembersResource:
    """The authenticated organization's roster; visibility is enforced by the server."""

    def __init__(self, context: ResourceContext) -> None:
        self._context = context

    async def list(self, *, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> list[OrgMember]:
        raw = await self._context.request("/api/org/members", HttpOptions(signal=signal, timeout_ms=timeout_ms))
        try:
            rows: list[dict[str, Any]] = []
            for item in array(raw, 100_000):
                v = obj(item)
                org = v.get("org_uuid")
                rows.append(
                    {
                        "user_uuid": identity(required(v, "user_uuid"), 256),
                        "name": string(required(v, "name"), 0, 4096),
                        "email": string(required(v, "email"), 0, 4096),
                        "role": string(required(v, "role"), 1, 256),
                        "status": string(required(v, "status"), 1, 256),
                        "org_uuid": None if org is None else identity(org),
                    }
                )
        except Invalid:
            raise ProtocolError("Invalid organization members response") from None
        seen: set[str] = set()
        result: list[OrgMember] = []
        for row in rows:
            if row["user_uuid"] in seen or (row["org_uuid"] is not None and row["org_uuid"] != self._context.org_id):
                raise ProtocolError("Organization member identity or scope mismatch")
            seen.add(row["user_uuid"])
            result.append(OrgMember(row["user_uuid"], row["name"], row["email"], row["role"], row["status"]))
        return result
