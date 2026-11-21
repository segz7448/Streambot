"""Legacy-fixture shim (TEST ONLY, disclosed).

Older tests build publish/handoff fixtures with bare clip ids: no schema,
no clip row, or a clip without a session. Production denies all of those
(unresolved provenance). So that the pre-existing publish/native/handoff
tests keep exercising their own behavior, THIS shim reports those three
specific "no provenance recorded at all" reasons as allowed. Orphan
session/source rows, unknown modes, DB errors and playback origin are NOT
shimmed. The gate tests (test_provenance_gate, test_repost_paths) patch
provenance.repost_allowed back to the real function.
"""
from app.core import provenance as _p

_real = _p._repost_allowed_real
_LEGACY = {"provenance schema missing", "clip row missing",
           "clip has no session origin"}


def _shim(conn, clip_id):
    o = _real(conn, clip_id)
    if not o.allowed and o.reason in _LEGACY:
        return _p.Origin(True, None, "", False)
    return o


_p.repost_allowed = _shim
