"""Split a long reply into Telegram-sized messages (limit 4096 chars)."""
from __future__ import annotations

LIMIT = 4000


def chunks(text: str, limit: int = LIMIT) -> list:
    out, cur = [], ""
    for line in (text or "").split("\n"):
        while len(line) > limit:
            if cur:
                out.append(cur)
                cur = ""
            out.append(line[:limit])
            line = line[limit:]
        add = line if not cur else cur + "\n" + line
        if len(add) > limit:
            out.append(cur)
            cur = line
        else:
            cur = add
    if cur or not out:
        out.append(cur)
    return out
