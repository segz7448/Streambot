"""Counted executor work that cannot leak or hide its ownership."""
from __future__ import annotations

import asyncio


async def run_counted(fn, inc, dec):
    """Run blocking `fn` in the default executor with an owner count.

    The count goes up before dispatch and comes down exactly once, in the
    thread callable's own finally. The submitted future is shielded and kept
    until it completes, through repeated cancellation of the awaiting task:
    a cancel that arrives while the work is still queued behind a busy
    executor does not cancel the future, so the callable always runs and
    always balances the count (no leak, no early 'drained'). The caller sees
    the cancellation only after the work has really finished."""
    inc()

    def run():
        try:
            return fn()
        finally:
            dec()
    try:
        fut = asyncio.get_running_loop().run_in_executor(None, run)
    except BaseException:
        dec()                   # never dispatched: balance exactly once
        raise
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(fut)
            break
        except asyncio.CancelledError:
            if fut.cancelled():
                raise           # cannot happen for a shielded future
            cancelled = True
            continue
    if cancelled:
        raise asyncio.CancelledError()
    return result
