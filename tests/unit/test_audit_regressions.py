"""Security regression tests from the independent audit (2026-11-19).

Originally reproduced FAILING against 6f2f72b by the audit agent;
adapted to the fixed interfaces. These pin the containment behavior:
a stored inbox entry can never authorize itself, and destination URLs
cannot smuggle credentials past the stream-key refusal.
"""
import os
import pathlib
import tempfile
import unittest

from app.bot.commands import CommandRouter
from app.bot.inbox import PreAckInbox
from app.bot.ops import drain_inbox, make_pre_ack_wrapper
from app.bot.watermark import UpdateWatermark
from app.distribution.relay import secret_in_rtmp_url


class DrainAuthorizationTests(unittest.TestCase):
    def test_drain_rejects_non_admin_entry(self):
        d = pathlib.Path(tempfile.mkdtemp())
        hit = []
        r = CommandRouter(admin_ids=[111])
        r.register("sources", lambda c: hit.append(c.user_id) or "ran", "x")
        inbox = PreAckInbox(str(d / "i.json"))
        wm = UpdateWatermark(str(d / "u.json"))
        inbox.add(5, 999, "/sources")  # 999 is a stranger
        result = drain_inbox(d, r, wm, inbox, admin_id=111)
        self.assertEqual(hit, [])               # never dispatched
        self.assertEqual(result["replayed"], 0)
        self.assertEqual(inbox.pending(), [])    # dropped, cannot arm later
        self.assertTrue(wm.already_handled(5))

    def test_drain_without_pairing_executes_nothing(self):
        d = pathlib.Path(tempfile.mkdtemp())
        hit = []
        r = CommandRouter(admin_ids=[])
        r.register("sources", lambda c: hit.append(1) or "ran", "x")
        inbox = PreAckInbox(str(d / "i.json"))
        wm = UpdateWatermark(str(d / "u.json"))
        inbox.add(5, 111, "/sources")
        result = drain_inbox(d, r, wm, inbox, admin_id=None)
        self.assertEqual(hit, [])
        self.assertEqual(result["replayed"], 0)

    def test_wrapper_persists_only_paired_admin(self):
        import asyncio
        import types
        d = pathlib.Path(tempfile.mkdtemp())
        inbox = PreAckInbox(str(d / "i.json"))
        wm = UpdateWatermark(str(d / "u.json"))

        def upd(update_id, text, user_id):
            return types.SimpleNamespace(
                update_id=update_id,
                message=types.SimpleNamespace(text=text),
                effective_user=types.SimpleNamespace(id=user_id))

        async def orig(_u):
            return None

        wrapped = make_pre_ack_wrapper(inbox, wm, orig, lambda: 111)
        asyncio.run(wrapped(upd(1, "/sources", 999)))       # stranger
        asyncio.run(wrapped(upd(2, "/pair sekrit", 999)))   # pair code
        asyncio.run(wrapped(upd(3, "/pair sekrit", 111)))   # even admin's
        asyncio.run(wrapped(upd(4, "/sources", 111)))       # admin command
        texts = [e["text"] for e in inbox.pending()]
        self.assertEqual(texts, ["/sources"])
        # unpaired (no admin yet): nothing persisted at all
        inbox.remove(4)
        wrapped2 = make_pre_ack_wrapper(inbox, wm, orig, lambda: None)
        asyncio.run(wrapped2(upd(5, "/sources", 111)))
        self.assertEqual(inbox.pending(), [])


class InboxBoundTests(unittest.TestCase):
    def test_backpressure_refuses_and_records_overflow(self):
        from app.bot import inbox as inbox_mod
        d = pathlib.Path(tempfile.mkdtemp())
        old = inbox_mod.MAX_PENDING
        inbox_mod.MAX_PENDING = 3
        try:
            box = PreAckInbox(str(d / "i.json"))
            self.assertTrue(box.add(1, 111, "/a"))
            self.assertTrue(box.add(2, 111, "/b"))
            self.assertTrue(box.add(3, 111, "/c"))
            self.assertFalse(box.add(4, 111, "/d"))  # refused, not evicted
            self.assertEqual(len(box.pending()), 3)  # nothing dropped
            overflow = d / "i.overflow.json"
            self.assertTrue(overflow.exists())
            self.assertIn('"update_id": 4', overflow.read_text())
            mode = os.stat(overflow).st_mode & 0o777
            self.assertEqual(mode, 0o600)
        finally:
            inbox_mod.MAX_PENDING = old


class SecretDetectionTests(unittest.TestCase):
    def test_secret_detection_variants(self):
        for u in ["rtmp://user:pw@h/", "RTMP://h/live/KEY",
                  "srt://h:1?passphrase=x", "rtmp://h/live/KEY",
                  "rtmp://h/?streamkey=abc", "rtmps://h:443/app/key",
                  "rtmp://h/app#frag", "not a url at all://x",
                  "rtmp:host/live/key", "rtmp:/h/key", "rtmps:h",
                  "pipe:1", "tcp://h:1", "udp://h:1",
                  "concat:/a|/b", "subfile:,,start,0,:x"]:
            self.assertTrue(secret_in_rtmp_url(u), u)

    def test_keyless_destinations_still_accepted(self):
        for u in ["rtmp://a", "rtmp://a/", "rtmps://host:443/",
                  "/local/out.flv", "relative/out.flv"]:
            self.assertFalse(secret_in_rtmp_url(u), u)


class RestreamIdentityTests(unittest.TestCase):
    def test_unverified_pid_is_never_signaled(self):
        import subprocess
        import time
        d = pathlib.Path(tempfile.mkdtemp())
        from app.distribution.relay import RestreamManager
        mgr = RestreamManager(str(d / "rt.pids"))
        # legacy record: bare pid of a live process that is NOT ours
        victim = subprocess.Popen(["sleep", "30"])
        try:
            (d / "rt.pids").write_text(f"{victim.pid}\n")
            stopped = mgr.stop()
            self.assertEqual(stopped, 0)          # not signaled
            self.assertIsNone(victim.poll())       # still alive
        finally:
            victim.kill()
            victim.wait()

    def test_verified_record_is_stopped(self):
        import subprocess
        d = pathlib.Path(tempfile.mkdtemp())
        from app.distribution.relay import RestreamManager
        mgr = RestreamManager(str(d / "rt.pids"))
        victim = subprocess.Popen(["sleep", "30"])
        try:
            ident = mgr._identity(victim.pid)
            mgr._save([{"pid": victim.pid, "start": ident["start"],
                        "cmd": ident["cmd"]}])
            stopped = mgr.stop()
            self.assertEqual(stopped, 1)
            victim.wait(timeout=5)
        finally:
            if victim.poll() is None:
                victim.kill()
                victim.wait()

    def test_start_records_identity(self):
        d = pathlib.Path(tempfile.mkdtemp())
        from app.distribution.relay import RestreamManager
        mgr = RestreamManager(str(d / "rt.pids"))
        pids = mgr.start([["sleep", "30"]])
        try:
            recs = mgr._records()
            self.assertEqual(recs[0]["pid"], pids[0])
            self.assertTrue(recs[0]["start"])
            self.assertIn("sleep", recs[0]["cmd"])
        finally:
            mgr.stop()


if __name__ == "__main__":
    unittest.main()
