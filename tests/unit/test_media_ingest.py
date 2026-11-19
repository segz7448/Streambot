"""Media-ingest security regression tests (audit 2026-11-19, adapted
from the auditor's pytest file to unittest). Pin the bounded-download,
fail-closed validation and journaled-delivery behavior."""
import asyncio
import pathlib
import shutil
import tempfile
import types
import unittest

from app.bot import media, ops
from app.bot.ops import deliveries_reply
from app.core.database import connect, migrate

FFPROBE = shutil.which("ffprobe")


class FakeFile:
    def __init__(self, payload):
        self.payload = payload

    async def download_to_drive(self, dest):
        pathlib.Path(dest).write_bytes(self.payload)


class FakeBot:
    def __init__(self, payload):
        self.payload, self.fetched = payload, 0

    async def get_file(self, fid):
        self.fetched += 1
        return FakeFile(self.payload)


def _update(uid, file_size=100, mime="video/mp4"):
    video = types.SimpleNamespace(file_id="f", mime_type=mime,
                                  file_size=file_size)
    msg = types.SimpleNamespace(video=video, document=None)
    return types.SimpleNamespace(update_id=uid, message=msg)


class DownloadBoundTests(unittest.TestCase):
    def test_oversize_declared_file_refused_before_download(self):
        d = pathlib.Path(tempfile.mkdtemp())
        bot = FakeBot(b"x")
        out = asyncio.run(media.ingest_media(
            _update(1, file_size=5_000_000_000), bot, d))
        self.assertEqual(bot.fetched, 0)
        self.assertIn("too large", out.lower())

    def test_missing_file_size_refused(self):
        d = pathlib.Path(tempfile.mkdtemp())
        bot = FakeBot(b"x")
        asyncio.run(media.ingest_media(_update(2, file_size=None), bot, d))
        self.assertEqual(bot.fetched, 0)

    def test_non_video_mime_refused(self):
        d = pathlib.Path(tempfile.mkdtemp())
        bot = FakeBot(b"x")
        out = asyncio.run(media.ingest_media(
            _update(3, mime="application/pdf"), bot, d))
        self.assertEqual(bot.fetched, 0)
        self.assertIn("not a video", out.lower())

    def test_empty_mime_document_refused(self):
        d = pathlib.Path(tempfile.mkdtemp())
        doc = types.SimpleNamespace(file_id="f", mime_type="",
                                    file_size=10)
        msg = types.SimpleNamespace(video=None, document=doc)
        upd = types.SimpleNamespace(update_id=4, message=msg)
        bot = FakeBot(b"x")
        out = asyncio.run(media.ingest_media(upd, bot, d))
        self.assertEqual(bot.fetched, 0)
        self.assertIn("not a video", out.lower())


@unittest.skipUnless(FFPROBE, "ffprobe not available")
class ValidationTests(unittest.TestCase):
    def test_playlist_text_named_mp4_rejected_before_analysis(self):
        d = pathlib.Path(tempfile.mkdtemp())
        called = []
        media._audio = lambda *a, **k: called.append("audio") or []
        media._scene = lambda *a, **k: called.append("scene") or []
        payload = b"#EXTM3U\n#EXTINF:1,\nfile:///etc/hostname\n"
        out = asyncio.run(media.ingest_media(
            _update(5, file_size=len(payload)), FakeBot(payload), d))
        self.assertEqual(called, [])
        self.assertIn("not a video", out.lower())
        # payload removed after rejection
        self.assertEqual(list((d / "uploads").glob("*")), [])

    def test_probe_failure_does_not_continue(self):
        from app.clipping import retrospective as r
        orig = r.probe
        r.probe = lambda *a, **k: (_ for _ in ()).throw(
            RuntimeError("bad"))
        try:
            d = pathlib.Path(tempfile.mkdtemp())
            with self.assertRaises(Exception):
                r.process_recording(
                    "ffmpeg", "ffprobe", str(d / "x.mp4"), str(d / "w"),
                    str(d / "n.db"), memory=None,
                    audio_fn=lambda *a, **k: [],
                    scene_fn=lambda *a, **k: [])
        finally:
            r.probe = orig


class DeliveryJournalTests(unittest.TestCase):
    def test_deliveries_classified_side_effecting(self):
        self.assertIn("deliveries", ops.SIDE_EFFECTING)

    def _db_with_held(self, d, n=3):
        conn = connect(str(d / "nova.db"))
        migrate(conn)
        for i in range(n):
            conn.execute(
                "INSERT INTO clips (file_path, preset, duration_seconds,"
                " status, created_at) VALUES (?,?,?,?,0)",
                (f"/x{i}.mp4", "vertical", 5.0, "HELD"))
        conn.commit()
        conn.close()

    def test_flush_commits_per_clip(self):
        d = pathlib.Path(tempfile.mkdtemp())
        self._db_with_held(d)
        calls = []

        def deliver(path, cap):
            calls.append(path)
            if len(calls) == 3:
                raise KeyboardInterrupt  # crash, not caught by handler
        with self.assertRaises(KeyboardInterrupt):
            deliveries_reply(d, "flush", deliver_fn=deliver)
        conn = connect(str(d / "nova.db"))
        n = conn.execute(
            "SELECT count(*) FROM clips WHERE status='DELIVERED'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(n, 2)

    def test_failed_send_marked_uncertain_not_resent(self):
        d = pathlib.Path(tempfile.mkdtemp())
        self._db_with_held(d, n=1)
        calls = []

        def failing(path, cap):
            calls.append(path)
            raise RuntimeError("network died mid-send")
        out = deliveries_reply(d, "flush", deliver_fn=failing)
        self.assertIn("uncertain", out.lower())
        # second flush: the uncertain clip is NOT retried blindly
        out2 = deliveries_reply(d, "flush",
                                deliver_fn=lambda p, c: calls.append(p))
        self.assertEqual(len(calls), 1)
        self.assertIn("uncertain", out2.lower())

    def test_journaled_send_reconciles_without_resend(self):
        d = pathlib.Path(tempfile.mkdtemp())
        self._db_with_held(d, n=1)
        ops.journal_record(d, 1, "deliver", "message 99")
        calls = []
        out = deliveries_reply(d, "flush",
                               deliver_fn=lambda p, c: calls.append(p))
        self.assertEqual(calls, [])  # no resend
        self.assertIn("Delivered 1", out)
        conn = connect(str(d / "nova.db"))
        status = conn.execute("SELECT status FROM clips").fetchone()[0]
        conn.close()
        self.assertEqual(status, "DELIVERED")


if __name__ == "__main__":
    unittest.main()
