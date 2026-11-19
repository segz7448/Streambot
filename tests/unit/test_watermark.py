import os
import stat
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app.bot.watermark import UpdateWatermark


class WatermarkTests(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.path = str(Path(self.td.name) / "updates.json")
        self.wm = UpdateWatermark(self.path)

    def tearDown(self):
        self.td.cleanup()

    def test_fresh_watermark_handles_nothing(self):
        self.assertFalse(self.wm.already_handled(1))

    def test_mark_then_handled(self):
        self.wm.mark_handled(100)
        self.assertTrue(self.wm.already_handled(100))
        self.assertTrue(self.wm.already_handled(99))
        self.assertFalse(self.wm.already_handled(101))

    def test_monotonic(self):
        self.wm.mark_handled(100)
        self.wm.mark_handled(50)  # regress ignored
        self.assertFalse(self.wm.already_handled(101))
        self.assertTrue(self.wm.already_handled(99))

    def test_persistence_across_instances(self):
        self.wm.mark_handled(123)
        self.assertTrue(UpdateWatermark(self.path).already_handled(123))

    def test_file_permissions(self):
        self.wm.mark_handled(1)
        self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o600)

    def test_failed_update_not_suppressed_by_later_success(self):
        self.wm.mark_failed(100)
        self.wm.mark_handled(101)
        # the failed earlier update is still processed again on redelivery
        self.assertFalse(self.wm.already_handled(100))
        self.assertTrue(self.wm.already_handled(101))
        # once it succeeds, it joins the handled set
        self.wm.mark_handled(100)
        self.assertTrue(self.wm.already_handled(100))

    def test_failed_set_capped(self):
        for i in range(1, 300):
            self.wm.mark_failed(i)
        _, failed = self.wm._read()
        self.assertLessEqual(len(failed), 200)

    def test_corrupt_file_treated_as_fresh(self):
        Path(self.path).write_text("not json")
        self.assertFalse(self.wm.already_handled(5))


if __name__ == "__main__":
    unittest.main()
