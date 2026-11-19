import os
import tempfile
import unittest
from pathlib import Path

from app.intelligence.memory import Memory
from app.intelligence.planner import (CLAIM_SAFE_MAX_SECONDS,
                                      SHORTS_MAX_SECONDS, plan_clip)


class MemoryTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.mem = Memory(str(self.dir / "memory.json"))

    def test_remember_recall_provenance(self):
        self.mem.remember("username", "twitch:somechannel",
                          "added via /addsource", "owner telegram 2026-11-19")
        hit = self.mem.get("username", "twitch:somechannel")
        self.assertEqual(hit["source"], "owner telegram 2026-11-19")
        self.assertEqual(hit["confidence"], "stated")
        self.assertEqual(len(self.mem.recall("username")), 1)

    def test_update_replaces_and_forget(self):
        self.mem.remember("preference", "tags", "a,b", "owner")
        self.mem.remember("preference", "tags", "c,d", "owner")
        self.assertEqual(self.mem.get("preference", "tags")["value"], "c,d")
        self.assertTrue(self.mem.forget("preference", "tags"))
        self.assertIsNone(self.mem.get("preference", "tags"))

    def test_validation_and_permissions(self):
        with self.assertRaises(ValueError):
            self.mem.remember("nonsense", "k", "v", "s")
        self.mem.remember("pattern", "k", "v", "s", confidence="observed")
        mode = os.stat(self.dir / "memory.json").st_mode & 0o777
        self.assertEqual(mode, 0o600)


class PlannerTests(unittest.TestCase):
    def setUp(self):
        self.mem = Memory(str(Path(tempfile.mkdtemp()) / "m.json"))

    def test_default_plan_uses_verified_rules(self):
        plan = plan_clip("audio_spike", "somechannel", self.mem,
                         game="Elden Ring")
        self.assertEqual(plan.target_seconds, CLAIM_SAFE_MAX_SECONDS)
        self.assertEqual(plan.preset, "vertical")
        self.assertLessEqual(len(plan.title), 100)
        self.assertIn("eldenring", plan.tags)
        self.assertTrue(any("policy" in r for r in plan.reasons))

    def test_owner_preference_overrides_but_policy_caps(self):
        self.mem.remember("preference", "shorts_target_seconds", "240",
                          "owner")
        plan = plan_clip("audio_spike", "chan", self.mem)
        self.assertEqual(plan.target_seconds, SHORTS_MAX_SECONDS)
        self.mem.remember("preference", "watermark", "@zenas", "owner")
        plan = plan_clip("audio_spike", "chan", self.mem)
        self.assertEqual(plan.watermark, "@zenas")

    def test_horizontal_flagged_not_short(self):
        self.mem.remember("preference", "quality", "horizontal", "owner")
        plan = plan_clip("audio_spike", "chan", self.mem)
        self.assertTrue(any("NOT a Short" in r for r in plan.reasons))


if __name__ == "__main__":
    unittest.main()
