import time
import unittest

from app.core.lifecycle import HealthRegistry


class HealthRegistryTests(unittest.TestCase):
    def test_healthy_snapshot(self):
        reg = HealthRegistry()
        reg.mark("health", "running")
        reg.beat("health")
        snap = reg.snapshot()
        self.assertEqual(snap["status"], "ok")
        self.assertFalse(snap["services"]["health"]["stale"])

    def test_stale_service_degrades(self):
        reg = HealthRegistry()
        reg.mark("worker", "running")
        reg._beats["worker"] = time.time() - 3600
        snap = reg.snapshot()
        self.assertEqual(snap["status"], "degraded")
        self.assertTrue(snap["services"]["worker"]["stale"])


if __name__ == "__main__":
    unittest.main()
