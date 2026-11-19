import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class AppBootTests(unittest.TestCase):
    def test_app_boots_migrates_serves_health_and_stops(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = dict(os.environ)
            env.update({
                "NOVA_DATA_DIR": tmp,
                "NOVA_LOG_DIR": tmp,
                "NOVA_DB_PATH": str(Path(tmp) / "nova.db"),
                "NOVA_HEALTH_PORT": "18799",
            })
            proc = subprocess.Popen(
                [sys.executable, "-m", "app.main"],
                cwd=ROOT, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True)
            try:
                body = None
                for _ in range(50):
                    try:
                        with urllib.request.urlopen(
                                "http://127.0.0.1:18799/health",
                                timeout=1) as resp:
                            body = json.loads(resp.read().decode())
                            break
                    except Exception:
                        time.sleep(0.2)
                self.assertIsNotNone(body, "health endpoint never answered")
                self.assertEqual(body["status"], "ok")
                self.assertIn("health", body["services"])
                self.assertTrue((Path(tmp) / "nova.db").exists())
            finally:
                proc.send_signal(signal.SIGTERM)
                out, _ = proc.communicate(timeout=15)
            self.assertEqual(proc.returncode, 0, out)


if __name__ == "__main__":
    unittest.main()
