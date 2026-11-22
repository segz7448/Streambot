import tempfile,unittest
from pathlib import Path
from unittest import mock
from app.bot import commands_full as cf
from app.core.database import connect,migrate
from app.intelligence.memory import Memory
from app.intelligence.planner import plan_clip
from app.editing.presets import PRESETS
class QualityPreference(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.sd=Path(self.tmp.name)
  c=connect(str(self.sd/'nova.db'));migrate(c);c.close()
 def plan(self):return plan_clip('auto','fixture',Memory(str(self.sd/'memory.json'))).preset
 def test_each_selection_reaches_fresh_planner(self):
  for name in PRESETS:
   self.assertIn('set to '+name,cf.quality_reply(self.sd,name));self.assertEqual(self.plan(),name)
 def test_no_selection_preserves_vertical_default(self):
  self.assertEqual(self.plan(),'vertical');cf.quality_reply(self.sd,'');self.assertFalse((self.sd/'memory.json').exists())
 def test_invalid_selection_changes_nothing(self):
  cf.quality_reply(self.sd,'square');self.assertIn('Unknown',cf.quality_reply(self.sd,'4k'));self.assertEqual(self.plan(),'square')
 def test_memory_write_failure_does_not_acknowledge_or_commit_setting(self):
  with mock.patch.object(Memory,'remember',side_effect=OSError('fixture full disk')):
   with self.assertRaises(OSError):cf.quality_reply(self.sd,'horizontal')
  c=connect(str(self.sd/'nova.db'));self.assertIsNone(c.execute("SELECT value FROM settings WHERE key='quality'").fetchone());c.close()
if __name__=='__main__':unittest.main()
