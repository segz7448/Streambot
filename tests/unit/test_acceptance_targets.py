import tempfile,unittest
from pathlib import Path
from unittest import mock
from app.bot import acceptance,source_auth
from app.core.database import connect,migrate
class Targets(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.sd=Path(self.tmp.name)
  c=connect(str(self.sd/'nova.db'));migrate(c)
  for target in ('lacy','agent00','unapproved'):
   c.execute('INSERT INTO twitch_sources(username,created_at,updated_at) VALUES(?,0,0)',(target,))
  c.commit();c.close()
 def test_allowlist_exact(self):self.assertEqual(acceptance.APPROVED_TARGETS,frozenset({'lacy','agent00'}))
 def test_invalid_target_and_time_no_launch(self):
  with mock.patch.object(acceptance,'_run') as run:
   for target in ('unapproved','Agent00',' agent00','agent00 ','@agent00','https://example.invalid',None,[],True):
    with self.subTest(target=target),self.assertRaises(ValueError):acceptance.run_once(self.sd,'ff','fp',target=target)
   for seconds in (True,False,float('nan'),float('inf'),float('-inf'),0,-1,85.01,'85',None):
    with self.subTest(seconds=seconds),self.assertRaises(ValueError):acceptance.run_once(self.sd,'ff','fp',target='agent00',seconds=seconds)
   run.assert_not_called()
 def test_grants_fail_closed(self):
  with mock.patch.object(acceptance,'_run') as run:
   for target in acceptance.APPROVED_TARGETS:
    with self.assertRaises(ValueError):acceptance.run_once(self.sd,'ff','fp',target=target)
    source_auth.set_scope(self.sd,target,'record',True);source_auth.set_scope(self.sd,target,'repost',True)
    with self.assertRaises(ValueError):acceptance.run_once(self.sd,'ff','fp',target=target)
   run.assert_not_called()
 def test_current_title_refusal_never_resolves_or_constructs(self):
  from app.bot import sessions
  for target in acceptance.APPROVED_TARGETS:
   source_auth.set_scope(self.sd,target,'record',True)
   with mock.patch.object(sessions,'_gate_refusal',return_value='fixture gate closed'),mock.patch.object(acceptance.tp,'resolve') as resolve,mock.patch.object(acceptance,'ClipSession') as factory:
    result=acceptance.run_once(self.sd,'ff','fp',target=target,seconds=.1)
    self.assertFalse(result['launched']);self.assertTrue(result['terminated']);resolve.assert_not_called();factory.assert_not_called()
 def test_exact_approved_target_and_bound_passed_unchanged(self):
  for target in acceptance.APPROVED_TARGETS:
   source_auth.set_scope(self.sd,target,'record',True)
   for seconds in (0.1,85):
    with mock.patch.object(acceptance,'_run',return_value={'fixture':True}) as run:
     acceptance.run_once(self.sd,'ff','fp',target=target,seconds=seconds)
     self.assertEqual(run.call_args.args,(self.sd,'ff','fp',target,seconds))
if __name__=='__main__':unittest.main()
