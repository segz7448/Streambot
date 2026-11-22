import asyncio,contextlib,socket,tempfile,time,unittest
from pathlib import Path
from unittest import mock
from app.bot import acceptance,sessions,source_auth
from app.core.database import connect,migrate
from app.pipeline.session import ClipSession
from tests.unit.test_autoclip import AutoClipTests,_realish_extract

class BoundedTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.addCleanup(self.t.cleanup);self.sd=Path(self.t.name)
  c=connect(str(self.sd/'nova.db'));migrate(c);c.execute("INSERT INTO twitch_sources(username,created_at,updated_at) VALUES('lacy',0,0)");c.commit();c.close()
  source_auth.set_scope(self.sd,'lacy','record',True)
 def test_invalid_inputs_fail_before_source(self):
  with mock.patch.object(sessions,'startplayback_reply') as launch:
   for target,seconds in [('other',85),('lacy',86),('lacy',0)]:
    with self.assertRaises(ValueError):acceptance.run_once(self.sd,'ff','fp',target=target,seconds=seconds)
   launch.assert_not_called()
 def run_fixture(self,monitor=None,delay=0):
  class LocalWorker:
   def __init__(self,*args,**kwargs):self.finished=False;self.end_reason='stopped';self.error_text=None;self._stop_req=False
   async def run(self,ctx):
    while not ctx.stopping and not self._stop_req:await asyncio.sleep(.005)
    self.finished=True
   def drained(self):return True
   async def ensure_stopped(self):self._stop_req=True
  created=[]
  def factory(*args,**kwargs):
   obj=ClipSession(*args,**kwargs);obj.tick_seconds=.01;created.append(obj);return obj
  def launch(sd,target,ff,fp,session_factory):
   if delay:time.sleep(delay)
   return sessions.startplayback_reply(sd,target,ff,fp,session_factory=session_factory,resolve_fn=lambda *a,**k:object())
  with contextlib.ExitStack() as st:
   st.enter_context(mock.patch.object(socket.socket,'connect',side_effect=AssertionError('network denied in disposable test')))
   st.enter_context(mock.patch('app.recording.playback_worker.PlaybackWorker',LocalWorker))
   st.enter_context(mock.patch('app.detection.detector.CandidateDetector.process_new_segments',return_value=0))
   st.enter_context(mock.patch.object(sessions,'_gate_refusal',return_value=None))
   st.enter_context(mock.patch('app.distribution.telegram_delivery.deliver_clip_sync',side_effect=AssertionError('real sender forbidden')))
   result=acceptance._run(self.sd,'ff','fp','lacy',.05,monitor=monitor,start_fn=launch,factory_base=factory)
  self.assertFalse(any(s.alive() for s in created));return result,created
 def test_deadline_real_session_owned_and_drained(self):
  result,created=self.run_fixture();self.assertTrue(result['terminated']);self.assertEqual(len(created),1);self.assertIsNotNone(result['session_id']);self.assertEqual(result['clips_finished'],0)
 def test_monitor_error_stops_and_drains_exact_session(self):
  result,created=self.run_fixture(monitor=lambda s:(_ for _ in ()).throw(RuntimeError('injected')))
  self.assertTrue(result['terminated']);self.assertEqual(result['monitor_fault'],'RuntimeError')
 def test_timeout_during_launch_never_spawns_session(self):
  with self.assertRaises(TimeoutError):self.run_fixture(delay=.07)
 def test_acceptance_suppresses_admission_normal_path_unchanged(self):
  import app.pipeline.session as ps
  for deadline in (time.monotonic()+60,None):
   session=ClipSession('ff','fp','','lacy',self.sd,playback={},acceptance_deadline=deadline,max_session_clips=1 if deadline else None)
   c=connect(str(self.sd/'nova.db'))
   try:
    with mock.patch.object(ps,'finish_pending_clips',return_value=[]) as finish:
     session._finish_clips_db(c,1,self.sd/'out',self.sd/'runtime')
    self.assertEqual(finish.call_args.kwargs['enqueue_admission'],deadline is None)
    self.assertTrue(finish.call_args.kwargs['auto_deliver'])
   finally:c.close()
 def test_scope_fails_closed(self):
  source_auth.set_scope(self.sd,'lacy','repost',True)
  with self.assertRaises(ValueError):acceptance.run_once(self.sd,'ff','fp',target='lacy')

class CapTest(AutoClipTests):
 def test_session_total_one_even_after_repeated_passes(self):
  self._add_event(120,'audio_spike',.95)
  clipper=self._clipper(_realish_extract);clipper.max_session_clips=1
  self.assertEqual(clipper.process_pending(),1);self.assertEqual(clipper.process_pending(),0)
  c=connect(self.db);self.assertEqual(c.execute('SELECT count(*) FROM clips').fetchone()[0],1);c.close()

if __name__=='__main__':unittest.main()

class WorkerBounds(unittest.TestCase):
 def worker(self,deadline):
  import types
  from app.recording.playback_worker import PlaybackWorker
  buf=mock.Mock(session_id=999);buf.next_seq.side_effect=lambda:0
  return PlaybackWorker(buf,'lacy','unused',Path('/unused'),'ff',initial_resolved=object(),acceptance_deadline=deadline,single_cycle=True)
 def test_capture_deadline_propagated(self):
  import types
  w=self.worker(time.monotonic()+.2);calls=[]
  w.capture_fn=lambda *a,**k:calls.append(k) or types.SimpleNamespace(segments=0,reason='ended',ffmpeg_rc=0)
  with mock.patch.object(w,'_recheck',return_value=True):self.assertEqual(w._cycle_inner(lambda:False),'ended')
  self.assertEqual(len(calls),1);self.assertLessEqual(calls[0]['deadline_sec'],.2)
  with mock.patch.object(w,'record_ok',return_value=True):
   self.assertFalse(calls[0]['should_stop']())
   time.sleep(.21);self.assertTrue(calls[0]['should_stop']())
 def test_single_cycle_never_retries(self):
  import types
  for outcome in ('continue','retry'):
   w=self.worker(time.monotonic()+1);w.buffer.reconcile.return_value=0;w.buffer.enforce_storage_cap.return_value=None
   w._cycle=mock.Mock(return_value=outcome);w._closed_media_ready=lambda:False
   ctx=types.SimpleNamespace(stopping=False,heartbeat=lambda:None)
   if outcome=='retry':
    with self.assertRaises(RuntimeError):asyncio.run(w.run(ctx))
   else:asyncio.run(w.run(ctx))
   self.assertEqual(w._cycle.call_count,1);self.assertEqual(w.reconnects,0)
