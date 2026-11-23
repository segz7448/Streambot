import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock
from app.bot.service import BotService
from app.bot import sessions
from app.core.database import connect, migrate

class StatusTruthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.svc = types.SimpleNamespace(_state_dir=lambda: self.path)
        self.clear = mock.patch.dict(sessions._sessions, {}, clear=True)
        self.clear.start(); self.addCleanup(self.clear.stop)
    def db(self):
        c = connect(str(self.path/'nova.db')); migrate(c); return c
    def text(self):
        return BotService._pipeline_status(self.svc)
    def test_idle_is_not_unauthorized_claim(self):
        self.db().close()
        self.assertIn('no in-process session registered', self.text())
        self.assertIn('0 source(s) record-authorized', self.text())
    def test_authorized_source_and_active_row(self):
        c=self.db();c.execute("INSERT INTO twitch_sources(username,created_at,updated_at) VALUES('chan',0,0)")
        c.execute("INSERT INTO settings(key,value) VALUES('srcauth:chan:record','1')")
        c.execute("INSERT INTO stream_sessions(source_id,state) VALUES(1,'RECORDING')");c.close()
        sessions._sessions['chan']=types.SimpleNamespace(phase='editing')
        t=self.text();self.assertIn('1 in-process session(s): editing',t)
        self.assertIn('1 source(s) record-authorized',t);self.assertIn('1 recorded active session',t)
        self.assertIn('not process-liveness proof',t)
    def test_missing_db_unavailable_without_creating_it(self):
        self.assertIn('unavailable',self.text());self.assertFalse((self.path/'nova.db').exists())
    def test_bad_schema_unavailable_not_zero(self):
        sqlite3.connect(str(self.path/'nova.db')).close()
        self.assertIn('unavailable',self.text())
