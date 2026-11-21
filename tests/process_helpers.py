"""Track only children created by the current test, assert and always reap."""
import subprocess
from unittest import mock


def track_children(test):
    original = subprocess.Popen
    children = []
    def launch(*args, **kwargs):
        proc = original(*args, **kwargs)
        children.append(proc)
        return proc
    patcher = mock.patch('subprocess.Popen', side_effect=launch)
    patcher.start()
    test.addCleanup(patcher.stop)
    def cleanup():
        live = [proc for proc in children if proc.poll() is None]
        try:
            for proc in live:
                proc.terminate()
            for proc in children:
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        finally:
            if live:
                test.fail(f'{len(live)} test-created child process(es) survived test teardown')
    test.addCleanup(cleanup)
    return children
