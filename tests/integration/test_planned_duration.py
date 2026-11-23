"""Local synthetic real FFmpeg editing, not action/live/Telegram acceptance."""
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from app.editing.editor import edit_clip
from app.editing.presets import OutputPreset

class PlannedDurationMediaTests(unittest.TestCase):
    def test_120_and_180_second_limits_and_short_input_not_padded(self):
        with tempfile.TemporaryDirectory() as d:
            d=Path(d);src=d/'source.mp4'
            subprocess.run(['ffmpeg','-v','error','-y','-f','lavfi','-i',
                'color=c=blue:s=64x64:r=1:d=182','-f','lavfi','-i',
                'sine=frequency=440:sample_rate=8000:duration=182',
                '-c:v','libx264','-preset','ultrafast','-c:a','aac','-shortest',str(src)],check=True)
            for limit in (120,180):
                out=d/f'out-{limit}.mp4'
                edit_clip('ffmpeg',str(src),str(out),OutputPreset('fixture',256,256,30,'32k',limit),framing='full')
                meta=json.loads(subprocess.check_output(['ffprobe','-v','error','-show_format','-of','json',str(out)]))
                self.assertAlmostEqual(float(meta['format']['duration']),limit,delta=.3)
            short=d/'short.mp4'
            subprocess.run(['ffmpeg','-v','error','-y','-i',str(src),'-t','5','-c','copy',str(short)],check=True)
            out=d/'not-padded.mp4'
            edit_clip('ffmpeg',str(short),str(out),OutputPreset('fixture',256,256,30,'32k',180),framing='full')
            meta=json.loads(subprocess.check_output(['ffprobe','-v','error','-show_format','-of','json',str(out)]))
            self.assertLess(float(meta['format']['duration']),7)
