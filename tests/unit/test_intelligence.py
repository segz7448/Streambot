import unittest

from app.intelligence.providers import (HeuristicProvider,
                                        OpenAICompatibleProvider,
                                        provider_from_config)


class HeuristicTests(unittest.TestCase):
    def test_fuses_audio_and_scene_signals(self):
        p = HeuristicProvider()
        moments = p.score_moments(
            20.0, audio_spikes=[(10.1, -14.6)], scene_changes=[(10.0, 0.8)])
        self.assertEqual(len(moments), 2)
        top = moments[0]
        self.assertIn("audio_spike", top.reasons)
        self.assertGreater(top.score, 0.5)

    def test_empty_signals(self):
        self.assertEqual(HeuristicProvider().score_moments(10.0, [], []), [])


class RemoteConfigTests(unittest.TestCase):
    def test_remote_never_defaulted(self):
        self.assertIsInstance(provider_from_config({}), HeuristicProvider)
        self.assertIsInstance(provider_from_config({"intelligence": {}}),
                              HeuristicProvider)

    def test_remote_requires_explicit_fields(self):
        with self.assertRaises(ValueError):
            OpenAICompatibleProvider("", "model", "/k")
        with self.assertRaises(ValueError):
            OpenAICompatibleProvider("https://x", "", "/k")
        with self.assertRaises(ValueError):
            OpenAICompatibleProvider("https://x", "model", "")

    def test_remote_from_config(self):
        p = provider_from_config({"intelligence": {
            "provider": "openai-compatible",
            "base_url": "https://api.groq.com/openai/v1",
            "model": "gpt-oss-120b",
            "key_path": "~/k"}})
        self.assertIsInstance(p, OpenAICompatibleProvider)
        self.assertTrue(p.base_url.startswith("https://"))

    def test_unknown_provider_rejected(self):
        with self.assertRaises(ValueError):
            provider_from_config({"intelligence": {"provider": "magic"}})


if __name__ == "__main__":
    unittest.main()
