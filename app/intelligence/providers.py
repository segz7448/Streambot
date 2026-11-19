"""Phase 009: pluggable highlight-intelligence providers.

The engine scores candidate moments through a provider interface. Two
implementations exist:

- HeuristicProvider: the $0 default. Wraps the Phase 008 signal
  heuristics (audio spikes + scene changes). It is pattern matching on
  measurable signals and is never described as AI understanding.
- OpenAICompatibleProvider: remote chat-completions endpoint (Groq,
  OpenRouter, Gemini's OpenAI-compatible route, etc.). DISABLED unless
  explicitly configured with a base URL, model and key file; the engine
  never picks or defaults a remote provider on its own - that choice is
  the owner's, including any account creation and key custody.

Provider choice comes from config: intelligence.provider = "heuristic"
(default) or "openai-compatible".
"""
from __future__ import annotations

import json
import os
import urllib.request
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ScoredMoment:
    timestamp: float
    score: float
    reasons: tuple[str, ...] = field(default_factory=tuple)


class IntelligenceProvider:
    name = "abstract"

    def score_moments(self, duration_seconds: float,
                      audio_spikes: list[tuple[float, float]],
                      scene_changes: list[tuple[float, float]]) -> list[ScoredMoment]:
        raise NotImplementedError


class HeuristicProvider(IntelligenceProvider):
    """$0 signal-fusion heuristic. Not AI; measurable-signal scoring."""

    name = "heuristic"

    def score_moments(self, duration_seconds, audio_spikes,
                      scene_changes):
        moments: dict[float, ScoredMoment] = {}
        for ts, loudness in audio_spikes:
            moments[ts] = ScoredMoment(ts, min(1.0, max(0.0, (loudness + 60) / 60)),
                                       ("audio_spike",))
        for ts, scene_score in scene_changes:
            existing = moments.get(ts)
            if existing:
                moments[ts] = ScoredMoment(
                    ts, min(1.0, existing.score + 0.5 * scene_score),
                    existing.reasons + ("scene_change",))
            else:
                moments[ts] = ScoredMoment(ts, 0.5 * scene_score,
                                           ("scene_change",))
        return sorted(moments.values(), key=lambda m: m.score, reverse=True)


class OpenAICompatibleProvider(IntelligenceProvider):
    """Remote chat-completions scoring. Explicit configuration required."""

    name = "openai-compatible"

    def __init__(self, base_url: str, model: str, key_path: str,
                 timeout: int = 60):
        if not base_url or not model or not key_path:
            raise ValueError("remote provider needs base_url, model and "
                             "key_path; refusing to default any of them")
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.key_path = key_path
        self.timeout = timeout

    def _chat(self, prompt: str) -> str:
        with open(self.key_path) as fh:
            key = fh.read().strip()
        body = json.dumps({
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
        }).encode()
        req = urllib.request.Request(
            self.base_url + "/chat/completions", data=body,
            headers={"Authorization": "Bearer " + key,
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            payload = json.loads(resp.read().decode())
        return payload["choices"][0]["message"]["content"]

    def score_moments(self, duration_seconds, audio_spikes,
                      scene_changes):
        prompt = (
            "Given these candidate moments in a Twitch VOD segment, rank "
            "them as clip highlights. Return JSON: a list of objects with "
            "'timestamp' (number) and 'score' (0..1).\n"
            f"Segment duration: {duration_seconds:.1f}s\n"
            f"Audio spikes (t, LUFS): {audio_spikes}\n"
            f"Scene changes (t, score): {scene_changes}\n")
        raw = self._chat(prompt)
        data = json.loads(raw[raw.find("["):raw.rfind("]") + 1])
        return sorted(
            (ScoredMoment(float(m["timestamp"]), float(m["score"]),
                          ("remote_model",)) for m in data),
            key=lambda m: m.score, reverse=True)


def provider_from_config(config: dict) -> IntelligenceProvider:
    """Build the configured provider. The remote adapter is only
    constructed when the owner explicitly configures it; anything else
    yields the $0 heuristic provider."""
    section = (config or {}).get("intelligence", {})
    kind = section.get("provider", "heuristic")
    if kind == "heuristic":
        return HeuristicProvider()
    if kind == "openai-compatible":
        return OpenAICompatibleProvider(
            base_url=section.get("base_url", ""),
            model=section.get("model", ""),
            key_path=os.path.expanduser(section.get("key_path", "")),
        )
    raise ValueError(f"unknown intelligence provider: {kind!r}")
