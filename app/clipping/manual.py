"""Owner-selected clip events.

A manual mark is command evidence from the paired administrator, not a
model or heuristic confidence value. The AutoClipper handles this kind
explicitly so a provider outage never turns an owner mark into an AI
score or blocks it behind an AI quota.
"""

MANUAL_MARK_KIND = "manual_mark"
MANUAL_MARK_SCORE = 1.0
