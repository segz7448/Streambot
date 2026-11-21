# Vendored IANA tz data

`America/Los_Angeles` is the IANA TZif file from the PyPI `tzdata`
package, IANA database version 2026f (taken from tzdata-2026.6 wheel),
sha256 200d05754f6d83a371cf408d7085125797657b3b0bebeba1e508cffe86a3e5c8.
The IANA tz database is public domain.

Used by `app.core.intake.resolve_timezone` only when the host has no tz
database (the AGT Android phone has none). The host database always
wins. The file carries the POSIX footer rule, so future dates follow the
current US rule; refresh the file when IANA changes the zone.
