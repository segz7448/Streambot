# Vendored tz data (provenance and licenses)

Two separate things are redistributed here:

1. Data: `America/Los_Angeles` is the compiled IANA Time Zone Database
   entry (TZif), IANA release 2026f. The IANA tz database itself is in
   the public domain (https://www.iana.org/time-zones).
2. Packaging: the bytes were taken from the PyPI `tzdata` 2026.6 wheel
   (https://pypi.org/project/tzdata/2026.6/,
   https://files.pythonhosted.org/packages/0a/66/9a06c2227fedfd4919f0eaaa8b94377d9730882a7e4f95823c9a331ab5e1/tzdata-2026.6-py2.py3-none-any.whl).
   That package is licensed Apache-2.0, Copyright (c) 2020 Paul Ganssle
   (Google), Copyright (c) 2026 Stan Ulbrych. Its `LICENSE` and
   `LICENSE_APACHE` are carried unmodified next to the data in this
   directory.

File sha256 (America/Los_Angeles):
200d05754f6d83a371cf408d7085125797657b3b0bebeba1e508cffe86a3e5c8

Used by `app.core.intake.resolve_timezone` only when the host has no tz
database (the AGT Android phone has none). The host database always
wins. The file carries the POSIX footer rule, so future dates follow the
current US rule; refresh the file when IANA changes the zone. Wheel
installs include it through `[tool.setuptools.package-data]`.
