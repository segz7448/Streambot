"""Standalone one-shot HTTP GET worker (stdlib only).

Runs in its own process so the parent can enforce an absolute deadline and
cancellation by killing the process group, covering DNS, connect, TLS,
headers and body reads (none of which urllib can bound in total). Request
comes on stdin as JSON {url, headers, limit, timeout} so a signed URL never
appears in an argument list. Output on stdout: 4-byte big-endian status then
the body (at most limit+1 bytes read; the parent detects overflow). Exit
code 2 on any error, with no message text (nothing external is echoed).
"""
import json
import struct
import sys
import urllib.error
import urllib.request


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


def main() -> int:
    try:
        spec = json.loads(sys.stdin.buffer.read(65536))
        req = urllib.request.Request(spec["url"], headers=spec.get("headers") or {})
        limit = int(spec["limit"])
        op = urllib.request.build_opener(_NoRedirect)
        out = sys.stdout.buffer
        try:
            r = op.open(req, timeout=float(spec["timeout"]))
            status, cap = r.status, limit + 1
        except urllib.error.HTTPError as e:
            r, status, cap = e, e.code, 4096
        out.write(struct.pack(">I", status)); out.flush()
        n = 0
        while n < cap:
            b = r.read1(min(65536, cap - n)) if hasattr(r, "read1") else r.read(min(65536, cap - n))
            if not b:
                break
            out.write(b); out.flush(); n += len(b)
        return 0
    except BaseException:
        return 2


if __name__ == "__main__":
    sys.exit(main())
