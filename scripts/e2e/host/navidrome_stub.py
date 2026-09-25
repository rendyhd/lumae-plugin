"""A LAN-fast stand-in for the Navidrome server the gate's catalogue names.

The seeded catalogue (``server-a``) is synthetic, so no real Navidrome serves
it. ``/api/catalog/health`` pings the provider on every request (contract §2)
and the budget excludes that ping; with no server behind the URL the ping
instead waits for a network error. This stub answers every Subsonic
``/rest/<endpoint>.view`` call at once with an OpenSubsonic ``ok`` envelope
naming a stable Navidrome version, like a healthy LAN Navidrome. It serves no
library data: the gate never runs catalogue refreshes or analysis downloads.

Usage: ``python3 navidrome_stub.py [--port 4533]`` (stdlib only).
"""
import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = "0.53.3 (e2e-stub)"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _reply(self):
        body = json.dumps({"subsonic-response": {
            "status": "ok", "version": "1.16.1", "type": "navidrome",
            "serverVersion": VERSION, "openSubsonic": True}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _reply
    do_POST = _reply

    def log_message(self, *_args):
        return


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=4533)
    args = parser.parse_args()
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
