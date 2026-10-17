"""Mock Codex upstream for the GeoClass NixOS machine tests.

Behaviour is read from MODE_FILE on every request: ok, overloaded, quota, or unavailable.
GET /count returns how many upstream requests were served. Request headers are never printed.
"""
import http.server
import json
import os
import threading

REGION = os.environ.get("REGION", "unknown")
MODE_FILE = os.environ.get("MODE_FILE", "/var/lib/mock-upstream/mode")
PORT = int(os.environ.get("PORT", "8000"))
lock = threading.Lock()
count = 0


def mode():
    try:
        with open(MODE_FILE) as f:
            return f.read().strip() or "ok"
    except OSError:
        return "ok"


def sse(*events):
    return "".join(f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events).encode()


class Handler(http.server.BaseHTTPRequestHandler):
    # HTTP/1.0: one request per connection keeps the mock simple.
    protocol_version = "HTTP/1.0"

    def log_message(self, *args):
        pass

    def reply(self, status, body, content_type):
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(body)))
        self.send_header("cf-ray", "8a1b2c3d4e5f-TST")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/count":
            self.reply(200, str(count).encode(), "text/plain")
        else:
            self.reply(404, b"", "text/plain")

    def do_POST(self):
        global count
        length = int(self.headers.get("content-length", "0"))
        self.rfile.read(length)
        with lock:
            count += 1
        m = mode()
        if m == "overloaded":
            body = sse(
                {"type": "response.created"},
                {"type": "response.failed", "response": {"error": {"code": "server_is_overloaded", "message": "Selected model is at capacity."}}},
            )
            self.reply(200, body, "text/event-stream")
        elif m == "quota":
            self.reply(429, b'{"error":{"type":"usage_limit_reached"}}', "application/json")
        elif m == "unavailable":
            self.reply(503, b"down", "text/plain")
        else:
            body = sse(
                {"type": "response.created"},
                {"type": "response.output_item.added"},
                {"type": "response.output_text.delta", "delta": f"hello from {REGION}"},
                {"type": "response.completed", "response": {"usage": {"input_tokens": 100, "output_tokens": 5, "input_tokens_details": {"cached_tokens": 80}}}},
            )
            self.reply(200, body, "text/event-stream")


http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
