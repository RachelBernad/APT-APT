"""Network tap selftest: a local server, a page that does XHR + fetch; the tap must see exactly the matching ones,
with request bodies, and the document. No FB traffic. Run: python tools/tap_selftest.py"""
import http.server, json, os, ssl, subprocess, sys, tempfile, threading

HERE = os.path.dirname(os.path.abspath(__file__))
# Oxide speaks TLS only: make a throwaway self-signed cert for 127.0.0.1 and trust it via BROWSER_OXIDE_CA_BUNDLE.
TMP = tempfile.mkdtemp(prefix="taptest-")
CERT, KEY = os.path.join(TMP, "c.pem"), os.path.join(TMP, "k.pem")
subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=127.0.0.1",
                "-addext", "subjectAltName=IP:127.0.0.1", "-keyout", KEY, "-out", CERT],
               check=True, capture_output=True)
os.environ["BROWSER_OXIDE_CA_BUNDLE"] = CERT
from browser_oxide import Browser, Profile

class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, body, ctype):
        b = body.encode(); self.send_response(200); self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        if self.path.startswith("/page"):
            return self._send(open(os.path.join(HERE, "fixtures", "tap.html")).read(), "text/html; charset=utf-8")
        self._send('{"other":true}', "application/json")
    def do_POST(self):
        n = int(self.headers.get("content-length") or 0); body = self.rfile.read(n).decode()
        self._send('for (;;);{"data":{"echo":%s}}\n{"label":"chunk2"}' % json.dumps(body), "application/json")

srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H); port = srv.server_address[1]
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); ctx.load_cert_chain(CERT, KEY); ctx.set_alpn_protocols(["http/1.1"])
srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
threading.Thread(target=srv.serve_forever, daemon=True).start()
b = Browser(profile=Profile.chrome())
b.tap(["/api/graphql/", "/page"], max_kb=64)
b.navigate(f"https://127.0.0.1:{port}/page", max_iterations=3); b.pump(3000)
out = b.evaluate("document.getElementById('out').textContent")
recs, dropped = b.take_responses()
summary = [(r["kind"], r["method"], r["url"].split(str(port))[1], r["status"], r["request_body"][:40], len(r["body"])) for r in recs]
for s in summary: print(s)
ok = (out == "done true" and dropped == 0
      and [s[0] for s in summary] == ["document", "fetch", "fetch"]
      and "FromXhr" in recs[1]["request_body"] and "Fetched" in recs[2]["request_body"]
      and "data-sjs" in recs[0]["body"] and recs[1]["body"].startswith("for (;;);")
      and not any("/other.json" in r["url"] for r in recs))
b.tap([]); b.navigate(f"https://127.0.0.1:{port}/page", max_iterations=3); b.pump(2000)
ok = ok and b.take_responses() == ([], 0)
print("page:", out, "| TAP SELFTEST", "PASS" if ok else "FAIL"); sys.stdout.flush(); os._exit(0 if ok else 1)
