import http.server
import json
import urllib.parse

from store.kvstore import KVStore

PORT = 8080
STORE = KVStore()


class KVHandler(http.server.BaseHTTPRequestHandler):
    def _send_response(self, code, data=None):
        self.send_response(code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        if data is not None:
            self.wfile.write(json.dumps(data).encode("utf-8"))

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/get":
            key = query.get("key", [None])[0]
            if not key:
                self._send_response(400, {"error": "Missing key"})
                return

            val = STORE.get(key)
            if val is None:
                self._send_response(404, {"error": "Key not found"})
            else:
                self._send_response(200, {"key": key, "value": val})

        elif path == "/range":
            start_key = query.get("start", [None])[0]
            end_key = query.get("end", [None])[0]

            if not start_key:
                self._send_response(400, {"error": "Missing start key"})
                return

            # Implementation of range scan:
            # We enforce a limit to avoid blowing up memory in simple HTTP response
            limit = int(query.get("limit", [100])[0])

            items = STORE.range_scan(start_key, end_key)
            # Truncate to limit
            resp_items = [{"key": k, "value": v} for k, v in items[:limit]]

            self._send_response(200, {"items": resp_items, "count": len(resp_items)})

        else:
            self._send_response(404, {"error": "Not found"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        content_len = int(self.headers.get("Content-Length", 0))
        if content_len == 0:
            self._send_response(400, {"error": "Empty body"})
            return

        body = self.rfile.read(content_len)
        try:
            data = json.loads(body)
        except:
            self._send_response(400, {"error": "Invalid JSON"})
            return

        if path == "/put":
            key = data.get("key")
            value = data.get("value")
            if not key or value is None:
                self._send_response(400, {"error": "Missing key or value"})
                return

            STORE.put(key, value)
            self._send_response(200, {"status": "ok"})

        elif path == "/batchput":
            items_raw = data.get("items")
            if not items_raw or not isinstance(items_raw, list):
                self._send_response(400, {"error": "Missing items list"})
                return

            # Transform to tules
            batch = []
            for item in items_raw:
                k = item.get("key")
                v = item.get("value")
                if k and v is not None:
                    batch.append((k, v))

            STORE.batch_put(batch)
            self._send_response(200, {"status": "ok", "count": len(batch)})

        else:
            self._send_response(404)

    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/delete":
            key = query.get("key", [None])[0]
            if not key:
                self._send_response(400, {"error": "Missing key"})
                return

            STORE.delete(key)
            self._send_response(200, {"status": "deleted"})
        else:
            self._send_response(404)


def run():
    print(f"Starting KV Store on port {PORT}...")
    # ThreadingMixIn could be used for concurrency, keeping it simple for now
    http.server.HTTPServer.allow_reuse_address = True
    httpd = http.server.HTTPServer(("", PORT), KVHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        STORE.close()
        print("Server stopped.")


if __name__ == "__main__":
    run()
