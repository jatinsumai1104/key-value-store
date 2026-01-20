import argparse
import http.server
import json
import urllib.parse
from typing import Optional, Dict, Tuple

from store.kvstore import KVStore
from raft import RaftNode

# Global state
STORE: Optional[KVStore] = None
RAFT_NODE: Optional[RaftNode] = None
NODE_ID: str = "standalone"


def parse_peers(peers_str: str) -> Dict[str, Tuple[str, int]]:
    """
    Parse peers string in format: node-2:localhost:8081,node-3:localhost:8082
    Returns: {peer_id: (host, port), ...}
    """
    if not peers_str:
        return {}

    peers = {}
    for peer in peers_str.split(","):
        parts = peer.strip().split(":")
        if len(parts) == 3:
            peer_id = parts[0]
            host = parts[1]
            port = int(parts[2])
            peers[peer_id] = (host, port)
    return peers


class KVHandler(http.server.BaseHTTPRequestHandler):
    def _send_response(self, code, data=None):
        self.send_response(code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        if data is not None:
            self.wfile.write(json.dumps(data).encode("utf-8"))

    def _check_leader(self) -> bool:
        """
        Check if we're the leader. If not, return 503 with leader info.
        Returns True if we are leader, False otherwise.
        """
        if RAFT_NODE is None:
            return True  # Standalone mode

        if RAFT_NODE.is_leader():
            return True

        leader_addr = RAFT_NODE.get_leader_address()
        if leader_addr:
            leader_str = f"{leader_addr[0]}:{leader_addr[1]}"
        else:
            leader_str = None

        self._send_response(503, {"error": "not leader", "leader": leader_str})
        return False

    def _propose_command(self, command: dict) -> bool:
        """
        Propose a command through RAFT. Returns True on success.
        """
        if RAFT_NODE is None:
            # Standalone mode - apply directly
            STORE.apply_command(command)
            return True

        result = RAFT_NODE.propose(command)
        if result.get("success"):
            return True

        error = result.get("error", "unknown")
        if error == "not_leader":
            self._send_response(503, {"error": "not leader", "leader": result.get("leader")})
        else:
            self._send_response(500, {"error": error})
        return False

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/get":
            # Reads go to leader for strong consistency
            if not self._check_leader():
                return

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
            # Reads go to leader for strong consistency
            if not self._check_leader():
                return

            start_key = query.get("start", [None])[0]
            end_key = query.get("end", [None])[0]

            if not start_key:
                self._send_response(400, {"error": "Missing start key"})
                return

            limit = int(query.get("limit", [100])[0])
            items = STORE.range_scan(start_key, end_key)
            resp_items = [{"key": k, "value": v} for k, v in items[:limit]]

            self._send_response(200, {"items": resp_items, "count": len(resp_items)})

        elif path == "/raft/status":
            if RAFT_NODE is None:
                self._send_response(200, {"mode": "standalone"})
            else:
                self._send_response(200, RAFT_NODE.get_status())

        else:
            self._send_response(404, {"error": "Not found"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        content_len = int(self.headers.get("Content-Length", 0))
        if content_len == 0:
            body = b"{}"
        else:
            body = self.rfile.read(content_len)

        try:
            data = json.loads(body)
        except:
            self._send_response(400, {"error": "Invalid JSON"})
            return

        if path == "/put":
            if not self._check_leader():
                return

            key = data.get("key")
            value = data.get("value")
            if not key or value is None:
                self._send_response(400, {"error": "Missing key or value"})
                return

            command = {"op": "put", "key": key, "value": value}
            if self._propose_command(command):
                self._send_response(200, {"status": "ok"})

        elif path == "/batchput":
            if not self._check_leader():
                return

            items_raw = data.get("items")
            if not items_raw or not isinstance(items_raw, list):
                self._send_response(400, {"error": "Missing items list"})
                return

            items = []
            for item in items_raw:
                k = item.get("key")
                v = item.get("value")
                if k and v is not None:
                    items.append({"key": k, "value": v})

            command = {"op": "batch_put", "items": items}
            if self._propose_command(command):
                self._send_response(200, {"status": "ok", "count": len(items)})

        elif path == "/raft/vote":
            # RequestVote RPC
            if RAFT_NODE is None:
                self._send_response(400, {"error": "Not in cluster mode"})
                return
            resp = RAFT_NODE.handle_request_vote(data)
            self._send_response(200, resp)

        elif path == "/raft/append":
            # AppendEntries RPC
            if RAFT_NODE is None:
                self._send_response(400, {"error": "Not in cluster mode"})
                return
            resp = RAFT_NODE.handle_append_entries(data)
            self._send_response(200, resp)

        else:
            self._send_response(404)

    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/delete":
            if not self._check_leader():
                return

            key = query.get("key", [None])[0]
            if not key:
                self._send_response(400, {"error": "Missing key"})
                return

            command = {"op": "delete", "key": key}
            if self._propose_command(command):
                self._send_response(200, {"status": "deleted"})
        else:
            self._send_response(404)

    def log_message(self, format, *args):
        # Suppress default logging for cleaner output
        pass


def run():
    global STORE, RAFT_NODE, NODE_ID

    parser = argparse.ArgumentParser(description="Distributed KV Store with RAFT consensus")
    parser.add_argument("--node-id", default="standalone", help="Node ID (e.g., node-1)")
    parser.add_argument("--port", type=int, default=9001, help="Server port")
    parser.add_argument("--host", default="localhost", help="Server host")
    parser.add_argument("--peers", default="", help="Peer nodes (format: node-2:host:port,node-3:host:port)")
    parser.add_argument("--data-dir", default="app/data", help="Data directory")
    args = parser.parse_args()

    NODE_ID = args.node_id
    peers = parse_peers(args.peers)

    # Create data directory for this node
    if peers:
        data_dir = f"{args.data_dir}/{args.node_id}"
    else:
        data_dir = args.data_dir

    # Initialize store
    STORE = KVStore(data_dir=data_dir)

    # Initialize RAFT if peers are configured
    if peers:
        RAFT_NODE = RaftNode(
            node_id=args.node_id,
            host=args.host,
            port=args.port,
            peers=peers,
            data_dir=data_dir,
            apply_callback=STORE.apply_command
        )
        RAFT_NODE.start()
        print(f"Starting RAFT node {args.node_id} on port {args.port} with peers: {list(peers.keys())}")
    else:
        print(f"Starting standalone KV Store on port {args.port}...")

    http.server.HTTPServer.allow_reuse_address = True
    httpd = http.server.HTTPServer(("", args.port), KVHandler)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if RAFT_NODE:
            RAFT_NODE.stop()
        STORE.close()
        print("Server stopped.")


if __name__ == "__main__":
    run()
