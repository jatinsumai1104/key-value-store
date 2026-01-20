"""
RAFT Transport - HTTP client for peer RPC communication.
"""

import json
import urllib.request
import urllib.error
from typing import Optional, Dict, Any


class Transport:
    """HTTP client for RAFT peer communication."""

    def __init__(self, timeout: float = 0.5):
        self.timeout = timeout

    def _send_request(self, host: str, port: int, path: str, data: dict) -> Optional[dict]:
        """Send a POST request to a peer and return the response."""
        url = f"http://{host}:{port}{path}"
        try:
            body = json.dumps(data).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError:
            return None
        except urllib.error.HTTPError:
            return None
        except TimeoutError:
            return None
        except Exception:
            return None

    def send_request_vote(self, host: str, port: int, data: dict) -> Optional[dict]:
        """
        Send RequestVote RPC.

        Request format:
        {"term": int, "candidate_id": str, "last_log_index": int, "last_log_term": int}

        Response format:
        {"term": int, "vote_granted": bool}
        """
        return self._send_request(host, port, "/raft/vote", data)

    def send_append_entries(self, host: str, port: int, data: dict) -> Optional[dict]:
        """
        Send AppendEntries RPC.

        Request format:
        {
            "term": int,
            "leader_id": str,
            "prev_log_index": int,
            "prev_log_term": int,
            "entries": [{"term": int, "index": int, "command": dict}, ...],
            "leader_commit": int
        }

        Response format:
        {"term": int, "success": bool}
        """
        return self._send_request(host, port, "/raft/append", data)
