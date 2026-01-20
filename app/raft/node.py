"""
RAFT Node - Core state machine implementing leader election and log replication.
"""

import os
import json
import random
import threading
import time
from enum import Enum
from typing import Dict, Optional, Tuple, Any, Callable

from .log import RaftLog, LogEntry
from .transport import Transport


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


# Timing constants (in seconds)
ELECTION_TIMEOUT_MIN = 0.500  # 500ms (increased from 150ms)
ELECTION_TIMEOUT_MAX = 1.000  # 1000ms (increased from 300ms)
HEARTBEAT_INTERVAL = 0.050    # 50ms


class RaftNode:
    """
    RAFT consensus node implementing leader election and log replication.
    """

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peers: Dict[str, Tuple[str, int]],  # peer_id -> (host, port)
        data_dir: str,
        apply_callback: Optional[Callable[[dict], None]] = None
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers  # Does not include self
        self.data_dir = data_dir
        self.apply_callback = apply_callback

        os.makedirs(data_dir, exist_ok=True)

        # Persistent state (initialize defaults first)
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.commit_index: int = 0
        self.last_applied: int = 0
        self._state_path = os.path.join(data_dir, "raft_state.json")
        self._load_state()

        # RAFT log
        self.log = RaftLog(os.path.join(data_dir, "raft_log.bin"))

        # Volatile state
        self.state = NodeState.FOLLOWER
        self.leader_id: Optional[str] = None

        # Leader-only volatile state
        self.next_index: Dict[str, int] = {}   # peer_id -> next log index to send
        self.match_index: Dict[str, int] = {}  # peer_id -> highest replicated index

        # Transport
        self.transport = Transport(timeout=0.5)

        # Threading
        self.lock = threading.RLock()
        self._running = False
        self._last_heartbeat = time.time()
        self._election_timeout = self._random_election_timeout()

        # Condition for blocking propose calls
        self._commit_condition = threading.Condition(self.lock)

        # Pending proposals waiting for commit
        self._pending_proposals: Dict[int, threading.Event] = {}

    def _random_election_timeout(self) -> float:
        """Generate a random election timeout."""
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _load_state(self) -> None:
        """Load persistent state from disk."""
        if os.path.exists(self._state_path):
            try:
                with open(self._state_path, "r") as f:
                    data = json.load(f)
                    self.current_term = data.get("current_term", 0)
                    self.voted_for = data.get("voted_for")
                    self.commit_index = data.get("commit_index", 0)
                    self.last_applied = data.get("last_applied", 0)
            except Exception as e:
                print(f"[{self.node_id}] Error loading state: {e}")

    def _replay_log(self) -> None:
        """Replay committed log entries to rebuild state machine."""
        if self.apply_callback is None:
            return

        # Replay entries from last_applied+1 to commit_index
        for idx in range(self.last_applied + 1, self.commit_index + 1):
            entry = self.log.get(idx)
            if entry:
                try:
                    self.apply_callback(entry.command)
                    self.last_applied = idx
                except Exception as e:
                    print(f"[{self.node_id}] Error replaying entry {idx}: {e}")
                    break

    def _persist_state(self) -> None:
        """Persist state to disk."""
        data = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied
        }
        with open(self._state_path, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())

    def start(self) -> None:
        """Start the RAFT node threads."""
        # Replay committed entries to rebuild state machine
        self._replay_log()

        self._running = True
        self._last_heartbeat = time.time()

        # Election timer thread
        self._election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self._election_thread.start()

        # Heartbeat thread (only active when leader)
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

        # Apply loop thread
        self._apply_thread = threading.Thread(target=self._apply_loop, daemon=True)
        self._apply_thread.start()

        print(f"[{self.node_id}] RAFT node started")

    def stop(self) -> None:
        """Stop the RAFT node."""
        self._running = False

    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        with self.lock:
            return self.state == NodeState.LEADER

    def get_leader_address(self) -> Optional[Tuple[str, int]]:
        """Get the current leader's address."""
        with self.lock:
            if self.leader_id == self.node_id:
                return (self.host, self.port)
            if self.leader_id and self.leader_id in self.peers:
                return self.peers[self.leader_id]
            return None

    def get_status(self) -> dict:
        """Get current node status."""
        with self.lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "log_length": self.log.last_index()
            }

    # ========== Election ==========

    def _election_loop(self) -> None:
        """Monitor election timeout and trigger elections."""
        while self._running:
            time.sleep(0.010)  # 10ms tick

            with self.lock:
                if self.state == NodeState.LEADER:
                    continue

                elapsed = time.time() - self._last_heartbeat
                if elapsed >= self._election_timeout:
                    self._start_election()

    def _start_election(self) -> None:
        """Start a new election."""
        with self.lock:
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self._persist_state()
            self._election_timeout = self._random_election_timeout()
            self._last_heartbeat = time.time()

            current_term = self.current_term
            last_log_index = self.log.last_index()
            last_log_term = self.log.last_term()

        print(f"[{self.node_id}] Starting election for term {current_term}")

        # Vote for self
        votes = 1
        total_nodes = len(self.peers) + 1
        majority = (total_nodes // 2) + 1

        # Request votes from peers
        for peer_id, (host, port) in self.peers.items():
            req = {
                "term": current_term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }
            resp = self.transport.send_request_vote(host, port, req)

            with self.lock:
                # Check if still candidate for same term
                if self.state != NodeState.CANDIDATE or self.current_term != current_term:
                    return

                if resp:
                    if resp.get("term", 0) > self.current_term:
                        self._become_follower(resp["term"])
                        return
                    if resp.get("vote_granted"):
                        votes += 1

        # Check if won election
        with self.lock:
            if self.state == NodeState.CANDIDATE and self.current_term == current_term:
                if votes >= majority:
                    self._become_leader()

    def _become_leader(self) -> None:
        """Transition to leader state."""
        print(f"[{self.node_id}] Became leader for term {self.current_term}")
        self.state = NodeState.LEADER
        self.leader_id = self.node_id

        # Initialize leader state
        next_idx = self.log.last_index() + 1
        for peer_id in self.peers:
            self.next_index[peer_id] = next_idx
            self.match_index[peer_id] = 0

        # Send initial heartbeats immediately
        self._send_heartbeats()

    def _become_follower(self, term: int) -> None:
        """Transition to follower state."""
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self._persist_state()
        self._election_timeout = self._random_election_timeout()

    # ========== RequestVote RPC ==========

    def handle_request_vote(self, req: dict) -> dict:
        """Handle RequestVote RPC from a candidate."""
        with self.lock:
            term = req.get("term", 0)
            candidate_id = req.get("candidate_id")
            last_log_index = req.get("last_log_index", 0)
            last_log_term = req.get("last_log_term", 0)

            # Update term if needed
            if term > self.current_term:
                self._become_follower(term)

            vote_granted = False

            if term < self.current_term:
                # Stale request
                pass
            elif self.voted_for is None or self.voted_for == candidate_id:
                # Check if candidate's log is at least as up-to-date
                my_last_term = self.log.last_term()
                my_last_index = self.log.last_index()

                log_ok = (last_log_term > my_last_term or
                         (last_log_term == my_last_term and last_log_index >= my_last_index))

                if log_ok:
                    self.voted_for = candidate_id
                    self._persist_state()
                    self._last_heartbeat = time.time()
                    vote_granted = True

            return {"term": self.current_term, "vote_granted": vote_granted}

    # ========== AppendEntries RPC ==========

    def handle_append_entries(self, req: dict) -> dict:
        """Handle AppendEntries RPC from leader."""
        with self.lock:
            term = req.get("term", 0)
            leader_id = req.get("leader_id")
            prev_log_index = req.get("prev_log_index", 0)
            prev_log_term = req.get("prev_log_term", 0)
            entries = req.get("entries", [])
            leader_commit = req.get("leader_commit", 0)

            # Update term if needed
            if term > self.current_term:
                self._become_follower(term)

            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            # Valid leader heartbeat
            self._last_heartbeat = time.time()
            self.leader_id = leader_id

            if self.state != NodeState.FOLLOWER:
                self.state = NodeState.FOLLOWER

            # Check log consistency
            if prev_log_index > 0:
                prev_entry = self.log.get(prev_log_index)
                if prev_entry is None or prev_entry.term != prev_log_term:
                    return {"term": self.current_term, "success": False}

            # Process entries
            for entry_data in entries:
                entry = LogEntry(
                    term=entry_data["term"],
                    index=entry_data["index"],
                    command=entry_data["command"]
                )

                existing = self.log.get(entry.index)
                if existing:
                    if existing.term != entry.term:
                        # Conflict - truncate and append
                        self.log.truncate_from(entry.index)
                        self.log.append(entry)
                    # else: same entry, skip
                else:
                    self.log.append(entry)

            # Update commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.log.last_index())
                self._persist_state()

            return {"term": self.current_term, "success": True}

    # ========== Heartbeat / Replication ==========

    def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats when leader."""
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)

            with self.lock:
                if self.state != NodeState.LEADER:
                    continue

            self._send_heartbeats()

    def _send_heartbeats(self) -> None:
        """Send AppendEntries to all peers."""
        with self.lock:
            if self.state != NodeState.LEADER:
                return
            current_term = self.current_term
            commit_index = self.commit_index

        for peer_id, (host, port) in self.peers.items():
            self._send_append_entries_to_peer(peer_id, host, port, current_term, commit_index)

    def _send_append_entries_to_peer(
        self,
        peer_id: str,
        host: str,
        port: int,
        term: int,
        commit_index: int
    ) -> None:
        """Send AppendEntries to a single peer."""
        with self.lock:
            next_idx = self.next_index.get(peer_id, 1)
            prev_log_index = next_idx - 1
            prev_log_term = self.log.get_term_at(prev_log_index)

            # Get entries to send
            entries_to_send = []
            for entry in self.log.get_entries_from(next_idx):
                entries_to_send.append({
                    "term": entry.term,
                    "index": entry.index,
                    "command": entry.command
                })

        req = {
            "term": term,
            "leader_id": self.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries_to_send,
            "leader_commit": commit_index
        }

        resp = self.transport.send_append_entries(host, port, req)

        with self.lock:
            if self.state != NodeState.LEADER:
                return

            if resp:
                if resp.get("term", 0) > self.current_term:
                    self._become_follower(resp["term"])
                    return

                if resp.get("success"):
                    if entries_to_send:
                        # Update next_index and match_index
                        last_sent_index = entries_to_send[-1]["index"]
                        self.next_index[peer_id] = last_sent_index + 1
                        self.match_index[peer_id] = last_sent_index
                        self._maybe_advance_commit_index()
                else:
                    # Decrement next_index and retry
                    self.next_index[peer_id] = max(1, next_idx - 1)

    def _maybe_advance_commit_index(self) -> None:
        """Check if we can advance commit_index based on majority."""
        # Find the highest index replicated on a majority
        last_index = self.log.last_index()

        for n in range(last_index, self.commit_index, -1):
            entry = self.log.get(n)
            if entry and entry.term == self.current_term:
                # Count replications (including self)
                count = 1
                for peer_id in self.peers:
                    if self.match_index.get(peer_id, 0) >= n:
                        count += 1

                majority = (len(self.peers) + 1) // 2 + 1
                if count >= majority:
                    self.commit_index = n
                    self._persist_state()
                    self._commit_condition.notify_all()
                    break

    # ========== Apply Loop ==========

    def _apply_loop(self) -> None:
        """Apply committed entries to the state machine."""
        while self._running:
            time.sleep(0.010)  # 10ms tick

            with self.lock:
                while self.last_applied < self.commit_index:
                    self.last_applied += 1
                    entry = self.log.get(self.last_applied)
                    if entry and self.apply_callback:
                        try:
                            self.apply_callback(entry.command)
                            self._persist_state()
                        except Exception as e:
                            print(f"[{self.node_id}] Error applying entry {self.last_applied}: {e}")

                    # Signal any waiting proposals
                    if self.last_applied in self._pending_proposals:
                        event = self._pending_proposals.pop(self.last_applied)
                        event.set()

    # ========== Client Operations ==========

    def propose(self, command: dict, timeout: float = 5.0) -> dict:
        """
        Propose a command to the cluster. Only works on leader.
        Blocks until the command is committed or timeout.

        Returns: {"success": bool, "error": str (optional)}
        """
        with self.lock:
            if self.state != NodeState.LEADER:
                leader_addr = self.get_leader_address()
                return {
                    "success": False,
                    "error": "not_leader",
                    "leader": f"{leader_addr[0]}:{leader_addr[1]}" if leader_addr else None
                }

            # Append to log
            new_index = self.log.last_index() + 1
            entry = LogEntry(
                term=self.current_term,
                index=new_index,
                command=command
            )
            self.log.append(entry)

            # Track this proposal
            event = threading.Event()
            self._pending_proposals[new_index] = event

        # Send to peers immediately
        self._send_heartbeats()

        # Wait for commit
        if event.wait(timeout=timeout):
            return {"success": True}
        else:
            # Timeout - clean up
            with self.lock:
                self._pending_proposals.pop(new_index, None)
            return {"success": False, "error": "timeout"}
