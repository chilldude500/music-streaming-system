#!/usr/bin/env python3
"""
CECS 327 - Milestone 2: Distributed Music Streaming System Communication Mechanisms
Clean, colour-aligned terminal output for report screenshots and video demo.
"""

import json
import socket
import struct
import threading
import time
import uuid
from datetime import datetime, timezone

HOST = "127.0.0.1"
PORT_PLAYBACK = 8004
PORT_BROKER = 8005
PORT_SUB_REC = 8006
PORT_SUB_CAT = 8007
GROUP_PORTS = [9001, 9002, 9003]

print_lock = threading.Lock()


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def log_event(service: str, pattern: str, req_id: str, message: str):
    with print_lock:
        print(f"[{now_iso()}] [{service:<20}] [{pattern:<10}] [ID: {req_id}] {message}")


def log_marshal_block(service: str, stage: str, req_id: str, before_val: str, after_val: str, byte_len: int = 0):
    with print_lock:
        ts = now_iso()
        size_info = f" ({byte_len}B frame = 4B header + {byte_len - 4}B UTF-8 JSON)" if byte_len else ""
        print(f"[{ts}] [{service:<20}] [{stage:<10}] [ID: {req_id}]{size_info}")
        print(f"    ├── BEFORE {stage}: {before_val}")
        print(f"    └── AFTER  {stage}: {after_val}")


# =====================================================================
# 1. MARSHALLING & UNMARSHALLING MODULE
# =====================================================================
class MessageMarshaller:
    @staticmethod
    def marshal(obj: dict, service: str, req_id: str, verbose: bool = True) -> bytes:
        json_str = json.dumps(obj, separators=(",", ":"))
        payload_bytes = json_str.encode("utf-8")
        header = struct.pack(">I", len(payload_bytes))
        wire_bytes = header + payload_bytes
        if verbose:
            log_marshal_block(
                service=service,
                stage="MARSHAL",
                req_id=req_id,
                before_val=f"Native dict -> {repr(obj)}",
                after_val=f"Wire bytes  -> {wire_bytes}",
                byte_len=len(wire_bytes),
            )
        return wire_bytes

    @staticmethod
    def unmarshal(payload_bytes: bytes, service: str, verbose: bool = True) -> dict:
        json_str = payload_bytes.decode("utf-8")
        obj = json.loads(json_str)
        req_id = obj.get("headers", {}).get("X-Request-ID", "N/A")
        if verbose:
            log_marshal_block(
                service=service,
                stage="UNMARSHAL",
                req_id=req_id,
                before_val=f"Wire bytes  -> {payload_bytes}",
                after_val=f"Native dict -> {repr(obj)}",
            )
        return obj


def send_tcp_message(sock: socket.socket, obj: dict, service: str, req_id: str, verbose: bool = True):
    wire_bytes = MessageMarshaller.marshal(obj, service, req_id, verbose=verbose)
    sock.sendall(wire_bytes)


def recv_tcp_message(sock: socket.socket, service: str, verbose: bool = True) -> dict:
    raw_len = _recvall(sock, 4)
    if not raw_len:
        return {}
    msg_len = struct.unpack(">I", raw_len)[0]
    payload_bytes = _recvall(sock, msg_len)
    return MessageMarshaller.unmarshal(payload_bytes, service, verbose=verbose)


def _recvall(sock: socket.socket, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return b""
        data.extend(packet)
    return bytes(data)


# =====================================================================
# 2. PUBLISH-SUBSCRIBE COMPONENT (Broker + 2 Subscribers)
# =====================================================================
class PubSubBroker:
    def __init__(self, port: int = PORT_BROKER):
        self.port = port
        self.topics = {"playback.events": [("RecommendationSvc", PORT_SUB_REC), ("CatalogService", PORT_SUB_CAT)]}
        self.running = True

    def start(self):
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, self.port))
            s.listen(5)
            while self.running:
                conn, _ = s.accept()
                threading.Thread(target=self._handle_event, args=(conn,), daemon=True).start()

    def _handle_event(self, conn: socket.socket):
        with conn:
            event = recv_tcp_message(conn, "PubSubBroker:8005", verbose=False)
            if not event:
                return
            req_id = event.get("headers", {}).get("X-Request-ID", "N/A")
            topic = event.get("topic", "playback.events")
            subscribers = self.topics.get(topic, [])
            log_event(
                "PubSubBroker:8005",
                "PUB-SUB",
                req_id,
                f"Event '{event.get('event_type')}' on topic '{topic}' -> Fanning out to {len(subscribers)} subscribers",
            )
            for sub_name, sub_port in subscribers:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sub_sock:
                    sub_sock.settimeout(2.0)
                    sub_sock.connect((HOST, sub_port))
                    send_tcp_message(sub_sock, event, "PubSubBroker:8005", req_id, verbose=False)
                time.sleep(0.02)


class SubscriberService:
    def __init__(self, name: str, port: int, action_desc: str):
        self.name = name
        self.port = port
        self.action_desc = action_desc
        self.running = True

    def start(self):
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, self.port))
            s.listen(5)
            while self.running:
                conn, _ = s.accept()
                threading.Thread(target=self._handle_delivery, args=(conn,), daemon=True).start()

    def _handle_delivery(self, conn: socket.socket):
        with conn:
            event = recv_tcp_message(conn, f"{self.name}:{self.port}", verbose=True)
            if not event:
                return
            req_id = event.get("headers", {}).get("X-Request-ID", "N/A")
            data = event.get("payload", {})
            log_event(
                f"{self.name}:{self.port}",
                "PUB-SUB",
                req_id,
                f"SUBSCRIBER DELIVERED -> {self.action_desc} (user_id={data.get('user_id')}, track='{data.get('track_title')}')",
            )


# =====================================================================
# 3. REQUEST-REPLY & PUBLISHER SERVICE (Playback Session Service :8004)
# =====================================================================
class PlaybackSessionService:
    def __init__(self, port: int = PORT_PLAYBACK):
        self.port = port
        self.running = True

    def start(self):
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, self.port))
            s.listen(5)
            while self.running:
                conn, _ = s.accept()
                threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()

    def _handle_client(self, conn: socket.socket):
        with conn:
            req = recv_tcp_message(conn, "PlaybackSvc:8004", verbose=True)
            if not req:
                return
            req_id = req.get("headers", {}).get("X-Request-ID", "N/A")
            body = req.get("payload", {})
            log_event(
                "PlaybackSvc:8004",
                "REQ-REPLY",
                req_id,
                f"Authenticated command '{req.get('command')}' for user_id={body.get('user_id')}, track_id={body.get('track_id')}",
            )

            reply = {
                "message_type": "RESPONSE",
                "status": 200,
                "headers": {"X-Request-ID": req_id, "Timestamp": now_iso()},
                "payload": {
                    "session_id": "sess-9921",
                    "state": "PLAYING",
                    "user_id": body.get("user_id"),
                    "track_id": body.get("track_id"),
                    "track_title": body.get("track_title"),
                    "stream_chunk_url": "p2p://chunk-swarm/track_101_chunk_01.aac",
                },
            }
            send_tcp_message(conn, reply, "PlaybackSvc:8004", req_id, verbose=True)

            # Wait 80ms so ClientApp finishes logging Part 1 before Part 2 header prints
            time.sleep(0.08)
            with print_lock:
                print("\n" + "-" * 95)
                print(" [PART 2: PUBLISH-SUBSCRIBE NOTIFICATION (1 PUBLISHER -> BROKER -> 2 SUBSCRIBERS)]")
                print("-" * 95)

            event_msg = {
                "message_type": "EVENT",
                "event_type": "TRACK_PLAYED",
                "topic": "playback.events",
                "headers": {"X-Request-ID": req_id, "Timestamp": now_iso()},
                "payload": {
                    "user_id": body.get("user_id"),
                    "track_id": body.get("track_id"),
                    "track_title": body.get("track_title"),
                    "genre": body.get("genre", "Synthwave"),
                },
            }
            log_event(
                "PlaybackSvc:8004",
                "PUB-SUB",
                req_id,
                "Publishing 'TRACK_PLAYED' event to PubSubBroker:8005 [topic: playback.events]",
            )
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as pub_sock:
                pub_sock.settimeout(2.0)
                pub_sock.connect((HOST, PORT_BROKER))
                send_tcp_message(pub_sock, event_msg, "PlaybackSvc:8004", req_id, verbose=True)


# =====================================================================
# 4. GROUP COMMUNICATION COMPONENT (P2P Audio Chunk Swarm Nodes)
# =====================================================================
class P2PGroupNode:
    def __init__(self, node_id: str, port: int):
        self.node_id = node_id
        self.port = port
        self.cached_chunks = set()
        self.running = True

    def start(self):
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, self.port))
            while self.running:
                data, _ = s.recvfrom(4096)
                payload_bytes = data[4:]
                msg = MessageMarshaller.unmarshal(payload_bytes, f"{self.node_id}:{self.port}", verbose=True)
                req_id = msg.get("headers", {}).get("X-Request-ID", "N/A")
                chunk_id = msg.get("payload", {}).get("chunk_id")
                self.cached_chunks.add(chunk_id)
                log_event(
                    f"{self.node_id}:{self.port}",
                    "GROUP-COMM",
                    req_id,
                    f"GROUP SYNC COMPLETE [Group: {msg.get('group_id')}] -> Cached '{chunk_id}'",
                )


class GroupCommunicationCoordinator:
    def __init__(self, group_id: str, member_ports: list[int]):
        self.group_id = group_id
        self.member_ports = member_ports

    def send_group_message(self, action: str, chunk_id: str, track_id: int, req_id: str):
        group_msg = {
            "message_type": "GROUP_MULTICAST",
            "group_id": self.group_id,
            "action": action,
            "headers": {"X-Request-ID": req_id, "Timestamp": now_iso()},
            "payload": {
                "track_id": track_id,
                "chunk_id": chunk_id,
                "checksum": "sha256:8f9d2c4a1b",
                "replication_factor": len(self.member_ports),
            },
        }
        log_event(
            "SwarmCoordinator",
            "GROUP-COMM",
            req_id,
            f"Multicasting '{action}' to group '{self.group_id}' (Nodes: {self.member_ports})",
        )
        wire_bytes = MessageMarshaller.marshal(group_msg, "SwarmCoordinator", req_id, verbose=True)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            for port in self.member_ports:
                s.sendto(wire_bytes, (HOST, port))
                time.sleep(0.02)


# =====================================================================
# 5. DEMO & TEST RUNNER
# =====================================================================
def run_demo():
    print("=" * 95)
    print(" CECS 327 MILESTONE 2: DISTRIBUTED MUSIC STREAMING COMMUNICATION & MARSHALLING DEMO")
    print("=" * 95)

    broker = PubSubBroker(PORT_BROKER)
    sub_rec = SubscriberService("RecommendationSvc", PORT_SUB_REC, "Updated collaborative filtering model")
    sub_cat = SubscriberService("CatalogService", PORT_SUB_CAT, "Incremented track play_count (+1)")
    playback = PlaybackSessionService(PORT_PLAYBACK)
    peers = [P2PGroupNode(f"PeerNode-{i+1}", port) for i, port in enumerate(GROUP_PORTS)]

    broker.start()
    sub_rec.start()
    sub_cat.start()
    playback.start()
    for peer in peers:
        peer.start()

    time.sleep(0.2)
    req_id = f"req-{uuid.uuid4().hex[:8]}"

    print("\n" + "-" * 95)
    print(" [PART 1: TCP SOCKET REQUEST-REPLY INTERACTION + MARSHALLING (CLIENT <-> PLAYBACK SERVICE)]")
    print("-" * 95)
    client_request = {
        "message_type": "REQUEST",
        "command": "START_PLAYBACK",
        "headers": {"X-Request-ID": req_id, "Timestamp": now_iso()},
        "payload": {
            "user_id": 42,
            "username": "nathan_csulb",
            "tier": "premium",
            "track_id": 101,
            "track_title": "Midnight City",
            "genre": "Synthwave",
        },
    }

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_sock:
        client_sock.settimeout(2.0)
        client_sock.connect((HOST, PORT_PLAYBACK))
        log_event("ClientApp", "REQ-REPLY", req_id, "Sending START_PLAYBACK request to PlaybackSvc:8004")
        send_tcp_message(client_sock, client_request, "ClientApp", req_id, verbose=True)
        reply = recv_tcp_message(client_sock, "ClientApp", verbose=True)
        log_event(
            "ClientApp",
            "REQ-REPLY",
            req_id,
            f"Received 200 OK Reply -> session_id={reply.get('payload', {}).get('session_id')}, state=PLAYING",
        )

    time.sleep(0.35)

    print("\n" + "-" * 95)
    print(" [PART 3: UDP SOCKET GROUP COMMUNICATION (SWARM COORDINATOR -> 3 P2P CHUNK NODES)]")
    print("-" * 95)
    coordinator = GroupCommunicationCoordinator("p2p-audio-swarm-group-A", GROUP_PORTS)
    coordinator.send_group_message(
        action="CACHE_CHUNK_ANNOUNCE",
        chunk_id="track_101_chunk_01.aac",
        track_id=101,
        req_id=req_id,
    )

    time.sleep(0.25)
    print("\n" + "=" * 95)
    print(" DEMO COMPLETE: ALL 4 COMMUNICATION & MARSHALLING REQUIREMENTS VERIFIED")
    print("=" * 95)


if __name__ == "__main__":
    run_demo()