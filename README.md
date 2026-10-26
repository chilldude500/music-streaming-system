# Distributed Music Streaming System

Milestone 1 delivers the
architecture and a basic running skeleton: three-plus communicating
services, a client request/response, and end-to-end logging of the
messages exchanged between components.

See [docs/architecture.md](docs/architecture.md) for the full architecture
diagram, request-flow sequence diagram, and system-model assumptions.

## Services (this milestone)

| Service | Port | Purpose |
|---|---|---|
| API Gateway | 8000 | Single client entry point; routing; request IDs |
| User Service | 8001 | Account lookup |
| Playlist Service | 8002 | Playlists; hydrates track metadata from Catalog |
| Catalog Service | 8003 | Track metadata |

## Required software

- Docker and Docker Compose (Compose v2, i.e. `docker compose`, not `docker-compose`)
- Python 3.11+ (only needed to run `scripts/` and `pytest` from the host; the services themselves run inside containers)

## Install dependencies

For running the demo/trace scripts and tests from the host (not required
just to run the services in Docker):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


## Start everything

### With Docker Compose (recommended)

```bash
docker compose up --build
```

This builds and starts all four services. Verify they're up:

```bash
curl http://localhost:8000/health
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

Each should return `{"status": "ok", "service": "..."}`.

Logs are written to stdout (visible via `docker compose logs -f <service>`)
and to `logs/<service>.log` on the host, since `./logs` is mounted as a
volume into every container.

### Without Docker (running one service locally)

From the repo root, with dependencies installed (see above):

```bash
# Terminal 1
uvicorn services.user_service.main:app --port 8001

# Terminal 2
uvicorn services.catalog_service.main:app --port 8003

# Terminal 3
CATALOG_SERVICE_URL=http://localhost:8003 \
  uvicorn services.playlist_service.main:app --port 8002

# Terminal 4
USER_SERVICE_URL=http://localhost:8001 \
PLAYLIST_SERVICE_URL=http://localhost:8002 \
CATALOG_SERVICE_URL=http://localhost:8003 \
  uvicorn services.gateway.main:app --port 8000
```

Each service reads its downstream URLs from environment variables, so this
works the same whether services run in containers or as local processes.

## Run the demo

With the stack running (Docker or local):

```bash
python scripts/demo.py
```

This sends:
1. `GET /users/42/playlists` — the main multi-hop demo request (gateway → user-service → playlist-service → catalog-service).
2. `POST /users/42/playlists` — creates a playlist through the same gateway → user → playlist path.
3. `GET /users/9999/playlists` — an unknown user, to show the 404 path.
4. `GET /tracks/1` — a direct catalog lookup through the gateway.

Each call prints its correlation ID (`X-Request-ID`) and a ready-to-run
`trace.py` command.

## Trace a request

```bash
python scripts/trace.py <request_id>
```

Merges every `logs/<service>.log` file and prints the matching entries in
timestamp order — the full path of that one request across all services.
This is the easiest way to see the correlation ID propagating end to end.

## Run the tests

Tests are integration tests that run against the live stack, so start it
first:

```bash
docker compose up --build -d
pip install -r requirements.txt   # if not already installed
pytest
```

- `tests/test_happy_path.py` exercises the end-to-end flow through the
  gateway (health checks, fetching/creating playlists, 404 for an unknown
  user, correlation ID propagation).
- `tests/test_degraded.py` exercises fault handling. It shells out to
  `docker compose stop catalog-service` / `docker compose stop
  user-service` (and restarts them afterward) to prove:
  - with the catalog down, playlist responses still succeed but come back
    with `"degraded": true` and track IDs only (no titles);
  - with the user service down, the gateway returns `503`.

  These tests require the stack to be running via Docker Compose (not the
  local-uvicorn setup), since they need real containers to stop and start.
  If the stack isn't reachable, both test files skip automatically instead
  of failing.

## Logging

Every service logs one JSON object per line to stdout and to
`logs/<service>.log`, including:

- `timestamp`, `service`, `request_id`
- `direction`: `RECV_REQUEST` / `SEND_REQUEST` / `RECV_RESPONSE` / `SEND_RESPONSE`
- `peer`, `method`, `path`
- `status` and `latency_ms` on responses

The gateway assigns an `X-Request-ID` if the client doesn't send one; every
service forwards it on outbound calls so the whole chain shares one ID.
This is implemented once as shared middleware/client code in
[common/logging_utils.py](common/logging_utils.py) and
[common/http_client.py](common/http_client.py) — no ad hoc `print`s in any
service.

## Planned services (later milestones)

Not implemented yet — mentioned here for context:

- **Playback Session Service** — current track/position per user, one
  active device per account.
- **Recommendation Service** — consumes play events asynchronously,
  degrades gracefully when unavailable.
- **Content / Peer layer** — audio chunks served from storage nodes, with
  clients acting as peers sharing cached chunks (P2P integration).

## Repo structure

```
.
├── common/                   # shared JSON logger + HTTP client wrapper
├── services/
│   ├── gateway/
│   ├── user_service/
│   ├── playlist_service/
│   └── catalog_service/
├── scripts/
│   ├── demo.py                # runs the demo requests
│   └── trace.py                # reconstructs one request's path from logs
├── tests/
│   ├── test_happy_path.py
│   └── test_degraded.py
├── docs/
│   └── architecture.md        # Mermaid diagrams + system-model assumptions
└── logs/                      # gitignored, mounted as a volume
```

Milestone 2: Communication Mechanisms (CECS 327)
Required Software
Python: Python 3.9 or higher (python3 --version)
Operating System: macOS, Linux, or Windows (PowerShell / WSL2)
Network Ports Used (Localhost 127.0.0.1):
Milestone 1 Control Plane: 8000 (API Gateway), 8001 (User Service), 8002 (Playlist Service), 8003 (Catalog Service)
Milestone 2 Communication Plane:
8004 — Playback Session Service (TCP Socket Request-Reply & Event Publisher)
8005 — Pub-Sub Event Broker (TCP Socket Topic Router)
8006 — Recommendation Service (TCP Socket Subscriber 1)
8007 — Catalog Analytics Service (TCP Socket Subscriber 2)
9001, 9002, 9003 — P2P Audio Chunk Swarm Nodes (PeerNode-1, PeerNode-2, PeerNode-3 on UDP Sockets)

How to Install Dependencies
The Milestone 2 communication engine (milestone2_system.py) is built entirely with Python's standard library (socket, struct, json, threading, datetime, uuid), so no external packages are required to run Milestone 2.

```bash
git clone [https://github.com/Dakshaarvind/music-streaming-system.git](https://github.com/Dakshaarvind/music-streaming-system.git)
cd music-streaming-system
Optional (only needed if running Milestone 1 HTTP services):
pip install -r requirements.txt


## Milestone 3: P2P Integration (Chord DHT)

### Required Software[cite: 4]
*   Python 3.11+ (Local environment recommended for P2P simulation).
*   No external pip dependencies are required for the P2P engine.

### How to Install Dependencies[cite: 4]
The P2P overlay `milestone3_p2p.py` is built entirely using Python's standard library (`hashlib`, `socket`, `threading`, `json`). No installation is required beyond a standard Python 3.11 environment.

### How to Start Each Service and Run the Demo[cite: 4]
To execute the P2P network behavior and generate the terminal output for the video submission:
1.  Navigate to the repository root.
2.  Execute the standalone demo script:
    ```bash
    python3 milestone3_p2p.py
    ```
This script automatically orchestrates the startup, join, lookup, and leave procedures across four threaded local peer nodes (Ports 9001-9004)[cite: 2]. 

### How to Reproduce the Test Cases[cite: 4]
The script `milestone3_p2p.py` is structured as a chronological sequence of automated test cases. Running the file automatically reproduces:
1.  **Network Bootstrap:** Nodes 9001, 9002, 9003 instantiate and form the base ring.
2.  **Peer Join:** Node 9004 is instantiated and routes a `FIND_SUCCESSOR` request to insert itself into the hash space.
3.  **Lookup & Discovery:** A client generates a SHA-1 hash for `track_101_chunk_01.aac` and issues a lookup. The nodes traverse their successor pointers until the responsible node is located and the chunk is cached.
4.  **Graceful Leave:** Node 9002 executes the `leave()` method, issuing `UPDATE_SUCCESSOR` and `NOTIFY` messages to heal the ring before closing its socket.
