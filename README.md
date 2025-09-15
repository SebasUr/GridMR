# GridMR

GridMR is a university project developed for the Distributed Systems course. The main goal of this project is to design and implement a distributed computing platform capable of managing and executing tasks across multiple nodes.

- Master: Java service (HTTP + gRPC) that schedules tasks, tracks workers, and manages job lifecycle.
- Workers: C++ runtime that executes user-provided map and reduce functions on assigned shards.
- Client: Python CLI that splits inputs, uploads map and reduce functions, data, submits the job, and fetches results.

This document explains the architecture, technologies, components, APIs, local development flow, and operational notes.

---

## Technologies Used

- Orchestration and APIs
	- Java 17+ (master service)
	- gRPC + Protocol Buffers (master–worker control plane), see `src/main/proto/gridmr.proto`
	- Simple HTTP (REST) for job submission
- Worker Runtime
	- C++17 (worker implementation)
	- CMake (build system)
- Client CLI
	- Python 3.9+
	- `requests`, `python-dotenv`
	- SSH/SCP via OpenSSH (`ssh`, `scp`)
- Storage & Platform
	- Shared filesystem (e.g., AWS EFS) mounted at the same path on master and workers (default: `/shared`)
- Local Development Only
	- Docker & Docker Compose

---

## Repository Layout

- `src/`
	- `main/java/com/gridmr/master/` – Master server core (scheduler, queues, worker mgmt)
		- `http/HttpJobServer.java` – HTTP job submission endpoint
		- `service/ControlServiceImpl.java` – gRPC control plane for workers
		- `tasks/SchedulerState.java` – Job state enum/state holder
		- `Scheduler.java`, `TaskQueue.java`, `WorkerManager.java`, `MasterServer.java`, `util/Env.java`
	- `main/proto/gridmr.proto` – Protobuf definitions for master–worker communication
- `cpp/worker/`
	- `src/worker_main.cc` – Worker entrypoint (gRPC client, task execution loop)
	- `include/gridmr/worker/...` – Utilities: `env`, `fs`, `logger`, `sysmetrics`
	- Example mapper/reducer sources: `map.cc`, `reduce.cc`
- `tools/`
	- `gridmr_submit.py` – Python client to split inputs, upload artifacts, submit jobs, and fetch results
	- `.env` – Client configuration (SSH target, API URL, shared base, chunk size, PEM file)
- `infra/`
	- `docker-compose.yml`, `*/Dockerfile` – Local development and testing only
- `deployment/`
	- Terraform and shell scaffold for AWS

---

## Architecture Overview

GridMR uses a master/worker architecture with a shared filesystem:

1) Client (Python CLI)
	 - Splits input into shards (line-safe by size).
	 - Uploads map and reduce functions + input shards to the master host’s shared directory.
	 - Calls master HTTP API to submit the job.
	 - Polls for final output and downloads it when available.

2) Master (Java)
	 - HTTP Job Server: receives job submissions and validates payloads.
	 - Control gRPC Service: coordinates workers (assignment, heartbeats, reporting).
	 - Scheduler: plans map and reduce phases, assigns tasks, handles retries, transitions job state.
	 - Task Queue: queues tasks and supports lease/ack semantics.
	 - Worker Manager: tracks worker liveness and capacity; reassigns on missed heartbeats.

3) Workers (C++)
	 - Register with master via gRPC and poll/lease tasks.
	 - Read assigned shard(s) from the shared filesystem.
	 - Execute user-provided map/reduce functions (stdin → stdout contract).
	 - Write intermediate partitions and final reduce outputs back to the shared filesystem.

Shared filesystem (e.g., AWS EFS) is mounted at the same mount point (default: `/shared`) across master and workers.

### Data Flow (Simplified)

- Client uploads job artifacts to: `/shared/<job_id>/`
	- `/shared/<job_id>/input/…` (sharded text files)
	- `/shared/<job_id>/<map_bin_name>`, `/shared/<job_id>/<reduce_bin_name>`
- Master schedules map tasks → workers run map → write intermediate partitions
	- e.g., `/shared/<job_id>/intermediate/part-<mapIdx>-<reduceIdx>.*` (implementation-defined)
- Master schedules reduce tasks → workers merge/sort/group → produce final output
	- `/shared/results/<job_id>/final.txt`
- Client polls for `/shared/results/<job_id>/final.txt` and downloads it.

---

## Master Components (Java)

Location: `src/main/java/com/gridmr/master`

- `http/HttpJobServer`
	- Accepts job submissions via REST (POST).
	- Validates payload and enqueues a new job in the scheduler.
- `service/ControlServiceImpl` (gRPC)
	- Implements endpoints defined in `src/main/proto/gridmr.proto`.
	- Worker registration, task leasing, heartbeats, completion reporting.
- `Scheduler`
	- Transitions job states (e.g., PENDING → MAPPING → SHUFFLING → REDUCING → DONE/FAILED).
	- Handles retries for failed tasks and orchestrates phase transitions.
- `TaskQueue`
	- Queues pending tasks and supports lease/ack to tolerate transient worker failures.
- `WorkerManager`
	- Tracks worker liveness and capacity; reassigns tasks if heartbeats are missed.
- `util/Env`
	- Centralized configuration reader (env vars and defaults).

---
### Scheduler and Worker Manager: Deep Dive

This section documents how tasks are assigned, when reducers are scheduled, and how worker health impacts scheduling.

- Core structures
	- `TaskQueue` (FIFO): holds pending `AssignTask` items awaiting assignment.
	- `WorkerManager`: keeps the latest `Heartbeat` per worker and orders workers by availability using a weighted score (CPU 70% + RAM 30%). Lower score = more available.
	- `SchedulerState`: wraps `WorkerManager` and `TaskQueue`, tracks per-worker running tasks, and exposes atomic assignment helpers (e.g., `tryAssignNext`, `markAssigned`, `markFinished`).
	- Control layer (`ControlServiceImpl`): maintains live gRPC streams, `inFlight` tasks (workerId → task), last heartbeat timestamps, and job contexts.

- Assignment algorithm
	- On worker connect (`INFO`) or on each `HEARTBEAT`, the master attempts assignment:
		- Per-worker path: `tryAssign(workerId)` → `SchedulerState.tryAssignNext(workerId)` assigns next pending task if the worker is not busy.
		- Bulk path: `tryAssignAll()` orders free workers by availability and drains the queue.
	- Availability ordering: `WorkerManager.getWorkersByAvailability()` sorts by weighted CPU/RAM usage; `Scheduler` enforces thresholds `SCHED_MAX_CPU_PCT` and `SCHED_MAX_RAM_PCT` (defaults 90%).
	- On initial connect a synthetic heartbeat with CPU=RAM=100 is registered so the worker won’t receive tasks until it sends a real heartbeat with current metrics.

- Job lifecycle and phase transitions
	- HTTP submit builds a JobContext: `jobId`, input split list, `nReducers`, map/reduce binaries, and start gates (`MR_MIN_WORKERS`, `MR_START_DELAY_MS`).
	- Start gates: maps are enqueued only when enough workers are connected (≥ `MR_MIN_WORKERS`, default 3) and optional delay elapsed (`MR_START_DELAY_MS`, default 0ms).
	- Map phase: one MAP task per input split is created and enqueued. On `STATUS COMPLETED map-*`, the job increments `mapsCompleted`.
	- Intermediate tracking: workers send `PART` messages with uploaded partition info; the master tracks a `BitSet` per map to ensure all `nReducers` partitions exist.
	- Reduce scheduling condition: reducers are scheduled only when ALL maps are completed AND every map has uploaded ALL partitions. Then one REDUCE task per reducer id is created. Each reducer receives URIs like `${SHARED_DATA_ROOT}/intermediate/${jobId}/part-<r>-map-<mapId>.txt`.
	- Finalization: when all reducers report completed, the master concatenates `results/${jobId}/part-0.txt .. part-(nReducers-1).txt` into `results/${job_id}/final.txt`, with two short delayed retries (1s, 2s) to absorb eventual NFS visibility.

- Heartbeats and fault tolerance
	- Heartbeat monitor runs every `HEARTBEAT_CHECK_PERIOD_MS` (default 3000ms) and logs stale workers after `HEARTBEAT_LOG_STALE_MS` (5000ms).
	- If no heartbeat arrives within `HEARTBEAT_TIMEOUT_MS` (default 15000ms), the worker is considered expired: its in-flight task is re-queued, the worker is removed from active clients, and its busy flag cleared.
	- gRPC stream errors or completion trigger the same cleanup and requeue logic.

---

## Worker Runtime (C++)

Location: `cpp/worker`

- `src/worker_main.cc`
	- gRPC client to the master (per `gridmr.proto`).
	- Fetches tasks, runs mapper/reducer, reports status, and writes artifacts to `/shared`.
- Utilities
	- `env.{h,cc}` – environment/config
	- `fs.{h,cc}` – filesystem helpers (e.g., atomic writes, temp files)
	- `logger.{h,cc}` – structured logging
	- `sysmetrics.{h,cc}` – basic resource metrics
- Example map/reduce
	- `map.cc`, `reduce.cc` demonstrate the stdin/stdout contract. You can build your own and upload them instead.

Worker executes user-provided functions, piping input from stdin and capturing stdout to files.

---

## Client CLI (Python)

Location: `tools/gridmr_submit.py`

Responsibilities:
- Split a large input file into N shards (~`GRIDMR_CHUNK_SIZE_MB` each; line-safe splitting).
- Upload map and reduce functions + shards to the master’s shared directory via `scp`.
- POST a job submission to the master HTTP API.
- Poll for the final result and fetch it locally.

Environment (`tools/.env`):
- `GRIDMR_MASTER_SSH` – SSH target for the master (e.g., `ubuntu@<master_ip>`)
- `GRIDMR_REMOTE_BASE` – Shared base directory on the master (e.g., `/shared`)
- `GRIDMR_MASTER_API` – Master HTTP endpoint (e.g., `http://<master_ip>:8080/jobs`)
- `GRIDMR_CHUNK_SIZE_MB` – Integer shard size in MB (e.g., `32`)
- `GRIDMR_PEM_FILE` – Path to your SSH private key (absolute path recommended)

Usage:

```bash
# From repo root
cd tools
python3 -m venv .venv && source .venv/bin/activate
pip install requests python-dotenv

python gridmr_submit.py ./my_map.{cc,cpp} ./my_reduce.{cc,cpp} ./input.txt 4

# The client will:
#   - split input -> tools/input/input-***.txt
#   - upload to ${GRIDMR_REMOTE_BASE}/${job_id}
#   - POST to ${GRIDMR_MASTER_API}
#   - wait for /shared/results/${job_id}/final.txt
#   - download to tools/results/result-${job_id}.txt
```

Notes:
- The CLI includes a wait-for-result loop to avoid race conditions.
- If you see SSH/SCP permission errors, ensure your key file is `chmod 600` and the path is correct.
- The client does not delete remote artifacts; manage retention on the server side.
- **Contract:** The client must upload their map.{cc,cpp} and reduce.{cc,cpp} code and the output must be in the format {key}\t{value}

---

## HTTP Job API

Endpoint: `POST ${GRIDMR_MASTER_API}`

Request (JSON):

```json
{
	"job_id": "1694640000",
	"input_uris": "1694640000/input/input-001.txt,1694640000/input/input-002.txt",
	"n_reducers": "4",
	"map_bin_uri": "my_map.{cc,cpp}",
	"reduce_bin_uri": "my_reduce.{cc,cpp}"
}
```

- `job_id`: String identifier chosen by the client (the master may return a canonical UUID).
- `input_uris`: Comma-separated list of shard paths relative to `GRIDMR_REMOTE_BASE`.
- `n_reducers`: Number of reduce partitions.
- `map_bin_uri` / `reduce_bin_uri`: Filenames as uploaded to `${GRIDMR_REMOTE_BASE}/${job_id}/`.

Response (JSON):

```json
{
	"job_id": "08d70a6a-0c7b-422a-90ce-2e7cc38f99ee",
	"status": "ACCEPTED"
}
```

Status endpoints:
- To be finalized. For now, the Python client polls the filesystem for `final*`.

---

## Directory Layout on Shared Filesystem

- `${BASE} = GRIDMR_REMOTE_BASE` (default: `/shared`)
- `${BASE}/${job_id}/`
	- `input/input-***.txt`
	- `<map_bin_name>` (as uploaded)
	- `<reduce_bin_name>` (as uploaded)
	- `intermediate/part-<m>-<r>.*` (implementation-defined)
- `${BASE}/results/${job_id}/final*`

Ensure all nodes (master and workers) mount `${BASE}` at the same absolute path.

---

## Building the Master (Java)

The project uses Maven (see `pom.xml`).

```bash
# From repo root
mvn -DskipTests package
# Artifacts under target/
```

Running the master depends on your environment (ports, shared path) and may be packaged into a runnable jar or launched from your IDE. Check `util/Env.java` for configuration variables.

---

## Building Worker and Example Map/Reduce (C++)

```bash
cd cpp/worker
cmake -S . -B build
cmake --build build -j
# Produce your own map/reduce executables, or use examples compiled to runnable binaries.
```

---

## AWS Deployment (High Level)

Production deployment is intended for standard EC2 instances (no Docker in prod). You will complete the final AWS specifics. Recommended architecture:

- Compute
	- 1 master EC2 instance (HTTP + gRPC + shared mount).
	- N worker EC2 instances (C++ worker runtime + shared mount).
- Storage
	- AWS EFS mounted at `/shared` on all instances.
- Networking
	- Security groups: allow 22 (SSH), master HTTP port (e.g., 8080) from client network, gRPC port (e.g., 50051) from workers, and EFS mount targets.
- Credentials
	- Do not store private keys in the repository. Use a secure path (e.g., `~/.ssh/key.pem`) and proper permissions.

The `deployment/` folder contains initial Terraform/script scaffolding; finalize instance sizing, AMIs, networking, and IAM per your environment. Running the following will up the entire cluster.
```bash
cd deployment
terraform init
terraform apply
```
---

## Configuration

Client (`tools/.env`):

```ini
GRIDMR_MASTER_SSH=ubuntu@<master_ip_or_dns>
GRIDMR_REMOTE_BASE=/shared
GRIDMR_MASTER_API=http://<master_ip_or_dns>:8080/jobs
GRIDMR_CHUNK_SIZE_MB=32
GRIDMR_PEM_FILE=/home/you/.ssh/gridmr.pem
```

---

## Troubleshooting

- Client cannot fetch result (missing `final.txt`)
	- Ensure the job has completed; the client polls until the file appears.
	- Verify `GRIDMR_REMOTE_BASE` matches the path the master writes to (e.g., `/shared`).
	- Check that all nodes mount the same EFS path at the same mount point.
	- Inspect master/worker logs for task failures.

- SSH/SCP fails
	- `chmod 600` your PEM file; make sure `GRIDMR_PEM_FILE` points to the absolute path.
	- Test with: `ssh -i "$GRIDMR_PEM_FILE" $GRIDMR_MASTER_SSH 'echo ok'`

- Workers idle, no tasks assigned
	- Confirm workers can reach master’s gRPC port.
	- Verify uploaded input shards and map/reduce functions exist under `${BASE}/${job_id}/`.

- Mapper/Reducer crashes
	- Run locally with a sample shard via stdin to confirm behavior.
	- Ensure the binary is built for the worker’s architecture and is executable (`chmod +x`).

---

## Diagrams

Short, visual overview of the platform. Paste your Mermaid code or images in each placeholder below.

### 1) System Context Diagram (C4 L1)
High-level view of the User, Client CLI, Master, Workers, and Shared FS within AWS networking boundaries.

<img width="3565" height="3840" alt="Untitled diagram _ Mermaid Chart-2025-09-15-022439" src="https://github.com/user-attachments/assets/e92b8165-8547-4758-b479-cd2fdb633876" />


### 2) Master Component Diagram (C4 L2)
Internal components of the Master service: HTTP server, gRPC control, Scheduler, TaskQueue, WorkerManager, and configuration.

<img width="3817" height="3840" alt="Untitled diagram _ Mermaid Chart-2025-09-15-024819" src="https://github.com/user-attachments/assets/53ed979b-e858-4122-973d-da4004c8126b" />

### 3) Deployment Diagram (AWS EC2 + EFS)
Production-oriented layout with EC2 Master/Workers, EFS at /shared, subnets/AZs, and open ports.

Testing Deployment
<img width="1219" height="899" alt="image" src="https://github.com/user-attachments/assets/ea48dd4b-c0ef-4f27-8262-19b9c369af5f" />

Production Deployment
<img width="1103" height="805" alt="image" src="https://github.com/user-attachments/assets/5ff64b37-6da8-4d1d-9df5-7c45529d4606" />

<!-- ![Deployment](docs/diagrams/deployment-aws.png) -->

### 4) Job Submission Sequence
End-to-end flow: split input, SCP artifacts, HTTP submit, map→reduce execution, final.txt creation, client fetch.

<img width="3840" height="3259" alt="mermaid-ai-diagram-2025-09-15-025046" src="https://github.com/user-attachments/assets/5f1e5015-8a52-4304-b832-a3c08248442b" />

### 5) Scheduler State Machine
Job lifecycle states and transitions: PENDING → MAPPING → SHUFFLING → REDUCING → FINALIZING → DONE/FAILED.

<img width="2184" height="3840" alt="mermaid-ai-diagram-2025-09-15-025551" src="https://github.com/user-attachments/assets/9c6699d9-7828-466e-8148-8aa486cbdfd0" />







