# Distributed Job Queue and Worker Management using TCP

## Overview
A coordinator accepts jobs from clients, persists them in SQLite, and assigns queued jobs to registered workers over TCP. Workers execute supported tasks and report results.

## Requirements
- Python 3.9 or newer
- No third-party Python packages (standard library only)

## Project structure
- `src/server.py` — TCP coordinator, queue, SQLite persistence, worker registry
- `src/worker.py` — worker process
- `src/client.py` — job submission/status CLI
- `tests/test_tasks.py` — local unit tests for worker task execution
- `documents/` — SRS, report, architecture, protocol, test plan and test report template

## Run on one computer
Open three terminals from the project root.

Terminal 1 — start coordinator:
```bash
python src/server.py --host 127.0.0.1 --port 5050
```
Terminal 2 — start worker:
```bash
python src/worker.py --host 127.0.0.1 --port 5050 --id worker-1
```
Terminal 3 — submit a job:
```bash
python src/client.py --host 127.0.0.1 submit add --numbers 10 20 30
```
Copy the returned `job_id`, then:
```bash
python src/client.py status YOUR_JOB_ID
python src/client.py workers
```

Try other tasks:
```bash
python src/client.py submit multiply --numbers 2 3 4
python src/client.py submit uppercase --text "hello tcp"
python src/client.py submit word_count --text "distributed systems are interesting"
```

## Run across computers / Mininet
1. Start the coordinator on the server host using `--host 0.0.0.0`.
2. Allow inbound TCP traffic on port 5050 in the host firewall.
3. Start each worker with `--host COORDINATOR_IP`.
4. Submit jobs from a client using the same coordinator IP.
5. For Mininet, use the IP address assigned to the coordinator host (not `127.0.0.1`).

## Protocol
Each TCP connection carries one UTF-8 JSON object terminated by newline. The server returns one JSON response terminated by newline. See `documents/TCP_Protocol_Specification.docx`.

## Supported tasks
- `add`: payload `{"numbers":[1,2,3]}`
- `multiply`: payload `{"numbers":[2,3,4]}`
- `uppercase`: payload `{"text":"hello"}`
- `word_count`: payload `{"text":"hello world"}`

## Important limitations
This is an academic prototype, not a production-grade queue. Jobs are claimed once and are not automatically re-queued if a worker crashes mid-execution. Add authentication, TLS, bounded payload validation, retry/lease recovery, and stronger monitoring before production use. The coordinator should be run only on a trusted lab network.
