#!/usr/bin/env python3
"""Worker node: registers, polls for jobs, executes supported tasks and reports results."""
import argparse, json, socket, time, uuid

def rpc(host, port, message, timeout=8):
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.sendall((json.dumps(message) + "\n").encode())
        f = s.makefile("r", encoding="utf-8")
        return json.loads(f.readline())

def execute(job):
    p = job["payload"]
    task = job["task"]
    if task == "add": return sum(p["numbers"])
    if task == "multiply":
        out = 1
        for n in p["numbers"]: out *= n
        return out
    if task == "uppercase": return p["text"].upper()
    if task == "word_count": return len(p["text"].split())
    raise ValueError("Unsupported task")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5050)
    ap.add_argument("--id", default=None)
    ap.add_argument("--poll", type=float, default=2)
    a = ap.parse_args()
    wid = a.id or "worker-" + str(uuid.uuid4())[:8]
    rpc(a.host, a.port, {"op":"register", "worker_id":wid})
    print(f"{wid} registered with {a.host}:{a.port}")
    last_heartbeat = 0
    while True:
        try:
            if time.time() - last_heartbeat > 5:
                rpc(a.host, a.port, {"op":"heartbeat", "worker_id":wid})
                last_heartbeat = time.time()
            response = rpc(a.host, a.port, {"op":"get_job", "worker_id":wid})
            job = response.get("job")
            if not job:
                time.sleep(a.poll)
                continue
            print(f"Executing {job['id']} ({job['task']})")
            try:
                result = execute(job)
                rpc(a.host, a.port, {"op":"complete","worker_id":wid,
                    "job_id":job["id"],"result":result})
                print(f"Completed {job['id']}: {result}")
            except Exception as e:
                rpc(a.host, a.port, {"op":"complete","worker_id":wid,
                    "job_id":job["id"],"error":str(e)})
                print(f"Failed {job['id']}: {e}")
        except (OSError, ValueError, KeyError) as e:
            print(f"Coordinator connection/problem: {e}; retrying...")
            time.sleep(3)

if __name__ == "__main__":
    main()
