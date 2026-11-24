#!/usr/bin/env python3
"""TCP job-queue coordinator. Each request is one newline-delimited JSON message."""
import argparse, json, socket, socketserver, sqlite3, threading, time, uuid
from pathlib import Path

DB_LOCK = threading.Lock()

class Store:
    def __init__(self, path):
        self.path = path
        self.init_db()

    def connect(self):
        con = sqlite3.connect(self.path, timeout=10)
        con.row_factory = sqlite3.Row
        return con

    def init_db(self):
        with DB_LOCK, self.connect() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS jobs(
                id TEXT PRIMARY KEY, task TEXT NOT NULL, payload TEXT NOT NULL,
                status TEXT NOT NULL, submitted REAL NOT NULL, worker_id TEXT,
                started REAL, finished REAL, result TEXT, error TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS workers(
                id TEXT PRIMARY KEY, host TEXT, status TEXT NOT NULL,
                last_seen REAL NOT NULL, current_job TEXT)""")
            c.commit()

    def submit(self, task, payload):
        jid = str(uuid.uuid4())
        with DB_LOCK, self.connect() as c:
            c.execute("INSERT INTO jobs(id,task,payload,status,submitted) VALUES(?,?,?,?,?)",
                      (jid, task, json.dumps(payload), "queued", time.time()))
            c.commit()
        return jid

    def register(self, wid, host):
        with DB_LOCK, self.connect() as c:
            c.execute("""INSERT INTO workers(id,host,status,last_seen) VALUES(?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET host=excluded.host,status='online',last_seen=excluded.last_seen""",
                (wid, host, "online", time.time()))
            c.commit()

    def heartbeat(self, wid):
        with DB_LOCK, self.connect() as c:
            cur = c.execute("UPDATE workers SET status='online',last_seen=? WHERE id=?",
                            (time.time(), wid))
            c.commit()
            return cur.rowcount > 0

    def claim(self, wid):
        with DB_LOCK, self.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT id,task,payload FROM jobs WHERE status='queued' ORDER BY submitted LIMIT 1").fetchone()
            if not row:
                c.commit()
                return None
            now = time.time()
            c.execute("UPDATE jobs SET status='running',worker_id=?,started=? WHERE id=? AND status='queued'",
                      (wid, now, row["id"]))
            c.execute("UPDATE workers SET current_job=?,last_seen=?,status='online' WHERE id=?",
                      (row["id"], now, wid))
            c.commit()
            return {"id": row["id"], "task": row["task"], "payload": json.loads(row["payload"])}

    def complete(self, wid, jid, result=None, error=None):
        status = "failed" if error else "completed"
        with DB_LOCK, self.connect() as c:
            cur = c.execute("""UPDATE jobs SET status=?,finished=?,result=?,error=?
                WHERE id=? AND worker_id=? AND status='running'""",
                (status, time.time(), json.dumps(result), error, jid, wid))
            c.execute("UPDATE workers SET current_job=NULL,last_seen=? WHERE id=? AND current_job=?",
                      (time.time(), wid, jid))
            c.commit()
            return cur.rowcount > 0

    def job(self, jid):
        with self.connect() as c:
            r = c.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
            if not r: return None
            d = dict(r)
            d["payload"] = json.loads(d["payload"])
            d["result"] = json.loads(d["result"]) if d["result"] else None
            return d

    def workers(self, timeout):
        now = time.time()
        with DB_LOCK, self.connect() as c:
            c.execute("UPDATE workers SET status='offline' WHERE last_seen < ?", (now-timeout,))
            c.commit()
            return [dict(r) for r in c.execute("SELECT * FROM workers ORDER BY id")]

STORE = None

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            data = b""
            while not data.endswith(b"\n") and len(data) < 1_000_000:
                chunk = self.request.recv(4096)
                if not chunk: break
                data += chunk
            req = json.loads(data.decode("utf-8"))
            response = self.dispatch(req)
        except Exception as e:
            response = {"ok": False, "error": str(e)}
        try:
            self.request.sendall((json.dumps(response) + "\n").encode("utf-8"))
        except OSError:
            pass

    def dispatch(self, r):
        op = r.get("op")
        if op == "submit":
            task, payload = r.get("task"), r.get("payload", {})
            if task not in {"add", "multiply", "uppercase", "word_count"}:
                return {"ok": False, "error": "Unsupported task type"}
            jid = STORE.submit(task, payload)
            return {"ok": True, "job_id": jid, "status": "queued"}
        if op == "status":
            j = STORE.job(r.get("job_id", ""))
            return {"ok": True, "job": j} if j else {"ok": False, "error": "Job not found"}
        if op == "register":
            STORE.register(r["worker_id"], self.client_address[0])
            return {"ok": True}
        if op == "heartbeat":
            return {"ok": STORE.heartbeat(r["worker_id"])}
        if op == "get_job":
            job = STORE.claim(r["worker_id"])
            return {"ok": True, "job": job}
        if op == "complete":
            ok = STORE.complete(r["worker_id"], r["job_id"], r.get("result"), r.get("error"))
            return {"ok": ok}
        if op == "workers":
            return {"ok": True, "workers": STORE.workers(SERVER.worker_timeout)}
        return {"ok": False, "error": "Unknown operation"}

class ThreadedServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

def main():
    global STORE, SERVER
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=5050)
    ap.add_argument("--db", default="jobs.db")
    ap.add_argument("--worker-timeout", type=int, default=15)
    a = ap.parse_args()
    STORE = Store(a.db)
    SERVER = type("Config", (), {"worker_timeout": a.worker_timeout})
    with ThreadedServer((a.host, a.port), Handler) as server:
        print(f"Coordinator listening on {a.host}:{a.port}; database={a.db}")
        server.serve_forever()

if __name__ == "__main__":
    main()
