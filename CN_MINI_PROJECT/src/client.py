#!/usr/bin/env python3
"""Command-line client for submitting jobs and checking status."""
import argparse, json, socket, time

def rpc(host, port, message):
    with socket.create_connection((host, port), timeout=8) as s:
        s.sendall((json.dumps(message) + "\n").encode())
        return json.loads(s.makefile("r", encoding="utf-8").readline())

def main():
    ap = argparse.ArgumentParser(description="Distributed TCP job queue client")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5050)
    sub = ap.add_subparsers(dest="command", required=True)
    p = sub.add_parser("submit")
    p.add_argument("task", choices=["add","multiply","uppercase","word_count"])
    p.add_argument("--numbers", nargs="+", type=float)
    p.add_argument("--text")
    st = sub.add_parser("status"); st.add_argument("job_id")
    sub.add_parser("workers")
    a = ap.parse_args()
    if a.command == "submit":
        payload = {"numbers":a.numbers} if a.task in ("add","multiply") else {"text":a.text or ""}
        result = rpc(a.host,a.port,{"op":"submit","task":a.task,"payload":payload})
    elif a.command == "status":
        result = rpc(a.host,a.port,{"op":"status","job_id":a.job_id})
    else:
        result = rpc(a.host,a.port,{"op":"workers"})
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
