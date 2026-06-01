#!/usr/bin/env python3
"""Drive the ohouse Athena MCP server directly via mcp-remote stdio bridge.

Usage:
  python mcp_athena.py --list                      # discover tools
  python mcp_athena.py --sql-file q.sql --out r.json
  python mcp_athena.py --sql "SELECT ..." --out r.json
"""
import subprocess, sys, json, threading, time, argparse, os

ENDPOINT = "https://query.datapl.datahou.se/mcp?username=yys"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--sql")
    ap.add_argument("--sql-file")
    ap.add_argument("--manifest", help="JSON file: {name: sql, ...}. Runs all in one session, writes <outdir>/<name>.json")
    ap.add_argument("--outdir", default=".")
    ap.add_argument("--tool", default="execute_athena_query")
    ap.add_argument("--param", default="query")
    ap.add_argument("--out")
    ap.add_argument("--timeout", type=int, default=300)
    args = ap.parse_args()

    sql = None
    if args.sql_file:
        with open(args.sql_file, "r", encoding="utf-8") as f:
            sql = f.read()
    elif args.sql:
        sql = args.sql

    cmd = ["npx", "-y", "mcp-remote", ENDPOINT]
    proc = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, encoding="utf-8", bufsize=1,
        shell=(os.name == "nt"),
    )

    responses = {}
    lock = threading.Condition()

    def reader():
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except Exception:
                continue
            if isinstance(msg, dict) and "id" in msg:
                with lock:
                    responses[msg["id"]] = msg
                    lock.notify_all()

    def errreader():
        for line in proc.stderr:
            sys.stderr.write("[mcp] " + line)

    t1 = threading.Thread(target=reader, daemon=True); t1.start()
    t2 = threading.Thread(target=errreader, daemon=True); t2.start()

    def send(obj):
        proc.stdin.write(json.dumps(obj) + "\n")
        proc.stdin.flush()

    def wait_for(rid, timeout):
        deadline = time.time() + timeout
        with lock:
            while rid not in responses:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                lock.wait(remaining)
            return responses[rid]

    # 1. initialize
    send({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2024-11-05",
        "capabilities":{},
        "clientInfo":{"name":"hugo-athena-cli","version":"1.0.0"}}})
    init = wait_for(1, 90)
    if init is None:
        sys.stderr.write("ERROR: initialize timed out\n"); proc.kill(); sys.exit(2)
    # initialized notification
    send({"jsonrpc":"2.0","method":"notifications/initialized","params":{}})

    if args.list:
        send({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}})
        r = wait_for(2, 60)
        print(json.dumps(r, ensure_ascii=False, indent=2))
        proc.terminate(); return

    if args.manifest:
        with open(args.manifest, "r", encoding="utf-8") as f:
            jobs = json.load(f)
        rid = 100
        summary = {}
        for name, q in jobs.items():
            rid += 1
            t0 = time.time()
            send({"jsonrpc":"2.0","id":rid,"method":"tools/call","params":{
                "name": args.tool, "arguments": {args.param: q}}})
            r = wait_for(rid, args.timeout)
            elapsed = round(time.time()-t0, 1)
            if r is None:
                summary[name] = f"TIMEOUT after {elapsed}s"; sys.stderr.write(f"[{name}] TIMEOUT\n"); continue
            if "error" in r:
                summary[name] = "MCP_ERROR: " + json.dumps(r["error"], ensure_ascii=False)
                sys.stderr.write(f"[{name}] MCP_ERROR\n"); continue
            content = r.get("result", {}).get("content", [])
            payload = "\n".join(c.get("text","") for c in content if c.get("type")=="text")
            outp = os.path.join(args.outdir, f"{name}.json")
            with open(outp, "w", encoding="utf-8") as f:
                f.write(payload)
            # quick status peek
            try:
                pj = json.loads(payload)
                summary[name] = f"{pj.get('status')} rows={pj.get('result_count')} scan={round(pj.get('data_scanned_bytes',0)/1e6,1)}MB {elapsed}s"
            except Exception:
                summary[name] = f"wrote {len(payload)} chars {elapsed}s"
            sys.stderr.write(f"[{name}] {summary[name]}\n")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        proc.terminate(); return

    # tools/call
    send({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
        "name": args.tool,
        "arguments": {args.param: sql}}})
    r = wait_for(3, args.timeout)
    proc.terminate()
    if r is None:
        sys.stderr.write("ERROR: query timed out\n"); sys.exit(3)
    if "error" in r:
        sys.stderr.write("MCP ERROR: " + json.dumps(r["error"], ensure_ascii=False) + "\n")
        sys.exit(4)
    # extract text content
    content = r.get("result", {}).get("content", [])
    texts = [c.get("text","") for c in content if c.get("type")=="text"]
    payload = "\n".join(texts)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(payload)
        sys.stderr.write(f"WROTE {len(payload)} chars to {args.out}\n")
    else:
        print(payload)

if __name__ == "__main__":
    main()
