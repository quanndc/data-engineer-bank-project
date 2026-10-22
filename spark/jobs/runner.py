"""Control channel F -> C (sheet: "F gửi lệnh chạy/chạy lại C", "F yêu cầu D sao lưu/khôi phục").

A tiny HTTP service inside the spark container, reachable only on the Docker network
(http://spark:8000, no port published on the host). It runs an allow-listed command in the
background, one job at a time, and reports its status and output.

    POST /jobs          {"action": "scenario", "arg": "KB05"}  -> {"job_id": 1}
    GET  /jobs/<id>     -> {"status": "running|succeeded|failed", "output": "...", ...}
    GET  /preview/<KB>  -> scenario input preview without loading anything (sheet N01)
"""
import csv
import json
import os
import re
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCENARIOS = ("KB03", "KB05", "KB07", "KB08")
PIPELINE = ["spark-submit", "/opt/jobs/pipeline.py"]
NAME = re.compile(r"^[A-Za-z0-9_]{1,64}$")


def command(action, arg):
    """Allow-listed commands only; arguments are validated, never passed to a shell."""
    if action == "scenario" and arg in SCENARIOS:
        return PIPELINE + ["scenario", arg]
    if action == "rerun" and arg and NAME.match(arg):
        return PIPELINE + ["rerun", arg]
    if action == "alerts":
        return PIPELINE + ["alerts", "all"]
    if action == "reconcile":
        return PIPELINE + ["reconcile"]
    if action == "backup" and (not arg or NAME.match(arg)):
        return ["python3", "/opt/jobs/backup.py", "backup"] + ([arg] if arg else [])
    if action == "restore" and arg and NAME.match(arg):
        return ["python3", "/opt/jobs/backup.py", "restore", arg]
    return None


JOBS, LOCK, NEXT = {}, threading.Lock(), [1]


def run_job(job_id, cmd):
    job = JOBS[job_id]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            cwd="/opt/jobs", env={**os.environ, "BANKDW_TRIGGERED_BY": "app"})
    lines = []
    for line in proc.stdout:
        # keep the pipeline's own lines, drop Spark's INFO/WARN noise
        if not re.search(r" (INFO|WARN) |NativeCodeLoader|setLogLevel|log level|^\[Stage", line):
            lines.append(line.rstrip())
            job["output"] = "\n".join(lines[-200:])
    job["returncode"] = proc.wait()
    job["status"] = "succeeded" if job["returncode"] == 0 else "failed"
    job["finished_at"] = time.time()
    LOCK.release()


def preview(code):
    folder = f"/data/source/scenarios/{code}"
    out = {"scenario": code, "files": {}}
    for name in sorted(os.listdir(folder)):
        path = f"{folder}/{name}"
        if name.endswith(".json"):
            out["expected"] = json.load(open(path))
        elif name.endswith(".csv"):
            with open(path) as f:
                delim = ";" if name == "trans.csv" else ","
                rows = list(csv.reader(f, delimiter=delim))
            out["files"][name] = {"rows": len(rows) - 1, "header": rows[0], "sample": rows[1:11]}
    return out


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body):
        data = json.dumps(body, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        parts = self.path.strip("/").split("/")
        if parts[0] == "health":
            return self._send(200, {"ok": True, "busy": LOCK.locked()})
        if parts[0] == "jobs" and len(parts) == 2 and parts[1].isdigit() and int(parts[1]) in JOBS:
            return self._send(200, JOBS[int(parts[1])])
        if parts[0] == "preview" and len(parts) == 2 and parts[1] in SCENARIOS:
            return self._send(200, preview(parts[1]))
        return self._send(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
        if self.path.rstrip("/") != "/jobs":
            return self._send(404, {"error": "not found"})
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or "{}")
        cmd = command(body.get("action"), body.get("arg"))
        if cmd is None:
            return self._send(400, {"error": "action not allowed"})
        if not LOCK.acquire(blocking=False):
            return self._send(409, {"error": "another job is running"})
        job_id = NEXT[0]
        NEXT[0] += 1
        JOBS[job_id] = {"job_id": job_id, "action": body.get("action"), "arg": body.get("arg"),
                        "status": "running", "output": "", "started_at": time.time()}
        threading.Thread(target=run_job, args=(job_id, cmd), daemon=True).start()
        return self._send(202, {"job_id": job_id})

    def log_message(self, *args):  # quiet access log
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
