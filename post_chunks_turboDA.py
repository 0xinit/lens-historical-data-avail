#!/usr/bin/env python3
"""
sequential_poster.py  –  posts chunks in manifest order,
writes results to build/uploaded.ndjson (append-only).
"""

import os, json, time, requests, fileinput, pathlib
from requests.adapters import HTTPAdapter, Retry
from dotenv import load_dotenv; load_dotenv()

API  = os.getenv("TURBODA_ENDPOINT")
KEY  = os.getenv("TURBODA_KEY")
MAN  = pathlib.Path("build/manifest.ndjson")
OUT  = pathlib.Path("build/uploaded.ndjson")
RATE = 3          # req/s cap for staging

if not (API and KEY):
    raise SystemExit("Set TURBODA_ENDPOINT and TURBODA_KEY in .env")

# ── HTTP session with retries ───────────────────────────────────────────
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=1.5,
    status_forcelist=(429,500,502,503,504),
    allowed_methods=frozenset(["POST"]))))
HEAD = {"x-api-key": KEY, "Content-Type": "application/octet-stream"}

# ── load already-uploaded ids to skip them ──────────────────────────────
done = set()
if OUT.exists():
    with OUT.open() as f:
        for line in f:
            try:
                done.add(json.loads(line)["chunk_id"])
            except Exception:
                pass   # ignore malformed lines

print(f"skipping {len(done)} chunks already in {OUT.name}")

# ── throttle helper ─────────────────────────────────────────────────────
last_t = 0
def throttle():
    global last_t
    wait = max(0, (1/RATE) - (time.time() - last_t))
    if wait: time.sleep(wait)
    last_t = time.time()

# ── main loop ───────────────────────────────────────────────────────────
with OUT.open("a", encoding="utf-8") as fout, \
     fileinput.FileInput(MAN, openhook=fileinput.hook_encoded("utf-8")) as f:
    for line in f:
        obj = json.loads(line)
        cid = obj["chunk_id"]

        if cid in done:
            continue        # already uploaded in a previous run

        throttle()

        with open(obj["path"], "rb") as binf:
            r = sess.post(API, headers=HEAD, data=binf, timeout=30)
            r.raise_for_status()
            sid = r.json()["submission_id"]

        fout.write(json.dumps({
            "chunk_id":    cid,
            "submission_id": sid,
            "uploaded_at": int(time.time())
        }) + "\n")
        fout.flush()
        print(f"chunk {cid:07} → {sid}")

print("all pending chunks uploaded")
