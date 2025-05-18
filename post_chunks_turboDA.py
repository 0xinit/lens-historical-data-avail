#!/usr/bin/env python3
"""
Phase 2 – throttled poster.
Reads manifest.ndjson, POSTs each .bin to TurboDA, writes submission_id
and bytes_sent back into the manifest atomically.
"""

import os, json, time, shutil, tempfile, requests, fileinput
from requests.adapters import HTTPAdapter, Retry
from dotenv import load_dotenv; load_dotenv()

API  = os.getenv("TURBODA_ENDPOINT")
KEY  = os.getenv("TURBODA_KEY")
MAN  = "build/manifest.ndjson"               # adjust if you changed path
RATE = 3          # max requests per second (staging cap ~5)

if not (API and KEY):
    raise SystemExit("Set TURBODA_ENDPOINT and TURBODA_KEY in .env")

# ---------- HTTP session with retries -----------------------------------
sess = requests.Session()
retry = Retry(
    total=5, backoff_factor=1.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["POST"]),
)
sess.mount("https://", HTTPAdapter(max_retries=retry))
HEAD = {"x-api-key": KEY, "Content-Type": "application/octet-stream"}

# ---------- simple token bucket throttler -------------------------------
last_time = 0
def throttle():
    global last_time
    wait = max(0, (1 / RATE) - (time.time() - last_time))
    if wait: time.sleep(wait)
    last_time = time.time()

# ---------- main loop ----------------------------------------------------
tmp = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
with fileinput.FileInput(MAN, openhook=fileinput.hook_encoded("utf-8")) as f:
    for line in f:
        obj = json.loads(line)

        # already uploaded? just copy line over
        if "submission_id" in obj:
            tmp.write(line)
            continue

        # throttle to RATE/s
        throttle()

        # upload chunk
        with open(obj["path"], "rb") as binf:
            r = sess.post(API, headers=HEAD, data=binf, timeout=30)
            r.raise_for_status()
            sid = r.json()["submission_id"]

        obj["submission_id"] = sid
        obj["uploaded_at"]   = int(time.time())
        tmp.write(json.dumps(obj) + "\n")
        print(f"chunk {obj['chunk_id']:07} → {sid}")

tmp.flush(); os.fsync(tmp.fileno())
shutil.move(tmp.name, MAN)           # atomic manifest replace
print("all pending chunks uploaded (or manifest already complete)")
