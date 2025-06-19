#!/usr/bin/env python3
"""
Sequential uploader with per-chunk finality wait.

For each manifest row *without* submission_id:
  1. POST the .bin to TurboDA / Hex
  2. Poll /info every POLL_INTERVAL seconds until "finalized": true
  3. Append a JSON line to build/uploaded.ndjson
  4. Move on to the next chunk
"""

import os, json, time, pathlib, requests, random
from dotenv import load_dotenv; load_dotenv()

# ── choose target here ───────────────────────────────────────────────────
TARGET        = "mainnet"    # "turbo" | "hex" | "mainnet"
MAX_QPS       = 3
POLL_INTERVAL = 15
JITTER        = 3

MAN = pathlib.Path("build/manifest.ndjson")
LOG = pathlib.Path("build/uploaded.ndjson")

# ── endpoint / key resolution ────────────────────────────────────────────
if TARGET == "turbo":
    API_SUB  = os.getenv("TURBODA_ENDPOINT")      # …/v1/submit_raw_data
    KEY      = os.getenv("TURBODA_KEY")
elif TARGET == "mainnet":
    API_SUB  = os.getenv("MAINNET_ENDPOINT")
    KEY      = os.getenv("MAINNET_KEY")
elif TARGET == "hex":
    API_SUB  = os.getenv("HEX_ENDPOINT")
    KEY      = os.getenv("HEX_KEY")
else:
    raise SystemExit(f"Unknown TARGET {TARGET}")

API_INFO = API_SUB.rsplit("/", 1)[0] + "/get_submission_info"

if not (API_SUB and KEY):
    raise SystemExit("Missing endpoint or key in .env for selected target")

HEAD = {"x-api-key": KEY, "Content-Type": "application/octet-stream"}

sess = requests.Session()
sess.mount("https://", requests.adapters.HTTPAdapter(max_retries=5))

# ── resume set ───────────────────────────────────────────────────────────
done = {json.loads(l)["chunk_id"] for l in LOG.open()} if LOG.exists() else set()
print(f"skipping {len(done)} chunks already logged")

last_post = 0
def throttle():
    global last_post
    wait = max(0, (1/MAX_QPS) - (time.time() - last_post))
    if wait: time.sleep(wait)
    last_post = time.time()

def wait_finalized(sid: str):
    while True:
        try:
            r = sess.get(API_INFO, params={"submission_id": sid},
                         headers={"x-api-key": KEY}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data.get("state") == "Finalized":    # Turbo format
                    return data["data"]
                if data.get("finalized"):                # Hex format
                    return data
        except (requests.RequestException, ValueError,
                json.JSONDecodeError):
            pass
        time.sleep(POLL_INTERVAL + random.uniform(0, JITTER))

# ── main loop ────────────────────────────────────────────────────────────
with LOG.open("a") as fout, MAN.open() as manifest:
    for line in manifest:
        obj = json.loads(line); cid = obj["chunk_id"]
        if cid in done: continue

        throttle()
        with open(obj["path"], "rb") as binf:
            sid = sess.post(API_SUB, headers=HEAD, data=binf,
                            timeout=30).json()["submission_id"]
        print(f"POST  chunk {cid:07} → {sid}")

        info = wait_finalized(sid)
        block = info.get("block_number") or info.get("block")
        print(f"FIN   chunk {cid:07}  block={block}")

        fout.write(json.dumps({
            "chunk_id":      cid,
            "submission_id": sid,
            "avail_block":   block,
            "block_hash":    info.get("block_hash"),
            "avail_tx":      info.get("tx_hash") or info.get("extrinsic"),
            "uploaded_at":   int(time.time())
        }) + "\n")
        fout.flush()