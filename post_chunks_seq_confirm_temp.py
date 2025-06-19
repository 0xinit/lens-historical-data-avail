#!/usr/bin/env python3
"""
upload_sequential.py – one-by-one upload with finality wait
Writes build/uploaded.ndjson (append-only).
"""

import os, json, time, pathlib, requests
from dotenv import load_dotenv; load_dotenv()

TARGET = "turbo"          # "turbo" or "hex"
POLL   = 15               # seconds between /info polls
QPS    = 3                # max posts/sec to stay gentle

MAN   = pathlib.Path("build/manifest.ndjson")
LOG   = pathlib.Path("build/uploaded.ndjson")

if TARGET=="turbo":
    SUB = os.getenv("TURBODA_ENDPOINT")
    KEY = os.getenv("TURBODA_KEY")
else:
    SUB = os.getenv("HEX_ENDPOINT")
    KEY = os.getenv("HEX_KEY")
INFO = SUB.replace("submit","info")

HEAD = {"x-api-key":KEY,"Content-Type":"application/octet-stream"}
sess = requests.Session(); last=0

uploaded=set()
if LOG.exists():
    for l in LOG.open(): uploaded.add(json.loads(l)["chunk_id"])
print("resume – already done:", len(uploaded))

def throttle():
    global last
    wait=max(0, (1/QPS)-(time.time()-last))
    if wait: time.sleep(wait); last=time.time()

with LOG.open("a") as fout, MAN.open() as f:
    for line in f:
        obj=json.loads(line); cid=obj["chunk_id"]
        if cid in uploaded: continue

        throttle()
        with open(obj["path"],"rb") as binf:
            sid=sess.post(SUB,headers=HEAD,data=binf,timeout=30).json()["submission_id"]
        print(f"POST chunk {cid:07} → {sid}")

        while True:
            meta=sess.get(f"{INFO}/{sid}",timeout=10).json()
            if meta["finalized"]: break
            time.sleep(POLL)
        print(f"FIN  chunk {cid:07}  block={meta['block']}")

        fout.write(json.dumps({"chunk_id":cid,"submission_id":sid,
                               "avail_block":meta["block"],
                               "avail_tx":meta["extrinsic"],
                               "uploaded_at":int(time.time())})+"\n")
        fout.flush()
