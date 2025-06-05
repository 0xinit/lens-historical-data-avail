#!/usr/bin/env python3
"""
Phase 2 – concurrent uploader (order-aware -- this means we are still looking out for order -- my ocd kicked).
Uploads ≤10 chunks in parallel but patches manifest strictly in chunk_id order.
Safe for staging if MAX_QPS is 20 total.
"""

import os, json, time, asyncio, aiohttp, pathlib, tempfile, shutil

from dotenv import load_dotenv; load_dotenv()
API  = os.getenv("TURBODA_ENDPOINT")
KEY  = os.getenv("TURBODA_KEY")
MAN  = pathlib.Path("build/manifest.ndjson")
CONCURRENCY = 10
MAX_QPS     = 20          # cap total across workers

sem = asyncio.Semaphore(CONCURRENCY)
qps_window = []

async def post_chunk(session, obj):
    async with sem:
        # QPS throttle
        now = time.time()
        qps_window[:] = [t for t in qps_window if now - t < 1]
        if len(qps_window) >= MAX_QPS:
            await asyncio.sleep(1 - (now - qps_window[0]))
        qps_window.append(time.time())

        async with session.post(API,
                headers={"x-api-key":KEY,"Content-Type":"application/octet-stream"},
                data=open(obj["path"],"rb")) as r:
            if r.status != 200:
                text = await r.text()
                raise RuntimeError(f"{r.status}: {text}")
            sid = (await r.json())["submission_id"]
            return obj["chunk_id"], sid

async def main():
    # load manifest into a list of objs
    objs=[]
    with MAN.open(encoding="utf-8") as f:
        for line in f:
            obj=json.loads(line); objs.append(obj)

    # writer will pop chunk_id in order
    next_id = 1
    pending = {}
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

    async def writer():
        nonlocal next_id
        tmp = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        for obj in objs:
            if "submission_id" in obj:
                tmp.write(json.dumps(obj)+"\n")
            else:
                while next_id not in pending:
                    await asyncio.sleep(0.1)
                obj["submission_id"]=pending.pop(next_id)
                obj["uploaded_at"]=int(time.time())
                tmp.write(json.dumps(obj)+"\n")
                print(f"chunk {next_id:07} → {obj['submission_id']}")
            next_id+=1
        tmp.flush(); os.fsync(tmp.fileno())
        shutil.move(tmp.name, MAN)
        await session.close()

    # kick off uploads
    tasks=[]
    for obj in objs:
        if "submission_id" in obj: continue
        tasks.append(asyncio.create_task(post_chunk(session, obj)))

    # collect completions
    async def collector():
        for t in asyncio.as_completed(tasks):
            cid,sid=await t
            pending[cid]=sid

    await asyncio.gather(writer(), collector())

if __name__=="__main__":
    asyncio.run(main())
