#!/usr/bin/env python3
"""
step 1-  deterministic 1 MB chunk builder
( events + storage_logs )  →  chunks/*.bin  +  manifest.ndjson
"""

import os, json, hashlib, pathlib, psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv; load_dotenv()

# ── config ───────────────────────────────────────────────────────────────
DB_DSN          = os.getenv("DB_DSN", "dbname=lens_raw user=postgres host=localhost")
OUT_DIR         = pathlib.Path("build/chunks")
OUT_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH   = pathlib.Path("build/manifest.ndjson")
CHUNK_BYTES     = 1_000_000 - 1024       # 1 MB minus 1 KB safety margin

# ── helper – deterministic merged row stream ────────────────────────────
def stream_rows():
    conn = psycopg2.connect(dsn=DB_DSN, cursor_factory=DictCursor)
    cur  = conn.cursor(name="stream")    # server-side cursor
    cur.itersize = 10_000

    # UNION events + storage_logs, deriving tx_index_in_block for logs -- we are looking only at 2 tables for this one
    #TODO: probably add the other two tables as well ?
    cur.execute("""
        /* Events come first */
        SELECT  e.miniblock_number,
                e.tx_index_in_block,
                e.event_index_in_block        AS local_idx,
                'events'                      AS tbl,
                row_to_json(e)::text          AS payload
        FROM    public.events e

        UNION ALL

        /* Storage_logs for the same tx, ordered by operation_number */
        SELECT  s.miniblock_number,
                COALESCE(ev.tx_index_in_block, 0)  AS tx_index_in_block,
                s.operation_number           AS local_idx,
                'storage_logs',
                row_to_json(s)::text
        FROM    public.storage_logs s
        /* look up the tx_index_in_block once per row */
        LEFT JOIN LATERAL (
            SELECT tx_index_in_block
            FROM   public.events e
            WHERE  e.tx_hash = s.tx_hash
            LIMIT  1
        ) ev ON TRUE

        ORDER BY miniblock_number,
                 tx_index_in_block,
                 tbl,            -- events before storage_logs
                 local_idx
    """)

    for row in cur:
        yield f"{row['tbl']}:{row['payload']}".encode() + b"\n"

    cur.close(); conn.close()

# ── main build loop ───────────────────────────────────────────────────────
def main():
    man = MANIFEST_PATH.open("w", encoding="utf-8")
    cid, buf = 1, bytearray()

    for line in stream_rows():
        if len(buf) + len(line) > CHUNK_BYTES:
            write_chunk(cid, buf, man)
            cid, buf = cid + 1, bytearray()
        buf.extend(line)

    if buf:
        write_chunk(cid, buf, man)
    man.close()
    print(f"Finished – manifest at {MANIFEST_PATH}")

def write_chunk(cid: int, data: bytes, man_file):
    sha = hashlib.sha256(data).hexdigest()
    fname = OUT_DIR / f"{cid:07}.bin"
    fname.write_bytes(data)

    man_file.write(json.dumps({
        "chunk_id": cid,
        "sha256":   sha,
        "bytes":    len(data),
        "path":     str(fname)
    }) + "\n")
    print(f"chunk {cid:07}  {len(data):7,d} B  {sha[:12]}…")

# ── entry ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
