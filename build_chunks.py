#!/usr/bin/env python3
"""
step 1 – deterministic 1 MB chunk builder
(events + storage_logs + factory_deps + initial_writes) -> chunks/*.bin + manifest.ndjson
"""

import os, json, hashlib, pathlib, psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv; load_dotenv()

# ── config ─────────────────────────────────────────────────────────────
DB_DSN        = os.getenv("DB_DSN", "dbname=lens_raw user=postgres host=localhost")
OUT_DIR       = pathlib.Path("build/chunks"); OUT_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH = pathlib.Path("build/manifest.ndjson")
CHUNK_BYTES   = 1_000_000 - 1024        # 1 MB – 1 KB safety

# ── helper – deterministic merged row stream ───────────────────────────
def stream_rows():
    conn = psycopg2.connect(dsn=DB_DSN, cursor_factory=DictCursor)
    cur  = conn.cursor(name="stream_all")    # server-side cursor
    cur.itersize = 10_000

    # All four tables in one total order
    cur.execute("""
        /* 0 — events --------------------------------------------------- */
        SELECT  e.miniblock_number,
                e.tx_index_in_block,
                0                      AS priority,
                e.event_index_in_block AS local_idx,
                'events'               AS tbl,
                row_to_json(e)::text   AS payload
        FROM    public.events e

        UNION ALL

        /* 1 — storage_logs -------------------------------------------- */
        SELECT  s.miniblock_number,
                COALESCE(ev.tx_index_in_block, 0),
                1,
                s.operation_number,
                'storage_logs',
                row_to_json(s)::text
        FROM    public.storage_logs s
        LEFT JOIN LATERAL (
            SELECT tx_index_in_block
            FROM   public.events e
            WHERE  e.tx_hash = s.tx_hash
            LIMIT  1
        ) ev ON TRUE

        UNION ALL

        /* 2 — factory_deps (after all tx in same miniblock) ----------- */
        SELECT  fd.miniblock_number,
                2147483647            AS tx_index_in_block,
                2,
                ROW_NUMBER() OVER (PARTITION BY fd.miniblock_number
                                   ORDER BY fd.bytecode_hash) AS local_idx,
                'factory_deps',
                row_to_json(fd)::text
        FROM    public.factory_deps fd

        UNION ALL

        /* 3 — initial_writes (after factory_deps) --------------------- */
        SELECT  iw.l1_batch_number     AS miniblock_number,
                4294967295             AS tx_index_in_block,
                3,
                iw.index,
                'initial_writes',
                row_to_json(iw)::text
        FROM    public.initial_writes iw

        ORDER BY miniblock_number,
                 tx_index_in_block,
                 priority,
                 local_idx;
    """)

    for row in cur:
        yield f"{row['tbl']}:{row['payload']}".encode() + b"\n"

    cur.close(); conn.close()

# ── main build loop ────────────────────────────────────────────────────
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

# ── entry ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
