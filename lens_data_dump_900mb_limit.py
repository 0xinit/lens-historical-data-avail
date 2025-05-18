#!/usr/bin/env python3
"""
Upload zkSync Lens data to TurboDA in ≤1 MB blobs, stopping at 900 MB.

Each blob contains:
  • events rows for one L2 tx
  • storage_logs rows for the same tx
If either table would exceed the 1 MB limit, it’s split into -part1/-part2/etc.

Prereqs
-------
$ pip install python-dotenv psycopg2-binary requests
.env file in the same folder:

DB_DSN=dbname=lens_raw user=postgres password=YourPGPwd host=localhost port=5432
TURBODA_ENDPOINT=https://staging.turbo-api.availproject.org/v1/submit_raw_data
TURBODA_KEY=pa_your-real-key-here
"""

import os, time, logging, requests, psycopg2
from psycopg2.extras import NamedTupleCursor
from requests.adapters import HTTPAdapter, Retry
from dotenv import load_dotenv; load_dotenv()


# ── configs ────────────────────────────────────────────────────────
DB_DSN           = os.getenv("DB_DSN", "dbname=lens_raw user=postgres host=localhost")
TURBODA_ENDPOINT = os.getenv("TURBODA_ENDPOINT")
TURBODA_KEY      = os.getenv("TURBODA_KEY")

TABLES           = ("events", "storage_logs")   # tx-scoped tables only
MAX_BLOB_BYTES   = 1_000_000                    # hard limit per TurboDA call
SAFETY_MARGIN    = 1_024                        # leave 1 KB head-room
MAX_PAYLOAD      = MAX_BLOB_BYTES - SAFETY_MARGIN
CHUNK_BYTES      = MAX_PAYLOAD                  # builder threshold
MAX_TOTAL_BYTES  = 900_000_000                  # 900 MB quota for testing purposes only, should be removed later

# ── logging ──────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("loader")


# ── TurboDA session ──────────────────────────────────────────────────────
def make_session(retries: int = 5) -> requests.Session:
    if not (TURBODA_ENDPOINT and TURBODA_KEY):
        raise RuntimeError("Set TURBODA_ENDPOINT and TURBODA_KEY in env/.env")
    s = requests.Session()
    rt = Retry(total=retries, backoff_factor=0.5,
               status_forcelist=(429, 500, 502, 503, 504),
               allowed_methods=frozenset(["POST"]))
    s.mount("https://", HTTPAdapter(max_retries=rt))
    s.headers.update({"x-api-key": TURBODA_KEY,
                      "Content-Type": "application/octet-stream"})
    return s

def submit_blob(sess: requests.Session, payload: bytes) -> str:
    if len(payload) >= MAX_BLOB_BYTES:
        raise ValueError("payload too large for TurboDA (internal bug)")
    r = sess.post(TURBODA_ENDPOINT, data=payload, timeout=30)
    r.raise_for_status()
    return r.json().get("submission_id", "<unknown>")

# ── helpers ──────────────────────────────────────────────────────────────
def fetch_tx_list(cur) -> None:
    cur.execute("""
        SELECT DISTINCT
               miniblock_number  AS blk,
               tx_index_in_block AS idx,
               tx_hash
        FROM   public.events
        ORDER  BY blk, idx
    """)
    for row in cur:
        yield row.blk, row.idx, row.tx_hash

def fetch_rows_for_tx(cur, blk, idx, tx_hash):
    """Yield (table_tag, json_text) for all rows of this transaction."""
    # events
    cur.execute(
        """SELECT row_to_json(t)::text
             FROM public.events t
            WHERE miniblock_number = %s
              AND tx_index_in_block = %s
         ORDER BY event_index_in_block""",
        (blk, idx))
    for (txt,) in cur:
        yield "events", txt

    # storage_logs
    cur.execute(
        """SELECT row_to_json(t)::text
             FROM public.storage_logs t
            WHERE tx_hash = %s
         ORDER BY operation_number""",
        (tx_hash,))
    for (txt,) in cur:
        yield "storage_logs", txt

# ── main ─────────────────────────────────────────────────────────────────
def main() -> None:
    conn      = psycopg2.connect(dsn=DB_DSN, cursor_factory=NamedTupleCursor)
    tx_cur    = conn.cursor()
    data_cur  = conn.cursor()
    sess      = make_session()

    buffer, buf_size     = [], 0
    blobs, bytes_sent    = 0, 0
    tx_count             = 0
    t0                   = time.time()

    for blk, idx, tx_hash in fetch_tx_list(tx_cur):
        if bytes_sent >= MAX_TOTAL_BYTES:
            log.info("quota hit (%d bytes) – stopping", bytes_sent)
            break

        # ── collect & possibly split rows for this tx ──────────────────
        parts, part_buf, part_size, part_num = [], [], 0, 1
        for tbl, row_json in fetch_rows_for_tx(data_cur, blk, idx, tx_hash):
            line = f"{tbl}:{row_json}"
            line_bytes = len(line.encode())

            # flush current part if adding line would exceed MAX_PAYLOAD
            if part_size + line_bytes > MAX_PAYLOAD and part_buf:
                label = f"{tbl}-part{part_num}" if part_num > 1 else tbl
                parts.append((label, "\n".join(part_buf)))
                part_buf, part_size, part_num = [], 0, part_num + 1

            part_buf.append(line)
            part_size += line_bytes

        # final piece for this table
        if part_buf:
            label = f"{tbl}-part{part_num}" if part_num > 1 else tbl
            parts.append((label, "\n".join(part_buf)))

        # ── build tx blob from parts ───────────────────────────────────
        tx_blob = "\n".join(f"{label}:\n{payload}" for label, payload in parts)
        tx_bytes = len(tx_blob.encode())

        # impossible edge-case: single row > 1 MB
        if tx_bytes > MAX_PAYLOAD:
            log.warning("tx blk=%d idx=%d is %d bytes – skipping",
                        blk, idx, tx_bytes)
            continue

        # flush buffer if adding this tx would exceed 1 MB
        if buf_size + tx_bytes > MAX_PAYLOAD and buffer:
            blob_bytes = "\n".join(buffer).encode()
            if bytes_sent + len(blob_bytes) > MAX_TOTAL_BYTES:
                log.info("next blob would exceed quota – stopping")
                break
            sid = submit_blob(sess, blob_bytes)
            blobs += 1
            bytes_sent += len(blob_bytes)
            log.info("blob %-5d %7d bytes (total %d MB) – id=%s",
                     blobs, len(blob_bytes), bytes_sent // 1_000_000, sid)
            buffer, buf_size = [], 0

        buffer.append(tx_blob)
        buf_size += tx_bytes
        tx_count += 1

        if tx_count % 1_000 == 0:
            rate = tx_count / (time.time() - t0)
            log.info("progress: %d tx (latest blk=%d idx=%d, %.1f tx/s)",
                     tx_count, blk, idx, rate)

    # flush remainder
    if buffer and bytes_sent < MAX_TOTAL_BYTES:
        blob_bytes = "\n".join(buffer).encode()
        if bytes_sent + len(blob_bytes) <= MAX_TOTAL_BYTES:
            sid = submit_blob(sess, blob_bytes)
            blobs += 1
            bytes_sent += len(blob_bytes)
            log.info("blob %-5d %7d bytes (total %d MB) – id=%s",
                     blobs, len(blob_bytes), bytes_sent // 1_000_000, sid)

    log.info("DONE – %d blobs, %d tx, %.1f s elapsed",
             blobs, tx_count, time.time() - t0)

    tx_cur.close(); data_cur.close(); conn.close()

# ── entrypoint ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()