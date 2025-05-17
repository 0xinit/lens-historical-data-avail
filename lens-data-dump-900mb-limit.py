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