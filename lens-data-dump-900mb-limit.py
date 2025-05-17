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