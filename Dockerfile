# Stdlib-only Python service. No pip install, no requirements.txt, because
# server.py deliberately imports nothing outside the standard library -- that is
# what keeps the image small and the build near-instant.
#
# Pinned to 3.12 to match the machine the database was built and verified on
# (box python 3.12.3). sqlite3 behaviour that this app depends on -- dbstat,
# INDEXED BY, query_only, uri filenames -- is stable across 3.11+, but the
# 86-route output-identity harness was only ever run against 3.12.
FROM python:3.12-slim

# tini reaps zombies and forwards signals, so a Railway redeploy stops the
# server cleanly instead of waiting for a SIGKILL. procps is for `ps` when
# debugging over `railway ssh`; xz-utils decompresses the seed archive.
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini procps xz-utils tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# typed.py and retype.py are required, not optional: build-db.py does
# `import typed` and calls typed.retype() as its last step, so a container-side
# rebuild fails with ImportError without it. server.py imports only stdlib and
# reads the typed schema through SQL expressions, so it does not need them --
# they travel with build-db.py.
COPY server.py build-db.py typed.py retype.py entrypoint.sh ./
RUN chmod +x entrypoint.sh

# The database lives on a Railway volume, NOT in the image. It is ~0.9-1.8 GB,
# it changes on a data refresh rather than a code change, and baking it in would
# make every code deploy re-upload it.
# TZ matters because the pages carry human-readable timestamps ("Data read into
# this page", newest registry answer). A container defaults to UTC, so those
# rendered five hours ahead of the Austin reader they are written for. The data
# was identical either way -- only the label was wrong -- but a wrong clock on a
# page whose whole argument is "check the work yourself" is worth not shipping.
# tzdata is installed above because python:3.12-slim has no zoneinfo database,
# and without it Python falls back to UTC and this setting does nothing.
# MALLOC_ARENA_MAX caps a measured memory leak that is invisible on a small host.
# server.py is a ThreadingMixIn server, so EVERY request runs on a brand-new
# thread, and each thread's first allocation earns it its own 64 MB glibc arena.
# Store.conn() then allocates an 8 MB SQLite page cache inside that arena. The
# thread dies, its memory returns to the arena free list, and the 64 MB heap is
# never handed back to the OS.
#
# The arena ceiling is 8 x CPU count, so it scales with the HOST, not with the
# workload: 8 arenas on a 1-vCPU droplet, but 384 on this container's 48 visible
# cores. Measured on Railway before this line existed -- process heap grew
# 247 -> 267 -> 317 -> 540 MB over 0/12/36/96 requests, roughly 3 MB per request
# with no plateau, and /proc/<pid>/maps showed 26 regions totalling 555 MB with
# several at exactly 64.0 MB. It also cannibalised the useful half of memory: the
# kernel evicted 89 MB of SQLite page cache to feed it, which makes the cold-hit
# latency worse over time.
#
# 2 arenas means a ceiling near 128 MB instead of 24 GB. The tradeoff is more
# malloc contention between concurrent request threads, which is the right trade
# here because this service is I/O bound and measures 0.0013 vCPU average -- there
# is no allocator contention to relieve.
ENV TZ=America/Chicago \
    MALLOC_ARENA_MAX=2 \
    LM_DB=/data/lm.sqlite3 \
    LM_DATA=/data/csv \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["./entrypoint.sh"]
