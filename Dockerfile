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
#
# backfill-rank-totals.py must be listed here even though nothing imports it.
# It is run by hand over `railway ssh` to write the meta.rank_totals key that
# /rankings reads on its fast path, and this COPY is an explicit enumeration with
# no wildcard: leaving it out does not fail the build, it fails the deploy step
# with Errno 2, and /rankings then takes the 13,462-page fallback scan forever
# with nothing logged to say which path is live.
COPY server.py build-db.py typed.py retype.py backfill-rank-totals.py entrypoint.sh ./
RUN chmod +x entrypoint.sh

# brand/ is the Austin DSA logo files and the two chapter brand faces, served by
# server.py at /brand/. Only the DSA skin references them; the field skin never
# requests one, so a missing brand/ would go unnoticed until someone switched
# skins and got an unstyled page. 178 KB, all of it immutably cacheable.
COPY brand/ ./brand/

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
# MALLOC_ARENA_MAX is a secondary bound on heap growth, NOT the fix. Do not read
# it as one: it was shipped on the theory that glibc arena proliferation caused
# the measured leak, and the follow-up measurement disproved that. With the cap
# confirmed active in the running container (MALLOC_ARENA_MAX=2 in
# /proc/<pid>/environ, only two 64 MB regions left in /proc/<pid>/maps) the
# process heap still grew 20.6 -> 102.9 -> 243.4 -> 441.0 MB over 0/12/36/96
# requests -- same ~3-4 MB per request, same absence of a plateau. Arenas were
# where the memory sat, not why it accumulated.
#
# The real mechanism is per-connection allocation churn: ThreadingMixIn spawns a
# thread per TCP connection, and each one built and discarded an 8 MB SQLite page
# cache, fragmenting the heap in a way glibc never trims back.
#
# CAUTION for whoever reads this next: an earlier version of this comment credited
# a connection pool of 8 long-lived Sessions. That pool is NOT what ships. It was
# reverted in ac35848 because it violates sqlite3's same-thread rule -- it tested
# clean locally and then broke every page in production. Do not reintroduce it
# from this comment.
#
# What ships instead (d2f284b) is much smaller: each thread keeps its own
# connection in a threading.local and the page cache is capped at 2 MB
# (cache_size=-2000) rather than 8 MB, which measured 65% off peak memory. That
# shrinks the per-request churn but does not prove it plateaus, so treat
# unbounded growth over a very long-lived container as still open.
#
# The cap stays because a ceiling near 128 MB instead of 24 GB is still worth
# having on a 48-visible-core host, and the malloc contention it costs is free
# here -- this service is I/O bound and measures 0.0013 vCPU average.
ENV TZ=America/Chicago \
    MALLOC_ARENA_MAX=2 \
    LM_DB=/data/lm.sqlite3 \
    LM_DATA=/data/csv \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["./entrypoint.sh"]
