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
 && apt-get install -y --no-install-recommends tini procps xz-utils \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY server.py build-db.py entrypoint.sh ./
RUN chmod +x entrypoint.sh

# The database lives on a Railway volume, NOT in the image. It is ~0.9-1.8 GB,
# it changes on a data refresh rather than a code change, and baking it in would
# make every code deploy re-upload it.
ENV LM_DB=/data/lm.sqlite3 \
    LM_DATA=/data/csv \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["./entrypoint.sh"]
