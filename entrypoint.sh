#!/bin/sh
# Container entrypoint. Three jobs, in order: translate Railway's port into the
# variable server.py reads, seed the volume if it is empty, then exec the server.
set -eu

# --- port -------------------------------------------------------------------
# Railway injects $PORT and routes public traffic to it. server.py reads
# $LM_PORT and binds 0.0.0.0. Bridge the two rather than teaching the app about
# Railway, so the same server.py runs unchanged on the box.
export LM_PORT="${PORT:-8080}"

DB="${LM_DB:-/data/lm.sqlite3}"
DB_DIR=$(dirname "$DB")
mkdir -p "$DB_DIR"

# --- seed -------------------------------------------------------------------
# The database arrives as a compressed archive shipped in the image at
# /seed/lm.sqlite3.<ext>, driven by seed-image.sh: it ships the archive for
# exactly one deploy, then removes it and redeploys so later code deploys stay
# small. Volumes persist, so in the steady state /seed is absent and this whole
# block is skipped.
#
# Decompress to .building and rename, so a container that dies mid-seed leaves no
# half-written database for the next boot to open.
#
# Streaming the database in over `railway ssh` is NOT a supported path, and that
# is measured rather than assumed: 8 MB of piped stdin had not completed after
# five minutes, and railway ssh intermittently drops the first line of stdout.
STAGE="$DB_DIR/.seedparts"

# --- seed from a URL --------------------------------------------------------
# The preferred route, and the only one that scales. $LM_SEED_URL points at the
# gzipped database in object storage (a Railway bucket, via a presigned GET so no
# credentials live in the container). The container pulls it directly, so the
# archive never has to pass through a code upload and the ~100 MB deploy cap
# stops mattering.
#
# Uses python3 rather than curl: python3 is guaranteed present (it is the app),
# curl is not in python:3.12-slim, and stdlib urllib streams and decompresses in
# one pass without buffering 1.8 GB in memory.
if [ ! -f "$DB" ] && [ -n "${LM_SEED_URL:-}" ]; then
  echo "seeding $DB from LM_SEED_URL"
  if python3 - "$LM_SEED_URL" "$DB.building" <<'PY'
import gzip, shutil, sys, urllib.request
url, out = sys.argv[1], sys.argv[2]
req = urllib.request.Request(url, headers={"User-Agent": "landlord-mapper-seed"})
with urllib.request.urlopen(req) as r:
    total = r.headers.get("Content-Length")
    print("  fetching %s bytes" % (total or "unknown"), flush=True)
    # Decompress while streaming so peak memory is a buffer, not the database.
    with gzip.GzipFile(fileobj=r) as gz, open(out, "wb") as fh:
        shutil.copyfileobj(gz, fh, 1 << 22)
print("  wrote %s bytes" % __import__("os").path.getsize(out), flush=True)
PY
  then
    mv "$DB.building" "$DB"
    echo "seeded $DB ($(wc -c < "$DB") bytes)"
  else
    # Leave .building behind deliberately? No: a partial file is worse than none,
    # because a later boot could find a truncated database and serve errors.
    echo "seed from LM_SEED_URL FAILED; removing partial file" >&2
    rm -f "$DB.building"
  fi
fi

if [ ! -f "$DB" ]; then
  # Whole-archive seed, when the compressed database fits one deploy.
  for candidate in /seed/lm.sqlite3.zst /seed/lm.sqlite3.xz /seed/lm.sqlite3.gz /seed/lm.sqlite3; do
    [ -f "$candidate" ] || continue
    echo "seeding $DB from $candidate"
    case "$candidate" in
      *.zst) zstd -dc "$candidate" > "$DB.building" ;;
      *.xz)  xz -dc   "$candidate" > "$DB.building" ;;
      *.gz)  gzip -dc "$candidate" > "$DB.building" ;;
      *)     cp       "$candidate"   "$DB.building" ;;
    esac
    mv "$DB.building" "$DB"
    echo "seeded $DB ($(wc -c < "$DB") bytes)"
    break
  done
fi

# --- chunked seed -----------------------------------------------------------
# Railway's code upload is capped: a 499,756,144-byte context was rejected with
# HTTP 413 from Cloudflare. The gzipped database is 518,716,849 bytes, so it
# cannot arrive in one deploy at all.
#
# So it arrives in pieces. Each deploy carries ONE part, this block copies that
# part onto the VOLUME (which persists across deploys, unlike the image), and
# once every part has landed it concatenates and decompresses them. /seed/parts.total
# says how many to expect, so a partial set waits instead of assembling garbage.
if [ ! -f "$DB" ] && [ -d /seed ]; then
  mkdir -p "$STAGE"
  # -n so redeploying a part already staged is a no-op rather than a rewrite.
  for p in /seed/lm.sqlite3.gz.part-*; do
    [ -f "$p" ] || continue
    cp -n "$p" "$STAGE/" 2>/dev/null || true
  done
  [ -f /seed/parts.total ] && cp -f /seed/parts.total "$STAGE/parts.total"

  if [ -f "$STAGE/parts.total" ]; then
    want=$(cat "$STAGE/parts.total")
    have=$(find "$STAGE" -name 'lm.sqlite3.gz.part-*' | wc -l)
    echo "seed parts on volume: $have of $want"
    if [ "$have" -eq "$want" ] && [ "$want" -gt 0 ]; then
      echo "all parts present, assembling"
      # Concatenated gzip members decompress as one stream, and `split` produces
      # byte-exact pieces, so cat-then-gunzip reproduces the original archive.
      # Sorted glob expansion keeps part order; that is what makes this correct.
      cat "$STAGE"/lm.sqlite3.gz.part-* | gzip -dc > "$DB.building"
      mv "$DB.building" "$DB"
      echo "seeded $DB ($(wc -c < "$DB") bytes)"
      # Reclaim the staged parts: they are a full second copy of the archive.
      rm -rf "$STAGE"
    fi
  fi
fi

# Wait rather than exit when the volume is empty.
#
# Exiting here looks like the responsible choice and is actually a deadlock: the
# only way to get the database onto the volume is `railway ssh`, and that needs a
# RUNNING container. An entrypoint that dies on first boot can never be seeded,
# so the service crash-loops until the retry limit and there is no way in.
#
# Waiting breaks the cycle and makes seeding self-completing: deploy, stream the
# database in, and this loop picks it up and starts serving without a restart.
if [ ! -f "$DB" ]; then
  echo "no database at $DB yet -- the volume is unseeded."
  echo "waiting. seed it from the machine holding the built database:"
  echo "    ./seed-image.sh /path/to/lm.sqlite3"
  waited=0
  while [ ! -f "$DB" ]; do
    sleep 15
    waited=$((waited + 15))
    # Only mention it every 5 minutes; a line every 15 s would bury the log.
    if [ $((waited % 300)) -eq 0 ]; then
      echo "still waiting for $DB (${waited}s)"
    fi
  done
  # seed-image.sh renames into place, so the file appearing means it is
  # complete. Guard the streamed-in case anyway, since a future seeding route
  # might not be atomic.
  sleep 2
fi

echo "starting server: LM_PORT=$LM_PORT LM_DB=$DB ($(wc -c < "$DB") bytes)"
exec python3 server.py
