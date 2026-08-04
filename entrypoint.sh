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
if [ ! -f "$DB" ]; then
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
