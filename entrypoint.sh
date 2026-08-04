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
# Two supported ways to get the database onto the volume. Both land it at $DB
# atomically, so a container that dies mid-seed leaves no half-written database
# for the next boot to open.
#
#   1. A compressed seed shipped in the image at /seed/lm.sqlite3.<ext>. Simple
#      and self-healing, but it makes every code deploy re-upload the archive.
#   2. seed-volume.sh, which streams the database in over `railway ssh` once.
#      Keeps the image small. This is the default; /seed is normally absent.
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

# Fail loudly and immediately rather than letting the server start and 500 every
# request. A missing database here means the volume was never seeded, which is an
# operator action, so say exactly what to run.
if [ ! -f "$DB" ]; then
  echo "FATAL: no database at $DB" >&2
  echo "The volume is empty. Seed it once from the machine holding the built" >&2
  echo "database:  ./seed-volume.sh /path/to/lm.sqlite3" >&2
  exit 1
fi

echo "starting server: LM_PORT=$LM_PORT LM_DB=$DB ($(wc -c < "$DB") bytes)"
exec python3 server.py
