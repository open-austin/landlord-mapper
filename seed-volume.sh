#!/usr/bin/env bash
# One-time (and on-refresh) push of the built SQLite database onto the Railway
# volume, streamed and compressed over `railway ssh`.
#
# Why stream rather than bake the database into the image: it is ~0.9-1.8 GB and
# changes on a data refresh, not a code change. Baking it in would re-upload the
# whole thing on every code deploy, and Railway build contexts are not the place
# for a gigabyte of parcel rolls.
#
# Why compress in flight: the database is almost entirely repeated ASCII -- every
# row spells out its county, and money and unit counts are stored as digit text.
# It compresses several-fold, so the wire time is dominated by the compressor,
# not the link.
#
# Atomicity: the remote side writes to .building and renames only after a clean
# decompress. `mv` within one filesystem is atomic, so a connection dropped
# mid-transfer leaves the previous database in place and serving, and the next
# boot never opens a truncated file. This is the same contract build-db.py uses
# locally.
set -euo pipefail

DB="${1:-}"
REMOTE_DB="${REMOTE_DB:-/data/lm.sqlite3}"

if [[ -z "$DB" ]]; then
  echo "usage: $0 /path/to/lm.sqlite3" >&2
  echo "  or:  REMOTE_DB=/data/other.sqlite3 $0 /path/to/lm.sqlite3" >&2
  exit 2
fi
if [[ ! -f "$DB" ]]; then
  echo "no such database: $DB" >&2
  exit 1
fi

# Verify BEFORE spending upload time on a corrupt file. quick_check is the cheap
# variant; it still catches a truncated or partially written database, which is
# the realistic failure here.
echo "checking $DB locally"
python -c "
import sqlite3, sys
con = sqlite3.connect('file:$DB?mode=ro', uri=True)
ok = con.execute('PRAGMA quick_check').fetchone()[0]
if ok != 'ok':
    sys.exit('quick_check failed: %s' % ok)
n = con.execute('SELECT COUNT(*) FROM parcel').fetchone()[0]
print('  quick_check ok, %d parcel rows' % n)
"

BYTES=$(wc -c < "$DB")
echo "streaming $BYTES bytes to $REMOTE_DB (compressed in flight)"

# gzip -6 rather than a stronger setting on purpose: past about -6 the
# compressor becomes the bottleneck and total wall time gets worse, not better.
gzip -6 -c "$DB" | railway ssh -- sh -c "
  set -eu
  mkdir -p \"\$(dirname '$REMOTE_DB')\"
  gzip -dc > '$REMOTE_DB.building'
  mv '$REMOTE_DB.building' '$REMOTE_DB'
  echo \"remote now holds \$(wc -c < '$REMOTE_DB') bytes\"
"

echo
echo "seeded. restart the service so it opens the new file:"
echo "  railway restart"
