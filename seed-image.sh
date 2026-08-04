#!/usr/bin/env bash
# Seed the Railway volume by shipping the database into the image ONCE.
#
# WHY NOT `railway ssh`, which is the obvious answer
#
# It does not work for bulk data. Measured on this project: piping 8 MB of
# random bytes into `railway ssh -- sh -c 'cat > /data/f'` had not completed
# after five minutes and produced no output. `railway ssh` also intermittently
# swallows the first line of a command's stdout, so it is not a reliable data
# channel even for small transfers. A ~1 GB database over that path is not a
# slow plan, it is a broken one.
#
# So: ship the database as a compressed file inside the image for exactly one
# deploy. entrypoint.sh finds /seed/lm.sqlite3.gz, decompresses it onto the
# volume atomically, and starts serving. Then delete seed/ and redeploy, so
# every later code deploy uploads a few hundred KB instead of a few hundred MB.
# The database persists on the volume across deploys, so this happens once per
# data refresh, not once per code change.
#
# USAGE
#   ./seed-image.sh /path/to/lm.sqlite3     # step 1: build seed + deploy
#   ./seed-image.sh --finish                # step 2: drop seed + redeploy
set -euo pipefail

SERVICE="${SERVICE:-web}"
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE"

if [[ "${1:-}" == "--finish" ]]; then
  if [[ ! -d seed ]]; then
    echo "no seed/ directory; nothing to finish" >&2
    exit 0
  fi
  echo "confirming the volume actually holds a database before dropping the seed"
  # Dropping the seed while the volume is still empty would leave no way to seed
  # except re-uploading, so check first rather than trusting that step 1 worked.
  railway ssh --service "$SERVICE" -- sh -c 'wc -c < /data/lm.sqlite3' \
    || { echo "could not confirm /data/lm.sqlite3; keeping seed/" >&2; exit 1; }
  rm -rf seed
  echo "seed/ removed. redeploying without it"
  railway up --service "$SERVICE" --ci
  exit 0
fi

DB="${1:-}"
if [[ -z "$DB" || ! -f "$DB" ]]; then
  echo "usage: $0 /path/to/lm.sqlite3   (or --finish)" >&2
  exit 2
fi

# Verify BEFORE spending upload time. quick_check catches the realistic failure
# here, which is a truncated or half-written file.
echo "checking $DB"
python -c "
import sqlite3, sys
con = sqlite3.connect('file:$DB?mode=ro', uri=True)
ok = con.execute('PRAGMA quick_check').fetchone()[0]
if ok != 'ok':
    sys.exit('quick_check failed: %s' % ok)
print('  quick_check ok, %d parcel rows' %
      con.execute('SELECT COUNT(*) FROM parcel').fetchone()[0])
"

mkdir -p seed
echo "compressing into seed/lm.sqlite3.gz (this is the slow part)"
# -6 rather than -9: past about -6 the compressor becomes the bottleneck and
# total wall time gets worse, not better. The data is mostly repeated ASCII, so
# even -6 gets most of the available ratio.
gzip -6 -c "$DB" > seed/lm.sqlite3.gz

RAW=$(wc -c < "$DB")
GZ=$(wc -c < seed/lm.sqlite3.gz)
echo "  $RAW bytes -> $GZ bytes ($(python -c "print('%.1f' % ($GZ*100.0/$RAW))")%)"

echo
echo "deploying with the seed included"
railway up --service "$SERVICE" --ci

cat <<'NOTE'

Deploy submitted. Watch the container pick it up:

    railway logs

You are looking for "seeding /data/lm.sqlite3 from /seed/lm.sqlite3.gz" followed
by "starting server". Once it is serving, shrink the image back down:

    ./seed-image.sh --finish
NOTE
