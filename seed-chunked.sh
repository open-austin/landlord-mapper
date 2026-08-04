#!/usr/bin/env bash
# Seed the Railway volume with a database too large to upload in one deploy.
#
# WHY THIS EXISTS
#
# Three seeding routes were tried. Two do not work:
#
#   railway ssh piping   -- 8 MB of piped stdin had not completed after five
#                           minutes and produced no output. railway ssh also
#                           intermittently swallows the first line of stdout, so
#                           it is not a dependable data channel at any size.
#   whole archive in the -- Railway's code upload is capped. A 499,756,144-byte
#   image                   context was rejected with HTTP 413 by Cloudflare, and
#                           the gzipped database is 518,716,849 bytes.
#
# What works is shipping the archive in parts, one per deploy, staged onto the
# VOLUME (which persists across deploys) until every part has landed. entrypoint.sh
# then concatenates and decompresses them.
#
# If the database ever compresses to under ~400 MB, prefer seed-image.sh: one
# deploy, no assembly step. This script is for when it does not.
#
# USAGE
#   ./seed-chunked.sh /path/to/lm.sqlite3.gz
set -euo pipefail

SERVICE="${SERVICE:-web}"
# 350 MB parts. The observed 413 was at ~499 MB; 350 leaves room for the rest of
# the build context and for whatever the real limit actually is, which Railway
# does not document precisely.
CHUNK="${CHUNK:-350m}"

HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE"

GZ="${1:-}"
if [[ -z "$GZ" || ! -f "$GZ" ]]; then
  echo "usage: $0 /path/to/lm.sqlite3.gz" >&2
  exit 2
fi

echo "verifying archive"
gzip -t "$GZ"
TOTAL_BYTES=$(wc -c < "$GZ")
echo "  ok, $TOTAL_BYTES bytes"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
split -b "$CHUNK" -d -a 2 "$GZ" "$WORK/lm.sqlite3.gz.part-"
N=$(find "$WORK" -name 'lm.sqlite3.gz.part-*' | wc -l | tr -d ' ')
echo "split into $N parts of up to $CHUNK"

# Prove the split is reversible BEFORE spending N uploads on it. This is cheap
# next to the upload time and it catches a bad split or a truncated part now
# rather than after the last deploy.
echo "verifying the parts reassemble to the same bytes"
SUM_IN=$(python -c "
import hashlib,sys
h=hashlib.sha256()
with open(r'$GZ','rb') as f:
    for b in iter(lambda: f.read(1<<20), b''): h.update(b)
print(h.hexdigest())")
SUM_OUT=$(cat "$WORK"/lm.sqlite3.gz.part-* | python -c "
import hashlib,sys
h=hashlib.sha256()
for b in iter(lambda: sys.stdin.buffer.read(1<<20), b''): h.update(b)
print(h.hexdigest())")
if [[ "$SUM_IN" != "$SUM_OUT" ]]; then
  echo "FAIL: reassembled bytes differ from the archive" >&2
  echo "  archive     $SUM_IN" >&2
  echo "  reassembled $SUM_OUT" >&2
  exit 1
fi
echo "  sha256 matches: $SUM_IN"

# One deploy per part. Only that part is in seed/ each time, so no single upload
# approaches the cap.
i=0
for part in "$WORK"/lm.sqlite3.gz.part-*; do
  i=$((i + 1))
  name=$(basename "$part")
  rm -rf seed && mkdir -p seed
  cp "$part" "seed/$name"
  echo "$N" > seed/parts.total
  echo
  echo "=== deploy $i of $N: $name ($(wc -c < "$part") bytes) ==="
  railway up --service "$SERVICE" --ci || {
    echo "deploy of $name failed. Parts already staged on the volume are kept," >&2
    echo "so rerunning this script resumes rather than starting over." >&2
    exit 1
  }
done

rm -rf seed
cat <<'NOTE'

All parts deployed. The final deploy's container assembles them:

    railway logs

Look for "all parts present, assembling" then "starting server". Then redeploy
once more WITHOUT seed/ so later code deploys stay small:

    railway up --service web --ci
NOTE
