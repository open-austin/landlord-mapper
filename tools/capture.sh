#!/bin/bash
# Capture every route in routes.txt from one server instance into a directory of
# response files, one file per route, plus a manifest of sha256 + byte count.
#
#   capture.sh <port> <out-dir> <routes-file>
#
# Bodies are kept verbatim (no text mangling) because the whole point is byte
# identity.  Response headers are deliberately NOT captured: Date and
# Content-Length are either clock-dependent or a function of the body.
set -u
PORT="$1"
OUT="$2"
ROUTES="$3"
mkdir -p "$OUT/body"
: > "$OUT/manifest.txt"
n=0
while IFS= read -r route; do
    [ -z "$route" ] && continue
    n=$((n + 1))
    f=$(printf "%03d" "$n")
    code=$(curl -sS -o "$OUT/body/$f" -w "%{http_code}" \
        --max-time 900 "http://127.0.0.1:$PORT$route")
    sum=$(sha256sum < "$OUT/body/$f" | cut -d' ' -f1)
    bytes=$(wc -c < "$OUT/body/$f")
    printf "%s\t%s\t%s\t%s\t%s\n" "$f" "$code" "$bytes" "$sum" "$route" \
        >> "$OUT/manifest.txt"
done < "$ROUTES"
echo "captured $n routes from port $PORT into $OUT"
