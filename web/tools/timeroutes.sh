#!/bin/bash
# Wall-clock timing for the routes that matter, N runs each, reporting the
# median.  Runs against an already-warm server so the number is the query and
# not the first-touch page faults.
#
#   timeroutes.sh <port> [runs]
set -u
PORT="$1"
RUNS="${2:-5}"
ROUTES=(
  "/rankings?rank=value"
  "/rankings?rank=parcels&page=7"
  "/rankings?rank=value&county=travis"
  "/rankings?rank=units&units_min=50"
  "/explore?scope=all&sort=county&dir=asc&page=5"
  "/explore?scope=all&sort=owner&dir=asc&page=6"
  "/explore?scope=all&sort=pid&dir=asc&page=13"
  "/explore?scope=all&sort=acquired&dir=asc&page=4"
  "/explore?scope=all&sort=zip&dir=asc&page=3"
  "/explore?units_min=5&units_max=40&sort=units&dir=desc"
  "/explore?scope=all&county=travis&zip=78704&units_min=2&sort=sqft&dir=desc"
  "/search?q=1201%20S%20LAMAR"
  "/search?q=guadalupe%20st"
  "/search?q=main"
  "/OWNERBUSIEST"
  "/method"
)
BUSY=$(sed -n 's#^/owner/\([0-9a-f]*\)$#\1#p' "$HOME/lm-typed-work/routes.txt" | head -1)
for r in "${ROUTES[@]}"; do
    r="${r/\/OWNERBUSIEST//owner/$BUSY}"
    # warm once, then time RUNS times and take the median
    curl -s -o /dev/null "http://127.0.0.1:$PORT$r"
    ts=()
    for _ in $(seq 1 "$RUNS"); do
        t=$(curl -s -o /dev/null -w "%{time_total}" "http://127.0.0.1:$PORT$r")
        ts+=("$t")
    done
    med=$(printf "%s\n" "${ts[@]}" | sort -g | awk -v n="$RUNS" 'NR==int((n+1)/2)')
    printf "%8.3fs  %s\n" "$med" "$r"
done
