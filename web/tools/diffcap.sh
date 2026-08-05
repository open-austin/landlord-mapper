#!/bin/bash
# Compare two capture directories route by route.
#
#   diffcap.sh <dir-a> <dir-b>
#
# Prints one line per route whose status, byte count or sha256 differs, then a
# count.  Nothing is "explained away" here: this is the raw list.
set -u
A="$1"
B="$2"
n=0
while IFS=$'\t' read -r f code bytes sum route; do
    line=$(awk -F'\t' -v k="$f" '$1==k' "$B/manifest.txt")
    bcode=$(echo "$line" | cut -f2)
    bbytes=$(echo "$line" | cut -f3)
    bsum=$(echo "$line" | cut -f4)
    if [ "$code" != "$bcode" ] || [ "$bytes" != "$bbytes" ] || [ "$sum" != "$bsum" ]; then
        n=$((n + 1))
        printf "DIFF %s  %s->%s  %sB->%sB  %s\n" "$f" "$code" "$bcode" \
            "$bytes" "$bbytes" "$route"
    fi
done < "$A/manifest.txt"
echo "---- $n differing routes out of $(wc -l < "$A/manifest.txt")  ($A vs $B)"
