#!/usr/bin/env python3
"""Build the fixed route list the byte-identity harness replays.

The data-derived routes (owner ids, parcel ids) are read from the CURRENT
database once and written to routes.txt, so both capture runs replay the exact
same URLs even though one of them is talking to a rebuilt file.

Usage: routes.py <db-path> > routes.txt
"""
import sqlite3
import sys

cx = sqlite3.connect("file:%s?mode=ro" % sys.argv[1], uri=True)
out = []

out += ["/", "/health", "/method", "/favicon.ico", "/nope"]

# rankings: every rank value, then paging and shape variants
for rank in ("parcels", "units", "value"):
    out.append("/rankings?rank=%s" % rank)
    out.append("/rankings?rank=%s&page=2" % rank)
    out.append("/rankings?rank=%s&as=owners" % rank)
out += [
    "/rankings",
    "/rankings?page=7",
    "/rankings?rank=value&county=travis",
    "/rankings?rank=units&units_min=50",
    "/rankings?rank=value&val_min=1000000&val_max=50000000",
    "/rankings?rank=parcels&scope=all",
    "/rankings?rank=value&financialized=1&occupied=0",
]

# explore: every sort key, both directions, plus filter combinations
for s in ("value", "units", "sqft", "year_built", "address", "county", "zip",
          "owner", "pid", "acquired"):
    out.append("/explore?sort=%s&dir=asc" % s)
    out.append("/explore?sort=%s&dir=desc" % s)
out += [
    "/explore",
    "/explore?page=2",
    "/explore?page=13&sort=pid",
    "/explore?scope=all",
    "/explore?scope=all&sort=county&dir=asc&page=5",
    "/explore?scope=all&sort=zip&dir=asc&page=3",
    "/explore?scope=all&sort=acquired&dir=asc&page=4",
    "/explore?scope=all&sort=owner&dir=asc&page=6",
    "/explore?scope=all&sort=address&dir=desc&page=2",
    "/explore?scope=all&sort=year_built&dir=asc",
    "/explore?scope=all&sort=sqft&dir=asc&page=2",
    "/explore?scope=all&sort=units&dir=asc&page=9",
]

counties = [r[0] for r in cx.execute(
    "select distinct county from parcel order by 1")]
for c in counties:
    out.append("/explore?scope=all&county=%s" % c.lower())
out.append("/explore?county=%s" % "&county=".join(c.lower() for c in counties[:3]))

zips = [r[0] for r in cx.execute(
    "select zip_trim from parcel where in_scope = 1 and zip_trim <> '' "
    "group by zip_trim order by count(*) desc limit 4")]
for z in zips:
    out.append("/explore?zip=%s" % z)
out.append("/explore?zip=%s&sort=value&dir=asc" % "&zip=".join(zips))
out += [
    "/explore?units_min=5&units_max=40&sort=units&dir=desc",
    "/explore?yb_min=1960&yb_max=1979&sort=year_built",
    "/explore?val_min=250000&val_max=900000&sort=value&dir=asc",
    "/explore?out_of_state=1&sort=value&dir=desc",
    "/explore?out_of_state=0&occupied=1&scope=all",
    "/explore?mom_and_pop=1&scope=all&sort=owner",
    "/explore?financialized=0&scope=all&page=3",
    "/explore?scope=all&county=travis&zip=78704&units_min=2&sort=sqft&dir=desc",
]

# parcels: one per county roll, plus a shared id that lands on the chooser,
# plus the bare-id form and a miss
for c in counties:
    pid = cx.execute(
        "select situs_pID from parcel where county = ? order by rowid limit 1",
        (c,)).fetchone()[0]
    out.append("/parcel/%s/%s" % (c.lower(), pid.strip()))
shared = cx.execute(
    "select pid_norm from parcel group by pid_norm having count(distinct county_norm) > 3 "
    "order by count(*) desc limit 1").fetchone()[0]
out.append("/parcel/%s" % shared)
lone = cx.execute(
    "select pid_norm from parcel group by pid_norm having count(*) = 1 "
    "order by rowid limit 1").fetchone()[0]
out.append("/parcel/%s" % lone)
out.append("/parcel/travis/999999999999")
out.append("/parcel/nosuchcounty/1")
big = cx.execute(
    "select county_norm, pid_norm from parcel where in_scope = 1 "
    "order by n_val desc limit 1").fetchone()
out.append("/parcel/%s/%s" % (big[0].lower(), big[1]))

# owners: the busiest by each metric, an unmatched one, a single-parcel one
seen = []
for metric in ("n_parcels_scope", "scope_units", "scope_value", "n_parcels",
               "tot_value"):
    for r in cx.execute("select owner_id from owner order by %s desc limit 3"
                        % metric):
        if r[0] not in seen:
            seen.append(r[0])
for r in cx.execute(
        "select owner_id from owner where state = 'no_record' order by n_parcels desc "
        "limit 2"):
    seen.append(r[0])
for r in cx.execute(
        "select owner_id from owner where n_parcels = 1 order by owner_id limit 2"):
    seen.append(r[0])
for oid in seen:
    out.append("/owner/%s" % oid)
out.append("/owner/%s?page=2" % seen[0])
out.append("/owner/%s?sort=value&dir=asc" % seen[0])
out.append("/owner/%s?sort=address" % seen[0])
out.append("/owner/000000000000")

# search: short, mid, long, no-match, punctuation, numeric
addr = cx.execute(
    "select addr_upper from parcel where length(addr_upper) > 24 order by rowid "
    "limit 1").fetchone()[0]
for q in ("a", "st", "main", "congress", "lamar", "1 e", "guadalupe st",
          addr, addr[:18], "zzzzzznotathing", "78704", "%", "_", "o'connor"):
    out.append("/search?q=%s" % q.replace("%", "%25").replace("&", "%26")
               .replace("#", "%23").replace("+", "%2B").replace(" ", "%20"))
out.append("/search?q=main&page=2")
out.append("/search?q=")

# exports
out += [
    "/export.csv",
    "/export.csv?as=owners",
    "/export.csv?as=owners&rank=units",
    "/export.csv?county=travis&sort=value&dir=desc",
    "/export.csv?scope=all&county=%s" % counties[-1].lower(),
    "/export.csv?units_min=20&sort=units&dir=desc",
    "/export.csv?owner=%s" % seen[0],
    "/export.csv?owner=%s" % seen[3],
    "/export.csv?owner=000000000000",
]

for u in out:
    print(u)
