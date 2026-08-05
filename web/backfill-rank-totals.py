#!/usr/bin/env python3
"""Add the precomputed ranking totals to an ALREADY BUILT database.

Why this exists as a separate script
------------------------------------
build-db.py now writes `rank_totals_in_scope` into `meta`, so any database built
from here on carries it. The database on the Railway volume was built before
that and does not, and it is 1.14 GB -- rebuilding and re-seeding it to add four
integers would mean a gigabyte back through object storage for ~80 bytes of data.

So this backfills the one key in place. It writes exactly what build-db.py's
rank_totals() would have written, computed by the same SQL against the same
file, so the figures cannot differ from a rebuild.

server.py does not need this. Without the key it falls back to computing the
same numbers from the partial ranking indexes, which is correct and about half
the cold I/O; with the key it does no aggregate read at all. This is the
difference between a fast cold /rankings and an instant one.

Usage, on the machine or container that can see the file:

    python3 backfill-rank-totals.py /data/lm.sqlite3

It is idempotent -- rerunning it recomputes and rewrites the same row -- and it
is the ONLY write this repo makes to a live database. Stop the server first if
you want to be strict about it; you do not have to, because a reader holding a
shared lock and a one-row write are exactly the case SQLite's locking handles,
but a stopped server is one less thing to reason about.
"""
import json
import os
import sqlite3
import sys

KEY = "rank_totals_in_scope"
SQL = ("SELECT COUNT(*), SUM(n_parcels_scope), SUM(scope_units), "
       "SUM(scope_value) FROM owner WHERE in_scope = 1")


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: backfill-rank-totals.py <path to lm.sqlite3>")
    path = sys.argv[1]
    if not os.path.exists(path):
        raise SystemExit("no such database: %s" % path)

    cx = sqlite3.connect(path, timeout=30)
    # This is the scan the server is being relieved of. It is slow on a cold
    # volume -- that is the entire point of doing it once, here, instead of on
    # every unfiltered /rankings request.
    tot = [int(x or 0) for x in cx.execute(SQL).fetchone()]

    # Cross-check against the two figures build-db.py counted independently, the
    # same assertion build-db.py's rank_totals() makes. A silent disagreement
    # would put a denominator on /rankings that contradicts /health.
    have = {}
    for k, v in cx.execute("SELECT k, v FROM meta WHERE k IN "
                           "('owners_in_scope','parcels_in_scope')"):
        have[k] = json.loads(v)
    for name, mine in (("owners_in_scope", tot[0]),
                       ("parcels_in_scope", tot[1])):
        theirs = have.get(name)
        if theirs is not None and int(theirs) != mine:
            raise SystemExit(
                "refusing to write: %s is %r in meta but %r summed off the "
                "owner table" % (name, theirs, mine))

    before = cx.execute("SELECT v FROM meta WHERE k = ?", (KEY,)).fetchone()
    cx.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
               (KEY, json.dumps(tot)))
    cx.commit()
    cx.close()

    sys.stderr.write(
        "%s: %s = %s%s\n"
        % (path, KEY, json.dumps(tot),
           "" if before is None else " (was %s)" % before[0]))
    sys.stderr.write(
        "owners %s, in-scope parcels %s, units %s, value %s\n"
        % tuple(format(x, ",d") for x in tot))
    sys.stderr.write("restart the service so server.py reads the new meta row\n")


main()
