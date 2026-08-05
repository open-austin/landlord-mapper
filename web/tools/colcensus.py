#!/usr/bin/env python3
"""Per-column storage / type census for a table in the landlord-mapper db.

Usage: colcensus.py <db-path> <table> [more tables...]
Prints, per column: serialized byte total, distinct count, typeof() histogram,
NULL count, empty-string count.  Read-only (opens the db with mode=ro).
"""
import sqlite3
import sys
import time


def census(conn, table):
    cols = [r[1] for r in conn.execute("pragma table_info(%s)" % table)]
    n = conn.execute("select count(*) from %s" % table).fetchone()[0]
    print("== %s: %d rows, %d columns" % (table, n, len(cols)))
    parts = []
    for c in cols:
        q = '"%s"' % c
        parts.append(
            "sum(length(cast(%s as blob)))," % q
            + "count(distinct %s)," % q
            + "sum(typeof(%s)='text')," % q
            + "sum(typeof(%s)='integer')," % q
            + "sum(typeof(%s)='real')," % q
            + "sum(%s is null)," % q
            + "sum(%s='')" % q
        )
    t0 = time.time()
    row = conn.execute("select " + ",".join(parts) + " from %s" % table).fetchone()
    print("   scan %.1fs" % (time.time() - t0))
    hdr = ("column", "MB", "distinct", "text", "int", "real", "null", "empty")
    print("   %-26s%9s%11s%10s%10s%8s%9s%9s" % hdr)
    total = 0.0
    for i, c in enumerate(cols):
        b, d, tt, ii, rr, nn, ee = row[i * 7:i * 7 + 7]
        total += (b or 0)
        print("   %-26s%9.1f%11d%10d%10d%8d%9d%9d" % (
            c, (b or 0) / 1048576.0, d, tt or 0, ii or 0, rr or 0, nn or 0, ee or 0))
    print("   %-26s%9.1f  (payload only, excludes headers/overhead)"
          % ("TOTAL", total / 1048576.0))


def main():
    db = sys.argv[1]
    conn = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
    for table in sys.argv[2:]:
        census(conn, table)


main()
