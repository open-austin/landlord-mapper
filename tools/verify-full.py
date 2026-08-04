#!/usr/bin/env python3
"""Whole-table proof that the typed database reconstructs the untyped one.

The route harness only exercises the rows the pages and the capped /export.csv
actually reach.  This compares EVERY row of the typed database against the
untyped one, column by column, for the twenty roll columns the server hands to
its formatters plus every derived column the SQL orders or filters on.

    verify-full.py <untyped.sqlite3> <typed.sqlite3>

Both are opened read-only.  Exit status is nonzero if anything at all differs.
"""
import sqlite3
import sys
import time

OLD, NEW = sys.argv[1], sys.argv[2]

cx = sqlite3.connect("file:%s?mode=ro" % NEW, uri=True)
cx.execute("PRAGMA cache_size = -1000000")
cx.execute("ATTACH DATABASE 'file:%s?mode=ro' AS old" % OLD)

# the twenty roll columns, as the typed database reconstructs them
NEWP = """
  (SELECT t FROM d_situs_year WHERE c = p.situs_year),
  p.situs_pID, p.situs_address,
  (SELECT t FROM d_zip WHERE c = p.situs_zip),
  p.totalsqftlivingarea, p.property_units,
  (SELECT t FROM d_year_built WHERE c = p.year_built),
  (SELECT t FROM d_state_code WHERE c = p.state_code),
  (SELECT t FROM d_bool3 WHERE c = p.is_owner_out_of_state),
  (SELECT t FROM d_bool3 WHERE c = p.is_owner_occupied),
  (SELECT t FROM d_bool3 WHERE c = p.is_financialized),
  (SELECT t FROM d_bool3 WHERE c = p.is_mom_and_pop),
  p.legallocationdesc,
  COALESCE(p.owner_name_x, o.name),
  o.address,
  (SELECT t FROM d_owner_zip WHERE c = p.owner_zip),
  (SELECT t FROM d_agent WHERE c = p.agent_name),
  (SELECT t FROM d_rpd WHERE c = p.recent_purchase_date),
  COALESCE(p.totalpropmktvalue_x, CAST(p.n_val AS TEXT)),
  (SELECT t FROM d_county WHERE c = p.county),
  -- derived columns the SQL uses
  p.pid_norm,
  (SELECT n FROM d_county WHERE c = p.county),
  p.situs_address,
  (SELECT n FROM d_zip WHERE c = p.situs_zip),
  (SELECT t FROM d_pdate WHERE c = p.pdate),
  p.owner_id, p.in_scope, p.f_oos, p.f_occ, p.f_fin, p.f_mom,
  p.n_val, p.n_units, p.n_sqft, p.n_yb
"""
OLDP = """
  situs_year, situs_pID, situs_address, situs_zip, totalsqftlivingarea,
  property_units, year_built, state_code, is_owner_out_of_state,
  is_owner_occupied, is_financialized, is_mom_and_pop, legallocationdesc,
  owner_name, owner_address, owner_zip, agent_name, recent_purchase_date,
  totalpropmktvalue, county,
  pid_norm, county_norm, addr_upper, zip_trim, pdate,
  owner_id, in_scope, f_oos, f_occ, f_fin, f_mom,
  n_val, n_units, n_sqft, n_yb
"""
NAMES = [x.strip() for x in OLDP.replace("\n", " ").split(",") if x.strip()]

failures = 0


def stream(sql):
    cur = cx.execute(sql)
    while True:
        rows = cur.fetchmany(20000)
        if not rows:
            return
        for r in rows:
            yield r


print("== parcel: %d columns over every row, in rowid order" % len(NAMES))
t0 = time.time()
a = stream("SELECT p.rowid, " + NEWP + " FROM parcel p "
           "JOIN owner o ON o.owner_id = p.owner_id ORDER BY p.rowid")
b = stream("SELECT rowid, " + OLDP + " FROM old.parcel ORDER BY rowid")
n = 0
bad = {}
for ra, rb in zip(a, b):
    n += 1
    if ra != rb:
        if ra[0] != rb[0]:
            print("   ROWID MISMATCH at row %d: %r vs %r" % (n, ra[0], rb[0]))
            failures += 1
            break
        for i in range(1, len(ra)):
            if ra[i] != rb[i]:
                bad[NAMES[i - 1]] = bad.get(NAMES[i - 1], 0) + 1
                if bad[NAMES[i - 1]] == 1:
                    print("   first mismatch %s at rowid %d: typed=%r old=%r"
                          % (NAMES[i - 1], ra[0], ra[i], rb[i]))
print("   compared %d rows in %.1fs" % (n, time.time() - t0))
if bad:
    failures += 1
    for k, v in sorted(bad.items(), key=lambda kv: -kv[1]):
        print("   MISMATCH %-24s %d rows" % (k, v))
else:
    print("   all %d parcel rows identical on all %d columns" % (n, len(NAMES)))

print("== parcel: the two dense-rank columns are order-and-tie equivalent")
for seq, txt in (("pid_seq", "pid_sort"), ("owner_seq", "owner_name_norm")):
    # Walk the distinct (text, rank) pairs in text order.  If that list is a
    # bijection and the rank is strictly increasing along it, then for every
    # pair of rows "seq_a < seq_b" iff "text_a < text_b" and "seq_a = seq_b" iff
    # "text_a = text_b", which is exactly what ORDER BY ..., rowid needs.  This
    # is the pairwise property without materialising 4.5e12 pairs.
    rows = cx.execute(
        "SELECT o.%s, n.%s FROM old.parcel o JOIN parcel n ON n.rowid = o.rowid "
        "GROUP BY 1, 2 ORDER BY 1" % (txt, seq)).fetchall()
    ok = (len(rows) == len(set(r[0] for r in rows))
          == len(set(r[1] for r in rows))
          and all(rows[i][1] < rows[i + 1][1] for i in range(len(rows) - 1)))
    print("   %-12s %d distinct values, bijective and increasing: %s"
          % (seq, len(rows), ok))
    if not ok:
        failures += 1

print("== owner: every column over every row")
OLDO = ("owner_id, name, address, in_scope, state, n_parcels, tot_value, "
        "tot_sqft, tot_units, median_value, n_out_of_state, n_owner_occupied, "
        "counties_all, zips_all, first_purchase, last_purchase, "
        "n_parcels_scope, scope_units, scope_value, counties_scope, "
        "first_rowid, first_scope_rowid, corp_name, agent")
NEWO = OLDO
for col, d in (("state", "d_ostate"), ("counties_all", "d_counties_all"),
               ("zips_all", "d_zips_all"), ("first_purchase", "d_pdate"),
               ("last_purchase", "d_pdate"), ("counties_scope",
                                              "d_counties_scope")):
    NEWO = NEWO.replace(
        " %s," % col, " (SELECT t FROM %s WHERE c = o.%s)," % (d, col))
onames = [x.strip() for x in OLDO.split(",")]
a = stream("SELECT " + NEWO + " FROM owner o ORDER BY owner_id")
b = stream("SELECT " + OLDO + " FROM old.owner ORDER BY owner_id")
n = 0
bado = {}
for ra, rb in zip(a, b):
    n += 1
    if ra != rb:
        for i in range(len(ra)):
            if ra[i] != rb[i]:
                bado[onames[i]] = bado.get(onames[i], 0) + 1
                if bado[onames[i]] == 1:
                    print("   first mismatch %s at %s: typed=%r old=%r"
                          % (onames[i], ra[0], ra[i], rb[i]))
if bado:
    failures += 1
    for k, v in sorted(bado.items(), key=lambda kv: -kv[1]):
        print("   MISMATCH %-24s %d rows" % (k, v))
else:
    print("   all %d owner rows identical on all %d columns" % (n, len(onames)))

print("== small tables copied verbatim")
for t in ("filing", "officer", "owner_group", "meta"):
    d = cx.execute(
        "SELECT (SELECT COUNT(*) FROM main.%s), (SELECT COUNT(*) FROM old.%s)"
        % (t, t)).fetchone()
    same = cx.execute(
        "SELECT COUNT(*) FROM (SELECT * FROM main.%s EXCEPT SELECT * FROM old.%s)"
        % (t, t)).fetchone()[0]
    print("   %-12s rows %d vs %d, rows only in typed: %d" % (t, d[0], d[1], same))
    if d[0] != d[1] or same:
        failures += 1

print("VERDICT: %s" % ("FAILURES: %d" % failures if failures else "identical"))
raise SystemExit(1 if failures else 0)
