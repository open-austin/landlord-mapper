#!/usr/bin/env python3
"""Prove (or disprove) the redundancies the typed rebuild wants to exploit.

Every question here is "is column A an exact function of column B", because a
typed rebuild may only drop a column whose displayed text can be reconstructed
byte for byte -- /export.csv writes the twenty raw roll columns verbatim.

Read-only.  Usage: equalities.py <db-path>
"""
import sqlite3
import sys

DB = sys.argv[1]
cx = sqlite3.connect("file:%s?mode=ro" % DB, uri=True)
cx.create_function("normtxt", 1, lambda v: " ".join((v or "").upper().split()))
cx.create_function(
    "datestamp", 1,
    lambda v: "" if not (v or "").strip() or (v or "").strip().upper() in ("NA", "NULL")
    else (v or "").strip().split(" ")[0])


def q(label, sql):
    n = cx.execute(sql).fetchone()[0]
    print("%-58s %s" % (label, n))
    return n


print("== parcel: rows where the derived column is NOT the raw one")
q("addr_upper <> situs_address",
  "select count(*) from parcel where addr_upper is not situs_address")
q("addr_upper <> upper(situs_address)",
  "select count(*) from parcel where addr_upper is not upper(situs_address)")
q("owner_name_norm <> owner_name",
  "select count(*) from parcel where owner_name_norm is not owner_name")
q("owner_name_norm <> normtxt(owner_name)",
  "select count(*) from parcel where owner_name_norm is not normtxt(owner_name)")
q("county_norm <> normtxt(county)",
  "select count(*) from parcel where county_norm is not normtxt(county)")
q("zip_trim <> trim(situs_zip)",
  "select count(*) from parcel where zip_trim is not trim(situs_zip)")
q("pdate <> datestamp(recent_purchase_date)",
  "select count(*) from parcel where pdate is not datestamp(recent_purchase_date)")
q("pid_norm <> ltrim(trim(situs_pID),'0') (or '0')",
  "select count(*) from parcel where pid_norm is not "
  "coalesce(nullif(ltrim(trim(situs_pID),'0'),''),'0')")
q("pid_sort <> substr('00000000000000'||pid_norm,-14)",
  "select count(*) from parcel where pid_sort is not "
  "substr('00000000000000'||pid_norm,-14)")

print("== parcel: raw numeric text vs the precomputed integer")
q("totalpropmktvalue <> cast(n_val as text)",
  "select count(*) from parcel where totalpropmktvalue is not cast(n_val as text)")
q("totalsqftlivingarea <> cast(n_sqft as text)",
  "select count(*) from parcel where totalsqftlivingarea is not cast(n_sqft as text)")
q("property_units <> cast(n_units as text)",
  "select count(*) from parcel where property_units is not cast(n_units as text)")
q("year_built <> cast(n_yb as text)  (n_yb=0 means unusable)",
  "select count(*) from parcel where year_built is not cast(n_yb as text)")
q("property_units: to_int==0 but value nonzero (scope_reason risk)",
  "select count(*) from parcel where n_units = 0 and cast(property_units as real) <> 0")

print("== parcel: flag text vs the precomputed bit")
for raw, bit in (("is_owner_out_of_state", "f_oos"), ("is_owner_occupied", "f_occ"),
                 ("is_financialized", "f_fin"), ("is_mom_and_pop", "f_mom")):
    q("%s vs %s disagree" % (raw, bit),
      "select count(*) from parcel where (case when upper(trim(%s)) in "
      "('TRUE','T','1','YES') then 1 else 0 end) <> %s" % (raw, bit))

print("== low-cardinality value sets (exact text, repr'd)")
for col in ("situs_year", "county", "is_owner_out_of_state", "is_owner_occupied",
            "is_financialized", "is_mom_and_pop"):
    vals = [r[0] for r in cx.execute(
        "select distinct %s from parcel order by 1" % col)]
    print("   %-24s %s" % (col, [repr(v) for v in vals]))
print("   %-24s %s" % ("owner.state", [r[0] for r in cx.execute(
    "select distinct state from owner order by 1")]))

print("== owner text vs the parcel row it came from (first_rowid)")
q("owner.name <> parcel.owner_name at first_rowid",
  "select count(*) from owner o join parcel p on p.rowid = o.first_rowid "
  "where o.name is not p.owner_name")
q("owner.address <> parcel.owner_address at first_rowid",
  "select count(*) from owner o join parcel p on p.rowid = o.first_rowid "
  "where o.address is not p.owner_address")
q("parcel.owner_name <> its owner row's name",
  "select count(*) from parcel p join owner o on o.owner_id = p.owner_id "
  "where p.owner_name is not o.name")
q("parcel.owner_address <> its owner row's address",
  "select count(*) from parcel p join owner o on o.owner_id = p.owner_id "
  "where p.owner_address is not o.address")

print("== owner_id shape (candidate for INTEGER storage)")
q("owner_id not 12 lowercase hex chars (parcel)",
  "select count(*) from parcel where owner_id not glob "
  "'[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]"
  "[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]'")
q("owner_id not 12 lowercase hex chars (owner)",
  "select count(*) from owner where owner_id not glob "
  "'[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]"
  "[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]'")
