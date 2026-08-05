import os
from lm.schema import PARCEL_COLS

# ---------------------------------------------------------------------------
# store
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# store: a read-only SQLite database
# ---------------------------------------------------------------------------
# The whole roll used to live in Python objects, which cost 3.1 GiB resident and
# 48 s of startup. It now lives in an indexed SQLite file that build-db.py writes
# from the same CSVs, and this process only ever reads it.
#
# The page code above and below this section is deliberately unchanged. It still
# says STORE.parcels[i], STORE.owners[oid], STORE.filings.get(oid), and it still
# gets the same tuples and dicts it always got; the classes here answer those
# reads from the database instead of from RAM. That is what keeps every displayed
# number identical: the formatting functions are untouched and they are handed
# the same values.
#
# rowid is the old in-memory parcel index plus one, by construction in
# build-db.py. Search order, unsorted browse order and every sort tie-break
# inherit that ordering, so it is load-bearing, not incidental.
DB_PATH = os.environ.get(
    "LM_DB", os.path.expanduser("~/landlord-mapper-db/lm.sqlite3"))

# ---------------------------------------------------------------------------
# the typed store: reading the original text back out of typed storage
# ---------------------------------------------------------------------------
# Every column used to be TEXT holding the exact CSV string, which is what made
# the CSV -> SQLite port output-identical. It also cost 1.80 GB, because two
# million rows each spell out "travis" and "TRUE" and their digits in full.
# typed.py now stores the small-value-set columns as INTEGER codes into tiny d_*
# dictionaries, drops the columns that were proven exact functions of another
# column, and keeps only the exceptions where a column was nearly one.
#
# Nothing above or below this section changed shape: PARCEL_SQL still yields the
# twenty roll columns in PARCEL_COLS order and still yields the original strings,
# it just spells each one as the expression that reconstructs it. A scalar
# subquery against a dictionary of at most 34,417 rows is a lookup in a page or
# two that stays in cache.
#
# owner_name and owner_address come off the joined owner row: measured on the
# 1.80 GB file, parcel.owner_address equalled its owner row's address for all
# 2,117,593 rows and parcel.owner_name equalled its owner row's name for all but
# 3,508, which are the only ones stored (in owner_name_x).
def _dict_sql(dict_table, col):
    return "(SELECT t FROM %s WHERE c = %s)" % (dict_table, col)

PARCEL_EXPR = {
    "situs_year": lambda p: _dict_sql("d_situs_year", p + "situs_year"),
    "situs_pID": lambda p: p + "situs_pID",
    "situs_address": lambda p: p + "situs_address",
    "situs_zip": lambda p: _dict_sql("d_zip", p + "situs_zip"),
    "totalsqftlivingarea": lambda p: p + "totalsqftlivingarea",
    "property_units": lambda p: p + "property_units",
    "year_built": lambda p: _dict_sql("d_year_built", p + "year_built"),
    "state_code": lambda p: _dict_sql("d_state_code", p + "state_code"),
    "is_owner_out_of_state":
        lambda p: _dict_sql("d_bool3", p + "is_owner_out_of_state"),
    "is_owner_occupied": lambda p: _dict_sql("d_bool3", p + "is_owner_occupied"),
    "is_financialized": lambda p: _dict_sql("d_bool3", p + "is_financialized"),
    "is_mom_and_pop": lambda p: _dict_sql("d_bool3", p + "is_mom_and_pop"),
    "legallocationdesc": lambda p: p + "legallocationdesc",
    "owner_name": lambda p: "COALESCE(%sowner_name_x, o.name)" % p,
    "owner_address": lambda p: "o.address",
    "owner_zip": lambda p: _dict_sql("d_owner_zip", p + "owner_zip"),
    "agent_name": lambda p: _dict_sql("d_agent", p + "agent_name"),
    "recent_purchase_date":
        lambda p: _dict_sql("d_rpd", p + "recent_purchase_date"),
    "totalpropmktvalue":
        lambda p: "COALESCE(%stotalpropmktvalue_x, CAST(%sn_val AS TEXT))" % (p, p),
    "county": lambda p: _dict_sql("d_county", p + "county"),
}

def parcel_select(prefix="p."):
    """The twenty roll columns, in PARCEL_COLS order, as the original strings.

    Requires the owner row to be joined as `o`, because two of the twenty are
    read off it now."""
    return ", ".join("%s AS %s" % (PARCEL_EXPR[c](prefix), c)
                     for c in PARCEL_COLS)

# `parcel p JOIN owner o` rather than `parcel` alone. The join is on owner's
# INTEGER PRIMARY KEY-shaped text primary key, one B-tree probe per row, and
# every parcel has an owner row by construction (build-db.py builds owners out of
# the parcels; typed.py refuses to run if that is ever untrue).
PARCEL_FROM = "FROM parcel p JOIN owner o ON o.owner_id = p.owner_id "

PARCEL_SQL = parcel_select()

# The owner columns that became dictionary codes. Spelled here once so the four
# hand-rolled owner SELECTs elsewhere can reuse them.
O_STATE = _dict_sql("d_ostate", "o.state")

O_COUNTIES_SCOPE = _dict_sql("d_counties_scope", "o.counties_scope")

OWNER_COLS = (
    "owner_id", "name", "address", "in_scope", "state", "n_parcels",
    "tot_value", "tot_sqft", "tot_units", "median_value", "n_out_of_state",
    "n_owner_occupied", "counties_all", "zips_all", "first_purchase",
    "last_purchase", "n_parcels_scope", "scope_units", "scope_value",
    "counties_scope", "first_rowid", "first_scope_rowid", "corp_name", "agent",
)

# Same idea as PARCEL_EXPR: the columns whose value set is small enough to be
# worth a dictionary are selected back through it, so an owner row still arrives
# as the strings OwnerDict always held.
OWNER_EXPR = {
    "state": _dict_sql("d_ostate", "state"),
    "counties_all": _dict_sql("d_counties_all", "counties_all"),
    "zips_all": _dict_sql("d_zips_all", "zips_all"),
    "first_purchase": _dict_sql("d_pdate", "first_purchase"),
    "last_purchase": _dict_sql("d_pdate", "last_purchase"),
    "counties_scope": _dict_sql("d_counties_scope", "counties_scope"),
}

OWNER_SQL = ", ".join(
    "%s AS %s" % (OWNER_EXPR[c], c) if c in OWNER_EXPR else c
    for c in OWNER_COLS)

FILING_COLS = (
    "owner_id", "corp_name", "ttn", "mail", "mail_norm", "rtt", "formation",
    "sos_status", "sos_date", "file_num", "agent", "agent_norm",
    "queried_rows", "raw_status",
)

FILING_SQL = ", ".join(FILING_COLS)
