#!/usr/bin/env python3
"""
Landlord Mapper web UI.

Standard library only. Reads the pipeline's CSV output from a data directory,
builds in-memory indexes at startup, and serves the ownership chain, the
landlord profile, and the address search over HTTP.

Data contract:
  <data>/parcel_roll_5county.csv         PREFERRED. The whole harmonised parcel
                                         roll, exported straight out of the
                                         pipeline's austin_parcel_data_merged
                                         target, so it carries every county roll
                                         the scrape filters, not one of them.
                                         Written with row.names = FALSE, so it
                                         has no leading row-number column.
  <data>/austin_parcel_data_merged.csv   FALLBACK, used only when the file above
                                         is absent. A Travis-only side-effect
                                         write from the pipeline, and written by
                                         R write.csv() with row names, so its
                                         column 0 is an unnamed row-number
                                         column.
  <data>/owner_data_total.csv            franchise-registry scrape output.
  <data>/owner_data_part_*.csv           same schema, unioned in.

The two parcel files differ in that leading column, which is exactly why all
field access here is by header NAME and never by position: a positional read is
correct for one of them and off by one for the other.

Join key is situs_pID plus situs_address. The scrape writes the pID zero-padded
to 12 characters and the parcel roll does not, so every comparison goes through
norm_pid(). Padding is not the only hazard: the roll is the rbind of a dozen
county rolls (_targets.R, austin_parcel_data_merged) and the counties reuse the
same numeric pID space, so a pID is not a key on its own. Roughly 468k of the
IDs loaded here are held by more than one county roll. by_pid therefore maps one
ID to every parcel carrying it, and a registry row is placed only on the
candidate whose situs_address agrees. Rows that agree with none of them are
counted and held back rather than joined to the wrong building, and a parcel URL
carries its county for the same reason.

Owner identity is the pair (owner_name, owner_address) from the parcel roll,
because that pair is what the scrape was keyed on.

The registry lookup is scoped to the rental-shaped part of the roll on purpose.
parcel_in_scope() reproduces that scope so the coverage figures are quoted
against the parcels the scrape was ever going to ask about, not against the
whole roll.

Env:
  LM_DATA        data directory (default ~/landlord-mapper-ui/data)
  LM_PORT        listen port (default 8099)
  LM_EXTRA_OWNER_CSV
                 optional extra scrape CSV, comma separated, unioned in after
                 the real files. Used to exercise scrape_status values that the
                 in-flight run has not written yet. Never set in production.
"""

import csv
import glob
import hashlib
import html
import io
import json
import os
import sqlite3
import socketserver
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler

csv.field_size_limit(10 * 1024 * 1024)

DATA = os.environ.get("LM_DATA", os.path.expanduser("~/landlord-mapper-ui/data"))
PORT = int(os.environ.get("LM_PORT", "8099"))

PAGE_SIZE = 40
MAX_HITS = 400

# Filtered-browse guards.
#   RANK_LIMIT how deep the ranked owner tables go. Past this, use the export.
#              A product limit, not a technical one: nobody picks a campaign
#              target on page 24. Sorting has no cap at all any more, because
#              every sort column carries a covering index.
#   EXPORT_CAP hard row cap on /export.csv. Hitting it writes a trailing
#              comment row: silent truncation would be a lie.
RANK_LIMIT = 1000
EXPORT_CAP = 250000

# The five-county export first, the Travis-only side-effect write as a fallback.
# Order is the preference order.
PARCEL_FILES = ("parcel_roll_5county.csv", "austin_parcel_data_merged.csv")


def parcel_path():
    for name in PARCEL_FILES:
        p = os.path.join(DATA, name)
        if os.path.exists(p):
            return p
    return os.path.join(DATA, PARCEL_FILES[-1])

# Network fan-out guards. A key held by more owners than this is a hub, not a
# link, so it is reported as a count instead of drawn as edges.
HUB_OFFICER = 40
HUB_AGENT = 25
HUB_MAIL = 25
MAX_HOP1 = 6
MAX_HOP2 = 3

# ---------------------------------------------------------------------------
# parcel record layout
# ---------------------------------------------------------------------------
PARCEL_COLS = [
    "situs_year", "situs_pID", "situs_address", "situs_zip",
    "totalsqftlivingarea", "property_units", "year_built", "state_code",
    "is_owner_out_of_state", "is_owner_occupied", "is_financialized",
    "is_mom_and_pop", "legallocationdesc", "owner_name", "owner_address",
    "owner_zip", "agent_name", "recent_purchase_date", "totalpropmktvalue",
    "county",
]
P = {name: i for i, name in enumerate(PARCEL_COLS)}

SCRAPE_COLS = [
    "owner_name_scraped", "owner_scraped_title", "owner_address_scraped",
    "owner_active_year", "corp_business_name", "corp_TTN", "corp_mail_address",
    "corp_right_to_transact_business_tx_status", "corp_state_of_formation",
    "corp_sos_registration_status", "corp_effective_sos_registration_date",
    "corp_tx_sos_file_num", "corp_registered_agent_name",
    "corp_registered_agent_mail_add", "scrape_status", "situs_pID",
    "situs_address",
]
S = {name: i for i, name in enumerate(SCRAPE_COLS)}

# resolved states
MATCHED = "matched"
NO_RECORD = "no_record"
NOT_RESOLVED = "not_resolved"
NOT_LOOKED_UP = "not_looked_up"
# not a scrape_status: the scrape was never going to ask about this parcel. It
# borrows the dashed and open treatment because the chain still does not end in
# an answer, and the copy says which coverage rule put it outside.
OUT_OF_SCOPE = "out_of_scope"

STATE_LABEL = {
    MATCHED: "Matched",
    NO_RECORD: "No record",
    NOT_RESOLVED: "Lookup rejected",
    NOT_LOOKED_UP: "Not looked up",
    OUT_OF_SCOPE: "Outside coverage",
}
STATE_CHIP = {
    MATCHED: "chip--matched",
    NO_RECORD: "chip--norec",
    NOT_RESOLVED: "chip--unknown",
    NOT_LOOKED_UP: "chip--unknown",
    OUT_OF_SCOPE: "chip--unknown",
}
# the run/terminator stroke that encodes the state
STATE_NODE = {
    MATCHED: "",
    NO_RECORD: " node--stop",
    NOT_RESOLVED: " node--dashed",
    NOT_LOOKED_UP: " node--dashed",
    OUT_OF_SCOPE: " node--dashed",
}
STATE_GLYPH = {
    MATCHED: "g--matched",
    NO_RECORD: "g--norec",
    NOT_RESOLVED: "g--unknown",
    NOT_LOOKED_UP: "g--unknown",
    OUT_OF_SCOPE: "g--unknown",
}


def norm_pid(v):
    v = (v or "").strip()
    v = v.lstrip("0")
    return v or "0"


def norm_txt(v):
    return " ".join((v or "").upper().split())


def owner_key(name, addr):
    return norm_txt(name) + "\x1f" + norm_txt(addr)


def owner_id(name, addr):
    return hashlib.sha1(owner_key(name, addr).encode("utf-8")).hexdigest()[:12]


def to_int(v):
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    try:
        return int(float((v or "").strip()))
    except (TypeError, ValueError, AttributeError):
        return 0


def to_float(v):
    """None when the field does not hold a number. R writes large values in
    scientific notation, so this never goes near int()."""
    try:
        return float((v or "").strip())
    except (TypeError, ValueError, AttributeError):
        return None


def is_true(v):
    return (v or "").strip().upper() in ("TRUE", "T", "1", "YES")


def is_false(v):
    return (v or "").strip().upper() in ("FALSE", "F", "0", "NO")


# ---------------------------------------------------------------------------
# facet bits
# ---------------------------------------------------------------------------
# One byte a parcel carries every boolean the filtered pages test, so a filter
# pass reads a bytearray instead of re-parsing four text fields per row.
F_OOS = 1     # is_owner_out_of_state
F_OCC = 2     # is_owner_occupied
F_FIN = 4     # is_financialized
F_MOM = 8     # is_mom_and_pop
F_SCOPE = 16  # parcel_in_scope(), cached


def fast_int(v):
    """to_int() with a fast path. Most roll numbers are plain digit strings;
    R writes the large ones in scientific notation, and those fall through."""
    return int(v) if v.isdigit() else to_int(v)


def fast_true(v):
    """is_true() for the roll's own spelling. The rolls write TRUE / FALSE
    literally, so the first character decides it."""
    return v[:1] in ("T", "t", "1", "Y", "y")


# ---------------------------------------------------------------------------
# the lookup scope
# ---------------------------------------------------------------------------
# The registry scrape never asks about the whole roll. scrape_helper_functions.R,
# owner_scrape_actual(), builds its target set with one dplyr::filter:
#
#   target_properties = dplyr::filter(austin_parcel_data_merged,
#                                     ((is_financialized == TRUE) &
#                                        (is_owner_occupied == FALSE)) |
#                                       (property_units > 5),
#                                     property_units != 0)
#
# Reproduced below, leg for leg. dplyr::filter keeps a row only when the whole
# condition evaluates TRUE, so an unreadable value anywhere in it drops the row
# rather than defaulting it in. That is why each leg tests for the literal value
# instead of for truthiness, and why an unparseable property_units is out.
#
# Note property_units > 5, strictly, not 5 and over: a clean 5 unit building is
# outside the scope unless the roll also flags it financialized and not
# owner-occupied. Units are themselves an estimate from floor area.
SCOPE_OCCUPIED = "occupied"
SCOPE_SIZE = "size"
SCOPE_NOSIZE = "nosize"


def parcel_in_scope(rec):
    units = to_float(rec[P["property_units"]])
    if units is None or units == 0:
        return False
    if units > 5:
        return True
    return (is_true(rec[P["is_financialized"]])
            and is_false(rec[P["is_owner_occupied"]]))


def scope_reason(rec):
    """Which coverage rule put this parcel outside the lookup. Owner-occupied
    first, because it is the fact a reader can check against the roll; the
    property_units != 0 leg second, because a zero there means the roll gave us
    no floor area to size the building from, which is not the same claim as the
    building being small."""
    if is_true(rec[P["is_owner_occupied"]]):
        return SCOPE_OCCUPIED
    units = to_float(rec[P["property_units"]])
    if units is None or units == 0:
        return SCOPE_NOSIZE
    return SCOPE_SIZE


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


class Conn:
    """One read-only connection per thread.

    This server is threaded and a sqlite3 connection must not be shared across
    threads, so each thread opens its own and keeps it. Read-only is enforced by
    the open mode and by PRAGMA query_only, not by convention: the process has no
    business writing this file, and the file deliberately does not live in the
    root-owned CSV drop.
    """

    def __init__(self, path):
        self.path = path
        self.local = threading.local()

    def conn(self):
        c = getattr(self.local, "c", None)
        if c is None:
            c = sqlite3.connect("file:%s?mode=ro" % urllib.parse.quote(self.path),
                                uri=True, timeout=15)
            c.execute("PRAGMA query_only = 1")
            # 2 MB of pages per thread, not 8. This is the whole memory fix.
            #
            # ThreadingMixIn opens a connection per TCP connection, so this cache
            # is allocated and discarded per connection, and glibc never returns
            # the fragmented heap to the OS. The cache size is therefore a
            # multiplier on peak memory, not a fixed cost. Measured over the same
            # 143-route replay plus an 8-way concurrent pass: peak RSS 494.0 MB at
            # 8 MB, 170.3 MB at 2 MB.
            #
            # It is nearly free. Warm p95 moves 0.223 -> 0.228 s and the median
            # does not move at all; the only route that visibly pays is
            # /export.csv, +0.203 s on a ~3 s request. All 143 responses stay byte
            # identical. Raising it back is the first thing to try if a future
            # query pattern starts thrashing -- check /export.csv timing first,
            # since it is the most cache-sensitive route.
            c.execute("PRAGMA cache_size = -2000")
            self.local.c = c
        return c

    def all(self, sql, args=()):
        return self.conn().execute(sql, args).fetchall()

    def one(self, sql, args=()):
        return self.conn().execute(sql, args).fetchone()

    def val(self, sql, args=()):
        r = self.one(sql, args)
        return None if r is None else r[0]

    def cursor(self, sql, args=()):
        return self.conn().execute(sql, args)


class ParcelRows:
    """STORE.parcels[i]: a 0-based index into the roll in load order, giving the
    same tuple the old in-memory list gave, in PARCEL_COLS order.

    A small per-thread cache means a page that touches one row from several
    helpers pays for one query, and warm() turns a whole table page into a single
    query. The database is read-only and static, so a cached row can never go
    stale while the process lives.
    """

    def __init__(self, db):
        self.db = db
        self.local = threading.local()

    def cache(self):
        c = getattr(self.local, "c", None)
        if c is None:
            c = self.local.c = {}
        return c

    def warm(self, idxs):
        c = self.cache()
        want = [i for i in idxs if i not in c]
        if not want:
            return
        if len(c) > 4000:
            c.clear()
        for n in range(0, len(want), 500):
            chunk = want[n:n + 500]
            qs = ",".join("?" * len(chunk))
            for row in self.db.all(
                    "SELECT p.rowid, " + PARCEL_SQL + " " + PARCEL_FROM
                    + "WHERE p.rowid IN (%s)" % qs,
                    [i + 1 for i in chunk]):
                c[row[0] - 1] = tuple(row[1:])

    def __getitem__(self, i):
        c = self.cache()
        got = c.get(i)
        if got is None:
            row = self.db.one(
                "SELECT " + PARCEL_SQL + " " + PARCEL_FROM + "WHERE p.rowid = ?",
                (i + 1,))
            if row is None:
                raise IndexError(i)
            got = c[i] = tuple(row)
        return got

    def __len__(self):
        return STORE.stats.get("parcel_rows", 0)


class InScopeView:
    """STORE.in_scope[i]. parcel_in_scope() is a pure function of the record and
    the record is already cached, so this needs no query of its own and cannot
    drift from the predicate the rest of the site quotes."""

    def __getitem__(self, i):
        return parcel_in_scope(STORE.parcels[i])


class AddrUpperView:
    def __getitem__(self, i):
        return STORE.parcels[i][P["situs_address"]].upper()


class OwnerDict(dict):
    """One owner row, shaped like the dict the page code already reads. The
    parcel index list is fetched only if something actually asks for it, because
    most readers only want the totals, which are columns."""

    def __init__(self, row, db):
        dict.__init__(self, zip(OWNER_COLS, row))
        self["id"] = row[0]
        self["in_scope"] = bool(row[3])
        self.db = db

    def __missing__(self, k):
        if k == "parcels":
            v = [r[0] - 1 for r in self.db.all(
                "SELECT rowid FROM parcel WHERE owner_id = ? ORDER BY rowid",
                (self["id"],))]
            self["parcels"] = v
            return v
        raise KeyError(k)


class OwnerTable:
    """Owner rows, with a per-thread memo so the several helpers that each ask
    for the same owner while rendering one page pay for one query between them.

    Safe because the database is read-only and static for the life of the
    process: a memoised row cannot go stale. warm() batches a known set of ids
    into the memo in one query, which is what turns the network panel from a
    query per node into a query per hop.
    """

    def __init__(self, db):
        self.db = db
        self.local = threading.local()

    def memo(self):
        m = getattr(self.local, "m", None)
        if m is None:
            m = self.local.m = {}
        return m

    def warm(self, ids):
        m = self.memo()
        want = sorted(set(i for i in ids if i and i not in m))
        if not want:
            return
        if len(m) > 3000:
            m.clear()
            want = sorted(set(i for i in ids if i))
        for n in range(0, len(want), 400):
            chunk = want[n:n + 400]
            qs = ",".join("?" * len(chunk))
            for row in self.db.all(
                    "SELECT " + OWNER_SQL + " FROM owner WHERE owner_id IN (%s)" % qs,
                    chunk):
                m[row[0]] = OwnerDict(row, self.db)
        for i in want:
            m.setdefault(i, None)

    def get(self, oid, default=None):
        if not oid:
            return default
        m = self.memo()
        if oid in m:
            got = m[oid]
            return default if got is None else got
        row = self.db.one(
            "SELECT " + OWNER_SQL + " FROM owner WHERE owner_id = ?", (oid,))
        got = OwnerDict(row, self.db) if row is not None else None
        if len(m) > 3000:
            m.clear()
        m[oid] = got
        return default if got is None else got

    def __getitem__(self, oid):
        o = self.get(oid)
        if o is None:
            raise KeyError(oid)
        return o

    def __contains__(self, oid):
        return self.get(oid) is not None


class FilingTable:
    """Returns None for an owner the registry was never asked about, and a dict
    for one it was, exactly as the old filings dict did. Non-matched owners get a
    dict whose corp_name is empty, which is the distinction every caller tests.

    Memoised per thread for the same reason as OwnerTable, and warm() fetches a
    set of filings plus all their officers in two queries rather than two per
    owner.
    """

    def __init__(self, db):
        self.db = db
        self.local = threading.local()

    def memo(self):
        m = getattr(self.local, "m", None)
        if m is None:
            m = self.local.m = {}
        return m

    def row_to_dict(self, row, officers):
        d = dict(zip(FILING_COLS, row))
        try:
            d["raw_status"] = json.loads(d["raw_status"] or "[]")
        except ValueError:
            d["raw_status"] = []
        d["officers"] = officers
        return d

    def officers_for(self, oid):
        return [{"name": n, "title": t, "year": y} for n, t, y in self.db.all(
            "SELECT name, title, year FROM officer WHERE owner_id = ? ORDER BY ord",
            (oid,))]

    def warm(self, ids):
        m = self.memo()
        want = sorted(set(i for i in ids if i and i not in m))
        if not want:
            return
        if len(m) > 3000:
            m.clear()
            want = sorted(set(i for i in ids if i))
        for n in range(0, len(want), 400):
            chunk = want[n:n + 400]
            qs = ",".join("?" * len(chunk))
            offs = {}
            for oid, nm, t, y in self.db.all(
                    "SELECT owner_id, name, title, year FROM officer "
                    "WHERE owner_id IN (%s) ORDER BY owner_id, ord" % qs, chunk):
                offs.setdefault(oid, []).append(
                    {"name": nm, "title": t, "year": y})
            for row in self.db.all(
                    "SELECT " + FILING_SQL + " FROM filing WHERE owner_id IN (%s)"
                    % qs, chunk):
                m[row[0]] = self.row_to_dict(row, offs.get(row[0], []))
        for i in want:
            m.setdefault(i, None)

    def get(self, oid, default=None):
        if not oid:
            return default
        m = self.memo()
        if oid in m:
            got = m[oid]
            return default if got is None else got
        row = self.db.one(
            "SELECT " + FILING_SQL + " FROM filing WHERE owner_id = ?", (oid,))
        got = None if row is None else self.row_to_dict(row, self.officers_for(oid))
        if len(m) > 3000:
            m.clear()
        m[oid] = got
        return default if got is None else got


class Store:
    def __init__(self):
        self.db = Conn(DB_PATH)
        self.parcels = ParcelRows(self.db)
        self.addr_upper = AddrUpperView()
        self.in_scope = InScopeView()
        self.owners = OwnerTable(self.db)
        self.filings = FilingTable(self.db)
        self.stats = {}
        self.loaded_at = 0.0
        # ?county= and ?zip= arrive as text and the stored columns are INTEGER
        # codes, so the filter has to be translated. Both dictionaries are tiny
        # (13 counties, 540 zip spellings) and static, so they are read once.
        self.county_code = {}
        self.zip_codes = {}

    # -- load -------------------------------------------------------------
    def load(self):
        """Read the load report out of the database. Every figure the old load
        computed while walking the CSVs was computed once by build-db.py, so
        there is nothing to recompute here and startup is a single small query."""
        st = {}
        for k, v in self.db.all("SELECT k, v FROM meta"):
            st[k] = json.loads(v)
        st.setdefault("errors", [])
        self.stats = st
        # d_county.n is norm_txt(county); d_zip.n is the trimmed spelling, which
        # is what the old zip_trim column held. Several raw zip spellings can
        # trim to one value, so a requested ZIP maps to a LIST of codes.
        self.county_code = {n: c for c, n in self.db.all("SELECT c, n FROM d_county")}
        self.zip_codes = {}
        for c, n in self.db.all("SELECT c, n FROM d_zip"):
            self.zip_codes.setdefault(n, []).append(c)
        # "Data read into this page" means when the data was read, which is when
        # the database was built, not when this process happened to start
        self.loaded_at = st.get("built_at") or time.time()

    # -- derived ----------------------------------------------------------
    def owner_totals(self, o):
        return {"value": o["tot_value"], "sqft": o["tot_sqft"],
                "units": o["tot_units"], "count": o["n_parcels"]}

    def pid_candidates(self, pid_raw, county=None):
        """Every parcel carrying this ID, narrowed to one county roll when the
        URL names one. A bare ID is ambiguous across the rolls loaded here, so
        callers must be ready for more than one."""
        pid = norm_pid(pid_raw)
        if county:
            # county is an INTEGER code now, so an unknown county name resolves
            # to no code and must select nothing, which is what the text
            # comparison did
            code = self.county_code.get(norm_txt(county))
            if code is None:
                return []
            rows = self.db.all(
                "SELECT rowid FROM parcel WHERE county = ? AND pid_norm = ? "
                "ORDER BY rowid", (code, pid))
        else:
            rows = self.db.all(
                "SELECT rowid FROM parcel WHERE pid_norm = ? ORDER BY rowid",
                (pid,))
        got = [r[0] - 1 for r in rows]
        self.parcels.warm(got)
        return got

    def owner_for_parcel(self, i):
        rec = self.parcels[i]
        return self.owners[owner_id(rec[P["owner_name"]],
                                    rec[P["owner_address"]])]

    def search(self, q):
        """Address substring search.

        The old search was a plain Python substring test against the uppercased
        situs address, scanning in load order and stopping at MAX_HITS. This is
        the same test: LIKE with the pattern wildcards escaped, ordered by rowid
        so "the first 400" means the same 400, then sorted shortest-first the
        same way.

        FTS5 is compiled into this SQLite and is deliberately NOT used. FTS
        matches tokens, so it would quietly change which addresses match: a
        search for part of a street number, or for a fragment inside a word,
        finds rows today that a tokeniser would miss. Changing the result set was
        not on the table, so this stays a substring match against an indexed
        uppercased column.
        """
        needle = norm_txt(q)
        if not needle:
            return []
        pat = "%" + (needle.replace("\\", "\\\\").replace("%", "\\%")
                     .replace("_", "\\_")) + "%"
        # INDEXED BY is not an optimisation hint here, it is a memory bound.
        # `parcel` is 796.5 MB and ix_p_addr is 82.9 MB, so on a memory-capped
        # host the table never fits page cache and the index always does. Left
        # to itself SQLite scans the table, which costs 2.6-2.8 s per search AND
        # evicts everything else -- measured /rankings going 0.179 s -> 4.469 s
        # on the request following one search. Same WHERE, same ORDER BY rowid,
        # same LIMIT, so the selected rows are byte-identical either way.
        # ix_p_addr is on situs_address now, and answers what addr_upper used to:
        # the roll's address text is already uppercase, so addr_upper equalled
        # situs_address for all 2,117,593 rows (checked, not assumed) and the
        # column was pure duplication.
        rows = self.db.all(
            "SELECT rowid FROM parcel INDEXED BY ix_p_addr "
            "WHERE situs_address LIKE ? ESCAPE '\\' "
            "ORDER BY rowid LIMIT ?", (pat, MAX_HITS))
        hits = [r[0] - 1 for r in rows]
        self.parcels.warm(hits)
        au = self.addr_upper
        hits.sort(key=lambda i: (len(au[i]), au[i]))
        return hits

    def agent_fanout(self, key):
        """How many matched filings name this registered agent, self included."""
        if not key:
            return 0
        return self.db.val(
            "SELECT COUNT(*) FROM filing WHERE agent_norm = ? AND corp_name <> ''",
            (key,)) or 0

    def officer_peers(self, keys):
        """{officer name -> [owner_id, ...]} for a set of names, in one query.

        One query for the whole set rather than one per officer. Order within a
        name is by owner id, and the caller does its own exclusion of the focus
        owner, because the two hops want different things: hop 1 counts peers
        excluding the focus owner, hop 2 counts them including it.
        """
        out = {}
        keys = sorted(set(k for k in keys if k))
        if not keys:
            return out
        for n in range(0, len(keys), 300):
            chunk = keys[n:n + 300]
            qs = ",".join("?" * len(chunk))
            for k, p in self.db.all(
                    "SELECT DISTINCT name_norm, owner_id FROM officer "
                    "WHERE name_norm IN (%s) ORDER BY name_norm, owner_id" % qs,
                    chunk):
                out.setdefault(k, []).append(p)
        for k in keys:
            out.setdefault(k, [])
        return out

    def neighbourhood(self, oid):
        """1-2 hop shell network around one owner. Every edge carries a reason.

        Peers come back ordered by owner id. The in-memory code iterated a Python
        set, whose order is randomised per process, so which peers survived the
        MAX_HOP1 cut could change between restarts of the same build. This is the
        same selection rule made repeatable.

        Query shape matters here more than it looks: this panel used to issue one
        query per officer, per neighbour name, per neighbour filing and per
        neighbour's officers, which on the biggest owners was scores of round
        trips. It is now a fixed handful of batched queries regardless of how
        connected the owner is, because the machine this runs on has one shared
        vCPU and per-request work multiplies there.
        """
        fl = self.filings.get(oid)
        if not fl or not fl.get("corp_name"):
            return None
        hubs = []
        hop1 = {}

        # every officer name on this filing, resolved in one query
        peers_by_name = self.officer_peers(
            norm_txt(of["name"]) for of in fl["officers"])

        def take(others, key, kind, detail, hub_limit):
            if not others:
                return
            if len(others) > hub_limit:
                hubs.append((kind, key, len(others)))
                return
            for p in others:
                hop1.setdefault(p, []).append((kind, detail))

        for of in fl["officers"]:
            k = norm_txt(of["name"])
            if not k:
                continue
            take([p for p in peers_by_name.get(k, ()) if p != oid],
                 k, "officer", "shared officer", HUB_OFFICER)
        k = norm_txt(fl.get("agent"))
        if k:
            take([r[0] for r in self.db.all(
                "SELECT owner_id FROM filing WHERE agent_norm = ? "
                "AND owner_id <> ? AND corp_name <> '' ORDER BY owner_id",
                (k, oid))], k, "agent", "shared registered agent", HUB_AGENT)
        k = norm_txt(fl.get("mail"))
        if k:
            take([r[0] for r in self.db.all(
                "SELECT owner_id FROM filing WHERE mail_norm = ? "
                "AND owner_id <> ? AND corp_name <> '' ORDER BY owner_id",
                (k, oid))], k, "mail", "shared mailing address", HUB_MAIL)

        rank = {"officer": 0, "mail": 1, "agent": 2}
        # one query for every candidate's name, then the ranking is pure Python
        self.owners.warm(hop1.keys())
        names = {}
        for p in hop1:
            o = self.owners.get(p)
            names[p] = (o["name"] if o else "") or ""
        order = sorted(hop1.items(),
                       key=lambda kv: (min(rank[k] for k, _ in kv[1]),
                                       -len(kv[1]), names[kv[0]]))
        omitted1 = max(0, len(order) - MAX_HOP1)
        order = order[:MAX_HOP1]
        keep = set(k for k, _ in order)

        # second hop, shared officer only: the only link strong enough to be
        # worth following twice
        first = [pid1 for pid1, _ in order]
        self.filings.warm(first)
        f1s = dict((p, self.filings.get(p)) for p in first)
        hop2_keys = []
        for p in first:
            if f1s.get(p):
                hop2_keys.extend(norm_txt(of["name"]) for of in f1s[p]["officers"])
        peers2 = self.officer_peers(hop2_keys)
        hop2 = []
        seen2 = set(keep) | {oid}
        for pid1 in first:
            f1 = f1s.get(pid1)
            if not f1:
                continue
            for of in f1["officers"]:
                peers = peers2.get(norm_txt(of["name"]), [])
                if len(peers) - 1 > HUB_OFFICER:
                    continue
                for p2 in peers:
                    if p2 in seen2:
                        continue
                    seen2.add(p2)
                    hop2.append((p2, pid1, "officer", "shared officer"))
        omitted2 = max(0, len(hop2) - MAX_HOP2)
        hop2 = hop2[:MAX_HOP2]
        # the panel draws these next, so pull them in one round trip each
        second = [p2 for p2, _p, _k, _t in hop2]
        self.owners.warm(second)
        self.filings.warm(second)
        return {"hop1": order, "hop2": hop2, "hubs": hubs,
                "omitted1": omitted1, "omitted2": omitted2}


def parcel_path_for(county, pid):
    """parcel_link() without needing the row in hand, for the streaming export.
    Must stay byte-identical to parcel_link()."""
    return "/parcel/%s/%s" % (
        urllib.parse.quote((county or "").strip() or "unknown"),
        urllib.parse.quote((pid or "").strip()))


def mtime(path):
    try:
        return time.strftime("%Y-%m-%d %H:%M",
                             time.localtime(os.path.getmtime(path)))
    except OSError:
        return "n/a"


STORE = Store()

# ---------------------------------------------------------------------------
# formatting
# ---------------------------------------------------------------------------
def e(v):
    return html.escape("" if v is None else str(v), quote=True)


def money(n):
    return "$%s" % format(to_int(n), ",d")


def num(n):
    # values arrive as strings from CSV and R writes large ones in scientific
    # notation ("1.1e+08"), so everything goes through the float parse
    return format(to_int(n), ",d")


def dash(v):
    v = (v or "").strip()
    return v if v and v.upper() not in ("NA", "N/A", "NULL") else "not on the roll"


def datestamp(v):
    v = (v or "").strip()
    if not v or v.upper() in ("NA", "NULL"):
        return ""
    return v.split(" ")[0]


def sosdate(v):
    v = (v or "").strip()
    if "/" in v:
        parts = v.split("/")
        if len(parts) == 3:
            return "%s-%s-%s" % (parts[2], parts[0].zfill(2), parts[1].zfill(2))
    return v


def title_case(v):
    return (v or "").strip().title()


# ---------------------------------------------------------------------------
# CSS, carried over from the approved design
# ---------------------------------------------------------------------------
CSS = r"""
:root {
  --paper:#E9EBE0; --paper-2:#F3F4EC; --paper-3:#E1E4D8;
  --ink:#171E1B; --ink-2:#58625B; --rule:#A7B0A4;
  --survey:#2E5FA3; --survey-w:#D6DFEE;
  --ochre:#7D5F16; --oxide:#9E3226; --focus:#2E5FA3;
  --mono: ui-monospace, "Cascadia Mono", "SF Mono", SFMono-Regular, Menlo,
          Consolas, "Liberation Mono", "Courier New", monospace;
  --serif: Charter, "Iowan Old Style", "Palatino Linotype", Palatino,
           Georgia, Cambria, "Times New Roman", serif;
  --gut: 30px; --wrap: 74rem; --col: 40rem;
}
@media (prefers-color-scheme: dark) {
  :root {
    --paper:#101310; --paper-2:#191E19; --paper-3:#151A15;
    --ink:#E2E7DE; --ink-2:#97A296; --rule:#333B33;
    --survey:#86ADE8; --survey-w:#1C2A3B;
    --ochre:#DCB25E; --oxide:#E58A78; --focus:#A8C6F0;
  }
}
:root[data-theme="dark"] {
  --paper:#101310; --paper-2:#191E19; --paper-3:#151A15;
  --ink:#E2E7DE; --ink-2:#97A296; --rule:#333B33;
  --survey:#86ADE8; --survey-w:#1C2A3B;
  --ochre:#DCB25E; --oxide:#E58A78; --focus:#A8C6F0;
}
:root[data-theme="light"] {
  --paper:#E9EBE0; --paper-2:#F3F4EC; --paper-3:#E1E4D8;
  --ink:#171E1B; --ink-2:#58625B; --rule:#A7B0A4;
  --survey:#2E5FA3; --survey-w:#D6DFEE;
  --ochre:#7D5F16; --oxide:#9E3226; --focus:#2E5FA3;
}
@media (min-width: 46rem) { :root { --gut: 56px; } }

html { -webkit-text-size-adjust: 100%; }
body {
  margin: 0;
  background: var(--paper); color: var(--ink);
  font-family: var(--serif);
  font-size: clamp(1rem, 0.96rem + 0.2vw, 1.09rem);
  line-height: 1.62; overflow-x: hidden;
}
*, *::before, *::after { box-sizing: border-box; }
img, svg, table { max-width: 100%; }
a { color: var(--survey); text-decoration-thickness: 1px; text-underline-offset: 2px; }
:focus-visible { outline: 2px solid var(--focus); outline-offset: 3px; }

.m { font-family: var(--mono); font-variant-numeric: tabular-nums; }
.eyebrow {
  font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.13em;
  text-transform: uppercase; color: var(--ink-2); line-height: 1.4;
}
.num { font-family: var(--mono); font-variant-numeric: tabular-nums; }
.wrap { max-width: var(--wrap); margin-inline: auto; padding-inline: clamp(1rem, 4vw, 3rem); }
.prose { max-width: var(--col); }
.band { padding-block: clamp(2.6rem, 7vw, 5rem); }
.band + .band { border-top: 1px solid var(--rule); }

.masthead { padding-block: clamp(1.4rem, 4vw, 2.4rem) clamp(2rem, 6vw, 3.4rem); }
.topline {
  display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between;
  gap: 1rem; border-bottom: 2px solid var(--ink); padding-bottom: 0.7rem;
}
.orgmark {
  font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.2em;
  text-transform: uppercase; color: var(--ink);
}
.orgmark a { color: var(--ink); text-decoration: none; }
.orgmark a:hover { color: var(--survey); }
.orgmark b { font-weight: 700; }
.orgmark span { color: var(--ink-2); }
.themebtn {
  font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.12em;
  text-transform: uppercase; background: transparent; color: var(--ink-2);
  border: 1px solid var(--rule); padding: 0.4rem 0.7rem; cursor: pointer;
}
.themebtn:hover { color: var(--ink); border-color: var(--ink); }

h1 {
  font-family: var(--mono); font-weight: 700; text-transform: uppercase;
  font-size: clamp(2.05rem, 8.4vw, 4.4rem); line-height: 0.94;
  letter-spacing: -0.035em; text-wrap: balance;
  margin-block: clamp(1.6rem, 5vw, 2.6rem) 0; max-width: 22ch;
}
h1 em { font-style: normal; color: var(--survey); }
.deck { max-width: 42rem; margin-top: 1.1rem; font-size: 1.06em; }

.stamp {
  font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.11em;
  text-transform: uppercase; line-height: 1.5; color: var(--ochre);
  border: 2px solid var(--ochre); outline: 1px solid var(--ochre);
  outline-offset: 3px; padding: 0.6rem 0.85rem; max-width: 36rem; margin-top: 2rem;
}
@media (min-width: 52rem) { .stamp { transform: rotate(-0.9deg); transform-origin: left center; } }

.datestrip {
  display: flex; flex-wrap: wrap; gap: 0 1.6rem; margin-top: 1.8rem;
  padding-top: 0.9rem; border-top: 1px solid var(--rule); max-width: 52rem;
}
.datestrip div {
  font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.06em;
  text-transform: uppercase; color: var(--ink-2); font-variant-numeric: tabular-nums;
}
.datestrip b { color: var(--ink); font-weight: 700; }

.lookup {
  background: var(--paper-2); border: 1px solid var(--rule);
  padding: clamp(1.1rem, 4vw, 1.8rem); max-width: 46rem;
}
.lookup label {
  display: block; font-family: var(--mono); font-size: 0.6875rem;
  letter-spacing: 0.13em; text-transform: uppercase; color: var(--ink-2);
  margin-bottom: 0.55rem;
}
.field { display: flex; flex-wrap: wrap; gap: 0.6rem; }
.field input {
  flex: 1 1 16rem; min-width: 0; font-family: var(--mono); font-size: 1rem;
  letter-spacing: 0.01em; text-transform: uppercase; background: var(--paper);
  color: var(--ink); border: 2px solid var(--ink); padding: 0.7rem 0.75rem;
}
.field input::placeholder { color: var(--ink-2); text-transform: uppercase; }
.btn {
  font-family: var(--mono); font-size: 0.8125rem; letter-spacing: 0.1em;
  text-transform: uppercase; font-weight: 700; background: var(--ink);
  color: var(--paper); border: 2px solid var(--ink); padding: 0.7rem 1.1rem;
  cursor: pointer;
}
.btn:hover { background: var(--survey); border-color: var(--survey); color: var(--paper); }
.btn-quiet { background: transparent; color: var(--ink); border: 1px solid var(--rule); font-weight: 400; }
.btn-quiet:hover { background: transparent; color: var(--survey); border-color: var(--survey); }
.btn-off { opacity: 0.4; pointer-events: none; }
.scopenote { margin-top: 1rem; max-width: 44rem; font-size: 0.95em; color: var(--ink-2); }

/* the surveyor's dimension run: stroke style is the match state */
.chain {
  display: grid; grid-template-columns: var(--gut) minmax(0, 1fr);
  column-gap: clamp(0.8rem, 3vw, 1.5rem); row-gap: 0;
  border-top: 1px solid var(--ink); border-bottom: 1px solid var(--ink);
  max-width: 56rem;
}
.node { position: relative; }
.node .run {
  position: absolute; top: 0; bottom: 0; right: 0; width: 2px;
  background: linear-gradient(var(--survey), var(--survey));
}
.node--dashed .run {
  background: repeating-linear-gradient(180deg, var(--ochre) 0 5px, transparent 5px 11px);
}
.node--stop .run { background: linear-gradient(var(--ink), var(--ink)); }
.node .tick {
  position: absolute; top: 1.22em; right: 0; width: 100%; height: 1px; background: var(--rule);
}
.node .mark {
  position: absolute; top: calc(1.22em - 5px); right: -4px; width: 11px; height: 11px;
  background: var(--survey);
}
.node--dashed .mark { background: var(--paper); border: 2px solid var(--ochre); }
.node--stop .mark { background: var(--ink); }
.node--end .run { bottom: auto; height: 2.4em; }
.node--end::after {
  content: ""; position: absolute; top: 2.4em; right: -7px; width: 17px; height: 3px;
  background: var(--survey);
}
.node--stop.node--end::after { background: var(--ink); width: 23px; right: -10px; height: 4px; }
.node--dashed.node--end::after {
  background: none; border-bottom: 2px dashed var(--ochre); height: 0; width: 11px;
  right: -4px; opacity: 0.55;
}

.rec { padding-block: clamp(1.15rem, 3.4vw, 1.7rem); min-width: 0; }
.chain > .rec:not(:first-of-type) { border-top: 1px solid var(--rule); }
.rec h3 {
  font-family: var(--serif); font-size: 1.12em; font-weight: 600; line-height: 1.3;
  margin: 0.15rem 0 0.6rem;
}
.dl { display: grid; grid-template-columns: minmax(0, 1fr); gap: 0.55rem 1.4rem; margin: 0; }
@media (min-width: 34rem) { .dl--2 { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
.dl > div { min-width: 0; }
.dl dt {
  font-family: var(--mono); font-size: 0.625rem; letter-spacing: 0.13em;
  text-transform: uppercase; color: var(--ink-2);
}
.dl dd {
  margin: 0.1rem 0 0; font-family: var(--mono); font-size: 0.875rem;
  font-variant-numeric: tabular-nums; line-height: 1.45; word-break: break-word;
}
.dl dd.raw { color: var(--ink-2); font-size: 0.8125rem; }
.dl dd .approx { color: var(--ochre); }
.srcstamp {
  margin-top: 1rem; padding-top: 0.5rem; border-top: 1px dotted var(--rule);
  font-family: var(--mono); font-size: 0.625rem; letter-spacing: 0.09em;
  text-transform: uppercase; color: var(--ink-2); font-variant-numeric: tabular-nums;
}
.tell {
  border-left: 3px solid var(--oxide); padding-left: 0.8rem; margin-top: 0.9rem;
  font-size: 0.95em; color: var(--ink); max-width: 38rem;
}
.tell--quiet { border-left-color: var(--rule); color: var(--ink-2); }

.matchcheck {
  display: grid; grid-template-columns: minmax(0, 1fr); gap: 1px;
  background: var(--rule); border: 1px solid var(--rule); margin-top: 0.3rem;
}
@media (min-width: 38rem) { .matchcheck { grid-template-columns: 1fr 1fr; } }
.matchcheck > div { background: var(--paper-2); padding: 0.7rem 0.8rem; }
.matchcheck .hd {
  font-family: var(--mono); font-size: 0.625rem; letter-spacing: 0.13em;
  text-transform: uppercase; color: var(--ink-2); display: block; margin-bottom: 0.3rem;
}
.matchcheck .val { font-family: var(--mono); font-size: 0.875rem; word-break: break-word; }
.matchcheck .val.hit { color: var(--survey); font-weight: 700; }

.chip {
  display: inline-flex; align-items: center; gap: 0.4rem; font-family: var(--mono);
  font-size: 0.625rem; letter-spacing: 0.13em; text-transform: uppercase;
  padding: 0.22rem 0.5rem; white-space: nowrap;
}
.chip--matched { background: var(--survey); color: var(--paper); border: 1px solid var(--survey); }
.chip--norec {
  background: transparent; color: var(--ink); border: 1px solid var(--ink);
  box-shadow: 0 0 0 2px var(--paper), 0 0 0 3px var(--ink); margin-right: 3px;
}
.chip--unknown { background: transparent; color: var(--ochre); border: 1px dashed var(--ochre); }
.rechead { display: flex; flex-wrap: wrap; align-items: center; gap: 0.6rem; }

.payload {
  background: var(--survey-w); border: 2px solid var(--survey);
  padding: clamp(1rem, 3.5vw, 1.5rem); margin-top: 0.5rem;
}
.payload--stop { background: var(--paper-2); border-color: var(--ink); }
.payload--open { background: var(--paper-2); border: 2px dashed var(--ochre); }
.figs { display: flex; flex-wrap: wrap; gap: clamp(1.2rem, 5vw, 2.6rem); }
.fig { min-width: 0; }
.fig .v {
  display: block; font-family: var(--mono); font-weight: 700;
  font-variant-numeric: tabular-nums; font-size: clamp(1.6rem, 6vw, 2.6rem);
  line-height: 1; letter-spacing: -0.03em; color: var(--ink);
}
.fig .k {
  display: block; font-family: var(--mono); font-size: 0.625rem; letter-spacing: 0.15em;
  text-transform: uppercase; color: var(--ink-2); margin-top: 0.4rem;
}
.fig .k .approx { color: var(--ochre); }
.payload .who {
  font-family: var(--mono); font-size: 0.9375rem; font-weight: 700;
  word-break: break-word; margin-bottom: 0.9rem; display: block;
}

.legendband { background: var(--paper-3); }
.endings {
  display: grid; grid-template-columns: minmax(0, 1fr); gap: 1px;
  background: var(--rule); border: 1px solid var(--rule); margin-top: 1.6rem;
}
@media (min-width: 50rem) { .endings { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
.ending {
  background: var(--paper-2); padding: clamp(1rem, 3vw, 1.4rem);
  display: grid; grid-template-columns: 28px minmax(0, 1fr);
  column-gap: 0.9rem; row-gap: 0;
}
.ending .glyph { position: relative; height: 100%; min-height: 74px; }
.ending .body { min-width: 0; display: flex; flex-direction: column; gap: 0.55rem; }
.ending h3 {
  font-family: var(--mono); font-size: 0.75rem; letter-spacing: 0.13em;
  text-transform: uppercase; margin: 0;
}
.ending p { margin: 0; font-size: 0.95em; }
.ending .ex {
  font-family: var(--mono); font-size: 0.75rem; color: var(--ink-2);
  word-break: break-word; border-top: 1px dotted var(--rule); padding-top: 0.5rem;
}
.glyph .g-run { position: absolute; top: 4px; right: 8px; width: 2px; height: 44px; }
.glyph .g-mark { position: absolute; top: 0; right: 3px; width: 11px; height: 11px; }
.glyph .g-term { position: absolute; right: 0; }
.g--matched .g-run { background: var(--survey); height: 58px; }
.g--matched .g-mark { background: var(--survey); }
.g--matched .g-term { top: 62px; right: 1px; width: 17px; height: 3px; background: var(--survey); }
.g--norec .g-run { background: var(--ink); height: 44px; }
.g--norec .g-mark { background: var(--ink); }
.g--norec .g-term { top: 48px; right: -2px; width: 23px; height: 4px; background: var(--ink); }
.g--unknown .g-run {
  background: repeating-linear-gradient(180deg, var(--ochre) 0 5px, transparent 5px 11px);
  height: 52px; opacity: 0.75;
}
.g--unknown .g-mark { background: var(--paper-2); border: 2px solid var(--ochre); }
.g--unknown .g-term {
  top: 58px; right: 3px; width: 11px; height: 0;
  border-bottom: 2px dashed var(--ochre); opacity: 0.4;
}

.sharenote {
  display: flex; flex-wrap: wrap; align-items: baseline; gap: 0.7rem;
  margin-top: 1.4rem; max-width: 46rem;
}
.sharenote .big {
  font-family: var(--mono); font-weight: 700; font-size: 1.6rem;
  letter-spacing: -0.02em; color: var(--ink); font-variant-numeric: tabular-nums;
}
.sharenote p { margin: 0; font-size: 0.95em; max-width: 34rem; }

.empty {
  margin-top: 1.8rem; border: 1px solid var(--rule); border-left: 3px solid var(--ochre);
  background: var(--paper-2); padding: clamp(0.9rem, 3vw, 1.3rem); max-width: 40rem;
}
.empty h3 { font-family: var(--mono); font-size: 0.8125rem; letter-spacing: 0.06em; margin: 0 0 0.5rem; }
.empty p { margin: 0 0 0.6rem; font-size: 0.95em; }
.empty p:last-child { margin-bottom: 0; }

.profhead { display: flex; flex-direction: column; gap: 0.9rem; align-items: flex-start; }
.profhead h2 {
  font-family: var(--mono); font-weight: 700; text-transform: uppercase;
  font-size: clamp(1.35rem, 5.2vw, 2.5rem); line-height: 1.02;
  letter-spacing: -0.03em; margin: 0; word-break: break-word;
}
.profhead .alias { font-family: var(--mono); font-size: 0.75rem; color: var(--ink-2); word-break: break-word; }

.headfigs {
  display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1px;
  background: var(--rule); border: 1px solid var(--rule); margin-top: 1.8rem;
}
@media (min-width: 44rem) { .headfigs { grid-template-columns: repeat(4, minmax(0, 1fr)); } }
.headfigs .cell { background: var(--paper-2); padding: clamp(0.9rem, 3vw, 1.3rem); }
.headfigs .cell--soft { background: var(--paper-3); }
.headfigs .v {
  display: block; font-family: var(--mono); font-weight: 700;
  font-variant-numeric: tabular-nums; font-size: clamp(1.4rem, 5vw, 2.1rem);
  line-height: 1; letter-spacing: -0.03em;
}
.headfigs .k {
  display: block; font-family: var(--mono); font-size: 0.625rem; letter-spacing: 0.14em;
  text-transform: uppercase; color: var(--ink-2); margin-top: 0.45rem;
}
.headfigs .k .approx { color: var(--ochre); }

.subhead {
  font-family: var(--mono); font-size: 0.75rem; letter-spacing: 0.16em;
  text-transform: uppercase; color: var(--ink); border-bottom: 2px solid var(--ink);
  padding-bottom: 0.4rem; margin: clamp(2rem, 6vw, 3rem) 0 0;
}
.tablescroll { overflow-x: auto; border: 1px solid var(--rule); border-top: 0; }
table {
  border-collapse: collapse; min-width: 46rem; width: 100%;
  font-family: var(--mono); font-variant-numeric: tabular-nums; font-size: 0.8125rem;
}
thead th {
  text-align: left; font-size: 0.625rem; letter-spacing: 0.11em; text-transform: uppercase;
  font-weight: 400; color: var(--ink-2); background: var(--paper-3);
  padding: 0.6rem 0.75rem; border-bottom: 1px solid var(--rule); white-space: nowrap;
}
tbody td { padding: 0.6rem 0.75rem; border-bottom: 1px solid var(--rule); white-space: nowrap; }
tbody tr:nth-child(even) td { background: var(--paper-3); }
tbody tr:hover td { background: var(--survey-w); }
td.r, th.r { text-align: right; }
tfoot td {
  padding: 0.65rem 0.75rem; font-weight: 700; border-top: 2px solid var(--ink);
  background: var(--paper-2); white-space: nowrap;
}
.cty {
  font-size: 0.625rem; letter-spacing: 0.09em; text-transform: uppercase;
  border: 1px solid var(--rule); padding: 0.1rem 0.35rem; color: var(--ink-2);
}
.tblnote {
  font-family: var(--mono); font-size: 0.625rem; letter-spacing: 0.08em;
  text-transform: uppercase; color: var(--ink-2); margin-top: 0.6rem;
}
.pager {
  display: flex; flex-wrap: wrap; align-items: center; gap: 0.7rem; margin-top: 1rem;
  font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.1em;
  text-transform: uppercase; color: var(--ink-2);
}
.pager a { text-decoration: none; }

.netwrap { border: 1px solid var(--rule); border-top: 0; background: var(--paper-2); }
.netscroll { overflow-x: auto; padding: clamp(0.6rem, 2vw, 1.1rem); }
.netscroll svg { min-width: 62rem; width: 100%; height: auto; display: block; }
.n-box { fill: var(--paper); stroke: var(--rule); stroke-width: 1; }
.n-box--focus { fill: var(--survey-w); stroke: var(--survey); stroke-width: 2; }
.n-name { fill: var(--ink); font-family: var(--mono); font-size: 12.5px; font-weight: 700; letter-spacing: 0.01em; }
.n-state { fill: var(--ink-2); font-family: var(--mono); font-size: 9.5px; letter-spacing: 0.11em; }
.n-link { text-decoration: none; }
.e-line { stroke: var(--ink-2); stroke-width: 1.6; fill: none; }
.e-line--officer { stroke: var(--survey); stroke-width: 2.2; }
.e-line--agent { stroke: var(--ink-2); stroke-width: 1.4; stroke-dasharray: 8 5; stroke-opacity: 0.6; }
.e-line--mail { stroke: var(--ink-2); stroke-width: 1.8; stroke-dasharray: 2 4.5; }
.e-knock { fill: var(--paper-2); }
.e-label { fill: var(--ink); font-family: var(--mono); font-size: 10px; letter-spacing: 0.09em; }
.e-label--weak { fill: var(--ink-2); }
.sw-fill { fill: var(--survey); }
.sw-ink { fill: var(--ink); }
.sw-hollow { fill: none; stroke: var(--ochre); stroke-width: 2; }
.edgekey {
  border-top: 1px solid var(--rule); padding: clamp(0.9rem, 3vw, 1.3rem);
  display: grid; grid-template-columns: minmax(0, 1fr); gap: 0.85rem;
}
@media (min-width: 44rem) { .edgekey { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
.edgekey .k { display: flex; flex-direction: column; gap: 0.35rem; min-width: 0; }
.edgekey .k .t { font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.11em; text-transform: uppercase; }
.edgekey .k p { margin: 0; font-size: 0.9em; color: var(--ink-2); }
.edgekey svg { display: block; height: 10px; width: 78px; min-width: 0; }
.netnote { padding: 0 clamp(0.9rem, 3vw, 1.3rem) clamp(0.9rem, 3vw, 1.3rem); }
.netnote p { margin: 0.4rem 0 0; font-size: 0.9em; color: var(--ink-2); max-width: 46rem; }

.foot { background: var(--paper-3); }
.footgrid { display: grid; grid-template-columns: minmax(0, 1fr); gap: 2rem; }
@media (min-width: 48rem) { .footgrid { grid-template-columns: 1.1fr 1fr; } }
.footgrid h3 {
  font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.16em;
  text-transform: uppercase; color: var(--ink-2); margin: 0 0 0.7rem;
}
.footgrid p { margin: 0 0 0.7rem; font-size: 0.95em; max-width: 34rem; }
.srclist { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 0.5rem; }
.srclist li {
  font-family: var(--mono); font-size: 0.75rem; color: var(--ink-2);
  font-variant-numeric: tabular-nums;
}
.srclist b { color: var(--ink); font-weight: 700; }

@media (prefers-reduced-motion: no-preference) {
  .chain .node .run { transform-origin: top; animation: drawdown 620ms ease-out both; }
  .chain > .rec { animation: liftin 460ms ease-out both; }
  .chain > .rec:nth-of-type(1) { animation-delay: 90ms; }
  .chain > .rec:nth-of-type(2) { animation-delay: 190ms; }
  .chain > .rec:nth-of-type(3) { animation-delay: 290ms; }
  .chain > .rec:nth-of-type(4) { animation-delay: 390ms; }
  .chain > .rec:nth-of-type(5) { animation-delay: 490ms; }
  @keyframes drawdown { from { transform: scaleY(0); } to { transform: scaleY(1); } }
  @keyframes liftin { from { opacity: 0; transform: translateY(7px); } to { opacity: 1; transform: none; } }
}
/* filtered browse furniture. No new visual system: the stroke still carries
   the state, the mono/serif split and the paper palette are unchanged. */
.navmark { display: flex; flex-wrap: wrap; gap: 0.2rem 1.05rem; }
.navmark a {
  font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.12em;
  text-transform: uppercase; color: var(--ink-2); text-decoration: none;
  padding-bottom: 2px; border-bottom: 2px solid transparent;
}
.navmark a:hover { color: var(--survey); }
.navmark a[aria-current="page"] { color: var(--ink); border-bottom-color: var(--survey); }

.facets {
  background: var(--paper-2); border: 1px solid var(--rule);
  padding: clamp(1rem, 3.4vw, 1.6rem); display: grid;
  grid-template-columns: minmax(0, 1fr); gap: 1.1rem;
}
@media (min-width: 40rem) { .facets { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (min-width: 62rem) { .facets { grid-template-columns: repeat(4, minmax(0, 1fr)); } }
.facets .fset { display: flex; flex-direction: column; gap: 0.35rem; min-width: 0; }
.facets .fset--wide { grid-column: 1 / -1; }
.facets label, .facets .flab {
  font-family: var(--mono); font-size: 0.625rem; letter-spacing: 0.13em;
  text-transform: uppercase; color: var(--ink-2);
}
.facets input, .facets select {
  font-family: var(--mono); font-size: 0.8125rem; background: var(--paper);
  color: var(--ink); border: 1px solid var(--ink); padding: 0.45rem 0.5rem;
  min-width: 0; width: 100%;
}
.facets select[multiple] { height: 8.5rem; }
.facets .pair { display: flex; gap: 0.4rem; }
.facets .hint { font-size: 0.8em; color: var(--ink-2); font-family: var(--serif); }
.facets .go {
  grid-column: 1 / -1; display: flex; flex-wrap: wrap; gap: 0.7rem;
  align-items: center; border-top: 1px solid var(--rule); padding-top: 1rem;
}
.facets .go a.btn { text-decoration: none; display: inline-block; }

.countline {
  font-family: var(--mono); margin: 1.5rem 0 0; display: flex; flex-wrap: wrap;
  align-items: baseline; gap: 0.6rem;
}
.countline b {
  font-size: clamp(1.5rem, 5vw, 2.2rem); letter-spacing: -0.03em;
  font-variant-numeric: tabular-nums;
}
.countline span { font-size: 0.8125rem; color: var(--ink-2); max-width: 44rem; }
thead th a { color: var(--ink-2); text-decoration: none; white-space: nowrap; }
thead th a:hover { color: var(--survey); }
thead th .sortmark { color: var(--survey); font-weight: 700; }
td .rk {
  font-weight: 700; color: var(--ink-2); font-variant-numeric: tabular-nums;
}

.statebar {
  display: grid; grid-template-columns: minmax(0, 1fr); gap: 1px;
  background: var(--rule); border: 1px solid var(--rule); margin-top: 1.2rem;
}
@media (min-width: 40rem) { .statebar { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (min-width: 62rem) { .statebar { grid-template-columns: repeat(4, minmax(0, 1fr)); } }
.statebar > div { background: var(--paper-2); padding: clamp(0.8rem, 2.6vw, 1.1rem); }
.statebar .v {
  display: block; font-family: var(--mono); font-weight: 700; line-height: 1;
  font-size: clamp(1.25rem, 4.4vw, 1.8rem); letter-spacing: -0.03em;
  font-variant-numeric: tabular-nums;
}
.statebar .k {
  display: block; font-family: var(--mono); font-size: 0.625rem;
  letter-spacing: 0.13em; text-transform: uppercase; color: var(--ink-2);
  margin-top: 0.45rem;
}
.statebar p { margin: 0.55rem 0 0; font-size: 0.9em; }
.jobs {
  display: grid; grid-template-columns: minmax(0, 1fr); gap: 1px;
  background: var(--rule); border: 1px solid var(--rule); margin-top: 1.6rem;
}
@media (min-width: 40rem) {
  .jobs { grid-template-columns: repeat(auto-fit, minmax(15rem, 1fr)); }
}
.job {
  background: var(--paper-2); padding: clamp(1rem, 3vw, 1.35rem);
  display: flex; flex-direction: column; gap: 0.6rem; min-width: 0;
}
.job .ix {
  font-family: var(--mono); font-size: 0.625rem; letter-spacing: 0.18em;
  color: var(--ink-2); border-bottom: 1px solid var(--rule);
  padding-bottom: 0.45rem; font-variant-numeric: tabular-nums;
}
.job h3 {
  font-family: var(--mono); font-size: 0.8125rem; letter-spacing: 0.03em;
  text-transform: uppercase; line-height: 1.35; margin: 0;
}
.job p { margin: 0; font-size: 0.93em; flex: 1 1 auto; }
.job a.go {
  font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.11em;
  text-transform: uppercase; text-decoration: none; align-self: flex-start;
  border-bottom: 2px solid var(--survey); padding-bottom: 2px;
}
.job a.go:hover { color: var(--ink); border-bottom-color: var(--ink); }
.skiplink { position: absolute; left: -9999px; }
.skiplink:focus {
  left: 1rem; top: 1rem; z-index: 5; background: var(--ink); color: var(--paper);
  padding: 0.5rem 0.8rem; font-family: var(--mono); font-size: 0.75rem;
}
"""

THEME_JS = r"""
(function () {
  var root = document.documentElement;
  var btn = document.getElementById("themebtn");
  if (!btn) return;
  function prefersDark() {
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  }
  function currentIsDark() {
    var set = root.getAttribute("data-theme");
    if (set === "dark") return true;
    if (set === "light") return false;
    return prefersDark();
  }
  function paint() {
    var dark = currentIsDark();
    btn.textContent = dark ? "Light mode" : "Dark mode";
    btn.setAttribute("aria-pressed", dark ? "true" : "false");
  }
  btn.addEventListener("click", function () {
    var next = currentIsDark() ? "light" : "dark";
    root.setAttribute("data-theme", next);
    try { localStorage.setItem("lm-theme", next); } catch (err) {}
    paint();
  });
  try {
    var saved = localStorage.getItem("lm-theme");
    if (saved === "dark" || saved === "light") root.setAttribute("data-theme", saved);
  } catch (err) {}
  paint();
})();
"""


# ---------------------------------------------------------------------------
# shared page furniture
# ---------------------------------------------------------------------------
def shell(title, body, skip="#main"):
    return (
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        "<title>%s</title><style>%s</style></head><body>"
        "<a class=\"skiplink\" href=\"%s\">Skip to the record</a>"
        "%s<script>%s</script></body></html>"
        % (e(title), CSS, e(skip), body, THEME_JS)
    )


# Labelled by the question each page answers. A reader who has never heard the
# word "rankings" still recognises "biggest landlords".
NAV = (("/", "Who owns my building"), ("/rankings", "Biggest landlords"),
       ("/explore", "Where I organize"), ("/method", "Can I trust this"),
       ("/health", "Load report"))


def topline(current=""):
    nav = "".join(
        "<a href=\"%s\"%s>%s</a>"
        % (h, " aria-current=\"page\"" if h == current else "", e(t))
        for h, t in NAV)
    return (
        "<div class=\"topline\">"
        "<div class=\"orgmark\"><a href=\"/\"><b>LANDLORD MAPPER</b> <span>/ OPEN AUSTIN</span></a></div>"
        "<nav class=\"navmark\" aria-label=\"Sections\">%s</nav>"
        "<button class=\"themebtn\" id=\"themebtn\" type=\"button\" aria-pressed=\"false\">Dark mode</button>"
        "</div>" % nav
    )


def county_names():
    return sorted(k.strip() for k in STORE.stats.get("counties", {}) if k.strip())


def counties_loaded():
    """Every county roll in memory, named. Used where the copy promises a list
    the reader can check an address against."""
    names = [k.title() for k in county_names()]
    if not names:
        return "no county"
    if len(names) == 1:
        return names[0]
    return ", ".join(names[:-1]) + " or " + names[-1]


def counties_phrase():
    """The short form, for a form label. A dozen county names do not belong in
    one, so past a handful it counts them instead of listing them."""
    names = county_names()
    if not names:
        return "no county roll"
    if len(names) == 1:
        return "the %s county roll" % names[0].title()
    if len(names) <= 3:
        return "the %s county rolls" % counties_loaded()
    return "any of the %s county rolls" % num(len(names))


def joined_across():
    names = county_names()
    if len(names) == 1:
        return "Joined across the %s roll" % names[0]
    return "Joined across %s county rolls" % num(len(names))


def parcel_link(i):
    """A parcel URL carries its county because a parcel ID does not identify a
    building on its own once more than one county roll is loaded."""
    rec = STORE.parcels[i]
    return "/parcel/%s/%s" % (
        urllib.parse.quote(rec[P["county"]].strip() or "unknown"),
        urllib.parse.quote(rec[P["situs_pID"]].strip()))


def lookup_form(value="", label=None):
    lbl = label or ("Street address in %s" % counties_phrase())
    return (
        "<form class=\"lookup\" action=\"/search\" method=\"get\">"
        "<label for=\"q\">%s</label>"
        "<div class=\"field\">"
        "<input id=\"q\" name=\"q\" type=\"text\" value=\"%s\" autocomplete=\"off\" "
        "placeholder=\"e.g. 1201 S LAMAR BLVD\" />"
        "<button class=\"btn\" type=\"submit\">Look up owner</button>"
        "</div></form>"
        % (e(lbl), e(value))
    )


def scope_note():
    st = STORE.stats
    return (
        "<p class=\"scopenote\">The registry lookup covers rentals: parcels the owner does "
        "not live in that the roll flags as investor-held, and any building over 5 units. "
        "That is %s of the %s parcels on the rolls, and the other %s were never going to be "
        "looked up. A parcel outside those rules says so on its own page, which is not the "
        "same as nobody owning it</p>"
        % (num(st.get("parcels_in_scope", 0)), num(st.get("parcel_rows", 0)),
           num(st.get("parcels_out_of_scope", 0)))
    )


def dates_strip():
    st = STORE.stats
    return (
        "<div class=\"datestrip\">"
        "<div>Appraisal rolls: <b>%s counties, %s parcels, %s in scope, roll year %s</b></div>"
        "<div>Registry lookup: <b>complete, newest answer %s</b></div>"
        "<div>Data read into this page: <b>%s</b></div>"
        "</div>"
        % (num(len(county_names())),
           num(st.get("parcel_rows", 0)),
           num(st.get("parcels_in_scope", 0)),
           e(roll_year()),
           e(st.get("scrape_newest_mtime", "n/a")),
           e(time.strftime("%Y-%m-%d %H:%M", time.localtime(STORE.loaded_at))))
    )


_ROLL_YEAR = [""]


def roll_year():
    """One year when every roll agrees, a range when they do not: the counties
    publish on their own schedules, so a dozen rolls need not share a year."""
    if not _ROLL_YEAR[0]:
        # digits only: a handful of rows carry NA in situs_year, and an NA is
        # not the far end of a range. The full tally, NA included, is on the
        # load report
        ys = sorted(y for y in STORE.stats.get("roll_years", {}) if y.isdigit())
        if len(ys) == 1:
            _ROLL_YEAR[0] = ys[0]
        elif ys:
            _ROLL_YEAR[0] = "%s to %s" % (ys[0], ys[-1])
    return _ROLL_YEAR[0] or "unknown"


def footer():
    st = STORE.stats
    states = st.get("owner_states", {})
    scoped = scope_den()
    rows_by_status = st.get("scrape_status_rows", {})
    counties = ", ".join(
        "%s %s" % (k, num(v)) for k, v in
        sorted(st.get("counties", {}).items(), key=lambda kv: -kv[1]))
    status_bits = " &middot; ".join(
        "%s %s" % (e(k or "blank"), num(v)) for k, v in
        sorted(rows_by_status.items(), key=lambda kv: -kv[1])) or "none yet"
    return (
        "<footer class=\"band foot\" aria-labelledby=\"foot-h\"><div class=\"wrap footgrid\">"
        "<div>"
        "<h3 id=\"foot-h\">Why there are two dates</h3>"
        "<p>The appraisal roll is published once a year, so ownership shown here is as of "
        "the roll and can lag a sale by months. The business registry read has finished, and "
        "it worked through owners one at a time. Officers change between those two dates, "
        "which is why each record on a chain carries its own stamp instead of one date for "
        "the whole page</p>"
        "<p>Two things this data does not have: mailing addresses for tax agents, which the "
        "counties do not publish in the roll, and reliable dates on deed transfers. Nothing "
        "here is built on either one. Unit counts are estimated from floor area, so they are "
        "marked as estimates everywhere they appear</p>"
        "<p>Some registry answers cannot be placed on these rolls, and those are held back "
        "rather than guessed at: %s rows name a parcel ID no roll loaded here carries, and %s "
        "more carry an ID whose candidate parcels all sit at a different address. %s IDs here "
        "are held by more than one county roll, which is why an answer only ever lands on the "
        "candidate whose address agrees. All three counts are on the load report</p>"
        "<p>Officer home addresses are in the source records and are deliberately not shown, "
        "and there is no search by person name. This tool answers who owns a building, not "
        "what a named human owns</p>"
        "</div><div>"
        "<h3>What is loaded right now</h3>"
        "<ul class=\"srclist\">"
        "<li><b>Appraisal rolls</b> &middot; %s parcels &middot; %s</li>"
        "<li><b>In the lookup scope</b> &middot; %s parcels, %s owners &middot; the rest of "
        "the roll was never queued</li>"
        "<li><b>Distinct owners on the whole roll</b> &middot; %s &middot; keyed on name plus "
        "mailing address</li>"
        "<li><b>Registry rows joined</b> &middot; %s of %s read, across %s parcels</li>"
        "<li><b>Rows by status</b> &middot; %s</li>"
        "<li><b>Owners matched</b> &middot; %s (%s%% of the %s in scope)</li>"
        "<li><b>Owners with no Texas filing</b> &middot; %s (%s%% of those in scope)</li>"
        "<li><b>Owners in scope not looked up yet</b> &middot; %s (%s%% of those in scope)</li>"
        "<li><b>Open source</b> &middot; open-austin/landlord-mapper</li>"
        "</ul>"
        "<p style=\"margin-top:1rem\"><a href=\"/method\">Where every number comes from"
        "</a> &middot; <a href=\"/health\">Full load report</a></p>"
        "</div></div></footer>"
        % (num(st.get("scrape_rows_no_parcel", 0)),
           num(st.get("scrape_rows_addr_clash", 0)),
           num(st.get("parcel_pids_shared", 0)),
           num(st.get("parcel_rows", 0)), e(counties),
           num(st.get("parcels_in_scope", 0)), num(st.get("owners_in_scope", 0)),
           num(st.get("owners", 0)),
           num(st.get("scrape_rows_joined", 0)), num(st.get("scrape_rows", 0)),
           num(st.get("scrape_parcels", 0)),
           status_bits,
           num(states.get(MATCHED, 0)), pct(states.get(MATCHED, 0), scoped),
           num(st.get("owners_in_scope", 0)),
           num(states.get(NO_RECORD, 0)), pct(states.get(NO_RECORD, 0), scoped),
           num(states.get(NOT_LOOKED_UP, 0) + states.get(NOT_RESOLVED, 0)),
           pct(states.get(NOT_LOOKED_UP, 0) + states.get(NOT_RESOLVED, 0), scoped))
    )


def pct(n, d):
    if not d:
        return "0.0"
    return "%.1f" % (100.0 * n / d)


def scope_den():
    """Owners the registry lookup was ever going to ask about. The honest
    denominator for coverage."""
    return max(1, STORE.stats.get("owners_in_scope", 1))


def parcel_state(i, o):
    """State to draw for one parcel. The owner's state, except that a parcel
    outside the coverage rules is reported as that rather than as a lookup still
    to come. An owner already matched through another, in-scope parcel keeps its
    filing: the filing belongs to the name, not to the building."""
    state = o.get("state", NOT_LOOKED_UP)
    if state in (NOT_LOOKED_UP, OUT_OF_SCOPE) and not STORE.in_scope[i]:
        return OUT_OF_SCOPE
    return state


def legend_band():
    st = STORE.stats
    states = st.get("owner_states", {})
    scoped = scope_den()
    unknown = states.get(NOT_LOOKED_UP, 0) + states.get(NOT_RESOLVED, 0)
    return (
        "<section class=\"band legendband\" aria-labelledby=\"legend-h\"><div class=\"wrap\">"
        "<h2 class=\"eyebrow\" id=\"legend-h\">How to read the end of a chain</h2>"
        "<p class=\"prose\" style=\"margin:0.9rem 0 0\">A chain can end three ways, and the "
        "difference matters. Two of them are answers. One of them is a gap in what we know, "
        "and it is drawn as a gap so you never mistake it for an answer</p>"
        "<div class=\"endings\">"
        "<div class=\"ending\"><div class=\"glyph g--matched\" aria-hidden=\"true\">"
        "<span class=\"g-mark\"></span><span class=\"g-run\"></span><span class=\"g-term\"></span></div>"
        "<div class=\"body\"><h3>Matched</h3>"
        "<p>The name on the roll lines up with a Texas business filing. We show both names "
        "side by side so you can reject a bad match yourself</p>"
        "<p class=\"ex\">%s owners of the %s inside the coverage rules, %s%% so far</p></div></div>"
        "<div class=\"ending\"><div class=\"glyph g--norec\" aria-hidden=\"true\">"
        "<span class=\"g-mark\"></span><span class=\"g-run\"></span><span class=\"g-term\"></span></div>"
        "<div class=\"body\"><h3>No record</h3>"
        "<p>We searched and Texas has no business registration under this name. That is a "
        "finding, not a miss: plenty of rentals are held by a person or an out-of-state "
        "entity that never registered here. The line stops on a hard bar because the search "
        "finished</p>"
        "<p class=\"ex\">%s owners so far</p></div></div>"
        "<div class=\"ending\"><div class=\"glyph g--unknown\" aria-hidden=\"true\">"
        "<span class=\"g-mark\"></span><span class=\"g-run\"></span><span class=\"g-term\"></span></div>"
        "<div class=\"body\"><h3>No answer</h3>"
        "<p>No usable answer came back for this name. Almost all of these are lookups the "
        "registry rejected outright, which is our query failing, not Texas reporting that "
        "nothing is filed. That claim is the middle column, and this is not it. We do not "
        "know either way here, so the line trails off dashed and open</p>"
        "<p class=\"ex\">%s owners in scope: rejected lookup or never queried</p></div></div>"
        "</div>"
        "<p class=\"prose\" style=\"margin:1.6rem 0 0\">One more case borrows that same dashed "
        "ending: a parcel outside the coverage rules. The registry was never asked about it, on "
        "purpose, so the chain is a gap here too, and the page names the rule that put it "
        "outside instead of implying an answer is on its way. %s of the %s parcels on the rolls "
        "are inside the rules</p>"
        "<div class=\"sharenote\"><span class=\"big\">%s%%</span>"
        "<p>of the %s owners inside the coverage rules have no registry answer. The scrape has "
        "finished, so these are not owners waiting in a queue: almost all of them are lookups "
        "the registry rejected. Read them as unknown, never as unregistered</p></div>"
        "</div></section>"
        % (num(states.get(MATCHED, 0)), num(st.get("owners_in_scope", 0)),
           pct(states.get(MATCHED, 0), scoped),
           num(states.get(NO_RECORD, 0)),
           num(unknown),
           num(st.get("parcels_in_scope", 0)), num(st.get("parcel_rows", 0)),
           pct(unknown, scoped), num(st.get("owners_in_scope", 0)))
    )


# ---------------------------------------------------------------------------
# page: home
# ---------------------------------------------------------------------------
JOBS = (
    ("01", "Who owns my building, really?",
     "Put in a street address. You get the name on the county appraisal roll, "
     "then the Texas business filing behind that name, then the people who "
     "signed for it, with the source and the date on every step",
     "#lookup-h", "Start with an address"),
    ("02", "Who are the biggest landlords here?",
     "Owners ranked by how many parcels they hold, how many units those come to, "
     "and what the roll says they are worth. This is the table a campaign picks a "
     "target from, and it counts the rental part of the roll only",
     "/rankings", "See the ranked list"),
    ("03", "Narrow it to where I organize",
     "Filter by county, ZIP, building size, roll value, whether the tax bill "
     "leaves Texas, and the roll's own investor-held and owner-occupied flags. "
     "Every filter lives in the address bar, so the view you build is a link",
     "/explore", "Filter the rolls"),
    ("04", "Give me the list",
     "Any filtered view, any ranking, and any single landlord's portfolio leaves "
     "here as a CSV, so a canvass list can go into the field instead of staying "
     "on a screen. Filter first, then take the download",
     "/explore", "Build a list to download"),
    ("05", "Can I trust this number?",
     "Where the parcel data comes from, which county rolls and which roll year, "
     "what \"in scope\" means stated as the actual rule, and what each of the three "
     "match states does and does not claim. One page to hand a skeptic",
     "/method", "Read the method"),
)


def jobs_band():
    cards = "".join(
        "<div class=\"job\"><span class=\"ix\">%s</span><h3>%s</h3><p>%s</p>"
        "<a class=\"go\" href=\"%s\">%s &rarr;</a></div>"
        % (e(ix), e(title), copy, e(href), e(cta))
        for ix, title, copy, href, cta in JOBS)
    return (
        "<section class=\"band\" aria-labelledby=\"jobs-h\"><div class=\"wrap\">"
        "<h2 class=\"eyebrow\" id=\"jobs-h\">What this tool answers</h2>"
        "<p class=\"prose\" style=\"margin:0.9rem 0 0\">Five questions, in the order an "
        "organizer usually needs them. Each one is a page, and each page states the "
        "population its numbers are counted against</p>"
        "<div class=\"jobs\">%s</div>"
        "</div></section>" % cards)


def page_home():
    st = STORE.stats
    body = [
        "<header class=\"wrap masthead\">", topline("/"),
        "<h1>Who owns<br />the building<br /><em>you rent</em></h1>",
        "<p class=\"deck\">Start with an address. The county appraisal roll gives you the "
        "name on the property, and that name is usually an LLC. The state franchise tax "
        "registry gives you the people who signed for that LLC. This page joins the two "
        "records and shows you both, with the source and the date on every step, so you "
        "can check the work yourself</p>",
        "<p class=\"stamp\">The registry lookup has run to completion. It answered for %s of the "
        "%s owners inside the coverage rules, out of %s owners on the whole roll. Every owner "
        "without an answer is drawn as a gap, never as a clean record</p>"
        % (num(st.get("owners_in_scope_answered", 0)),
           num(st.get("owners_in_scope", 0)), num(st.get("owners", 0))),
        dates_strip(),
        "</header>",
        "<section class=\"wrap band\" id=\"main\" aria-labelledby=\"lookup-h\" style=\"padding-top:0\">",
        "<h2 class=\"eyebrow\" id=\"lookup-h\">Address lookup</h2>",
        "<div style=\"margin-top:0.9rem\">", lookup_form(), "</div>",
        scope_note(),
        "</section>",
        jobs_band(),
        legend_band(),
        footer(),
    ]
    return shell("Landlord Mapper - who owns your building", "".join(body))


# ---------------------------------------------------------------------------
# page: search results
# ---------------------------------------------------------------------------
def hit_rows(window):
    """The result rows shared by the address search and the ambiguous-ID
    chooser. Both are the same question: which of these parcels did you mean"""
    warm_owners_for(window)
    rows = []
    for i in window:
        rec = STORE.parcels[i]
        o = STORE.owner_for_parcel(i)
        state = parcel_state(i, o)
        rows.append(
            "<tr>"
            "<td><a href=\"%s\">%s</a></td>"
            "<td><span class=\"cty\">%s</span></td>"
            "<td>%s</td>"
            "<td class=\"r\">%s</td>"
            "<td class=\"r\">%s</td>"
            "<td>%s</td>"
            "<td><span class=\"chip %s\">%s</span></td>"
            "</tr>"
            % (parcel_link(i), e(rec[P["situs_address"]]),
               e(rec[P["county"]]), e(rec[P["situs_pID"]]),
               money(rec[P["totalpropmktvalue"]]),
               num(rec[P["totalsqftlivingarea"]]),
               e(rec[P["owner_name"]]),
               STATE_CHIP[state], e(STATE_LABEL[state])))
    return "".join(rows)


HIT_HEAD = ("<thead><tr><th scope=\"col\">Address</th><th scope=\"col\">County</th>"
            "<th scope=\"col\">Parcel ID</th><th scope=\"col\" class=\"r\">Market value</th>"
            "<th scope=\"col\" class=\"r\">Sq ft living area</th>"
            "<th scope=\"col\">Owner on the roll</th>"
            "<th scope=\"col\">Registry</th></tr></thead>")


def page_search(q, page):
    hits = STORE.search(q)
    if not hits:
        return page_no_hits(q)
    if len(hits) == 1:
        return None, hits[0]
    total = len(hits)
    pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    page = max(1, min(page, pages))
    window = hits[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]
    rows = hit_rows(window)
    prev_cls = "btn btn-quiet" + ("" if page > 1 else " btn-off")
    next_cls = "btn btn-quiet" + ("" if page < pages else " btn-off")
    qq = urllib.parse.quote(q)
    pager = (
        "<div class=\"pager\">"
        "<a class=\"%s\" href=\"/search?q=%s&amp;page=%d\">Previous</a>"
        "<span>Page %d of %d &middot; %s parcels matched%s</span>"
        "<a class=\"%s\" href=\"/search?q=%s&amp;page=%d\">Next</a>"
        "</div>"
        % (prev_cls, qq, max(1, page - 1), page, pages, num(total),
           " (capped at %s)" % num(MAX_HITS) if total >= MAX_HITS else "",
           next_cls, qq, min(pages, page + 1)))
    body = [
        "<header class=\"wrap masthead\">", topline(),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">%s parcels<br />match <em>%s</em></h1>"
        % (num(total), e(q.upper())),
        "<div style=\"margin-top:1.8rem\">", lookup_form(q), "</div>",
        scope_note(),
        "</header>",
        "<section class=\"wrap band\" id=\"main\" aria-labelledby=\"res-h\" style=\"padding-top:0\">",
        "<h3 class=\"subhead\" id=\"res-h\">Pick the parcel you meant</h3>",
        "<div class=\"tablescroll\"><table>", HIT_HEAD,
        "<tbody>", rows, "</tbody></table></div>",
        "<p class=\"tblnote\">Market value is the county value on the roll, not a sale price "
        "&middot; unit counts are estimates and are not shown in this list</p>",
        pager,
        "</section>",
        footer(),
    ]
    return shell("%s parcels match %s - Landlord Mapper" % (total, q), "".join(body)), None


def page_no_hits(q):
    body = [
        "<header class=\"wrap masthead\">", topline(),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">Nothing matched<br /><em>that address</em></h1>",
        "<div style=\"margin-top:1.8rem\">", lookup_form(q), "</div>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" style=\"padding-top:0\">",
        "<div class=\"empty\"><h3>No parcel on the rolls contains %s</h3>"
        "<p>The likely cause is that the address sits outside the county rolls loaded here. "
        "Those are %s. Every parcel on them is searchable, in scope for the registry lookup or "
        "not, so being owner-occupied or small is not what keeps an address out of this list</p>"
        "<p>Try the street number on its own, or the street name on its own. The match is a "
        "plain substring on the address as the county wrote it, so BLVD and BOULEVARD are "
        "not the same string</p></div>"
        % (e(q.upper()),
           e(", ".join(sorted(STORE.stats.get("counties", {}))) or "none loaded")),
        scope_note(),
        "</section>",
        footer(),
    ]
    return shell("Nothing matched - Landlord Mapper", "".join(body)), None


# ---------------------------------------------------------------------------
# page: the ownership chain
# ---------------------------------------------------------------------------
def node(cls=""):
    return ("<div class=\"node%s\" aria-hidden=\"true\"><span class=\"run\"></span>"
            "<span class=\"tick\"></span><span class=\"mark\"></span></div>" % cls)


def page_pid_choice(pid_raw, cands):
    """One parcel ID, several buildings. The rolls number their parcels
    independently, so an ID with no county on it is a question, not an answer,
    and it is asked with the same table the address search uses."""
    ctys = ", ".join(sorted(set(STORE.parcels[i][P["county"]] for i in cands)))
    body = [
        "<header class=\"wrap masthead\">", topline(),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">%s rolls carry<br />parcel "
        "<em>%s</em></h1>" % (num(len(cands)), e(pid_raw)),
        "<p class=\"deck\">Each county numbers its own parcels, so this ID is carried by a "
        "different building in each of %s. Pick the county you meant. Nothing here is a "
        "duplicate record, and none of them is the same property</p>" % e(ctys),
        "<div style=\"margin-top:1.8rem\">", lookup_form(), "</div>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" aria-labelledby=\"amb-h\" style=\"padding-top:0\">",
        "<h3 class=\"subhead\" id=\"amb-h\">Pick the parcel you meant</h3>",
        "<div class=\"tablescroll\"><table>", HIT_HEAD,
        "<tbody>", hit_rows(cands), "</tbody></table></div>",
        "<p class=\"tblnote\">Market value is the county value on the roll, not a sale price "
        "&middot; unit counts are estimates and are not shown in this list</p>",
        "</section>",
        footer(),
    ]
    return shell("Parcel %s - Landlord Mapper" % pid_raw, "".join(body))


def page_parcel(i):
    rec = STORE.parcels[i]
    o = STORE.owner_for_parcel(i)
    state = parcel_state(i, o)
    fl = STORE.filings.get(o["id"]) or {}
    tot = STORE.owner_totals(o)
    state_node = STATE_NODE[state]

    sqft = to_int(rec[P["totalsqftlivingarea"]])
    units = to_int(rec[P["property_units"]])
    acquired = datestamp(rec[P["recent_purchase_date"]])
    roll_mail = norm_txt(rec[P["owner_address"]])
    corp_mail = norm_txt(fl.get("mail"))

    out = ["<header class=\"wrap masthead\">", topline(),
           "<div style=\"margin-top:1.6rem\">", lookup_form(), "</div>",
           "</header>",
           "<section class=\"wrap band\" id=\"main\" aria-labelledby=\"chain-h\" style=\"padding-top:0\">",
           "<h2 class=\"eyebrow\" id=\"chain-h\">Ownership chain &nbsp;/&nbsp; %s</h2>"
           % e(rec[P["situs_address"]]),
           "<div class=\"chain\" style=\"margin-top:1.4rem\">"]

    # ---- 1. the property -------------------------------------------------
    prop_rows = [
        "<div style=\"grid-column:1/-1\"><dt>Address</dt><dd>%s</dd></div>"
        % e(rec[P["situs_address"]]),
        "<div><dt>Market value on the roll</dt><dd>%s</dd></div>"
        % money(rec[P["totalpropmktvalue"]]),
        "<div><dt>Living area</dt><dd>%s sq ft</dd></div>" % num(sqft),
        "<div><dt>Parcel ID</dt><dd>%s</dd></div>" % e(rec[P["situs_pID"]]),
        "<div><dt>Built</dt><dd>%s</dd></div>" % e(dash(rec[P["year_built"]])),
    ]
    if acquired:
        prop_rows.append("<div><dt>Acquired</dt><dd>%s</dd></div>" % e(acquired))
    prop_rows.append(
        "<div><dt>State class code</dt><dd>%s</dd></div>" % e(dash(rec[P["state_code"]])))
    prop_rows.append(
        "<div style=\"grid-column:1/-1\"><dt>Legal description, as written on the roll</dt>"
        "<dd class=\"raw\">%s</dd></div>" % e(dash(rec[P["legallocationdesc"]])))
    out += [
        node(), "<article class=\"rec\">",
        "<div class=\"rechead\"><span class=\"eyebrow\">County appraisal roll</span></div>",
        "<h3>The property</h3>",
        "<dl class=\"dl dl--2\">", "".join(prop_rows), "</dl>",
        "<p class=\"tell tell--quiet\">Roughly <b>%s units</b>, and that is an estimate, not a "
        "count. The roll does not publish a unit count for most buildings, so this figure is "
        "the floor area divided by 900 square feet, corrected by class code only for houses "
        "and small duplex-to-fourplex classes. It is the same number as the living area above, "
        "divided. Treat it as a size band, not a fact</p>" % num(units),
        "<p class=\"srcstamp\">Source: %s county appraisal roll &middot; roll year %s "
        "&middot; class %s &middot; owner-occupied flag: %s &middot; in the registry lookup "
        "scope: %s</p>"
        % (e(rec[P["county"]]), e(rec[P["situs_year"]]), e(dash(rec[P["state_code"]])),
           "yes" if is_true(rec[P["is_owner_occupied"]]) else "no",
           "yes" if STORE.in_scope[i] else "no"),
        "</article>",
    ]

    # ---- 2. the name on the roll ----------------------------------------
    owner_rows = [
        "<div style=\"grid-column:1/-1\"><dt>Owner of record</dt>"
        "<dd style=\"font-weight:700\">%s</dd></div>" % e(rec[P["owner_name"]]),
        "<div><dt>Where the tax bill is mailed</dt><dd>%s</dd></div>"
        % e(dash(rec[P["owner_address"]])),
    ]
    agent = (rec[P["agent_name"]] or "").strip()
    if agent:
        owner_rows.append(
            "<div><dt>Tax agent of record</dt><dd>%s</dd></div>" % e(agent))
    out += [
        node(), "<article class=\"rec\">",
        "<div class=\"rechead\"><span class=\"eyebrow\">County appraisal roll</span></div>",
        "<h3>The name on the roll</h3>",
        "<dl class=\"dl dl--2\">", "".join(owner_rows), "</dl>",
    ]
    if is_true(rec[P["is_owner_out_of_state"]]):
        out.append(
            "<p class=\"tell\">The building is here. The tax bill goes out of state. That gap "
            "between where a property sits and where its mail lands is often the first sign "
            "you are dealing with an investor and not a neighbor</p>")
    out += [
        "<p class=\"srcstamp\">Source: %s county appraisal roll &middot; roll year %s "
        "&middot; the roll lists no mailing address for the tax agent</p>"
        % (e(rec[P["county"]]), e(rec[P["situs_year"]])),
        "</article>",
    ]

    # ---- 3. the registry step, state-dependent ---------------------------
    out += [node(state_node), "<article class=\"rec\">",
            "<div class=\"rechead\">"
            "<span class=\"eyebrow\">Texas franchise tax registry</span>"
            "<span class=\"chip %s\">%s</span></div>"
            % (STATE_CHIP[state], e(STATE_LABEL[state]))]
    if state == MATCHED:
        out.append("<h3>The business filing behind that name</h3>")
        out.append(
            "<div class=\"matchcheck\">"
            "<div><span class=\"hd\">Name we searched</span>"
            "<span class=\"val\">%s</span></div>"
            "<div><span class=\"hd\">Filing we matched</span>"
            "<span class=\"val hit\">%s</span></div></div>"
            % (e(rec[P["owner_name"]]), e(fl.get("corp_name"))))
        frows = [
            "<div><dt>Taxpayer number</dt><dd>%s</dd></div>" % e(dash(fl.get("ttn"))),
            "<div><dt>Right to transact business</dt><dd>%s</dd></div>" % e(dash(fl.get("rtt"))),
            "<div><dt>Secretary of State status</dt><dd>%s</dd></div>" % e(dash(fl.get("sos_status"))),
            "<div><dt>Effective registration</dt><dd>%s</dd></div>" % e(dash(sosdate(fl.get("sos_date")))),
            "<div><dt>State of formation</dt><dd>%s</dd></div>" % e(dash(fl.get("formation"))),
            "<div><dt>Texas SOS file number</dt><dd>%s</dd></div>" % e(dash(fl.get("file_num"))),
        ]
        same = ""
        if corp_mail and corp_mail == roll_mail:
            same = ("<span style=\"color:var(--survey)\">&nbsp;&larr; same address as the "
                    "tax bill</span>")
        frows.append(
            "<div style=\"grid-column:1/-1\"><dt>Filing mailing address</dt><dd>%s%s</dd></div>"
            % (e(dash(fl.get("mail"))), same))
        out.append("<dl class=\"dl dl--2\" style=\"margin-top:1rem\">%s</dl>" % "".join(frows))
        out.append(
            "<p class=\"srcstamp\">Source: Texas Comptroller franchise tax registry "
            "&middot; matched on the owner name as printed on the roll &middot; %s registry "
            "rows carry this owner</p>" % num(fl.get("queried_rows", 0)))
    elif state == NO_RECORD:
        out.append("<h3>Texas has no business filing under that name</h3>")
        out.append(
            "<div class=\"matchcheck\">"
            "<div><span class=\"hd\">Name we searched</span>"
            "<span class=\"val\">%s</span></div>"
            "<div><span class=\"hd\">Filing we matched</span>"
            "<span class=\"val\">none</span></div></div>"
            % e(rec[P["owner_name"]]))
        out.append(
            "<p class=\"tell\">This is a finding, not a miss. The registry answered, and the "
            "answer was that nothing is filed in Texas under this name. Plenty of rentals are "
            "held by a person under their own name, by a trust, or by an out-of-state company "
            "that never registered here. The chain stops on a hard bar because the search "
            "finished</p>")
        out.append(
            "<p class=\"srcstamp\">Source: Texas Comptroller franchise tax registry "
            "&middot; searched and returned nothing &middot; %s registry rows carry this "
            "owner</p>" % num(fl.get("queried_rows", 0)))
    elif state == OUT_OF_SCOPE:
        st = STORE.stats
        why = {
            SCOPE_OCCUPIED: "the roll flags it owner-occupied",
            SCOPE_NOSIZE: "the roll carries no living area for it, so there is no size "
                          "to measure it by",
        }.get(scope_reason(rec),
              "the roll neither flags it investor-held nor puts it over 5 units")
        out.append("<h3>Outside the coverage rules</h3>")
        out.append(
            "<p class=\"tell\">The registry was never asked about this one, because %s. That is "
            "a rule about what this tool covers, not a lookup still to come, and not a finding "
            "either. The rules take parcels the owner does not live in that the roll flags as "
            "investor-held, plus any building over 5 units, which is %s of the %s parcels on "
            "the rolls</p>" % (why, num(st.get("parcels_in_scope", 0)),
                              num(st.get("parcel_rows", 0))))
        if o.get("in_scope"):
            out.append(
                "<p class=\"tell tell--quiet\">Other parcels on the rolls under this same name "
                "and mailing address are inside the rules, and the registry has not answered "
                "for them yet either. The landlord profile below carries whatever arrives</p>")
        out.append(
            "<p class=\"srcstamp\">Source: Texas Comptroller franchise tax registry "
            "&middot; outside the lookup scope, never queried &middot; reloading will not "
            "change this one</p>")
    else:
        looked = bool(fl.get("queried_rows"))
        out.append("<h3>%s</h3>"
                   % ("Our lookup was rejected" if looked
                      else "Not looked up yet"))
        out.append(
            "<p class=\"tell\">%s We do not know whether this name has a Texas filing, so the "
            "chain trails off dashed and open. Do not read it as a clean record, and do not "
            "read it as an absence either</p>"
            % ("The registry rejected our lookup for this name and returned nothing usable. "
               "That is our query failing, not Texas reporting that nothing is filed under "
               "the name." if looked else
               "The registry scrape has not reached this owner yet. It works through owners "
               "one at a time and is running right now."))
        out.append(
            "<p class=\"srcstamp\">Source: Texas Comptroller franchise tax registry "
            "&middot; %s &middot; reload this page later to pick up the answer</p>"
            % ("no usable answer on file" if looked else "not yet queried"))
    out.append("</article>")

    # ---- 4. officers, only when there is a filing -------------------------
    if state == MATCHED:
        officers = fl.get("officers") or []
        out += [node(state_node), "<article class=\"rec\">",
                "<div class=\"rechead\">"
                "<span class=\"eyebrow\">Texas franchise tax registry</span></div>"]
        if officers:
            out.append("<h3>The people who signed for it</h3>")
            cells = ["<div><dt>%s</dt><dd>%s</dd></div>"
                     % (e(title_case(of["title"])), e(of["name"])) for of in officers]
            if fl.get("agent"):
                cells.append("<div><dt>Registered agent</dt><dd>%s</dd></div>"
                             % e(fl["agent"]))
            out.append("<dl class=\"dl dl--2\">%s</dl>" % "".join(cells))
            out.append(
                "<p class=\"tell\">These are the names an organizer can put on a letter, a "
                "flyer, or a city council sign-up sheet. Everything above this line is a "
                "company. This line is people</p>")
            out.append(
                "<p class=\"srcstamp\">Source: Texas Comptroller franchise tax registry "
                "&middot; officers and directors as listed &middot; home addresses are in "
                "the filing and are not shown here</p>")
        else:
            out.append("<h3>The filing names no officers</h3>")
            out.append(
                "<p class=\"tell tell--quiet\">The registry returned the company but listed "
                "no officers or directors under it. The registered agent below is a hired "
                "filing service, not an owner, so it is not a name to hold responsible</p>")
            out.append(
                "<dl class=\"dl dl--2\"><div><dt>Registered agent</dt><dd>%s</dd></div></dl>"
                % e(dash(fl.get("agent"))))
            out.append(
                "<p class=\"srcstamp\">Source: Texas Comptroller franchise tax registry "
                "&middot; no officer rows returned for this filing</p>")
        out.append("</article>")

    # ---- 5. the payload --------------------------------------------------
    pay_cls = {MATCHED: "payload", NO_RECORD: "payload payload--stop"}.get(
        state, "payload payload--open")
    who = fl.get("corp_name") or rec[P["owner_name"]]
    out += [node(state_node + " node--end"), "<article class=\"rec\">",
            "<div class=\"rechead\"><span class=\"eyebrow\">%s</span></div>"
            % e(joined_across()),
            "<h3>What this landlord holds</h3>",
            "<div class=\"%s\">" % pay_cls,
            "<span class=\"who\">%s</span>" % e(who),
            "<div class=\"figs\">"
            "<div class=\"fig\"><span class=\"v\">%s</span><span class=\"k\">Market value</span></div>"
            "<div class=\"fig\"><span class=\"v\">%s</span><span class=\"k\">Properties</span></div>"
            "<div class=\"fig\"><span class=\"v\">%s</span><span class=\"k\">Sq ft living area</span></div>"
            "<div class=\"fig\"><span class=\"v\">&asymp;%s</span>"
            "<span class=\"k\">Units <span class=\"approx\">estimated</span></span></div>"
            "</div>" % (money(tot["value"]), num(tot["count"]), num(tot["sqft"]),
                        num(tot["units"])),
            ]
    if tot["count"] > 1:
        out.append(
            "<p style=\"margin:1.1rem 0 0;max-width:34rem\">This building is one of %s held "
            "under the same name and mailing address on the rolls</p>" % num(tot["count"]))
    else:
        out.append(
            "<p style=\"margin:1.1rem 0 0;max-width:34rem\">This is the only parcel on the "
            "rolls under this name and mailing address</p>")
    out += [
        "<p style=\"margin:1rem 0 0\"><a class=\"btn\" href=\"/owner/%s\" "
        "style=\"display:inline-block;text-decoration:none\">See the landlord profile</a></p>"
        % e(o["id"]),
        "</div>",
        "<p class=\"srcstamp\">Owner key: name plus mailing address, matched across the "
        "county appraisal rolls &middot; roll year %s</p>" % e(rec[P["situs_year"]]),
        "</article>",
        "</div>",  # .chain
        "</section>",
        legend_band(),
        footer(),
    ]
    return shell("%s - Landlord Mapper" % rec[P["situs_address"]], "".join(out))


# ---------------------------------------------------------------------------
# page: landlord profile
# ---------------------------------------------------------------------------
def page_owner(oid, qs=None):
    o = STORE.owners.get(oid)
    if o is None:
        return None
    state = o.get("state", NOT_LOOKED_UP)
    fl = STORE.filings.get(oid) or {}
    tot = STORE.owner_totals(o)
    # the table is sortable, and the sort lives in the query string like every
    # other view on this site, so a colleague opening the link sees the same rows
    f = Filt.from_qs(qs or {})
    idxs = owner_parcels_page(oid, f, 500)

    rows = []
    for i in idxs[:500]:
        rec = STORE.parcels[i]
        rows.append(
            "<tr>"
            "<td><a href=\"%s\">%s</a></td>"
            "<td>%s</td>"
            "<td><span class=\"cty\">%s</span></td>"
            "<td class=\"r\">%s</td>"
            "<td class=\"r\">%s</td>"
            "<td class=\"r\">%s</td>"
            "<td class=\"r\">%s</td>"
            "</tr>"
            % (parcel_link(i), e(rec[P["situs_pID"]]),
               e(rec[P["situs_address"]]), e(rec[P["county"]]),
               money(rec[P["totalpropmktvalue"]]),
               num(rec[P["totalsqftlivingarea"]]),
               e(dash(rec[P["year_built"]])),
               e(datestamp(rec[P["recent_purchase_date"]]) or "not on the roll")))

    alias_bits = []
    if fl.get("corp_name"):
        alias_bits.append("Filed as %s" % e(fl["corp_name"]))
    if fl.get("ttn"):
        alias_bits.append("taxpayer %s" % e(fl["ttn"]))
    alias_bits.append("on the county rolls as %s" % e(o["name"]))
    alias_bits.append("mail to %s" % e(dash(o["address"])))

    out = ["<header class=\"wrap masthead\">", topline(),
           "<div style=\"margin-top:1.6rem\">", lookup_form(), "</div>",
           "</header>",
           "<section class=\"wrap band\" id=\"main\" aria-labelledby=\"prof-h\" style=\"padding-top:0\">",
           "<div class=\"profhead\">",
           "<span class=\"eyebrow\">Landlord profile</span>",
           "<h2 id=\"prof-h\">%s</h2>" % e(title_case(fl.get("corp_name") or o["name"])),
           "<span class=\"alias\">%s</span>" % " &middot; ".join(alias_bits),
           "<span class=\"chip %s\">%s</span>" % (
               STATE_CHIP[state],
               "Matched to a Texas filing" if state == MATCHED
               else ("No Texas filing under this name" if state == NO_RECORD
                     else ("Outside the registry lookup scope"
                           if state == OUT_OF_SCOPE
                           else ("Our registry lookup was rejected"
                                 if state == NOT_RESOLVED
                                 else "No registry row for this owner")))),
           "</div>",
           "<div class=\"headfigs\">"
           "<div class=\"cell\"><span class=\"v\">%s</span><span class=\"k\">Market value</span></div>"
           "<div class=\"cell\"><span class=\"v\">%s</span><span class=\"k\">Properties</span></div>"
           "<div class=\"cell\"><span class=\"v\">%s</span><span class=\"k\">Sq ft living area</span></div>"
           "<div class=\"cell cell--soft\"><span class=\"v\">&asymp;%s</span>"
           "<span class=\"k\">Units <span class=\"approx\">estimated from floor area</span></span></div>"
           "</div>" % (money(tot["value"]), num(tot["count"]), num(tot["sqft"]),
                       num(tot["units"])),
           "<p class=\"tblnote\">The unit figure is the only estimate in that row. It is "
           "floor area divided by 900 square feet, so it is the third figure restated, not a "
           "fourth fact</p>",
           owner_profile_band(o, tot),
           owner_filing_band(oid, state, fl, o),
           "<h3 class=\"subhead\">Every property on the rolls under this owner</h3>",
           "<div class=\"tablescroll\"><table>",
           "<caption class=\"skiplink\">%s parcels held by this owner, with size, age, "
           "acquisition date, and market value</caption>" % num(tot["count"]),
           owner_table_head(oid, f),
           "<tbody>", "".join(rows), "</tbody>",
           "<tfoot><tr><td colspan=\"3\">%s properties</td><td class=\"r\">%s</td>"
           "<td class=\"r\">%s</td><td class=\"r\"></td><td class=\"r\"></td></tr></tfoot>"
           % (num(tot["count"]), money(tot["value"]), num(tot["sqft"])),
           "</table></div>",
           "<p class=\"tblnote\">Values are the county market value on the roll, not a sale "
           "price &middot; roll year %s%s</p>"
           % (e(roll_year()),
              " &middot; showing the first 500 of %s in the current order, and the CSV "
              "below carries all of them" % num(tot["count"])
              if tot["count"] > 500 else ""),
           ]

    if state == MATCHED and fl.get("officers"):
        cells = ["<div><dt>%s</dt><dd>%s</dd></div>"
                 % (e(title_case(of["title"])), e(of["name"])) for of in fl["officers"]]
        out += ["<h3 class=\"subhead\">The people who signed for the filing</h3>",
                "<div style=\"border:1px solid var(--rule);border-top:0;background:var(--paper-2);"
                "padding:clamp(0.9rem,3vw,1.3rem)\">",
                "<dl class=\"dl dl--2\">%s</dl>" % "".join(cells),
                "<p class=\"srcstamp\">Source: Texas Comptroller franchise tax registry "
                "&middot; officer home addresses are in the filing and are not shown, and "
                "there is no search by person name here</p>",
                "</div>"]

    out.append("<h3 class=\"subhead\">Companies linked to this one</h3>")
    out.append(network_panel(oid, state, fl))
    out += ["</section>", legend_band(), footer()]
    return shell("%s - Landlord Mapper" % (fl.get("corp_name") or o["name"]),
                 "".join(out))


# ---------------------------------------------------------------------------
# the shell network
# ---------------------------------------------------------------------------
EDGE_CLASS = {"officer": "e-line--officer", "agent": "e-line--agent",
              "mail": "e-line--mail"}
EDGE_TEXT = {"officer": "SHARED OFFICER", "agent": "SHARED REGISTERED AGENT",
             "mail": "SHARED MAILING ADDRESS"}


def wrap_name(s, width=24, lines=2):
    words = (s or "").split()
    out, cur = [], ""
    for w in words:
        cand = (cur + " " + w).strip()
        if len(cand) > width and cur:
            out.append(cur)
            cur = w
        else:
            cur = cand
        if len(out) == lines:
            break
    if cur and len(out) < lines:
        out.append(cur)
    if not out:
        out = ["(unnamed)"]
    if len(out) == lines and len(" ".join(words)) > sum(len(x) for x in out) + 1:
        out[-1] = out[-1][:width - 1] + "…"
    return out


def net_box(x, y, w, oid, label, state, sub, focus=False):
    lines = wrap_name(label)
    sw = {MATCHED: "sw-fill", NO_RECORD: "sw-ink"}.get(state, "sw-hollow")
    parts = ["<a class=\"n-link\" href=\"/owner/%s\">" % e(oid) if not focus else ""]
    parts.append("<rect class=\"n-box%s\" x=\"%d\" y=\"%d\" width=\"%d\" height=\"80\"%s />"
                 % (" n-box--focus" if focus else "", x, y, w,
                    " stroke-dasharray=\"7 4\""
                    if state in (NOT_LOOKED_UP, NOT_RESOLVED, OUT_OF_SCOPE)
                    and not focus else ""))
    ty = y + 26
    for ln in lines:
        parts.append("<text class=\"n-name\" x=\"%d\" y=\"%d\">%s</text>"
                     % (x + 15, ty, e(ln)))
        ty += 18
    parts.append("<rect class=\"%s\" x=\"%d\" y=\"%d\" width=\"9\" height=\"9\" />"
                 % (sw, x + 15, y + 60))
    parts.append("<text class=\"n-state\" x=\"%d\" y=\"%d\">%s</text>"
                 % (x + 30, y + 68, e(sub)))
    if not focus:
        parts.append("</a>")
    return "".join(parts)


def edge(x1, y1, x2, y2, kind, text):
    cls = EDGE_CLASS[kind]
    mid_x = (x1 + x2) / 2.0
    mid_y = (y1 + y2) / 2.0
    w = len(text) * 6.3 + 14
    weak = " e-label--weak" if kind == "agent" else ""
    return (
        "<line class=\"e-line %s\" x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" />"
        "<rect class=\"e-knock\" x=\"%.0f\" y=\"%.0f\" width=\"%.0f\" height=\"18\" />"
        "<text class=\"e-label%s\" x=\"%.0f\" y=\"%.0f\">%s</text>"
        % (cls, x1, y1, x2, y2, mid_x - w / 2, mid_y - 9, w, weak,
           mid_x - w / 2 + 7, mid_y + 4, e(text)))


def owner_sub(oid):
    o = STORE.owners[oid]
    tot = STORE.owner_totals(o)
    state = o.get("state", NOT_LOOKED_UP)
    if state == MATCHED:
        return "MATCHED · %s · %s PROPERTIES" % (money(tot["value"]), num(tot["count"]))
    if state == NO_RECORD:
        return "NO RECORD · %s PROPERTIES" % num(tot["count"])
    if state == OUT_OF_SCOPE:
        return "OUTSIDE COVERAGE · %s PROPERTIES" % num(tot["count"])
    return "NOT LOOKED UP · %s PROPERTIES" % num(tot["count"])


def network_panel(oid, state, fl):
    nb = STORE.neighbourhood(oid)
    if nb is None:
        return (
            "<div class=\"netwrap\"><div class=\"netnote\" style=\"padding-top:clamp(0.9rem,3vw,1.3rem)\">"
            "<p>There is no network to draw yet. Linking one company to another needs a "
            "franchise filing on both ends, and this owner has no matched filing. The links "
            "this tool will draw are a shared officer, a shared registered agent, and a "
            "shared mailing address, in that order of strength</p></div></div>")

    hop1, hop2 = nb["hop1"], nb["hop2"]
    if not hop1 and not hop2:
        note = ["<p>Nothing else in this data shares an officer, a registered agent, or a "
                "mailing address with this filing. That is a real answer about this owner, "
                "not a blank</p>"]
        for kind, key, n in nb["hubs"]:
            note.append(
                "<p>One link was withheld. The %s on this filing, %s, appears on %s other "
                "filings here. At that scale it is a hub, not a relationship, so it is "
                "reported as a count instead of drawn as lines</p>"
                % (e(EDGE_TEXT[kind].lower().replace("shared ", "")), e(title_case(key)),
                   num(n)))
        return ("<div class=\"netwrap\"><div class=\"netnote\" "
                "style=\"padding-top:clamp(0.9rem,3vw,1.3rem)\">%s</div>%s</div>"
                % ("".join(note), edge_key()))

    COL0, W0 = 8, 250
    COL1, W1 = 470, 250
    COL2, W2 = 980, 250
    ROW = 108
    rows = max(len(hop1), len(hop2), 1)
    height = 24 + rows * ROW
    width = 1240 if hop2 else 730

    y_of_1 = {}
    svg = []
    focus_y = 24 + (rows * ROW - 80) / 2.0

    # edges first so boxes sit on top
    for n, (pid1, reasons) in enumerate(hop1):
        y1 = 24 + n * ROW
        y_of_1[pid1] = y1
        kind = min(reasons, key=lambda r: {"officer": 0, "mail": 1, "agent": 2}[r[0]])[0]
        text = EDGE_TEXT[kind]
        if kind == "agent":
            fan = STORE.agent_fanout(norm_txt(fl.get("agent"))) - 1
            text = "%s · %s MORE HERE" % (text, num(max(fan, 0)))
        svg.append(edge(COL0 + W0, focus_y + 40, COL1, y1 + 40, kind, text))
        extra = [r for r in reasons if r[0] != kind]
        if extra:
            labels = ", ".join(sorted(set(EDGE_TEXT[k].lower() for k, _ in extra)))
            svg.append("<text class=\"n-state\" x=\"%d\" y=\"%d\">ALSO %s</text>"
                       % (COL1 + 15, y1 - 6, e(labels.upper())))
    for n, (pid2, parent, kind, _t) in enumerate(hop2):
        y2 = 24 + n * ROW
        py = y_of_1.get(parent, focus_y)
        svg.append(edge(COL1 + W1, py + 40, COL2, y2 + 40, kind, EDGE_TEXT[kind]))

    o = STORE.owners[oid]
    svg.append(net_box(COL0, focus_y, W0, oid,
                       fl.get("corp_name") or o["name"], state, owner_sub(oid),
                       focus=True))
    for n, (pid1, _r) in enumerate(hop1):
        p = STORE.owners[pid1]
        f1 = STORE.filings.get(pid1) or {}
        svg.append(net_box(COL1, 24 + n * ROW, W1, pid1,
                           f1.get("corp_name") or p["name"],
                           p.get("state", NOT_LOOKED_UP), owner_sub(pid1)))
    for n, (pid2, _p, _k, _t) in enumerate(hop2):
        p = STORE.owners[pid2]
        f2 = STORE.filings.get(pid2) or {}
        svg.append(net_box(COL2, 24 + n * ROW, W2, pid2,
                           f2.get("corp_name") or p["name"],
                           p.get("state", NOT_LOOKED_UP), owner_sub(pid2)))

    notes = ["<p>This shows one entity and what sits up to two links away from it. Every line "
             "is labelled with the reason for it, because the reason is the claim</p>"]
    if nb["omitted1"]:
        notes.append("<p>%s more first-hop companies were left out to keep this readable. "
                     "The strongest links are drawn first</p>" % num(nb["omitted1"]))
    if nb["omitted2"]:
        notes.append("<p>%s more second-hop companies were left out</p>" % num(nb["omitted2"]))
    for kind, key, n in nb["hubs"]:
        notes.append(
            "<p>One link was withheld. The %s on this filing, %s, appears on %s other filings "
            "here. At that scale it is a hub, not a relationship, so it is reported as a count "
            "instead of drawn as lines</p>"
            % (e(EDGE_TEXT[kind].lower().replace("shared ", "")), e(title_case(key)), num(n)))

    return (
        "<div class=\"netwrap\"><div class=\"netscroll\">"
        "<svg viewBox=\"0 0 %d %d\" role=\"img\" aria-label=\"%s\">%s</svg>"
        "</div>%s<div class=\"netnote\">%s</div></div>"
        % (width, int(height), e(net_alt(oid, fl, hop1, hop2)), "".join(svg),
           edge_key(), "".join(notes))
    )


def net_alt(oid, fl, hop1, hop2):
    o = STORE.owners[oid]
    bits = ["Diagram: %s sits on the left." % (fl.get("corp_name") or o["name"])]
    for pid1, reasons in hop1:
        p = STORE.owners[pid1]
        f1 = STORE.filings.get(pid1) or {}
        bits.append("%s is linked by %s."
                    % (f1.get("corp_name") or p["name"],
                       " and ".join(sorted(set(r[1] for r in reasons)))))
    for pid2, parent, _k, t in hop2:
        p = STORE.owners[pid2]
        f2 = STORE.filings.get(pid2) or {}
        pp = STORE.owners[parent]
        bits.append("Two hops out, %s is linked to %s by %s."
                    % (f2.get("corp_name") or p["name"],
                       (STORE.filings.get(parent) or {}).get("corp_name") or pp["name"], t))
    return " ".join(bits)


def edge_key():
    return (
        "<div class=\"edgekey\">"
        "<div class=\"k\">"
        "<svg viewBox=\"0 0 78 10\" aria-hidden=\"true\">"
        "<line x1=\"0\" y1=\"5\" x2=\"78\" y2=\"5\" class=\"e-line e-line--officer\" /></svg>"
        "<span class=\"t\">Shared officer</span>"
        "<p>The same person is named on both filings. This is the strongest link here, and "
        "the one worth naming out loud</p></div>"
        "<div class=\"k\">"
        "<svg viewBox=\"0 0 78 10\" aria-hidden=\"true\">"
        "<line x1=\"0\" y1=\"5\" x2=\"78\" y2=\"5\" class=\"e-line e-line--mail\" /></svg>"
        "<span class=\"t\">Shared mailing address</span>"
        "<p>Both filings collect mail at the same address. Suggestive, and worth a second "
        "look, but shared suites happen</p></div>"
        "<div class=\"k\">"
        "<svg viewBox=\"0 0 78 10\" aria-hidden=\"true\">"
        "<line x1=\"0\" y1=\"5\" x2=\"78\" y2=\"5\" class=\"e-line e-line--agent\" /></svg>"
        "<span class=\"t\">Shared registered agent</span>"
        "<p>Both hired the same filing service. Weakest line on the diagram, drawn faint on "
        "purpose: these firms sign for thousands of unrelated companies</p></div>"
        "</div>")


# ---------------------------------------------------------------------------
# page: not found
# ---------------------------------------------------------------------------
def page_404(what, ident):
    body = [
        "<header class=\"wrap masthead\">", topline(),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">No such<br /><em>%s</em></h1>" % e(what),
        "<div style=\"margin-top:1.8rem\">", lookup_form(), "</div>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" style=\"padding-top:0\">",
        "<div class=\"empty\"><h3>Nothing is loaded under %s</h3>"
        "<p>That %s is not in the data this page has in memory. Either the identifier is "
        "wrong, or it belongs to a county outside the %s parcels loaded here</p>"
        "<p>Start from an address instead. The lookup above searches the address exactly as "
        "the county wrote it</p></div>"
        % (e(ident) or "an empty identifier", e(what),
           num(STORE.stats.get("parcel_rows", 0))),
        "</section>",
        footer(),
    ]
    return shell("Not found - Landlord Mapper", "".join(body))


# ---------------------------------------------------------------------------
# page: health
# ---------------------------------------------------------------------------
def page_health():
    st = STORE.stats
    states = st.get("owner_states", {})
    owners = max(1, st.get("owners", 1))
    scoped = scope_den()
    lines = [
        ("Loaded at", time.strftime("%Y-%m-%d %H:%M:%S",
                                    time.localtime(STORE.loaded_at))),
        ("Load seconds", st.get("load_seconds")),
        ("Data directory", DATA),
        ("Parcel file", "%s (written %s)" % (st.get("parcel_file"), st.get("parcel_mtime"))),
        ("Parcel file preference order", ", ".join(PARCEL_FILES)),
        ("Parcel rows loaded", num(st.get("parcel_rows", 0))),
        ("Distinct parcel IDs", num(st.get("parcel_pids", 0))),
        ("Parcel IDs carried by more than one county roll",
         "%s (%s%% of distinct IDs)"
         % (num(st.get("parcel_pids_shared", 0)),
            pct(st.get("parcel_pids_shared", 0), max(1, st.get("parcel_pids", 1))))),
        ("Repeated records dropped, same ID and address in one county roll",
         num(st.get("parcel_dupes_dropped", 0))),
        ("Parcel rows with wrong column count", num(st.get("parcel_bad_width", 0))),
        ("Parcels flagged owner-occupied", num(st.get("parcels_owner_occupied", 0))),
        ("County rolls loaded",
         "%s: %s" % (num(len(county_names())),
                     ", ".join("%s %s" % (k, num(v)) for k, v in
                               sorted(st.get("counties", {}).items())))),
        ("Parcels in the lookup scope by county",
         ", ".join("%s %s" % (k, num(v)) for k, v in
                   sorted(st.get("scope_counties", {}).items()))),
        ("Roll years present",
         ", ".join("%s %s" % (k, num(v)) for k, v in
                   sorted(st.get("roll_years", {}).items()))),
        ("Distinct owners on the whole roll", num(st.get("owners", 0))),
        ("Parcels in the lookup scope",
         "%s (%s%% of the roll)" % (num(st.get("parcels_in_scope", 0)),
                                    pct(st.get("parcels_in_scope", 0),
                                        max(1, st.get("parcel_rows", 1))))),
        ("Parcels outside the lookup scope",
         "%s (%s%% of the roll)" % (num(st.get("parcels_out_of_scope", 0)),
                                    pct(st.get("parcels_out_of_scope", 0),
                                        max(1, st.get("parcel_rows", 1))))),
        ("Scope predicate",
         "((is_financialized AND NOT is_owner_occupied) OR property_units > 5) "
         "AND property_units != 0"),
        ("Owners in the lookup scope",
         "%s (%s%% of owners on the roll)" % (num(st.get("owners_in_scope", 0)),
                                              pct(st.get("owners_in_scope", 0), owners))),
        ("Scrape files read", st.get("scrape_files")),
        ("Scrape rows read", num(st.get("scrape_rows", 0))),
        ("Scrape rows joined to a parcel",
         "%s (%s%% of rows read)" % (num(st.get("scrape_rows_joined", 0)),
                                     pct(st.get("scrape_rows_joined", 0),
                                         max(1, st.get("scrape_rows", 1))))),
        ("Scrape rows held back, no roll here carries that parcel ID",
         "%s (%s%% of rows read)" % (num(st.get("scrape_rows_no_parcel", 0)),
                                     pct(st.get("scrape_rows_no_parcel", 0),
                                         max(1, st.get("scrape_rows", 1))))),
        ("Scrape rows held back, every candidate parcel sits at another address",
         "%s (%s%% of rows read)" % (num(st.get("scrape_rows_addr_clash", 0)),
                                     pct(st.get("scrape_rows_addr_clash", 0),
                                         max(1, st.get("scrape_rows", 1))))),
        ("Scrape rows with wrong column count", num(st.get("scrape_bad_width", 0))),
        ("Distinct parcels carrying a joined registry row",
         num(st.get("scrape_parcels", 0))),
        ("Newest scrape file", st.get("scrape_newest_mtime")),
        ("Owners with any registry row", num(st.get("owners_with_scrape_rows", 0))),
        ("Registry coverage of owners in scope",
         "%s of %s looked up (%s%%)"
         % (num(st.get("owners_in_scope_answered", 0)),
            num(st.get("owners_in_scope", 0)),
            pct(st.get("owners_in_scope_answered", 0), scoped))),
        ("Owners answered but outside the scope predicate",
         num(st.get("owners_answered_out_of_scope", 0))),
        ("Officer names indexed", num(st.get("network_officers", 0))),
        ("Registered agents indexed", num(st.get("network_agents", 0))),
        ("Filing mail addresses indexed", num(st.get("network_mail", 0))),
    ]
    for k, v in sorted(st.get("scrape_status_rows", {}).items(),
                       key=lambda kv: -kv[1]):
        lines.append(("Rows with scrape_status %s" % (k or "(blank)"),
                      "%s (%s%% of joined rows)"
                      % (num(v), pct(v, max(1, st.get("scrape_rows_joined", 1))))))
    for k in (MATCHED, NO_RECORD, NOT_RESOLVED, NOT_LOOKED_UP):
        lines.append(("Owners resolved as %s" % k,
                      "%s (%s%% of owners in scope)"
                      % (num(states.get(k, 0)), pct(states.get(k, 0), scoped))))
    lines.append(("Owners outside the lookup scope",
                  "%s (%s%% of owners on the roll)"
                  % (num(states.get(OUT_OF_SCOPE, 0)),
                     pct(states.get(OUT_OF_SCOPE, 0), owners))))
    rows = "".join("<tr><td>%s</td><td class=\"r\">%s</td></tr>" % (e(k), e(v))
                   for k, v in lines)
    errs = st.get("errors") or []
    body = [
        "<header class=\"wrap masthead\">", topline("/health"),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">Load<br /><em>report</em></h1>",
        "<p class=\"deck\">What this process has in memory right now. The registry scrape has "
        "finished, so these counts are final rather than a snapshot. Owners still shown as "
        "not looked up are ones the registry never gave a usable answer for, not ones waiting "
        "in a queue</p>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" style=\"padding-top:0\">",
        "<div class=\"tablescroll\"><table><thead><tr><th scope=\"col\">Measure</th>"
        "<th scope=\"col\" class=\"r\">Value</th></tr></thead><tbody>", rows,
        "</tbody></table></div>",
    ]
    if errs:
        body.append("<div class=\"empty\"><h3>Load warnings</h3>%s</div>"
                    % "".join("<p>%s</p>" % e(x) for x in errs))
    else:
        body.append("<p class=\"tblnote\">No load warnings</p>")
    body += [
        "<p class=\"scopenote\">Coverage note: the registry scrape is scoped to rentals, "
        "meaning parcels the owner does not live in that the roll flags as investor-held, plus "
        "any building over 5 units. That is %s of the %s parcels here and %s of the %s owners, "
        "and it is a decision taken in the pipeline, not a shortfall. Coverage is therefore "
        "quoted against the %s owners in scope: %s looked up, %s%%. The %s owners outside the "
        "scope are reported as outside it, never as a lookup pending. Neither case means there "
        "is no landlord</p>"
        % (num(st.get("parcels_in_scope", 0)), num(st.get("parcel_rows", 0)),
           num(st.get("owners_in_scope", 0)), num(st.get("owners", 0)),
           num(st.get("owners_in_scope", 0)),
           num(st.get("owners_in_scope_answered", 0)),
           pct(st.get("owners_in_scope_answered", 0), scoped),
           num(states.get(OUT_OF_SCOPE, 0))),
        "<p class=\"scopenote\">Join note: registry rows are matched on parcel ID and situs "
        "address together. The file loaded here now carries every county roll the pipeline "
        "filters, which is %s, and the counties number their parcels independently, so an ID on "
        "its own is not a key: %s of the %s distinct IDs here are held by more than one roll. An "
        "answer is therefore placed only on the candidate parcel whose situs address agrees. %s "
        "rows name an ID no roll here carries and %s carry an ID whose candidates all sit at "
        "another address. Both sets are held back rather than joined to the wrong building%s</p>"
        % (e(", ".join(sorted(st.get("counties", {}))) or "no county"),
           num(st.get("parcel_pids_shared", 0)), num(st.get("parcel_pids", 0)),
           num(st.get("scrape_rows_no_parcel", 0)),
           num(st.get("scrape_rows_addr_clash", 0)),
           (" &middot; for example " + e("; ".join(st.get("scrape_clash_examples", []))))
           if st.get("scrape_clash_examples") else ""),
        "</section>", footer()]
    return shell("Load report - Landlord Mapper", "".join(body))


# ---------------------------------------------------------------------------
# the shared filter
# ---------------------------------------------------------------------------
# One filter object serves /explore, /rankings and /export.csv, so a view is a
# URL and the export is exactly the population the table showed. It never
# copies parcel rows: it reads the facet arrays built at load time and the
# parcel tuples already in memory.
#
# Two populations exist and they are never mixed silently. "In scope" is the
# lookup scope predicate, reproduced in parcel_in_scope():
#     ((is_financialized AND NOT is_owner_occupied) OR property_units > 5)
#     AND property_units != 0
# Everything else is "the whole roll". Every count on these pages names which
# one it is counted against.
SCOPE_IN = "in"
SCOPE_ALL = "all"

FLAG_PARAMS = (
    ("oos", F_OOS, "Owner mail out of state", "out of state", "in Texas"),
    ("occ", F_OCC, "Owner-occupied", "owner-occupied", "not owner-occupied"),
    ("fin", F_FIN, "Flagged investor-held", "investor-held", "not investor-held"),
    ("mom", F_MOM, "Flagged mom-and-pop", "mom-and-pop", "not mom-and-pop"),
)

RANGE_PARAMS = (
    ("units_min", "units_max", "units", "Units, estimated"),
    ("val_min", "val_max", "val", "Market value"),
    ("yb_min", "yb_max", "yb", "Year built"),
)

# Sort key -> the parcel column that carries it. These are the same orderings the
# in-memory build used, written as columns: county and ZIP are the raw roll
# strings, owner is the normalised name, pid is the zero-padded id and acquired is
# the date-only purchase stamp, exactly as the old key functions computed them.
SORT_KEYS = {
    "value": "n_val",
    "units": "n_units",
    "sqft": "n_sqft",
    "year_built": "n_yb",
    # Four of these order by an INTEGER now instead of by text, and they still
    # produce the same page. county / situs_zip / pdate are dictionary codes
    # assigned in text sort order, so ordering by the code is the same
    # permutation as ordering by the string. owner_seq and pid_seq are DENSE
    # ranks of the old owner_name_norm and pid_sort, which is order preserving
    # AND tie preserving, so "ORDER BY seq, rowid" keeps the same rows tied and
    # broken by rowid as "ORDER BY text, rowid" did.
    "address": "situs_address",
    "county": "county",
    "zip": "situs_zip",
    "owner": "owner_seq",
    "pid": "pid_seq",
    "acquired": "pdate",
}
FLAG_COL = {F_OOS: "f_oos", F_OCC: "f_occ", F_FIN: "f_fin", F_MOM: "f_mom"}
RANK_SLOT = {"parcels": 0, "units": 1, "value": 2}
RANK_LABEL = (("value", "Total market value"), ("units", "Estimated units"),
              ("parcels", "Parcels in scope"))


class Filt:
    """A parsed query string. Every field round-trips to the URL, so any view
    of these pages is a link somebody else can open and get the same rows."""

    def __init__(self):
        self.scope = SCOPE_IN
        self.counties = set()
        self.zips = []
        self.rng = {}
        self.flags = {}
        self.sort = "value"
        self.desc = True
        self.page = 1
        self.rank = "value"
        self.owner = ""
        self.shape = "parcels"

    @classmethod
    def from_qs(cls, qs):
        f = cls()
        if (qs.get("scope", [""])[0] or "").strip().lower() == SCOPE_ALL:
            f.scope = SCOPE_ALL
        f.counties = set(norm_txt(x) for v in qs.get("county", [])
                         for x in v.split(",") if norm_txt(x))
        f.zips = sorted(set(x.strip() for v in qs.get("zip", [])
                            for x in v.split(",") if x.strip()))
        for lo, hi, _key, _lbl in RANGE_PARAMS:
            for name in (lo, hi):
                raw = (qs.get(name, [""])[0] or "").strip()
                if raw:
                    n = to_float(raw)
                    if n is not None:
                        f.rng[name] = int(n)
        for name, bit, _lbl, _y, _n in FLAG_PARAMS:
            raw = (qs.get(name, [""])[0] or "").strip().lower()
            if raw in ("1", "true", "yes"):
                f.flags[bit] = True
            elif raw in ("0", "false", "no"):
                f.flags[bit] = False
        s = (qs.get("sort", [""])[0] or "").strip().lower()
        if s in SORT_KEYS:
            f.sort = s
        f.desc = (qs.get("dir", ["desc"])[0] or "desc").strip().lower() != "asc"
        r = (qs.get("rank", [""])[0] or "").strip().lower()
        if r in RANK_SLOT:
            f.rank = r
        try:
            f.page = max(1, int(qs.get("page", ["1"])[0]))
        except (TypeError, ValueError):
            f.page = 1
        f.owner = (qs.get("owner", [""])[0] or "").strip()
        if (qs.get("as", [""])[0] or "").strip().lower() == "owners":
            f.shape = "owners"
        return f

    # -- predicate --------------------------------------------------------
    def trivial(self):
        """True when nothing narrows the population, so the scan can be
        skipped and the prebuilt index handed back whole."""
        return not (self.counties or self.zips or self.rng or self.flags)

    def where(self, prefix=""):
        """(sql, params) selecting the same parcels match() used to test one at a
        time. Same predicate, same legs, in the same order; SQLite just gets to
        use an index instead of walking two million rows."""
        w = []
        a = []
        if self.scope == SCOPE_IN:
            w.append("in_scope = 1")
        if self.counties:
            # county_norm and zip_trim are gone: the same information is the
            # INTEGER code, and a name with no code has to select nothing, which
            # is what comparing it against the text column did.
            codes = [STORE.county_code.get(c) for c in sorted(self.counties)]
            codes = [c for c in codes if c is not None] or [-1]
            w.append("county IN (%s)" % ",".join("?" * len(codes)))
            a.extend(codes)
        if self.zips:
            codes = []
            for z in self.zips:
                codes.extend(STORE.zip_codes.get(z, ()))
            codes = codes or [-1]
            w.append("situs_zip IN (%s)" % ",".join("?" * len(codes)))
            a.extend(codes)
        r = self.rng
        if "units_min" in r:
            w.append("n_units >= ?")
            a.append(r["units_min"])
        if "units_max" in r:
            w.append("n_units <= ?")
            a.append(r["units_max"])
        if "val_min" in r:
            w.append("n_val >= ?")
            a.append(r["val_min"])
        if "val_max" in r:
            w.append("n_val <= ?")
            a.append(r["val_max"])
        if "yb_min" in r:
            w.append("n_yb >= ?")
            a.append(r["yb_min"])
        if "yb_max" in r:
            # a zero year is the roll carrying none, not a building from year
            # zero, so an upper bound excludes it rather than sweeping it in
            w.append("n_yb <= ?")
            w.append("n_yb <> 0")
            a.append(r["yb_max"])
        for _name, bit, _lbl, _y, _n in FLAG_PARAMS:
            if bit in self.flags:
                w.append("%s = ?" % FLAG_COL[bit])
                a.append(1 if self.flags[bit] else 0)
        return (" AND ".join(prefix + x for x in w) if w else "1"), a

    # -- url --------------------------------------------------------------
    def params(self):
        p = []
        if self.scope != SCOPE_IN:
            p.append(("scope", self.scope))
        for c in sorted(self.counties):
            p.append(("county", c.lower()))
        for z in self.zips:
            p.append(("zip", z))
        for lo, hi, _key, _lbl in RANGE_PARAMS:
            for name in (lo, hi):
                if name in self.rng:
                    p.append((name, self.rng[name]))
        for name, bit, _lbl, _y, _n in FLAG_PARAMS:
            if bit in self.flags:
                p.append((name, "1" if self.flags[bit] else "0"))
        p.append(("sort", self.sort))
        p.append(("dir", "desc" if self.desc else "asc"))
        p.append(("rank", self.rank))
        return p

    def qs(self, **over):
        p = [(k, v) for k, v in self.params() if k not in over]
        for k, v in over.items():
            if v is not None:
                p.append((k, v))
        return urllib.parse.urlencode(p)

    # -- prose ------------------------------------------------------------
    def population(self):
        if self.scope == SCOPE_IN:
            return (STORE.stats.get("parcels_in_scope", 0),
                    "parcels inside the registry lookup scope")
        return (STORE.stats.get("parcel_rows", 0),
                "parcels on the county appraisal rolls")

    def describe(self):
        bits = []
        if self.counties:
            bits.append("county %s"
                        % ", ".join(sorted(c.title() for c in self.counties)))
        if self.zips:
            bits.append("ZIP %s" % ", ".join(self.zips))
        for lo, hi, _key, lbl in RANGE_PARAMS:
            if lo in self.rng and hi in self.rng:
                bits.append("%s %s to %s"
                            % (lbl.lower(), num(self.rng[lo]), num(self.rng[hi])))
            elif lo in self.rng:
                bits.append("%s %s and over" % (lbl.lower(), num(self.rng[lo])))
            elif hi in self.rng:
                bits.append("%s %s and under" % (lbl.lower(), num(self.rng[hi])))
        for _name, bit, _lbl, yes, no in FLAG_PARAMS:
            if bit in self.flags:
                bits.append(yes if self.flags[bit] else no)
        return bits

    def slug(self):
        parts = ["in-scope" if self.scope == SCOPE_IN else "whole-roll"]
        if self.counties:
            parts.append("-".join(sorted(c.lower() for c in self.counties)))
        if self.zips:
            parts.append("zip-" + "-".join(self.zips))
        for lo, hi, key, _lbl in RANGE_PARAMS:
            if lo in self.rng:
                parts.append("%s-from-%d" % (key, self.rng[lo]))
            if hi in self.rng:
                parts.append("%s-to-%d" % (key, self.rng[hi]))
        for name, bit, _lbl, _y, _n in FLAG_PARAMS:
            if bit in self.flags:
                parts.append("%s-%s" % (name, "yes" if self.flags[bit] else "no"))
        parts.append("owners-by-" + self.rank if self.shape == "owners"
                     else "by-" + self.sort)
        s = "_".join(parts)
        return "".join(ch for ch in s if ch.isalnum() or ch in "-_")[:110]


def order_by(f, prefix=""):
    """ORDER BY for a parcel query.

    rowid is always the final key. Python's sort is stable, so the old code kept
    roll order among equal values in BOTH directions; a bare ORDER BY in SQL does
    not promise that, and page one would quietly drift.
    """
    col = SORT_KEYS.get(f.sort) or "n_val"
    return "%s%s %s, %srowid ASC" % (prefix, col, "DESC" if f.desc else "ASC",
                                     prefix)


def count_parcels(f):
    w, a = f.where()
    return STORE.db.val("SELECT COUNT(*) FROM parcel WHERE " + w, a) or 0


def page_parcels(f, offset, limit):
    """One page of matching parcel indexes, warmed into the row cache so the
    table below costs no further parcel queries.

    Always ordered. Every sort column has a covering index, so this is an
    indexed read even across the whole roll and there is no cap to fall back
    from."""
    w, a = f.where()
    rows = STORE.db.all(
        "SELECT rowid FROM parcel WHERE %s ORDER BY %s LIMIT ? OFFSET ?"
        % (w, order_by(f)),
        list(a) + [limit, offset])
    idxs = [r[0] - 1 for r in rows]
    STORE.parcels.warm(idxs)
    return idxs


def warm_owners_for(idxs):
    """Resolve the owner rows a table page is about to need in one query.

    Each row of a table calls owner_for_parcel() for the owner link and the
    registry chip, which is a query per row unless they are fetched together.
    The ids come from parcel rows already in the row cache, so this adds no
    parcel reads, and owner_for_parcel() then finds every one of them in the
    memo."""
    STORE.parcels.warm(idxs)
    ids = []
    for i in idxs:
        rec = STORE.parcels[i]
        ids.append(owner_id(rec[P["owner_name"]], rec[P["owner_address"]]))
    STORE.owners.warm(ids)


def owner_parcels_page(oid, f, limit):
    rows = STORE.db.all(
        "SELECT rowid FROM parcel WHERE owner_id = ? ORDER BY %s LIMIT ?"
        % order_by(f), (oid, limit))
    idxs = [r[0] - 1 for r in rows]
    STORE.parcels.warm(idxs)
    return idxs


# the ranking metric, in RANK_SLOT order: parcels, units, value
RANK_METRIC = ("n_parcels_scope", "scope_units", "scope_value")
RANK_GROUP_METRIC = ("c", "u", "v")
RANK_ROW_SQL = ("owner_id, name, %s AS state, agent, n_parcels, "
                "n_parcels_scope, scope_units, scope_value, %s AS counties_scope"
                % (_dict_sql("d_ostate", "state"),
                   _dict_sql("d_counties_scope", "counties_scope")))


def rank_group_sql(f):
    """The GROUP BY a filtered ranking needs. A filter is a property of a parcel,
    not of an owner, so a filtered ranking cannot read the precomputed owner
    aggregates and has to re-group the parcels that matched."""
    w, a = f.where()
    # mr is the lowest MATCHING parcel, which is the order the old code
    # first saw each owner in, and therefore its tie-break. owner.
    # first_scope_rowid is the lowest in-scope parcel anywhere and is a
    # different number as soon as a filter excludes it.
    return ("SELECT owner_id, COUNT(*) c, SUM(n_units) u, SUM(n_val) v, "
            "MIN(rowid) mr FROM parcel WHERE %s GROUP BY owner_id" % w), a


def rank_owners_count(f):
    """(owners matching, [in-scope parcels, units, value] across all of them)."""
    if f.trivial():
        t = STORE.db.one(
            "SELECT COUNT(*), SUM(n_parcels_scope), SUM(scope_units), "
            "SUM(scope_value) FROM owner WHERE in_scope = 1")
    else:
        grp, a = rank_group_sql(f)
        t = STORE.db.one(
            "SELECT COUNT(*), SUM(c), SUM(u), SUM(v) FROM (%s)" % grp, a)
    return (t[0] or 0), [t[1] or 0, t[2] or 0, t[3] or 0]


def rank_owners_rows(f, offset, limit):
    """One page of ranked owners as
    (owner_id, name, state, agent, parcels_all, parcels_scope, units, value,
     counties_scope).

    Unfiltered, this is an indexed read of the precomputed owner table through a
    partial index on (metric DESC, name, first_scope_rowid), which is where the
    speed came from. name then first_scope_rowid reproduces the old tie-break
    exactly: a stable sort over a dict built in ascending in-scope parcel order.
    """
    slot = RANK_SLOT[f.rank]
    if f.trivial():
        return STORE.db.all(
            "SELECT " + RANK_ROW_SQL + " FROM owner WHERE in_scope = 1 "
            "ORDER BY %s DESC, name, first_scope_rowid LIMIT ? OFFSET ?"
            % RANK_METRIC[slot], (limit, offset))
    grp, a = rank_group_sql(f)
    return STORE.db.all(
        "SELECT g.owner_id, o.name, " + O_STATE + ", o.agent, o.n_parcels, "
        "g.c, g.u, g.v, " + O_COUNTIES_SCOPE + " FROM (%s) g JOIN owner o "
        "ON o.owner_id = g.owner_id "
        "ORDER BY g.%s DESC, o.name, g.mr LIMIT ? OFFSET ?"
        % (grp, RANK_GROUP_METRIC[slot]), list(a) + [limit, offset])


def space_list(txt):
    """A space joined, already sorted list column."""
    return [x for x in (txt or "").split(" ") if x]


def counties_all_list(txt):
    """(county, count) over an owner's whole portfolio, count descending then
    first seen, which is the order the old Python dict iterated in."""
    out = []
    for part in (txt or "").split("\x1f"):
        if not part:
            continue
        name, _sep, n = part.rpartition(" ")
        out.append((name, n))
    return out


# ---------------------------------------------------------------------------
# shared table furniture
# ---------------------------------------------------------------------------
def state_chip(state):
    return ("<span class=\"chip %s\">%s</span>"
            % (STATE_CHIP[state], e(STATE_LABEL[state])))


def sort_th(base, f, key, label, right=False):
    cls = " class=\"r\"" if right else ""
    if key not in SORT_KEYS:
        return "<th scope=\"col\"%s>%s</th>" % (cls, e(label))
    nxt = "asc" if (f.sort == key and f.desc) else "desc"
    mark = ""
    if f.sort == key:
        mark = (" <span class=\"sortmark\">%s</span>"
                % ("&darr;" if f.desc else "&uarr;"))
    return ("<th scope=\"col\"%s><a href=\"%s?%s\">%s</a>%s</th>"
            % (cls, base, e(f.qs(sort=key, dir=nxt, page=1)), e(label), mark))


def pager_bar(base, f, page, pages, mid):
    prev_cls = "btn btn-quiet" + ("" if page > 1 else " btn-off")
    next_cls = "btn btn-quiet" + ("" if page < pages else " btn-off")
    return ("<div class=\"pager\">"
            "<a class=\"%s\" href=\"%s?%s\">Previous</a>"
            "<span>%s</span>"
            "<a class=\"%s\" href=\"%s?%s\">Next</a></div>"
            % (prev_cls, base, e(f.qs(page=max(1, page - 1))), mid,
               next_cls, base, e(f.qs(page=min(pages, page + 1)))))


def facet_form(f, action):
    """The facets, as a GET form so the resulting view is a shareable URL."""
    cty = "".join(
        "<option value=\"%s\"%s>%s (%s)</option>"
        % (e(c), " selected" if norm_txt(c) in f.counties else "",
           e(c.title()), num(STORE.stats.get("counties", {}).get(c, 0)))
        for c in county_names())
    zl = "".join("<option value=\"%s\"></option>" % e(z)
                 for z in sorted(STORE.stats.get("scope_zips", {})))
    rng = []
    for lo, hi, _key, lbl in RANGE_PARAMS:
        rng.append(
            "<div class=\"fset\"><span class=\"flab\">%s</span>"
            "<div class=\"pair\">"
            "<input type=\"text\" inputmode=\"numeric\" name=\"%s\" value=\"%s\" "
            "aria-label=\"%s minimum\" placeholder=\"min\" />"
            "<input type=\"text\" inputmode=\"numeric\" name=\"%s\" value=\"%s\" "
            "aria-label=\"%s maximum\" placeholder=\"max\" />"
            "</div></div>"
            % (e(lbl), lo, e(f.rng.get(lo, "")), e(lbl),
               hi, e(f.rng.get(hi, "")), e(lbl)))
    flg = []
    for name, bit, lbl, yes, no in FLAG_PARAMS:
        cur = f.flags.get(bit)
        flg.append(
            "<div class=\"fset\"><label for=\"fx-%s\">%s</label>"
            "<select id=\"fx-%s\" name=\"%s\">"
            "<option value=\"\"%s>Either</option>"
            "<option value=\"1\"%s>Yes, %s</option>"
            "<option value=\"0\"%s>No, %s</option>"
            "</select></div>"
            % (name, e(lbl), name, name,
               "" if cur is None else " selected",
               " selected" if cur is True else "", e(yes),
               " selected" if cur is False else "", e(no)))
    sort_opts = "".join(
        "<option value=\"%s\"%s>%s</option>"
        % (k, " selected" if f.sort == k else "", e(SORT_LABEL[k]))
        for k in ("value", "units", "sqft", "year_built", "address", "county",
                  "zip", "owner", "pid", "acquired"))
    return (
        "<form class=\"facets\" action=\"%s\" method=\"get\">"
        "<div class=\"fset\"><label for=\"fx-county\">County roll</label>"
        "<select id=\"fx-county\" name=\"county\" multiple size=\"6\">%s</select>"
        "<span class=\"hint\">Nothing selected means every roll loaded</span></div>"
        "<div class=\"fset\"><label for=\"fx-zip\">Situs ZIP</label>"
        "<input id=\"fx-zip\" name=\"zip\" type=\"text\" value=\"%s\" list=\"ziplist\" "
        "autocomplete=\"off\" placeholder=\"78704, 78702\" />"
        "<datalist id=\"ziplist\">%s</datalist>"
        "<span class=\"hint\">Comma separated. This is the building's ZIP, not the "
        "owner's mailing ZIP</span></div>"
        "%s%s"
        "<div class=\"fset\"><label for=\"fx-scope\">Population</label>"
        "<select id=\"fx-scope\" name=\"scope\">"
        "<option value=\"in\"%s>In the registry lookup scope (%s parcels)</option>"
        "<option value=\"all\"%s>The whole appraisal roll (%s parcels)</option>"
        "</select>"
        "<span class=\"hint\">The whole roll includes owner-occupied homes and takes "
        "a few seconds to scan</span></div>"
        "<div class=\"fset\"><label for=\"fx-sort\">Order by</label>"
        "<select id=\"fx-sort\" name=\"sort\">%s</select></div>"
        "<div class=\"fset\"><label for=\"fx-dir\">Direction</label>"
        "<select id=\"fx-dir\" name=\"dir\">"
        "<option value=\"desc\"%s>Largest first</option>"
        "<option value=\"asc\"%s>Smallest first</option></select></div>"
        "<div class=\"fset\"><label for=\"fx-rank\">Rank owners by</label>"
        "<select id=\"fx-rank\" name=\"rank\">%s</select>"
        "<span class=\"hint\">Used by the rankings table only</span></div>"
        "<div class=\"go\">"
        "<button class=\"btn\" type=\"submit\">Apply filters</button>"
        "<a class=\"btn btn-quiet\" href=\"%s\">Clear</a>"
        "<a class=\"btn btn-quiet\" href=\"/export.csv?%s\">Download this as CSV</a>"
        "</div></form>"
        % (e(action), cty, e(", ".join(f.zips)), zl,
           "".join(rng), "".join(flg),
           " selected" if f.scope == SCOPE_IN else "",
           num(STORE.stats.get("parcels_in_scope", 0)),
           " selected" if f.scope == SCOPE_ALL else "",
           num(STORE.stats.get("parcel_rows", 0)),
           sort_opts,
           " selected" if f.desc else "", "" if f.desc else " selected",
           "".join("<option value=\"%s\"%s>%s</option>"
                   % (k, " selected" if f.rank == k else "", e(lbl))
                   for k, lbl in RANK_LABEL),
           e(action),
           e(f.qs(**({"as": "owners"} if action == "/rankings" else {}))))
    )


SORT_LABEL = {
    "value": "Market value", "units": "Units, estimated", "sqft": "Sq ft",
    "year_built": "Built", "address": "Address", "county": "County",
    "zip": "ZIP", "owner": "Owner on the roll", "pid": "Parcel ID",
    "acquired": "Acquired",
}


def filter_line(f, matched, unit_of="parcels", den=None, den_name=None):
    """The count, always attached to the population it is a count of. A count
    with no stated denominator is the failure mode this whole site exists to
    avoid, so the denominator is not optional here."""
    if den is None:
        den, den_name = f.population()
    bits = f.describe()
    return (
        "<p class=\"countline\"><b>%s</b> <span>%s match%s &mdash; out of the %s %s. "
        "%s</span></p>"
        % (num(matched), e(unit_of),
           "" if unit_of.endswith("s") else "es",
           num(den), e(den_name),
           ("Filters: " + e(", ".join(bits))) if bits
           else "No filters applied, so this is the whole population"))


# ---------------------------------------------------------------------------
# page: rankings
# ---------------------------------------------------------------------------
def page_rankings(f):
    f.scope = SCOPE_IN
    matched, tot = rank_owners_count(f)
    capped = matched > RANK_LIMIT
    shown = min(matched, RANK_LIMIT)
    pages = max(1, (shown + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(f.page, pages))
    start = (page - 1) * PAGE_SIZE
    window = rank_owners_rows(f, start, max(0, min(PAGE_SIZE, shown - start)))

    rows = []
    for n, r in enumerate(window, start=start + 1):
        oid, oname, state, agent, n_all, n_scope, o_units, o_value, ctys = r
        rows.append(
            "<tr>"
            "<td><span class=\"rk\">%s</span></td>"
            "<td><a href=\"/owner/%s\">%s</a></td>"
            "<td class=\"r\">%s</td>"
            "<td class=\"r\">%s</td>"
            "<td class=\"r\">&asymp;%s</td>"
            "<td class=\"r\">%s</td>"
            "<td>%s</td>"
            "<td>%s</td>"
            "<td>%s</td>"
            "</tr>"
            % (num(n), e(oid), e(oname),
               num(n_scope), num(n_all), num(o_units), money(o_value),
               e(", ".join(c.title() for c in space_list(ctys)) or "none"),
               e(agent or "not on the filing"),
               state_chip(state or NOT_LOOKED_UP)))

    head = ("<thead><tr>"
            "<th scope=\"col\">#</th>"
            "<th scope=\"col\">Owner on the roll</th>"
            "<th scope=\"col\" class=\"r\">Parcels in scope</th>"
            "<th scope=\"col\" class=\"r\">All parcels on the rolls</th>"
            "<th scope=\"col\" class=\"r\">Units, estimated</th>"
            "<th scope=\"col\" class=\"r\">Market value in scope</th>"
            "<th scope=\"col\">Counties</th>"
            "<th scope=\"col\">Registered agent</th>"
            "<th scope=\"col\">Registry</th></tr></thead>")

    picker = " &middot; ".join(
        ("<b>%s</b>" % e(lbl)) if f.rank == k
        else ("<a href=\"/rankings?%s\">%s</a>"
              % (e(f.qs(rank=k, page=1)), e(lbl)))
        for k, lbl in RANK_LABEL)

    body = [
        "<header class=\"wrap masthead\">", topline("/rankings"),
        "<h1 style=\"font-size:clamp(1.7rem,6.4vw,3.2rem)\">The biggest<br />"
        "<em>landlords</em> here</h1>",
        "<p class=\"deck\">Ranked by what they hold inside the registry lookup scope. "
        "This is the page a campaign picks a target from, so read the denominators: a "
        "row's parcel count is its in-scope parcels, not everything it owns, and the "
        "column beside it shows the difference</p>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" aria-labelledby=\"rank-h\" "
        "style=\"padding-top:0\">",
        "<h2 class=\"eyebrow\" id=\"rank-h\">Rank by</h2>",
        "<p class=\"tblnote\" style=\"margin-top:0.5rem;font-size:0.8125rem\">%s</p>"
        % picker,
        "<div style=\"margin-top:1.4rem\">", facet_form(f, "/rankings"), "</div>",
        filter_line(f, matched, "owners",
                    STORE.stats.get("owners_in_scope", 0),
                    "owners holding at least one in-scope parcel"),
        "<p class=\"scopenote\" style=\"margin-top:0.7rem\">Those %s owners hold %s "
        "in-scope parcels worth %s on the roll between them, out of %s owners with any "
        "in-scope parcel and %s distinct owners on the whole roll. Ranking is over "
        "in-scope parcels only: the owner-occupied half of the roll is deliberately not "
        "in this table, because a homeowner is not a campaign target</p>"
        % (num(matched), num(tot[0]), money(tot[2]),
           num(STORE.stats.get("owners_in_scope", 0)),
           num(STORE.stats.get("owners", 0))),
        "<div class=\"tablescroll\" style=\"margin-top:1.4rem\"><table>",
        "<caption class=\"skiplink\">Owners ranked by %s over in-scope parcels</caption>"
        % e(dict(RANK_LABEL)[f.rank].lower()),
        head, "<tbody>", "".join(rows) or
        "<tr><td colspan=\"9\">No owner in scope matches those filters</td></tr>",
        "</tbody></table></div>",
        "<p class=\"tblnote\">Unit counts are estimated from floor area &middot; market "
        "value is the county roll value, not a sale price &middot; the registry column is "
        "the owner's state, and a rejected lookup is not the same claim as no Texas "
        "filing%s</p>"
        % (" &middot; the table stops at the top %s of %s; the CSV carries the rest"
           % (num(RANK_LIMIT), num(matched)) if capped else ""),
        pager_bar("/rankings", f, page, pages,
                  "Page %s of %s &middot; showing %s of %s owners"
                  % (num(page), num(pages), num(shown), num(matched))),
        "<p style=\"margin-top:1.4rem\"><a class=\"btn\" href=\"/export.csv?%s\" "
        "style=\"display:inline-block;text-decoration:none\">Download this ranking as "
        "CSV</a></p>" % e(f.qs(**{"as": "owners"})),
        "</section>",
        legend_band(),
        footer(),
    ]
    return shell("Biggest landlords - Landlord Mapper", "".join(body))


# ---------------------------------------------------------------------------
# page: explore
# ---------------------------------------------------------------------------
def page_explore(f):
    matched = count_parcels(f)
    pages = max(1, (matched + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(f.page, pages))
    start = (page - 1) * PAGE_SIZE
    window = page_parcels(f, start, PAGE_SIZE)
    warm_owners_for(window)

    rows = []
    for i in window:
        rec = STORE.parcels[i]
        o = STORE.owner_for_parcel(i)
        state = parcel_state(i, o)
        rows.append(
            "<tr>"
            "<td><a href=\"%s\">%s</a></td>"
            "<td><span class=\"cty\">%s</span></td>"
            "<td>%s</td>"
            "<td><a href=\"/owner/%s\">%s</a></td>"
            "<td class=\"r\">&asymp;%s</td>"
            "<td class=\"r\">%s</td>"
            "<td class=\"r\">%s</td>"
            "<td class=\"r\">%s</td>"
            "<td>%s</td>"
            "<td>%s</td>"
            "</tr>"
            % (parcel_link(i), e(rec[P["situs_address"]]),
               e(rec[P["county"]]), e(rec[P["situs_zip"]]),
               e(o["id"]), e(rec[P["owner_name"]]),
               num(rec[P["property_units"]]), num(rec[P["totalsqftlivingarea"]]),
               money(rec[P["totalpropmktvalue"]]),
               e(dash(rec[P["year_built"]])),
               "in scope" if STORE.in_scope[i] else "outside",
               state_chip(state)))

    head = ("<thead><tr>%s%s%s%s%s%s%s%s"
            "<th scope=\"col\">Lookup scope</th>"
            "<th scope=\"col\">Registry</th></tr></thead>"
            % (sort_th("/explore", f, "address", "Address"),
               sort_th("/explore", f, "county", "County"),
               sort_th("/explore", f, "zip", "ZIP"),
               sort_th("/explore", f, "owner", "Owner on the roll"),
               sort_th("/explore", f, "units", "Units, est", True),
               sort_th("/explore", f, "sqft", "Sq ft", True),
               sort_th("/explore", f, "value", "Market value", True),
               sort_th("/explore", f, "year_built", "Built", True)))

    body = [
        "<header class=\"wrap masthead\">", topline("/explore"),
        "<h1 style=\"font-size:clamp(1.7rem,6.4vw,3.2rem)\">Narrow it to<br />"
        "<em>where you organize</em></h1>",
        "<p class=\"deck\">Every filter here lives in the address bar, so a view is a link. "
        "Counts on this page are counts of the population named under them, and the registry "
        "column carries the same three-state honesty as the chain pages</p>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" aria-labelledby=\"exp-h\" "
        "style=\"padding-top:0\">",
        "<h2 class=\"eyebrow\" id=\"exp-h\">Facets</h2>",
        "<div style=\"margin-top:0.9rem\">", facet_form(f, "/explore"), "</div>",
        filter_line(f, matched, "parcels"),
        "<div class=\"tablescroll\" style=\"margin-top:1.4rem\"><table>",
        "<caption class=\"skiplink\">Parcels matching the current filters</caption>",
        head, "<tbody>", "".join(rows) or
        "<tr><td colspan=\"10\">No parcel matches those filters</td></tr>",
        "</tbody></table></div>",
        "<p class=\"tblnote\">%s &middot; unit counts are estimated from floor area, so they "
        "are the sq ft column divided by 900 &middot; a ZIP filter is the building's ZIP, not "
        "the owner's mailing ZIP</p>"
        % ("Ordered by %s, %s" % (e(SORT_LABEL[f.sort].lower()),
                                  "largest first" if f.desc else "smallest first")),
        pager_bar("/explore", f, page, pages,
                  "Page %s of %s &middot; %s parcels matched"
                  % (num(page), num(pages), num(matched))),
        "<p style=\"margin-top:1.4rem\"><a class=\"btn\" href=\"/export.csv?%s\" "
        "style=\"display:inline-block;text-decoration:none\">Download these %s parcels as "
        "CSV</a></p>" % (e(f.qs()), num(min(matched, EXPORT_CAP))),
        scope_note(),
        "</section>",
        legend_band(),
        footer(),
    ]
    return shell("Explore the rolls - Landlord Mapper", "".join(body))


# ---------------------------------------------------------------------------
# page: method
# ---------------------------------------------------------------------------
def page_method():
    st = STORE.stats
    states = st.get("owner_states", {})
    scoped = scope_den()
    ctys = "".join(
        "<li><b>%s</b> &middot; %s parcels &middot; %s in the lookup scope</li>"
        % (e(k.title()), num(v), num(st.get("scope_counties", {}).get(k, 0)))
        for k, v in sorted(st.get("counties", {}).items(), key=lambda kv: -kv[1]))
    body = [
        "<header class=\"wrap masthead\">", topline("/method"),
        "<h1 style=\"font-size:clamp(1.7rem,6.4vw,3.2rem)\">Where every<br />"
        "<em>number</em> comes from</h1>",
        "<p class=\"deck\">One page to hand a skeptic. It names the sources, states the "
        "coverage rule as the predicate it actually is, gives the current count for each "
        "of the three match states, and lists the limits we know about rather than waiting "
        "for someone to find them</p>",
        dates_strip(),
        "</header>",

        "<section class=\"wrap band\" id=\"main\" aria-labelledby=\"m1\" "
        "style=\"padding-top:0\">",
        "<h2 class=\"eyebrow\" id=\"m1\">1. The two sources</h2>",
        "<div class=\"prose\">",
        "<p>The first source is the county appraisal roll: who the county bills for a "
        "property, what the county thinks it is worth, its floor area, its class code and "
        "its legal description. A roll is published once a year, so ownership here can lag "
        "a sale by months</p>",
        "<p>The second source is the Texas Comptroller's franchise tax registry: the "
        "business filing behind a company name, its taxpayer number, its right to transact "
        "business, its registered agent, and the officers and directors named on it. That "
        "read has run to completion for the parcels in scope</p>",
        "<p>Nothing here is geocoded and there are no coordinates in either source, which "
        "is why this tool has no map. ZIP is the finest geography we can honestly claim, so "
        "ZIP is what the filters offer</p>",
        "</div>",
        "<h3 class=\"subhead\">The %s county rolls loaded right now</h3>"
        % num(len(county_names())),
        "<div style=\"border:1px solid var(--rule);border-top:0;background:var(--paper-2);"
        "padding:clamp(0.9rem,3vw,1.3rem)\"><ul class=\"srclist\">%s</ul>"
        "<p class=\"srcstamp\">Parcel file %s, written %s &middot; roll year %s &middot; "
        "%s parcels, %s distinct parcel IDs</p></div>"
        % (ctys, e(st.get("parcel_file", "")), e(st.get("parcel_mtime", "")),
           e(roll_year()), num(st.get("parcel_rows", 0)),
           num(st.get("parcel_pids", 0))),
        "</section>",

        "<section class=\"wrap band\" aria-labelledby=\"m2\">",
        "<h2 class=\"eyebrow\" id=\"m2\">2. What \"in scope\" means</h2>",
        "<div class=\"prose\">",
        "<p>The registry lookup was never run against the whole roll. The pipeline picks "
        "its targets with one filter, and this is that filter, leg for leg:</p>",
        "</div>",
        "<p class=\"stamp\" style=\"font-family:var(--mono)\">"
        "((is_financialized = TRUE AND is_owner_occupied = FALSE) "
        "OR property_units &gt; 5) AND property_units != 0</p>",
        "<div class=\"prose\" style=\"margin-top:1.4rem\">",
        "<p>In plain words: a parcel the owner does not live in that the roll flags as "
        "investor-held, or any building the roll sizes at more than five units. Strictly "
        "more than five, so a clean five-unit building is outside unless it is also flagged "
        "investor-held. A parcel with no floor area on the roll is outside too, because "
        "there is no size to judge it by, and that is not the same claim as the building "
        "being small</p>",
        "<p>That predicate selects <b>%s</b> of the <b>%s</b> parcels on the rolls, which "
        "is %s%% of them, and <b>%s</b> of the <b>%s</b> distinct owners. An owner counts as "
        "in scope when any one of its parcels is</p>"
        % (num(st.get("parcels_in_scope", 0)), num(st.get("parcel_rows", 0)),
           pct(st.get("parcels_in_scope", 0), max(1, st.get("parcel_rows", 1))),
           num(st.get("owners_in_scope", 0)), num(st.get("owners", 0))),
        "<p>Owner identity is the pair of owner name and owner mailing address as printed "
        "on the roll, because that pair is what the registry read was keyed on. Two "
        "companies with the same name at different addresses are two owners here, and one "
        "company that changed mailing address mid-roll can appear as two</p>",
        "<p>Every count on this site is quoted against one of those two populations, and "
        "says which. Coverage is quoted against owners in scope, never against all "
        "%s owners: dividing by the whole roll would flatter the coverage figure by "
        "counting owner-occupied houses nobody ever intended to look up</p>"
        % num(st.get("owners", 0)),
        "</div></section>",

        "<section class=\"wrap band\" aria-labelledby=\"m3\">",
        "<h2 class=\"eyebrow\" id=\"m3\">3. The match states, and their counts</h2>",
        "<div class=\"prose\"><p>A registry lookup ends in one of these. They are different "
        "claims about the world and the site never lets one wear another's clothes. All four "
        "shares below are of the <b>%s</b> owners in scope</p></div>"
        % num(st.get("owners_in_scope", 0)),
        "<div class=\"statebar\">"
        "<div><span class=\"v\">%s</span><span class=\"k\">matched &middot; %s%%</span>"
        "<p>A Texas business filing lines up with the name on the roll. Both names are "
        "printed side by side on the parcel page so you can reject a bad match yourself</p>"
        "</div>"
        "<div><span class=\"v\">%s</span><span class=\"k\">no_record &middot; %s%%</span>"
        "<p>The registry answered, and the answer was that nothing is filed in Texas under "
        "this name. This is a finding. Plenty of rentals are held by a person, a trust, or "
        "an out-of-state entity that never registered here</p></div>"
        "<div><span class=\"v\">%s</span><span class=\"k\">not_resolved &middot; %s%%</span>"
        "<p><b>Our lookup was rejected.</b> The query was malformed or unresolvable and the "
        "registry returned nothing usable. It is not a statement that Texas has no record, "
        "and collapsing it into the column to its left would invent %s findings that do not "
        "exist</p></div>"
        "<div><span class=\"v\">%s</span><span class=\"k\">not_looked_up &middot; %s%%</span>"
        "<p>No registry row reached this owner at all, or the rows that did carry no status. "
        "Unknown, same as the column beside it, and drawn the same dashed and open way</p>"
        "</div></div>"
        % (num(states.get(MATCHED, 0)), pct(states.get(MATCHED, 0), scoped),
           num(states.get(NO_RECORD, 0)), pct(states.get(NO_RECORD, 0), scoped),
           num(states.get(NOT_RESOLVED, 0)), pct(states.get(NOT_RESOLVED, 0), scoped),
           num(states.get(NOT_RESOLVED, 0)),
           num(states.get(NOT_LOOKED_UP, 0)), pct(states.get(NOT_LOOKED_UP, 0), scoped)),
        "<p class=\"tblnote\">A fifth case is not a match state at all: <b>%s</b> owners are "
        "outside the coverage rules and were never queried on purpose. They are reported as "
        "outside, never as pending, and that share is of all %s owners on the roll</p>"
        % (num(states.get(OUT_OF_SCOPE, 0)), num(st.get("owners", 0))),
        "<p class=\"tblnote\">Underneath the owner states, the registry read produced %s rows "
        "joined to a parcel, by status: %s. One owner can carry several rows, and an owner "
        "counts as matched when any row matched</p>"
        % (num(st.get("scrape_rows_joined", 0)),
           " &middot; ".join("%s %s" % (e(k or "blank"), num(v)) for k, v in sorted(
               st.get("scrape_status_rows", {}).items(), key=lambda kv: -kv[1]))),
        "</section>",

        "<section class=\"wrap band\" aria-labelledby=\"m4\">",
        "<h2 class=\"eyebrow\" id=\"m4\">4. The limits we know about</h2>",
        "<div class=\"prose\">",
        "<p><b>not_resolved is our failure, not Texas's silence.</b> %s owners in scope sit "
        "in that state. Every one of them is a lookup the registry rejected. If you need to "
        "know whether one of those names has a Texas filing, the honest answer is that this "
        "tool does not know and you should search the Comptroller directly</p>"
        % num(states.get(NOT_RESOLVED, 0)),
        "<p><b>A bare parcel ID is ambiguous.</b> The counties number their parcels "
        "independently and this roll is a dozen of them stacked together, so %s of the %s "
        "distinct IDs loaded here are carried by more than one county. Every parcel link on "
        "this site carries its county for that reason, and a registry answer is only ever "
        "placed on the candidate whose situs address agrees. %s registry rows name an ID no "
        "roll here carries and %s carry an ID whose candidates all sit at another address; "
        "both sets are held back rather than joined to the wrong building</p>"
        % (num(st.get("parcel_pids_shared", 0)), num(st.get("parcel_pids", 0)),
           num(st.get("scrape_rows_no_parcel", 0)),
           num(st.get("scrape_rows_addr_clash", 0))),
        "<p><b>agent_address is empty upstream.</b> The rolls carry a tax agent name but the "
        "counties do not publish the agent's mailing address, so that column arrives blank "
        "and nothing here is built on it. Deed transfer dates are unreliable for the same "
        "kind of reason and are shown only as the roll's own recent purchase date</p>",
        "<p><b>Unit counts are estimates, everywhere.</b> The rolls do not publish a unit "
        "count for most buildings, so property_units is floor area divided by 900 square "
        "feet, corrected by class code only for houses and small duplex-to-fourplex classes. "
        "It is the sq ft column restated, not a second fact, and it is marked with a &asymp; "
        "wherever it appears. The five-unit line in the coverage rule inherits that "
        "estimate</p>",
        "<p><b>Market value is the county's value, not a price.</b> It is what the appraisal "
        "district put on the roll, which is neither a sale price nor an offer</p>",
        "<p><b>No coordinates, so no map.</b> Neither source carries a latitude or a "
        "longitude and nothing here is geocoded. ZIP-level filtering and the ZIP column are "
        "the honest substitute</p>",
        "<p><b>People are deliberately not searchable.</b> Officer names appear on the "
        "filing they belong to and nowhere else. Their home addresses are in the source "
        "records and are never shown, and there is no search by person name. This tool "
        "answers who owns a building, not what a named human owns</p>",
        "<p><b>Everything on this page is checkable.</b> The full load report, including the "
        "rows this process refused to join and why, is at <a href=\"/health\">/health</a></p>",
        "</div></section>",
        legend_band(),
        footer(),
    ]
    return shell("Method and limits - Landlord Mapper", "".join(body))


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------
EXPORT_PARCEL_COLS = PARCEL_COLS + [
    "parcel_in_lookup_scope", "owner_id", "owner_registry_state",
    "owner_corp_name_matched", "owner_registered_agent", "parcel_path",
]
EXPORT_OWNER_COLS = [
    "rank", "owner_id", "owner_name", "owner_address", "owner_registry_state",
    "parcels_in_lookup_scope", "parcels_on_whole_roll",
    "units_estimated_in_scope", "market_value_in_scope", "counties_in_scope",
    "owner_corp_name_matched", "owner_registered_agent", "owner_path",
]

# corp_name and agent are read off the owner row, not through a second join
# to filing: build-db.py denormalises them there for this query, and the two
# are equal for all 1,718,226 owners (checked, not assumed).
EXPORT_PARCEL_SELECT = (
    "SELECT " + PARCEL_SQL
    + ", p.in_scope, p.owner_id, " + O_STATE + ", o.corp_name, o.agent, "
    + PARCEL_EXPR["county"]("p.") + ", p.situs_pID "
    + PARCEL_FROM)


def export_parcel_rows(where, args, order, limit):
    """Row generator straight off a cursor: one row is built, yielded and
    dropped, so nothing here holds a second copy of the table. That is not a
    style preference; buffering a quarter of a million rows is how this process
    got itself OOM-killed once."""
    cur = STORE.db.cursor(
        EXPORT_PARCEL_SELECT + "WHERE %s ORDER BY %s LIMIT %d"
        % (where, order, limit), args)
    n = len(PARCEL_COLS)
    for row in cur:
        # parcel_state(): an out-of-scope parcel reports that rather than a
        # lookup still to come, even when its owner is answered elsewhere
        state = row[n + 2]
        if state in (NOT_LOOKED_UP, OUT_OF_SCOPE) and not row[n]:
            state = OUT_OF_SCOPE
        yield list(row[:n]) + [
            "TRUE" if row[n] else "FALSE", row[n + 1], state,
            row[n + 3], row[n + 4],
            parcel_path_for(row[n + 5], row[n + 6])]


def export_owner_rows(f):
    f.scope = SCOPE_IN
    slot = RANK_SLOT[f.rank]
    if f.trivial():
        sql = ("SELECT owner_id, name, address, "
               + _dict_sql("d_ostate", "state") + ", n_parcels_scope, "
               "n_parcels, scope_units, scope_value, "
               + _dict_sql("d_counties_scope", "counties_scope")
               + ", corp_name, agent FROM owner WHERE in_scope = 1 "
               "ORDER BY %s DESC, name, first_scope_rowid LIMIT %d"
               % (RANK_METRIC[slot], EXPORT_CAP))
        args = ()
    else:
        grp, a = rank_group_sql(f)
        sql = ("SELECT g.owner_id, o.name, o.address, " + O_STATE
               + ", g.c, o.n_parcels, g.u, g.v, " + O_COUNTIES_SCOPE
               + ", o.corp_name, o.agent FROM (%s) g "
               "JOIN owner o ON o.owner_id = g.owner_id "
               "ORDER BY g.%s DESC, o.name, g.mr LIMIT %d"
               % (grp, RANK_GROUP_METRIC[slot], EXPORT_CAP))
        args = a
    n = 0
    for r in STORE.db.cursor(sql, args):
        n += 1
        yield [n, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8],
               r[9], r[10], "/owner/" + r[0]]


# ---------------------------------------------------------------------------
# portfolio bands, used by page_owner
# ---------------------------------------------------------------------------
def owner_profile_band(o, tot):
    """Totals for the whole owner group. Every figure here counts all of this
    owner's parcels on the rolls, in the lookup scope or not, and the scope
    split is one of the cells so the reader can see the difference.

    All of it is a column on the owner row now, aggregated once by build-db.py
    rather than by walking the portfolio on every request."""
    med = o["median_value"]
    inscope = o["n_parcels_scope"]
    ctys = counties_all_list(o["counties_all"])
    zips = space_list(o["zips_all"])
    cty_line = ", ".join("%s %s" % (name.title(), num(n)) for name, n in ctys)
    return (
        "<h3 class=\"subhead\">Portfolio totals</h3>"
        "<div class=\"statebar\">"
        "<div><span class=\"v\">%s</span><span class=\"k\">parcels on the rolls</span>"
        "<p>%s of them are inside the registry lookup scope</p></div>"
        "<div><span class=\"v\">&asymp;%s</span><span class=\"k\">units, estimated</span>"
        "<p>Floor area divided by 900 sq ft, not a count</p></div>"
        "<div><span class=\"v\">%s</span><span class=\"k\">total market value</span>"
        "<p>County roll value across all %s parcels</p></div>"
        "<div><span class=\"v\">%s</span><span class=\"k\">median parcel value</span>"
        "<p>The middle parcel, which a total hides</p></div>"
        "</div>"
        "<div style=\"border:1px solid var(--rule);border-top:0;"
        "background:var(--paper-2);padding:clamp(0.9rem,3vw,1.3rem)\">"
        "<dl class=\"dl dl--2\">"
        "<div><dt>County spread</dt><dd>%s</dd></div>"
        "<div><dt>ZIPs</dt><dd>%s</dd></div>"
        "<div><dt>Parcels whose tax bill leaves Texas</dt><dd>%s of %s</dd></div>"
        "<div><dt>Parcels flagged owner-occupied</dt><dd>%s of %s</dd></div>"
        "<div><dt>Most recent purchase date on the roll</dt><dd>%s</dd></div>"
        "<div><dt>Earliest purchase date on the roll</dt><dd>%s</dd></div>"
        "</dl>"
        "<p class=\"srcstamp\">Source: %s county appraisal rolls &middot; roll year %s "
        "&middot; purchase dates are the roll's own recent-purchase field and are not "
        "reliable deed dates</p></div>"
        "<p style=\"margin:1.1rem 0 0\"><a class=\"btn\" href=\"/export.csv?owner=%s\" "
        "style=\"display:inline-block;text-decoration:none\">Download this portfolio as "
        "CSV</a></p>"
        % (num(tot["count"]), num(inscope), num(tot["units"]),
           money(tot["value"]), num(tot["count"]), money(med),
           e(cty_line or "none"),
           e(", ".join(zips) or "not on the roll"),
           num(o["n_out_of_state"]), num(tot["count"]),
           num(o["n_owner_occupied"]), num(tot["count"]),
           e(o["last_purchase"] or "not on the roll"),
           e(o["first_purchase"] or "not on the roll"),
           num(len(ctys)), e(roll_year()), e(o["id"]))
    )


def owner_filing_band(oid, state, fl, o):
    """The filing when there is one, and when there is not, which of the three
    states applies and what that state does and does not claim."""
    if state == MATCHED:
        rows = [
            "<div style=\"grid-column:1/-1\"><dt>Filing we matched</dt>"
            "<dd style=\"font-weight:700\">%s</dd></div>" % e(fl.get("corp_name")),
            "<div><dt>Name we searched</dt><dd>%s</dd></div>" % e(o["name"]),
            "<div><dt>Taxpayer number</dt><dd>%s</dd></div>" % e(dash(fl.get("ttn"))),
            "<div><dt>Right to transact business</dt><dd>%s</dd></div>"
            % e(dash(fl.get("rtt"))),
            "<div><dt>Secretary of State status</dt><dd>%s</dd></div>"
            % e(dash(fl.get("sos_status"))),
            "<div><dt>Effective registration</dt><dd>%s</dd></div>"
            % e(dash(sosdate(fl.get("sos_date")))),
            "<div><dt>State of formation</dt><dd>%s</dd></div>"
            % e(dash(fl.get("formation"))),
            "<div><dt>Texas SOS file number</dt><dd>%s</dd></div>"
            % e(dash(fl.get("file_num"))),
            "<div><dt>Registered agent</dt><dd>%s</dd></div>"
            % e(dash(fl.get("agent"))),
            "<div style=\"grid-column:1/-1\"><dt>Filing mailing address</dt>"
            "<dd>%s</dd></div>" % e(dash(fl.get("mail"))),
        ]
        return (
            "<h3 class=\"subhead\">The Texas business filing behind this name</h3>"
            "<div style=\"border:1px solid var(--rule);border-top:0;"
            "background:var(--paper-2);padding:clamp(0.9rem,3vw,1.3rem)\">"
            "<dl class=\"dl dl--2\">%s</dl>"
            "<p class=\"srcstamp\">Source: Texas Comptroller franchise tax registry "
            "&middot; matched on the owner name as printed on the roll &middot; %s "
            "registry rows carry this owner</p></div>"
            % ("".join(rows), num(fl.get("queried_rows", 0))))

    if state == NO_RECORD:
        cls, head, copy = (
            "payload payload--stop",
            "Texas has no business filing under this name",
            "The registry answered, and the answer was that nothing is filed in Texas "
            "under this name. That is a finding. Plenty of rentals are held by a person "
            "under their own name, by a trust, or by an out-of-state company that never "
            "registered here")
    elif state == NOT_RESOLVED:
        cls, head, copy = (
            "payload payload--open",
            "Our lookup for this name was rejected",
            "The registry returned nothing usable for this name, because the query was "
            "malformed or unresolvable. That is our failure, not a statement that Texas "
            "has no record. Do not read this as the row above it: no finding was made "
            "here either way, and %s owners in scope sit in this same state"
            % num(STORE.stats.get("owner_states", {}).get(NOT_RESOLVED, 0)))
    elif state == OUT_OF_SCOPE:
        cls, head, copy = (
            "payload payload--open",
            "Outside the registry lookup scope",
            "None of this owner's parcels is inside the coverage rules, so the registry "
            "was never asked about the name. That is a decision about what this tool "
            "covers, not a lookup still to come and not a finding")
    else:
        cls, head, copy = (
            "payload payload--open",
            "No registry row reached this owner",
            "No usable registry row is on file for this name at all. Unknown, and drawn "
            "as a gap for that reason")
    return (
        "<h3 class=\"subhead\">The Texas business filing behind this name</h3>"
        "<div style=\"border:1px solid var(--rule);border-top:0;"
        "background:var(--paper-2);padding:clamp(0.9rem,3vw,1.3rem)\">"
        "<div class=\"%s\"><span class=\"who\">%s</span><p style=\"margin:0;"
        "max-width:38rem\">%s</p></div>"
        "<p class=\"tell tell--quiet\">The three states a lookup can end in are different "
        "claims. <b>Matched</b> means a filing lines up. <b>No record</b> means the "
        "registry searched and found nothing filed. <b>Lookup rejected</b> means our query "
        "failed and we know nothing. This owner is in the %s state. "
        "<a href=\"/method\">The method page</a> gives the current count for each</p>"
        "<p class=\"srcstamp\">Source: Texas Comptroller franchise tax registry &middot; "
        "%s registry rows carry this owner</p></div>"
        % (cls, e(head), copy, e(STATE_LABEL[state].lower()),
           num(fl.get("queried_rows", 0))))


def owner_table_head(oid, f):
    return ("<thead><tr>%s%s%s%s%s%s%s</tr></thead>"
            % (sort_th("/owner/" + oid, f, "pid", "Parcel ID"),
               sort_th("/owner/" + oid, f, "address", "Address"),
               sort_th("/owner/" + oid, f, "county", "County"),
               sort_th("/owner/" + oid, f, "value", "Market value", True),
               sort_th("/owner/" + oid, f, "sqft", "Sq ft", True),
               sort_th("/owner/" + oid, f, "year_built", "Built", True),
               sort_th("/owner/" + oid, f, "acquired", "Acquired", True)))


# ---------------------------------------------------------------------------
# http
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    server_version = "landlord-mapper-ui/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("%s %s\n" % (self.log_date_time_string(), fmt % args))

    def send_html(self, html_text, code=200):
        payload = html_text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def redirect(self, to):
        self.send_response(303)
        self.send_header("Location", to)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        try:
            self.route()
        except BrokenPipeError:
            pass
        except Exception:
            import traceback
            traceback.print_exc()
            try:
                self.send_html(page_error(), 500)
            except Exception:
                pass

    def stream_csv(self, f):
        """Streamed, never buffered: a filtered export can be a quarter of a
        million rows and holding that in memory is how this process died once.
        HTTP/1.1 with no Content-Length needs the connection closed at the end,
        which is what Connection: close declares."""
        if f.owner:
            o = STORE.owners.get(f.owner)
            if o is None:
                return self.send_html(page_404("owner", f.owner), 404)
            total = o["n_parcels"]
            cols = EXPORT_PARCEL_COLS
            rows = export_parcel_rows("p.owner_id = ?", (f.owner,),
                                      order_by(f, "p."), EXPORT_CAP)
            name = "landlord-mapper_owner-%s_by-%s" % (f.owner, f.sort)
            what = "parcels held by this owner"
        elif f.shape == "owners":
            cols = EXPORT_OWNER_COLS
            total = rank_owners_count(f)[0]
            rows = export_owner_rows(f)
            name = "landlord-mapper_%s" % f.slug()
            what = "owners ranked over in-scope parcels"
        else:
            w, a = f.where("p.")
            total = count_parcels(f)
            # the CSV is the population the table showed, in the order the
            # table showed it, with no cap on either side any more
            cols = EXPORT_PARCEL_COLS
            rows = export_parcel_rows(w, a, order_by(f, "p."), EXPORT_CAP)
            name = "landlord-mapper_%s" % f.slug()
            what = "parcels matching the filter"
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition",
                         "attachment; filename=\"%s.csv\"" % name)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        if self.command == "HEAD":
            return
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\r\n")
        w.writerow(cols)
        n = 0
        try:
            for row in rows:
                w.writerow(row)
                n += 1
                if n % 2000 == 0:
                    self.wfile.write(buf.getvalue().encode("utf-8"))
                    buf.seek(0)
                    buf.truncate(0)
            if total > n:
                # never truncate silently
                w.writerow(["# TRUNCATED: %d of %d %s written, at the %d row export "
                            "cap. Narrow the filter, by county or ZIP, to get the rest"
                            % (n, total, what, EXPORT_CAP)])
            self.wfile.write(buf.getvalue().encode("utf-8"))
            self.wfile.flush()
        except BrokenPipeError:
            pass

    def route(self):
        u = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(u.path)
        qs = urllib.parse.parse_qs(u.query)

        if path in ("/", "/index.html"):
            return self.send_html(page_home())
        if path in ("/health", "/healthz", "/health.html"):
            return self.send_html(page_health())
        if path == "/favicon.ico":
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if path == "/search":
            q = (qs.get("q", [""])[0] or "").strip()
            if not q:
                return self.redirect("/")
            try:
                page = int(qs.get("page", ["1"])[0])
            except ValueError:
                page = 1
            html_text, single = page_search(q, page)
            if single is not None:
                return self.redirect(parcel_link(single))
            return self.send_html(html_text)
        if path.startswith("/parcel/"):
            # /parcel/<county>/<pid> is the canonical form. /parcel/<pid> is
            # still honoured, because old links carry it, and it resolves when
            # exactly one roll carries that ID; when several do it asks which.
            rest = path[len("/parcel/"):].strip("/")
            if "/" in rest:
                county, pid = rest.split("/", 1)
            else:
                county, pid = "", rest
            cands = STORE.pid_candidates(pid, county or None)
            if not cands:
                return self.send_html(page_404("parcel", rest), 404)
            if len(cands) > 1:
                return self.send_html(page_pid_choice(pid, cands))
            return self.send_html(page_parcel(cands[0]))
        if path.startswith("/owner/"):
            oid = path[len("/owner/"):].strip("/")
            out = page_owner(oid, qs)
            if out is None:
                return self.send_html(page_404("owner", oid), 404)
            return self.send_html(out)
        if path == "/rankings":
            return self.send_html(page_rankings(Filt.from_qs(qs)))
        if path == "/explore":
            return self.send_html(page_explore(Filt.from_qs(qs)))
        if path == "/method":
            return self.send_html(page_method())
        if path in ("/export.csv", "/export"):
            return self.stream_csv(Filt.from_qs(qs))
        return self.send_html(page_404("page", path), 404)


def page_error():
    body = [
        "<header class=\"wrap masthead\">", topline(),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">Something<br /><em>broke</em></h1>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" style=\"padding-top:0\">",
        "<div class=\"empty\"><h3>This page failed to render</h3>"
        "<p>The failure is written to the server log with a traceback. The data in memory is "
        "untouched, so the lookup above still works</p></div>",
        "<div style=\"margin-top:1.4rem\">", lookup_form(), "</div>",
        "</section>", footer()]
    return shell("Error - Landlord Mapper", "".join(body))


class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64


def main():
    t0 = time.time()
    if not os.path.exists(DB_PATH):
        sys.stderr.write("FATAL: no database at %s\n" % DB_PATH)
        sys.stderr.write("Build it from the CSVs first: python3 build-db.py\n")
        raise SystemExit(2)
    sys.stderr.write("opening %s (%.0f MB)\n"
                     % (DB_PATH, os.path.getsize(DB_PATH) / 1048576.0))
    STORE.load()
    st = STORE.stats
    sys.stderr.write(
        "parcel file %s (written %s)\n"
        % (st["parcel_file"], st["parcel_mtime"]))
    sys.stderr.write(
        "loaded %s parcels, %s owners, %s scrape rows in %ss\n"
        % (st["parcel_rows"], st["owners"], st["scrape_rows"], st["load_seconds"]))
    sys.stderr.write(
        "county rolls: %r\n" % (st["counties"],))
    sys.stderr.write(
        "%s distinct parcel IDs, %s of them carried by more than one roll\n"
        % (st["parcel_pids"], st["parcel_pids_shared"]))
    sys.stderr.write(
        "in scope: %s parcels, %s owners; %s in-scope owners answered\n"
        % (st["parcels_in_scope"], st["owners_in_scope"],
           st["owners_in_scope_answered"]))
    sys.stderr.write(
        "scrape rows joined %s, held back: no parcel %s, address clash %s\n"
        % (st["scrape_rows_joined"], st["scrape_rows_no_parcel"],
           st["scrape_rows_addr_clash"]))
    sys.stderr.write("scrape_status rows: %r\n" % (st["scrape_status_rows"],))
    sys.stderr.write("owner states: %r\n" % (st["owner_states"],))
    for w in st.get("errors", []):
        sys.stderr.write("WARNING: %s\n" % w)
    sys.stderr.write(
        "database opened in %.2fs; those figures were computed by build-db.py "
        "and are read, not recomputed\n" % (time.time() - t0))
    srv = Server(("0.0.0.0", PORT), Handler)
    sys.stderr.write("serving on 0.0.0.0:%d\n" % PORT)
    sys.stderr.flush()
    srv.serve_forever()


if __name__ == "__main__":
    main()
