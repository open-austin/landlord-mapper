#!/usr/bin/env python3
"""The typing pass: turn the all-TEXT parcel/owner tables into typed ones.

Why this exists
---------------
The CSV -> SQLite port stored every column as the exact text the CSV carried,
because that made the port provably output-identical: the server's money() /
num() / dash() formatters were handed the same strings they always got. It cost
1.80 GB, of which `parcel` was 796.5 MB and `owner` 277.9 MB, because 2,117,593
rows each spell out "travis", "TRUE", "FALSE" and digit strings in full.

This pass keeps the *values* byte-identical and changes only the *storage*:

  * dictionary coding - a low-cardinality text column becomes an INTEGER code
    into a tiny side table.  Codes are assigned in text sort order, so
    "ORDER BY code" is the same permutation as "ORDER BY the old text"; that is
    what lets /explore?sort=county and sort=zip and sort=acquired page
    identically.  The server selects the text back through a scalar subquery, so
    every consumer of a parcel tuple still receives the original string.

  * dropping a column that is a proven exact function of another.  Measured on
    the live 1.80 GB file, zero rows out of 2,117,593 disagree on any of:
        addr_upper           == situs_address        (already uppercase)
        county_norm          == normtxt(county)
        zip_trim             == trim(situs_zip)
        pdate                == datestamp(recent_purchase_date)
        pid_sort             == pid_norm padded to 14
        parcel.owner_address == its owner row's address
    so those columns carry no information and are reconstructed instead.

  * exception columns - where a column is *nearly* a function of another, only
    the exceptions are stored and the rest is NULL, which costs one header byte
    and no payload.  Measured exception counts: owner_name differs from the
    owner row's name in 3,508 of 2,117,593 rows; totalpropmktvalue differs from
    CAST(n_val AS TEXT) in 9,713.

  * dense rank columns - pid_sort and owner_name_norm exist only to be ordered
    by, never displayed, so they become INTEGER dense ranks over their distinct
    values.  A dense rank is order-preserving AND collapses ties exactly like
    the text did, so "ORDER BY seq, rowid" is the same order as
    "ORDER BY text, rowid" including the tie-break.

What is deliberately NOT touched
--------------------------------
rowid.  It equals the old in-memory parcel index plus one and the server maps
`rowid - 1` back to an index, so the copy writes rowid explicitly and in order.

The numeric text columns totalsqftlivingarea and property_units stay TEXT: the
roll writes them in forms that do not round-trip through an integer (630,445 and
173,023 rows respectively differ from CAST(n_sqft AS TEXT) / CAST(n_units AS
TEXT)), and /export.csv writes all twenty roll columns verbatim.

Used by build-db.py at the end of a build, and by retype.py to convert an
existing database without re-reading the CSVs.
"""
import sys
import time


def _log(msg):
    sys.stderr.write("[typed] %s\n" % msg)
    sys.stderr.flush()


def norm_txt(v):
    return " ".join((v or "").upper().split())


# --- the dictionaries -------------------------------------------------------
# (dict table, [source columns that feed it]).  Several columns may share one
# dictionary when they share a value space; the four roll booleans spell only
# TRUE / FALSE / NA between them, and owner.first_purchase / last_purchase are
# drawn from the same date strings as parcel.pdate.
PARCEL_DICTS = [
    ("d_situs_year", ["situs_year"]),
    ("d_zip", ["situs_zip"]),
    ("d_year_built", ["year_built"]),
    ("d_state_code", ["state_code"]),
    ("d_bool3", ["is_owner_out_of_state", "is_owner_occupied",
                 "is_financialized", "is_mom_and_pop"]),
    ("d_owner_zip", ["owner_zip"]),
    ("d_agent", ["agent_name"]),
    ("d_rpd", ["recent_purchase_date"]),
    ("d_county", ["county"]),
]
# parcel column -> dictionary it is coded against
PARCEL_CODE_COL = {}
for _d, _cols in PARCEL_DICTS:
    for _c in _cols:
        PARCEL_CODE_COL[_c] = _d

OWNER_DICTS = [
    ("d_ostate", ["state"]),
    ("d_counties_all", ["counties_all"]),
    ("d_zips_all", ["zips_all"]),
    ("d_counties_scope", ["counties_scope"]),
]
OWNER_CODE_COL = {}
for _d, _cols in OWNER_DICTS:
    for _c in _cols:
        OWNER_CODE_COL[_c] = _d
# pdate is shared between the parcel column and the two owner date columns
OWNER_CODE_COL["first_purchase"] = "d_pdate"
OWNER_CODE_COL["last_purchase"] = "d_pdate"

ALL_DICTS = ([d for d, _ in PARCEL_DICTS] + [d for d, _ in OWNER_DICTS]
             + ["d_pdate", "d_pid_sort", "d_owner_name_norm"])

# --- the typed schema -------------------------------------------------------
# Column order is the physical order; the server names every column explicitly,
# so this order is free to differ from PARCEL_COLS.
TYPED_SCHEMA = """
CREATE TABLE parcel (
  -- roll columns kept as the exact CSV text, because nothing smaller
  -- reproduces them: /export.csv writes all twenty verbatim
  situs_pID TEXT,
  situs_address TEXT,            -- also answers the old addr_upper, proven equal
  totalsqftlivingarea TEXT,
  property_units TEXT,
  legallocationdesc TEXT,
  -- roll columns as INTEGER codes into the d_* tables, codes in text sort order
  situs_year INTEGER,
  situs_zip INTEGER,             -- also answers the old zip_trim
  year_built INTEGER,
  state_code INTEGER,
  is_owner_out_of_state INTEGER,
  is_owner_occupied INTEGER,
  is_financialized INTEGER,
  is_mom_and_pop INTEGER,
  owner_zip INTEGER,
  agent_name INTEGER,
  recent_purchase_date INTEGER,
  county INTEGER,                -- also answers the old county_norm
  -- exception columns: NULL means "the same as the value it is derived from"
  owner_name_x TEXT,             -- NULL => the owner row's name
  totalpropmktvalue_x TEXT,      -- NULL => CAST(n_val AS TEXT)
  -- derived
  pid_norm TEXT,
  pid_seq INTEGER,               -- dense rank of the old pid_sort
  owner_seq INTEGER,             -- dense rank of the old owner_name_norm
  pdate INTEGER,                 -- code into d_pdate
  owner_id TEXT,
  in_scope INTEGER, f_oos INTEGER, f_occ INTEGER, f_fin INTEGER, f_mom INTEGER,
  n_val INTEGER, n_units INTEGER, n_sqft INTEGER, n_yb INTEGER
);

CREATE TABLE owner (
  owner_id TEXT PRIMARY KEY,
  name TEXT,                     -- also answers parcel.owner_name
  address TEXT,                  -- also answers parcel.owner_address
  in_scope INTEGER,
  state INTEGER,                 -- code into d_ostate
  n_parcels INTEGER, tot_value INTEGER, tot_sqft INTEGER, tot_units INTEGER,
  median_value INTEGER,
  n_out_of_state INTEGER, n_owner_occupied INTEGER,
  counties_all INTEGER,          -- code into d_counties_all
  zips_all INTEGER,              -- code into d_zips_all
  first_purchase INTEGER, last_purchase INTEGER,   -- codes into d_pdate
  n_parcels_scope INTEGER, scope_units INTEGER, scope_value INTEGER,
  counties_scope INTEGER,        -- code into d_counties_scope
  first_rowid INTEGER,
  first_scope_rowid INTEGER,
  corp_name TEXT, agent TEXT
);
"""

# The dictionary tables. d_county and d_zip carry a second text column because
# the server has to look a code up BY the normalised / trimmed form when it
# turns a ?county= or ?zip= filter into codes.
DICT_SCHEMA = """
CREATE TABLE %s (c INTEGER PRIMARY KEY, t TEXT);
"""
DICT_SCHEMA_COUNTY = """
CREATE TABLE d_county (c INTEGER PRIMARY KEY, t TEXT, n TEXT);
"""
DICT_SCHEMA_ZIP = """
CREATE TABLE d_zip (c INTEGER PRIMARY KEY, t TEXT, n TEXT);
"""

# Index names are unchanged from the untyped build, because server.py names
# ix_p_addr explicitly in an INDEXED BY clause and a rename would raise rather
# than silently degrade.
#
# ix_p_county_raw is gone: it indexed the raw county text while ix_p_county_pid
# indexed county_norm, and those were different columns.  Typed, both are the
# same INTEGER code column, so ix_p_county_raw became a strict prefix of
# ix_p_county_pid.  Every parcel ORDER BY in server.py ends in rowid, so a plan
# change cannot change a result set.
TYPED_INDEXES = """
CREATE INDEX ix_p_pid ON parcel(pid_norm);
CREATE INDEX ix_p_county_pid ON parcel(county, pid_norm);
CREATE INDEX ix_p_owner ON parcel(owner_id);
CREATE INDEX ix_p_addr ON parcel(situs_address);
CREATE INDEX ix_p_scope_val ON parcel(n_val) WHERE in_scope = 1;
CREATE INDEX ix_p_scope_units ON parcel(n_units) WHERE in_scope = 1;
CREATE INDEX ix_p_scope_county ON parcel(county, n_val) WHERE in_scope = 1;
CREATE INDEX ix_p_scope_zip ON parcel(situs_zip, n_val) WHERE in_scope = 1;
CREATE INDEX ix_p_county_zip ON parcel(county, situs_zip);
CREATE INDEX ix_p_val ON parcel(n_val);
CREATE INDEX ix_p_units ON parcel(n_units);
CREATE INDEX ix_p_sqft ON parcel(n_sqft);
CREATE INDEX ix_p_yb ON parcel(n_yb);
CREATE INDEX ix_p_zip_raw ON parcel(situs_zip);
CREATE INDEX ix_p_owner_name ON parcel(owner_seq);
CREATE INDEX ix_p_pid_sort ON parcel(pid_seq);
CREATE INDEX ix_p_pdate ON parcel(pdate);
CREATE INDEX ix_p_facets ON parcel(in_scope, f_fin, f_occ, f_mom, f_oos, n_yb, n_units);
CREATE INDEX ix_owner_rank_value ON owner(scope_value DESC, name, first_scope_rowid)
  WHERE in_scope = 1;
CREATE INDEX ix_owner_rank_units ON owner(scope_units DESC, name, first_scope_rowid)
  WHERE in_scope = 1;
CREATE INDEX ix_owner_rank_parcels ON owner(n_parcels_scope DESC, name, first_scope_rowid)
  WHERE in_scope = 1;
"""

# The physical parcel columns, in INSERT order.
PARCEL_PHYS = [
    "situs_pID", "situs_address", "totalsqftlivingarea", "property_units",
    "legallocationdesc", "situs_year", "situs_zip", "year_built", "state_code",
    "is_owner_out_of_state", "is_owner_occupied", "is_financialized",
    "is_mom_and_pop", "owner_zip", "agent_name", "recent_purchase_date",
    "county", "owner_name_x", "totalpropmktvalue_x", "pid_norm", "pid_seq",
    "owner_seq", "pdate", "owner_id", "in_scope", "f_oos", "f_occ", "f_fin",
    "f_mom", "n_val", "n_units", "n_sqft", "n_yb",
]

OWNER_PHYS = [
    "owner_id", "name", "address", "in_scope", "state", "n_parcels",
    "tot_value", "tot_sqft", "tot_units", "median_value", "n_out_of_state",
    "n_owner_occupied", "counties_all", "zips_all", "first_purchase",
    "last_purchase", "n_parcels_scope", "scope_units", "scope_value",
    "counties_scope", "first_rowid", "first_scope_rowid", "corp_name", "agent",
]


def _distinct(cx, table, cols):
    """The union of the distinct values of several columns of one table."""
    seen = set()
    for c in cols:
        for (v,) in cx.execute("SELECT DISTINCT %s FROM %s" % (c, table)):
            seen.add(v)
    return seen


def _write_dict(cx, name, values, second=None):
    """Codes are the position in sorted order, which is what makes ORDER BY on
    the code the same permutation as ORDER BY on the text.  Python sorts str by
    code point and SQLite's BINARY collation compares UTF-8 bytes; for UTF-8
    those are the same order."""
    ordered = sorted(values, key=lambda v: ("" if v is None else v))
    if second is None:
        cx.executemany("INSERT INTO %s (c, t) VALUES (?,?)" % name,
                       [(i, v) for i, v in enumerate(ordered)])
    else:
        cx.executemany("INSERT INTO %s (c, t, n) VALUES (?,?,?)" % name,
                       [(i, v, second(v)) for i, v in enumerate(ordered)])
    return {v: i for i, v in enumerate(ordered)}


def retype(cx, log=_log):
    """Convert the untyped `parcel` and `owner` tables in this connection into
    the typed schema, in place, preserving every rowid.

    On return the connection holds typed `parcel` / `owner` tables, the d_*
    dictionaries, and the typed indexes.  The caller still owes it an ANALYZE
    and a VACUUM.
    """
    t0 = time.time()

    orphans = cx.execute(
        "SELECT COUNT(*) FROM parcel p LEFT JOIN owner o "
        "ON o.owner_id = p.owner_id WHERE o.owner_id IS NULL").fetchone()[0]
    if orphans:
        raise SystemExit(
            "typed: %d parcels have no owner row; owner_name/owner_address "
            "cannot be reconstructed from the owner table" % orphans)

    log("building dictionaries")
    for name, _cols in PARCEL_DICTS + OWNER_DICTS:
        if name == "d_county":
            cx.executescript(DICT_SCHEMA_COUNTY)
        elif name == "d_zip":
            cx.executescript(DICT_SCHEMA_ZIP)
        else:
            cx.executescript(DICT_SCHEMA % name)
    cx.executescript(DICT_SCHEMA % "d_pdate")

    codes = {}
    for name, cols in PARCEL_DICTS:
        vals = _distinct(cx, "parcel", cols)
        if name == "d_county":
            codes[name] = _write_dict(cx, name, vals, second=norm_txt)
        elif name == "d_zip":
            codes[name] = _write_dict(cx, name, vals,
                                      second=lambda v: (v or "").strip())
        else:
            codes[name] = _write_dict(cx, name, vals)
        log("  %-18s %d values" % (name, len(codes[name])))
    for name, cols in OWNER_DICTS:
        codes[name] = _write_dict(cx, name, _distinct(cx, "owner", cols))
        log("  %-18s %d values" % (name, len(codes[name])))
    # one date dictionary shared by parcel.pdate and the two owner date columns
    pdates = _distinct(cx, "parcel", ["pdate"])
    pdates |= _distinct(cx, "owner", ["first_purchase", "last_purchase"])
    codes["d_pdate"] = _write_dict(cx, "d_pdate", pdates)
    log("  %-18s %d values" % ("d_pdate", len(codes["d_pdate"])))

    # dense ranks: order-preserving and tie-preserving, so ORDER BY seq, rowid
    # is the same order as ORDER BY the text, rowid
    pid_seq = {v: i for i, v in enumerate(
        sorted(x for (x,) in cx.execute("SELECT DISTINCT pid_sort FROM parcel")))}
    log("  pid_seq            %d ranks" % len(pid_seq))
    owner_seq = {v: i for i, v in enumerate(sorted(
        x for (x,) in cx.execute("SELECT DISTINCT owner_name_norm FROM parcel")))}
    log("  owner_seq          %d ranks" % len(owner_seq))

    log("creating typed tables")
    cx.executescript(TYPED_SCHEMA.replace("CREATE TABLE parcel (",
                                          "CREATE TABLE parcel_t (")
                     .replace("CREATE TABLE owner (", "CREATE TABLE owner_t ("))

    # --- owner first: parcel.owner_name_x is NULL relative to owner.name -----
    log("copying owner")
    o_state = codes["d_ostate"]
    o_call = codes["d_counties_all"]
    o_zall = codes["d_zips_all"]
    o_cscope = codes["d_counties_scope"]
    o_pd = codes["d_pdate"]
    ins_o = ("INSERT INTO owner_t (" + ", ".join(OWNER_PHYS) + ") VALUES ("
             + ",".join("?" * len(OWNER_PHYS)) + ")")
    name_of = {}
    batch = []
    n = 0
    cur = cx.execute(
        "SELECT owner_id, name, address, in_scope, state, n_parcels, tot_value, "
        "tot_sqft, tot_units, median_value, n_out_of_state, n_owner_occupied, "
        "counties_all, zips_all, first_purchase, last_purchase, "
        "n_parcels_scope, scope_units, scope_value, counties_scope, "
        "first_rowid, first_scope_rowid, corp_name, agent FROM owner")
    wr = cx.cursor()
    for r in cur:
        name_of[r[0]] = r[1]
        batch.append((
            r[0], r[1], r[2], r[3], o_state[r[4]], r[5], r[6], r[7], r[8], r[9],
            r[10], r[11], o_call[r[12]], o_zall[r[13]], o_pd[r[14]],
            o_pd[r[15]], r[16], r[17], r[18], o_cscope[r[19]], r[20], r[21],
            r[22], r[23]))
        if len(batch) >= 20000:
            wr.executemany(ins_o, batch)
            batch = []
        n += 1
    if batch:
        wr.executemany(ins_o, batch)
    cx.commit()
    log("  %d owner rows" % n)

    # --- parcel -------------------------------------------------------------
    log("copying parcel")
    c_year = codes["d_situs_year"]
    c_zip = codes["d_zip"]
    c_yb = codes["d_year_built"]
    c_sc = codes["d_state_code"]
    c_b3 = codes["d_bool3"]
    c_ozip = codes["d_owner_zip"]
    c_ag = codes["d_agent"]
    c_rpd = codes["d_rpd"]
    c_cty = codes["d_county"]
    c_pd = codes["d_pdate"]
    ins_p = ("INSERT INTO parcel_t (rowid, " + ", ".join(PARCEL_PHYS)
             + ") VALUES (" + ",".join("?" * (len(PARCEL_PHYS) + 1)) + ")")
    src = (
        "SELECT rowid, situs_pID, situs_address, totalsqftlivingarea, "
        "property_units, legallocationdesc, situs_year, situs_zip, year_built, "
        "state_code, is_owner_out_of_state, is_owner_occupied, "
        "is_financialized, is_mom_and_pop, owner_zip, agent_name, "
        "recent_purchase_date, county, owner_name, totalpropmktvalue, "
        "pid_norm, pid_sort, owner_name_norm, pdate, owner_id, in_scope, "
        "f_oos, f_occ, f_fin, f_mom, n_val, n_units, n_sqft, n_yb "
        "FROM parcel ORDER BY rowid")
    batch = []
    n = 0
    n_name_x = 0
    n_val_x = 0
    cur = cx.execute(src)
    wr = cx.cursor()
    for r in cur:
        oid = r[24]
        nm = r[18]
        name_x = None if nm == name_of[oid] else nm
        if name_x is not None:
            n_name_x += 1
        raw_val = r[19]
        val_x = None if raw_val == str(r[30]) else raw_val
        if val_x is not None:
            n_val_x += 1
        batch.append((
            r[0], r[1], r[2], r[3], r[4], r[5],
            c_year[r[6]], c_zip[r[7]], c_yb[r[8]], c_sc[r[9]],
            c_b3[r[10]], c_b3[r[11]], c_b3[r[12]], c_b3[r[13]],
            c_ozip[r[14]], c_ag[r[15]], c_rpd[r[16]], c_cty[r[17]],
            name_x, val_x,
            r[20], pid_seq[r[21]], owner_seq[r[22]], c_pd[r[23]], oid,
            r[25], r[26], r[27], r[28], r[29], r[30], r[31], r[32], r[33]))
        if len(batch) >= 20000:
            wr.executemany(ins_p, batch)
            batch = []
        n += 1
        if n % 500000 == 0:
            log("  %d parcel rows" % n)
    if batch:
        wr.executemany(ins_p, batch)
    cx.commit()
    log("  %d parcel rows, %d owner_name exceptions, %d value exceptions"
        % (n, n_name_x, n_val_x))
    del name_of, pid_seq, owner_seq

    log("swapping tables")
    cx.execute("DROP TABLE parcel")
    cx.execute("DROP TABLE owner")
    cx.execute("ALTER TABLE parcel_t RENAME TO parcel")
    cx.execute("ALTER TABLE owner_t RENAME TO owner")
    cx.commit()

    log("indexing")
    for stmt in TYPED_INDEXES.strip().split(";"):
        s = stmt.strip()
        if s:
            cx.execute(s)
    cx.commit()
    log("retype done in %.1fs" % (time.time() - t0))
