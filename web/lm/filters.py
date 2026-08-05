import urllib.parse
from lm.coerce import F_FIN, F_MOM, F_OCC, F_OOS, norm_txt, owner_id, to_float
from lm.fmt import num
from lm.schema import P
from lm.store import STORE

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
