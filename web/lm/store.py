import json
import os
import sqlite3
import threading
import time
import urllib.parse
from lm.coerce import norm_pid, norm_txt, owner_id
from lm.config import HUB_AGENT, HUB_MAIL, HUB_OFFICER, MAX_HITS, MAX_HOP1, MAX_HOP2
from lm.schema import P
from lm.scope import parcel_in_scope
from lm.sql import DB_PATH, FILING_COLS, FILING_SQL, OWNER_COLS, OWNER_SQL, PARCEL_FROM, PARCEL_SQL

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
