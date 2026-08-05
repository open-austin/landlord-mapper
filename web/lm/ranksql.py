import threading
from lm.filters import RANK_SLOT
from lm.sql import O_COUNTIES_SCOPE, O_STATE, _dict_sql
from lm.store import STORE

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

# The unfiltered ranking totals, memoised for the life of the process.
#
# This is a constant of the database, not a property of a request: it is
# COUNT/SUM over every in-scope owner with no filter narrowing it, and the file
# is opened read-only and never changes while this process lives. Caching it is
# therefore not a staleness risk, it is a statement of that fact.
#
# Only ever holds a 4-tuple of ints. Nothing that belongs to a thread -- no
# connection, no cursor, no memo dict -- goes in here, which is what keeps it
# clear of the same-thread rule that killed the session pool.
_RANK_TOTALS = None

_RANK_TOTALS_LOCK = threading.Lock()

# Read out of `meta` when build-db.py put it there, in which case the unfiltered
# rankings page costs no aggregate scan at all. Absent -- an older database file
# -- the code below still answers it, just by scanning.
RANK_TOTALS_KEY = "rank_totals_in_scope"

# The fallback, written as four scalar subqueries rather than one four-aggregate
# SELECT. That is the whole point of the shape: measured with EXPLAIN QUERY PLAN
# on the typed schema,
#
#   SELECT COUNT(*), SUM(n_parcels_scope), SUM(scope_units), SUM(scope_value)
#     FROM owner WHERE in_scope = 1                       ->  SCAN owner
#
# because no one index carries all three summed columns, so SQLite reads the
# whole 101 MB owner table. Split into one subquery per column, each leg is
# answered out of a partial index that already exists:
#
#   COUNT(*), SUM(n_parcels_scope) -> COVERING INDEX ix_owner_rank_parcels
#   SUM(scope_units)               -> COVERING INDEX ix_owner_rank_units
#   SUM(scope_value)               -> COVERING INDEX ix_owner_rank_value
#
# Same rows, same predicate, same numbers -- SQLite is just allowed to read
# 52 MB of index instead of 101 MB of table. No new index, no schema change, so
# this works on a volume that is already seeded.
RANK_TOTALS_SQL = (
    "SELECT (SELECT COUNT(*) FROM owner WHERE in_scope = 1), "
    "(SELECT SUM(n_parcels_scope) FROM owner WHERE in_scope = 1), "
    "(SELECT SUM(scope_units) FROM owner WHERE in_scope = 1), "
    "(SELECT SUM(scope_value) FROM owner WHERE in_scope = 1)")

def rank_totals_in_scope():
    """(owners, in-scope parcels, units, value) over every in-scope owner.

    Costs at most one scan per process, and none at all once the database
    carries the figure in `meta`. It used to cost one full scan of the owner
    table per request, which is what made a cold /rankings 21 s: pages 2..25 of
    the same ranking and every /export.csv?as=owners each paid it again.
    """
    global _RANK_TOTALS
    got = _RANK_TOTALS
    if got is not None:
        return got
    with _RANK_TOTALS_LOCK:
        if _RANK_TOTALS is None:
            m = STORE.stats.get(RANK_TOTALS_KEY)
            # `type(x) is int`, not isinstance: bool subclasses int, so
            # [true,true,true,true] in meta would otherwise be accepted and the
            # page would state 1 owner. Negatives are rejected for the same
            # reason -- these are a COUNT and three SUMs over a non-negative
            # column, so anything below zero is a corrupted meta row, and
            # falling through to the scan is the right answer in both cases.
            if (isinstance(m, (list, tuple)) and len(m) == 4
                    and all(type(x) is int and x >= 0 for x in m)):
                _RANK_TOTALS = tuple(m)
            else:
                t = STORE.db.one(RANK_TOTALS_SQL)
                _RANK_TOTALS = tuple((x or 0) for x in t)
        return _RANK_TOTALS

def rank_owners_count(f):
    """(owners matching, [in-scope parcels, units, value] across all of them)."""
    if f.trivial():
        t = rank_totals_in_scope()
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
