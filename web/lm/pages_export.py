from lm.config import EXPORT_CAP
from lm.filters import RANK_SLOT, SCOPE_IN
from lm.ranksql import RANK_GROUP_METRIC, RANK_METRIC, rank_group_sql
from lm.schema import NOT_LOOKED_UP, OUT_OF_SCOPE, PARCEL_COLS
from lm.sql import O_COUNTIES_SCOPE, O_STATE, PARCEL_EXPR, PARCEL_FROM, PARCEL_SQL, _dict_sql
from lm.store import STORE, parcel_path_for

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
