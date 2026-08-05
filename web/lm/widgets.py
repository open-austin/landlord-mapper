from lm.chrome import county_names, parcel_link, parcel_state
from lm.coerce import norm_txt
from lm.filters import FLAG_PARAMS, RANGE_PARAMS, RANK_LABEL, SCOPE_ALL, SCOPE_IN, SORT_KEYS, warm_owners_for
from lm.fmt import e, money, num
from lm.schema import P, STATE_CHIP, STATE_LABEL
from lm.store import STORE

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
