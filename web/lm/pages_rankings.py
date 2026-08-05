from lm.chrome import footer, legend_band, parcel_link, parcel_state, scope_note, shell, topline
from lm.config import EXPORT_CAP, PAGE_SIZE, RANK_LIMIT
from lm.filters import RANK_LABEL, SCOPE_IN, count_parcels, page_parcels, warm_owners_for
from lm.fmt import dash, e, money, num
from lm.ranksql import rank_owners_count, rank_owners_rows, space_list
from lm.schema import NOT_LOOKED_UP, P
from lm.store import STORE
from lm.widgets import SORT_LABEL, facet_form, filter_line, pager_bar, sort_th, state_chip

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
