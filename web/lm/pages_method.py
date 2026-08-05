from lm.chrome import county_names, dates_strip, footer, legend_band, pct, roll_year, scope_den, shell, topline
from lm.fmt import e, num
from lm.schema import MATCHED, NOT_LOOKED_UP, NOT_RESOLVED, NO_RECORD, OUT_OF_SCOPE
from lm.store import STORE

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
