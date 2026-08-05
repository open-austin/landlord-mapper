from lm.chrome import footer, legend_band, lookup_form, parcel_link, roll_year, shell, topline
from lm.filters import Filt, owner_parcels_page
from lm.fmt import dash, datestamp, e, money, num, sosdate, title_case
from lm.netdiagram import network_panel
from lm.ranksql import counties_all_list, space_list
from lm.schema import MATCHED, NOT_LOOKED_UP, NOT_RESOLVED, NO_RECORD, OUT_OF_SCOPE, P, STATE_CHIP, STATE_LABEL
from lm.store import STORE
from lm.widgets import sort_th

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
