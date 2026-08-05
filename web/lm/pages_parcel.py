from lm.chrome import footer, joined_across, legend_band, lookup_form, parcel_state, shell, topline
from lm.coerce import is_true, norm_txt, to_int
from lm.fmt import dash, datestamp, e, money, num, sosdate, title_case
from lm.schema import MATCHED, NO_RECORD, OUT_OF_SCOPE, P, STATE_CHIP, STATE_LABEL, STATE_NODE
from lm.scope import SCOPE_NOSIZE, SCOPE_OCCUPIED, scope_reason
from lm.store import STORE
from lm.widgets import HIT_HEAD, hit_rows

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
