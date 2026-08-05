import time
from lm.chrome import county_names, footer, pct, scope_den, shell, topline
from lm.config import DATA, PARCEL_FILES
from lm.fmt import e, num
from lm.schema import MATCHED, NOT_LOOKED_UP, NOT_RESOLVED, NO_RECORD, OUT_OF_SCOPE
from lm.store import STORE

# ---------------------------------------------------------------------------
# page: health
# ---------------------------------------------------------------------------
def page_health():
    st = STORE.stats
    states = st.get("owner_states", {})
    owners = max(1, st.get("owners", 1))
    scoped = scope_den()
    lines = [
        ("Loaded at", time.strftime("%Y-%m-%d %H:%M:%S",
                                    time.localtime(STORE.loaded_at))),
        ("Load seconds", st.get("load_seconds")),
        ("Data directory", DATA),
        ("Parcel file", "%s (written %s)" % (st.get("parcel_file"), st.get("parcel_mtime"))),
        ("Parcel file preference order", ", ".join(PARCEL_FILES)),
        ("Parcel rows loaded", num(st.get("parcel_rows", 0))),
        ("Distinct parcel IDs", num(st.get("parcel_pids", 0))),
        ("Parcel IDs carried by more than one county roll",
         "%s (%s%% of distinct IDs)"
         % (num(st.get("parcel_pids_shared", 0)),
            pct(st.get("parcel_pids_shared", 0), max(1, st.get("parcel_pids", 1))))),
        ("Repeated records dropped, same ID and address in one county roll",
         num(st.get("parcel_dupes_dropped", 0))),
        ("Parcel rows with wrong column count", num(st.get("parcel_bad_width", 0))),
        ("Parcels flagged owner-occupied", num(st.get("parcels_owner_occupied", 0))),
        ("County rolls loaded",
         "%s: %s" % (num(len(county_names())),
                     ", ".join("%s %s" % (k, num(v)) for k, v in
                               sorted(st.get("counties", {}).items())))),
        ("Parcels in the lookup scope by county",
         ", ".join("%s %s" % (k, num(v)) for k, v in
                   sorted(st.get("scope_counties", {}).items()))),
        ("Roll years present",
         ", ".join("%s %s" % (k, num(v)) for k, v in
                   sorted(st.get("roll_years", {}).items()))),
        ("Distinct owners on the whole roll", num(st.get("owners", 0))),
        ("Parcels in the lookup scope",
         "%s (%s%% of the roll)" % (num(st.get("parcels_in_scope", 0)),
                                    pct(st.get("parcels_in_scope", 0),
                                        max(1, st.get("parcel_rows", 1))))),
        ("Parcels outside the lookup scope",
         "%s (%s%% of the roll)" % (num(st.get("parcels_out_of_scope", 0)),
                                    pct(st.get("parcels_out_of_scope", 0),
                                        max(1, st.get("parcel_rows", 1))))),
        ("Scope predicate",
         "((is_financialized AND NOT is_owner_occupied) OR property_units > 5) "
         "AND property_units != 0"),
        ("Owners in the lookup scope",
         "%s (%s%% of owners on the roll)" % (num(st.get("owners_in_scope", 0)),
                                              pct(st.get("owners_in_scope", 0), owners))),
        ("Scrape files read", st.get("scrape_files")),
        ("Scrape rows read", num(st.get("scrape_rows", 0))),
        ("Scrape rows joined to a parcel",
         "%s (%s%% of rows read)" % (num(st.get("scrape_rows_joined", 0)),
                                     pct(st.get("scrape_rows_joined", 0),
                                         max(1, st.get("scrape_rows", 1))))),
        ("Scrape rows held back, no roll here carries that parcel ID",
         "%s (%s%% of rows read)" % (num(st.get("scrape_rows_no_parcel", 0)),
                                     pct(st.get("scrape_rows_no_parcel", 0),
                                         max(1, st.get("scrape_rows", 1))))),
        ("Scrape rows held back, every candidate parcel sits at another address",
         "%s (%s%% of rows read)" % (num(st.get("scrape_rows_addr_clash", 0)),
                                     pct(st.get("scrape_rows_addr_clash", 0),
                                         max(1, st.get("scrape_rows", 1))))),
        ("Scrape rows with wrong column count", num(st.get("scrape_bad_width", 0))),
        ("Distinct parcels carrying a joined registry row",
         num(st.get("scrape_parcels", 0))),
        ("Newest scrape file", st.get("scrape_newest_mtime")),
        ("Owners with any registry row", num(st.get("owners_with_scrape_rows", 0))),
        ("Registry coverage of owners in scope",
         "%s of %s looked up (%s%%)"
         % (num(st.get("owners_in_scope_answered", 0)),
            num(st.get("owners_in_scope", 0)),
            pct(st.get("owners_in_scope_answered", 0), scoped))),
        ("Owners answered but outside the scope predicate",
         num(st.get("owners_answered_out_of_scope", 0))),
        ("Officer names indexed", num(st.get("network_officers", 0))),
        ("Registered agents indexed", num(st.get("network_agents", 0))),
        ("Filing mail addresses indexed", num(st.get("network_mail", 0))),
    ]
    for k, v in sorted(st.get("scrape_status_rows", {}).items(),
                       key=lambda kv: -kv[1]):
        lines.append(("Rows with scrape_status %s" % (k or "(blank)"),
                      "%s (%s%% of joined rows)"
                      % (num(v), pct(v, max(1, st.get("scrape_rows_joined", 1))))))
    for k in (MATCHED, NO_RECORD, NOT_RESOLVED, NOT_LOOKED_UP):
        lines.append(("Owners resolved as %s" % k,
                      "%s (%s%% of owners in scope)"
                      % (num(states.get(k, 0)), pct(states.get(k, 0), scoped))))
    lines.append(("Owners outside the lookup scope",
                  "%s (%s%% of owners on the roll)"
                  % (num(states.get(OUT_OF_SCOPE, 0)),
                     pct(states.get(OUT_OF_SCOPE, 0), owners))))
    rows = "".join("<tr><td>%s</td><td class=\"r\">%s</td></tr>" % (e(k), e(v))
                   for k, v in lines)
    errs = st.get("errors") or []
    body = [
        "<header class=\"wrap masthead\">", topline("/health"),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">Load<br /><em>report</em></h1>",
        "<p class=\"deck\">What this process has in memory right now. The registry scrape has "
        "finished, so these counts are final rather than a snapshot. Owners still shown as "
        "not looked up are ones the registry never gave a usable answer for, not ones waiting "
        "in a queue</p>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" style=\"padding-top:0\">",
        "<div class=\"tablescroll\"><table><thead><tr><th scope=\"col\">Measure</th>"
        "<th scope=\"col\" class=\"r\">Value</th></tr></thead><tbody>", rows,
        "</tbody></table></div>",
    ]
    if errs:
        body.append("<div class=\"empty\"><h3>Load warnings</h3>%s</div>"
                    % "".join("<p>%s</p>" % e(x) for x in errs))
    else:
        body.append("<p class=\"tblnote\">No load warnings</p>")
    body += [
        "<p class=\"scopenote\">Coverage note: the registry scrape is scoped to rentals, "
        "meaning parcels the owner does not live in that the roll flags as investor-held, plus "
        "any building over 5 units. That is %s of the %s parcels here and %s of the %s owners, "
        "and it is a decision taken in the pipeline, not a shortfall. Coverage is therefore "
        "quoted against the %s owners in scope: %s looked up, %s%%. The %s owners outside the "
        "scope are reported as outside it, never as a lookup pending. Neither case means there "
        "is no landlord</p>"
        % (num(st.get("parcels_in_scope", 0)), num(st.get("parcel_rows", 0)),
           num(st.get("owners_in_scope", 0)), num(st.get("owners", 0)),
           num(st.get("owners_in_scope", 0)),
           num(st.get("owners_in_scope_answered", 0)),
           pct(st.get("owners_in_scope_answered", 0), scoped),
           num(states.get(OUT_OF_SCOPE, 0))),
        "<p class=\"scopenote\">Join note: registry rows are matched on parcel ID and situs "
        "address together. The file loaded here now carries every county roll the pipeline "
        "filters, which is %s, and the counties number their parcels independently, so an ID on "
        "its own is not a key: %s of the %s distinct IDs here are held by more than one roll. An "
        "answer is therefore placed only on the candidate parcel whose situs address agrees. %s "
        "rows name an ID no roll here carries and %s carry an ID whose candidates all sit at "
        "another address. Both sets are held back rather than joined to the wrong building%s</p>"
        % (e(", ".join(sorted(st.get("counties", {}))) or "no county"),
           num(st.get("parcel_pids_shared", 0)), num(st.get("parcel_pids", 0)),
           num(st.get("scrape_rows_no_parcel", 0)),
           num(st.get("scrape_rows_addr_clash", 0)),
           (" &middot; for example " + e("; ".join(st.get("scrape_clash_examples", []))))
           if st.get("scrape_clash_examples") else ""),
        "</section>", footer()]
    return shell("Load report - Landlord Mapper", "".join(body))
