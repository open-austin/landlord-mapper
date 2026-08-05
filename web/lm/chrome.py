import time
import urllib.parse
from lm.fmt import e, num
from lm.schema import MATCHED, NOT_LOOKED_UP, NOT_RESOLVED, NO_RECORD, OUT_OF_SCOPE, P
from lm.skin import SKIN_DSA, _CURRENT, other_skin, skin
from lm.store import STORE
from lm.styles import CSS_DSA, CSS_FIELD, THEME_JS

DSA_HEAD = (
    "<link rel=\"icon\" href=\"/brand/bat-circle-red.svg\" type=\"image/svg+xml\">"
    "<meta name=\"theme-color\" content=\"#ec1f27\">"
)

def shell(title, body, skip="#main"):
    dsa = skin() == SKIN_DSA
    return (
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        "<title>%s</title>%s<style>%s</style></head><body>"
        "<a class=\"skiplink\" href=\"%s\">Skip to the record</a>"
        "%s<script>%s</script></body></html>"
        % (e(title), DSA_HEAD if dsa else "",
           CSS_DSA if dsa else CSS_FIELD, e(skip), body, THEME_JS)
    )

# Labelled by the question each page answers. A reader who has never heard the
# word "rankings" still recognises "biggest landlords".
NAV = (("/", "Who owns my building"), ("/rankings", "Biggest landlords"),
       ("/explore", "Where I organize"), ("/method", "Can I trust this"),
       ("/health", "Load report"))

def topline(current=""):
    nav = "".join(
        "<a href=\"%s\"%s>%s</a>"
        % (h, " aria-current=\"page\"" if h == current else "", e(t))
        for h, t in NAV)
    dsa = skin() == SKIN_DSA
    if dsa:
        # The mark is decorative here: the adjacent text already names the
        # chapter, so alt="" keeps a screen reader from reading it twice.
        org = ("<div class=\"orgmark\"><a href=\"/\">"
               "<img class=\"batmark\" src=\"/brand/bat-circle-red.svg\" alt=\"\" "
               "width=\"34\" height=\"34\">"
               # .wm, not a bare span: the base rule `.orgmark span` paints
               # spans in --ink-2 to mute the "/ OPEN AUSTIN" half, and an
               # unclassed wrapper would inherit that down over the whole
               # wordmark. .wm puts --ink back.
               "<span class=\"wm\"><b>LANDLORD MAPPER</b> <span>/ AUSTIN DSA</span></span>"
               "</a></div>")
    else:
        org = ("<div class=\"orgmark\"><a href=\"/\"><b>LANDLORD MAPPER</b> "
               "<span>/ OPEN AUSTIN</span></a></div>")
    return (
        "<div class=\"topline\">%s"
        "<nav class=\"navmark\" aria-label=\"Sections\">%s</nav>"
        "<button class=\"themebtn\" id=\"themebtn\" type=\"button\" aria-pressed=\"false\">Dark mode</button>"
        "</div>%s" % (org, nav, brandband() if dsa else "")
    )

def brandband():
    """The red strip under the masthead. It says who publishes this, which is the
    one thing the field skin's markless header never did."""
    return (
        "<div class=\"brandband\"><div class=\"wrap\">"
        "<p>Austin DSA &middot; Housing Justice</p>"
        "<p class=\"thin\">Public records, put back in tenants' hands</p>"
        "</div></div>"
    )

def skinswitch():
    """Lives in the footer, not the masthead: it is a thing you do once, not a
    section of the site.

    Rebuilds the CURRENT url with skin= replaced rather than linking to
    "?skin=x", which would silently drop the filters on /explore and /rankings --
    the two pages where someone is most likely to be comparing the two skins."""
    to = other_skin()
    label = "Austin DSA styling" if to == SKIN_DSA else "Field-report styling"
    u = urllib.parse.urlsplit(getattr(_CURRENT, "url", "/") or "/")
    q = [(k, v) for k, v in urllib.parse.parse_qsl(u.query, keep_blank_values=True)
         if k != "skin"]
    q.append(("skin", to))
    href = urllib.parse.urlunsplit(("", "", u.path or "/",
                                    urllib.parse.urlencode(q), ""))
    return (
        "<p class=\"skinswitch\"><a href=\"%s\" rel=\"nofollow\">Switch to %s</a></p>"
        % (e(href), e(label))
    )

def county_names():
    return sorted(k.strip() for k in STORE.stats.get("counties", {}) if k.strip())

def counties_loaded():
    """Every county roll in memory, named. Used where the copy promises a list
    the reader can check an address against."""
    names = [k.title() for k in county_names()]
    if not names:
        return "no county"
    if len(names) == 1:
        return names[0]
    return ", ".join(names[:-1]) + " or " + names[-1]

def counties_phrase():
    """The short form, for a form label. A dozen county names do not belong in
    one, so past a handful it counts them instead of listing them."""
    names = county_names()
    if not names:
        return "no county roll"
    if len(names) == 1:
        return "the %s county roll" % names[0].title()
    if len(names) <= 3:
        return "the %s county rolls" % counties_loaded()
    return "any of the %s county rolls" % num(len(names))

def joined_across():
    names = county_names()
    if len(names) == 1:
        return "Joined across the %s roll" % names[0]
    return "Joined across %s county rolls" % num(len(names))

def parcel_link(i):
    """A parcel URL carries its county because a parcel ID does not identify a
    building on its own once more than one county roll is loaded."""
    rec = STORE.parcels[i]
    return "/parcel/%s/%s" % (
        urllib.parse.quote(rec[P["county"]].strip() or "unknown"),
        urllib.parse.quote(rec[P["situs_pID"]].strip()))

def lookup_form(value="", label=None):
    lbl = label or ("Street address in %s" % counties_phrase())
    return (
        "<form class=\"lookup\" action=\"/search\" method=\"get\">"
        "<label for=\"q\">%s</label>"
        "<div class=\"field\">"
        "<input id=\"q\" name=\"q\" type=\"text\" value=\"%s\" autocomplete=\"off\" "
        "placeholder=\"e.g. 1201 S LAMAR BLVD\" />"
        "<button class=\"btn\" type=\"submit\">Look up owner</button>"
        "</div></form>"
        % (e(lbl), e(value))
    )

def scope_note():
    st = STORE.stats
    return (
        "<p class=\"scopenote\">The registry lookup covers rentals: parcels the owner does "
        "not live in that the roll flags as investor-held, and any building over 5 units. "
        "That is %s of the %s parcels on the rolls, and the other %s were never going to be "
        "looked up. A parcel outside those rules says so on its own page, which is not the "
        "same as nobody owning it</p>"
        % (num(st.get("parcels_in_scope", 0)), num(st.get("parcel_rows", 0)),
           num(st.get("parcels_out_of_scope", 0)))
    )

def dates_strip():
    st = STORE.stats
    return (
        "<div class=\"datestrip\">"
        "<div>Appraisal rolls: <b>%s counties, %s parcels, %s in scope, roll year %s</b></div>"
        "<div>Registry lookup: <b>complete, newest answer %s</b></div>"
        "<div>Data read into this page: <b>%s</b></div>"
        "</div>"
        % (num(len(county_names())),
           num(st.get("parcel_rows", 0)),
           num(st.get("parcels_in_scope", 0)),
           e(roll_year()),
           e(st.get("scrape_newest_mtime", "n/a")),
           e(time.strftime("%Y-%m-%d %H:%M", time.localtime(STORE.loaded_at))))
    )

_ROLL_YEAR = [""]

def roll_year():
    """One year when every roll agrees, a range when they do not: the counties
    publish on their own schedules, so a dozen rolls need not share a year."""
    if not _ROLL_YEAR[0]:
        # digits only: a handful of rows carry NA in situs_year, and an NA is
        # not the far end of a range. The full tally, NA included, is on the
        # load report
        ys = sorted(y for y in STORE.stats.get("roll_years", {}) if y.isdigit())
        if len(ys) == 1:
            _ROLL_YEAR[0] = ys[0]
        elif ys:
            _ROLL_YEAR[0] = "%s to %s" % (ys[0], ys[-1])
    return _ROLL_YEAR[0] or "unknown"

def footer():
    st = STORE.stats
    states = st.get("owner_states", {})
    scoped = scope_den()
    rows_by_status = st.get("scrape_status_rows", {})
    counties = ", ".join(
        "%s %s" % (k, num(v)) for k, v in
        sorted(st.get("counties", {}).items(), key=lambda kv: -kv[1]))
    status_bits = " &middot; ".join(
        "%s %s" % (e(k or "blank"), num(v)) for k, v in
        sorted(rows_by_status.items(), key=lambda kv: -kv[1])) or "none yet"
    return (
        "<footer class=\"band foot\" aria-labelledby=\"foot-h\"><div class=\"wrap footgrid\">"
        "<div>"
        "<h3 id=\"foot-h\">Why there are two dates</h3>"
        "<p>The appraisal roll is published once a year, so ownership shown here is as of "
        "the roll and can lag a sale by months. The business registry read has finished, and "
        "it worked through owners one at a time. Officers change between those two dates, "
        "which is why each record on a chain carries its own stamp instead of one date for "
        "the whole page</p>"
        "<p>Two things this data does not have: mailing addresses for tax agents, which the "
        "counties do not publish in the roll, and reliable dates on deed transfers. Nothing "
        "here is built on either one. Unit counts are estimated from floor area, so they are "
        "marked as estimates everywhere they appear</p>"
        "<p>Some registry answers cannot be placed on these rolls, and those are held back "
        "rather than guessed at: %s rows name a parcel ID no roll loaded here carries, and %s "
        "more carry an ID whose candidate parcels all sit at a different address. %s IDs here "
        "are held by more than one county roll, which is why an answer only ever lands on the "
        "candidate whose address agrees. All three counts are on the load report</p>"
        "<p>Officer home addresses are in the source records and are deliberately not shown, "
        "and there is no search by person name. This tool answers who owns a building, not "
        "what a named human owns</p>"
        "</div><div>"
        "<h3>What is loaded right now</h3>"
        "<ul class=\"srclist\">"
        "<li><b>Appraisal rolls</b> &middot; %s parcels &middot; %s</li>"
        "<li><b>In the lookup scope</b> &middot; %s parcels, %s owners &middot; the rest of "
        "the roll was never queued</li>"
        "<li><b>Distinct owners on the whole roll</b> &middot; %s &middot; keyed on name plus "
        "mailing address</li>"
        "<li><b>Registry rows joined</b> &middot; %s of %s read, across %s parcels</li>"
        "<li><b>Rows by status</b> &middot; %s</li>"
        "<li><b>Owners matched</b> &middot; %s (%s%% of the %s in scope)</li>"
        "<li><b>Owners with no Texas filing</b> &middot; %s (%s%% of those in scope)</li>"
        "<li><b>Owners in scope not looked up yet</b> &middot; %s (%s%% of those in scope)</li>"
        "<li><b>Open source</b> &middot; open-austin/landlord-mapper</li>"
        "</ul>"
        "<p style=\"margin-top:1rem\"><a href=\"/method\">Where every number comes from"
        "</a> &middot; <a href=\"/health\">Full load report</a></p>"
        # NOT string-concatenated in: a url-encoded href carries %XX escapes and
        # would be eaten by the % formatting below. It goes through as an arg.
        "%s"
        "</div></div></footer>"
        % (num(st.get("scrape_rows_no_parcel", 0)),
           num(st.get("scrape_rows_addr_clash", 0)),
           num(st.get("parcel_pids_shared", 0)),
           num(st.get("parcel_rows", 0)), e(counties),
           num(st.get("parcels_in_scope", 0)), num(st.get("owners_in_scope", 0)),
           num(st.get("owners", 0)),
           num(st.get("scrape_rows_joined", 0)), num(st.get("scrape_rows", 0)),
           num(st.get("scrape_parcels", 0)),
           status_bits,
           num(states.get(MATCHED, 0)), pct(states.get(MATCHED, 0), scoped),
           num(st.get("owners_in_scope", 0)),
           num(states.get(NO_RECORD, 0)), pct(states.get(NO_RECORD, 0), scoped),
           num(states.get(NOT_LOOKED_UP, 0) + states.get(NOT_RESOLVED, 0)),
           pct(states.get(NOT_LOOKED_UP, 0) + states.get(NOT_RESOLVED, 0), scoped),
           skinswitch())
    )

def pct(n, d):
    if not d:
        return "0.0"
    return "%.1f" % (100.0 * n / d)

def scope_den():
    """Owners the registry lookup was ever going to ask about. The honest
    denominator for coverage."""
    return max(1, STORE.stats.get("owners_in_scope", 1))

def parcel_state(i, o):
    """State to draw for one parcel. The owner's state, except that a parcel
    outside the coverage rules is reported as that rather than as a lookup still
    to come. An owner already matched through another, in-scope parcel keeps its
    filing: the filing belongs to the name, not to the building."""
    state = o.get("state", NOT_LOOKED_UP)
    if state in (NOT_LOOKED_UP, OUT_OF_SCOPE) and not STORE.in_scope[i]:
        return OUT_OF_SCOPE
    return state

def legend_band():
    st = STORE.stats
    states = st.get("owner_states", {})
    scoped = scope_den()
    unknown = states.get(NOT_LOOKED_UP, 0) + states.get(NOT_RESOLVED, 0)
    return (
        "<section class=\"band legendband\" aria-labelledby=\"legend-h\"><div class=\"wrap\">"
        "<h2 class=\"eyebrow\" id=\"legend-h\">How to read the end of a chain</h2>"
        "<p class=\"prose\" style=\"margin:0.9rem 0 0\">A chain can end three ways, and the "
        "difference matters. Two of them are answers. One of them is a gap in what we know, "
        "and it is drawn as a gap so you never mistake it for an answer</p>"
        "<div class=\"endings\">"
        "<div class=\"ending\"><div class=\"glyph g--matched\" aria-hidden=\"true\">"
        "<span class=\"g-mark\"></span><span class=\"g-run\"></span><span class=\"g-term\"></span></div>"
        "<div class=\"body\"><h3>Matched</h3>"
        "<p>The name on the roll lines up with a Texas business filing. We show both names "
        "side by side so you can reject a bad match yourself</p>"
        "<p class=\"ex\">%s owners of the %s inside the coverage rules, %s%% so far</p></div></div>"
        "<div class=\"ending\"><div class=\"glyph g--norec\" aria-hidden=\"true\">"
        "<span class=\"g-mark\"></span><span class=\"g-run\"></span><span class=\"g-term\"></span></div>"
        "<div class=\"body\"><h3>No record</h3>"
        "<p>We searched and Texas has no business registration under this name. That is a "
        "finding, not a miss: plenty of rentals are held by a person or an out-of-state "
        "entity that never registered here. The line stops on a hard bar because the search "
        "finished</p>"
        "<p class=\"ex\">%s owners so far</p></div></div>"
        "<div class=\"ending\"><div class=\"glyph g--unknown\" aria-hidden=\"true\">"
        "<span class=\"g-mark\"></span><span class=\"g-run\"></span><span class=\"g-term\"></span></div>"
        "<div class=\"body\"><h3>No answer</h3>"
        "<p>No usable answer came back for this name. Almost all of these are lookups the "
        "registry rejected outright, which is our query failing, not Texas reporting that "
        "nothing is filed. That claim is the middle column, and this is not it. We do not "
        "know either way here, so the line trails off dashed and open</p>"
        "<p class=\"ex\">%s owners in scope: rejected lookup or never queried</p></div></div>"
        "</div>"
        "<p class=\"prose\" style=\"margin:1.6rem 0 0\">One more case borrows that same dashed "
        "ending: a parcel outside the coverage rules. The registry was never asked about it, on "
        "purpose, so the chain is a gap here too, and the page names the rule that put it "
        "outside instead of implying an answer is on its way. %s of the %s parcels on the rolls "
        "are inside the rules</p>"
        "<div class=\"sharenote\"><span class=\"big\">%s%%</span>"
        "<p>of the %s owners inside the coverage rules have no registry answer. The scrape has "
        "finished, so these are not owners waiting in a queue: almost all of them are lookups "
        "the registry rejected. Read them as unknown, never as unregistered</p></div>"
        "</div></section>"
        % (num(states.get(MATCHED, 0)), num(st.get("owners_in_scope", 0)),
           pct(states.get(MATCHED, 0), scoped),
           num(states.get(NO_RECORD, 0)),
           num(unknown),
           num(st.get("parcels_in_scope", 0)), num(st.get("parcel_rows", 0)),
           pct(unknown, scoped), num(st.get("owners_in_scope", 0)))
    )
