import urllib.parse
from lm.chrome import dates_strip, footer, legend_band, lookup_form, scope_note, shell, topline
from lm.config import MAX_HITS, PAGE_SIZE
from lm.fmt import e, num
from lm.store import STORE
from lm.widgets import HIT_HEAD, hit_rows

# ---------------------------------------------------------------------------
# page: home
# ---------------------------------------------------------------------------
JOBS = (
    ("01", "Who owns my building, really?",
     "Put in a street address. You get the name on the county appraisal roll, "
     "then the Texas business filing behind that name, then the people who "
     "signed for it, with the source and the date on every step",
     "#lookup-h", "Start with an address"),
    ("02", "Who are the biggest landlords here?",
     "Owners ranked by how many parcels they hold, how many units those come to, "
     "and what the roll says they are worth. This is the table a campaign picks a "
     "target from, and it counts the rental part of the roll only",
     "/rankings", "See the ranked list"),
    ("03", "Narrow it to where I organize",
     "Filter by county, ZIP, building size, roll value, whether the tax bill "
     "leaves Texas, and the roll's own investor-held and owner-occupied flags. "
     "Every filter lives in the address bar, so the view you build is a link",
     "/explore", "Filter the rolls"),
    ("04", "Give me the list",
     "Any filtered view, any ranking, and any single landlord's portfolio leaves "
     "here as a CSV, so a canvass list can go into the field instead of staying "
     "on a screen. Filter first, then take the download",
     "/explore", "Build a list to download"),
    ("05", "Can I trust this number?",
     "Where the parcel data comes from, which county rolls and which roll year, "
     "what \"in scope\" means stated as the actual rule, and what each of the three "
     "match states does and does not claim. One page to hand a skeptic",
     "/method", "Read the method"),
)

def jobs_band():
    cards = "".join(
        "<div class=\"job\"><span class=\"ix\">%s</span><h3>%s</h3><p>%s</p>"
        "<a class=\"go\" href=\"%s\">%s &rarr;</a></div>"
        % (e(ix), e(title), copy, e(href), e(cta))
        for ix, title, copy, href, cta in JOBS)
    return (
        "<section class=\"band\" aria-labelledby=\"jobs-h\"><div class=\"wrap\">"
        "<h2 class=\"eyebrow\" id=\"jobs-h\">What this tool answers</h2>"
        "<p class=\"prose\" style=\"margin:0.9rem 0 0\">Five questions, in the order an "
        "organizer usually needs them. Each one is a page, and each page states the "
        "population its numbers are counted against</p>"
        "<div class=\"jobs\">%s</div>"
        "</div></section>" % cards)

def page_home():
    st = STORE.stats
    body = [
        "<header class=\"wrap masthead\">", topline("/"),
        "<h1>Who owns<br />the building<br /><em>you rent</em></h1>",
        "<p class=\"deck\">Start with an address. The county appraisal roll gives you the "
        "name on the property, and that name is usually an LLC. The state franchise tax "
        "registry gives you the people who signed for that LLC. This page joins the two "
        "records and shows you both, with the source and the date on every step, so you "
        "can check the work yourself</p>",
        "<p class=\"stamp\">The registry lookup has run to completion. It answered for %s of the "
        "%s owners inside the coverage rules, out of %s owners on the whole roll. Every owner "
        "without an answer is drawn as a gap, never as a clean record</p>"
        % (num(st.get("owners_in_scope_answered", 0)),
           num(st.get("owners_in_scope", 0)), num(st.get("owners", 0))),
        dates_strip(),
        "</header>",
        "<section class=\"wrap band\" id=\"main\" aria-labelledby=\"lookup-h\" style=\"padding-top:0\">",
        "<h2 class=\"eyebrow\" id=\"lookup-h\">Address lookup</h2>",
        "<div style=\"margin-top:0.9rem\">", lookup_form(), "</div>",
        scope_note(),
        "</section>",
        jobs_band(),
        legend_band(),
        footer(),
    ]
    return shell("Landlord Mapper - who owns your building", "".join(body))

def page_search(q, page):
    hits = STORE.search(q)
    if not hits:
        return page_no_hits(q)
    if len(hits) == 1:
        return None, hits[0]
    total = len(hits)
    pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    page = max(1, min(page, pages))
    window = hits[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]
    rows = hit_rows(window)
    prev_cls = "btn btn-quiet" + ("" if page > 1 else " btn-off")
    next_cls = "btn btn-quiet" + ("" if page < pages else " btn-off")
    qq = urllib.parse.quote(q)
    pager = (
        "<div class=\"pager\">"
        "<a class=\"%s\" href=\"/search?q=%s&amp;page=%d\">Previous</a>"
        "<span>Page %d of %d &middot; %s parcels matched%s</span>"
        "<a class=\"%s\" href=\"/search?q=%s&amp;page=%d\">Next</a>"
        "</div>"
        % (prev_cls, qq, max(1, page - 1), page, pages, num(total),
           " (capped at %s)" % num(MAX_HITS) if total >= MAX_HITS else "",
           next_cls, qq, min(pages, page + 1)))
    body = [
        "<header class=\"wrap masthead\">", topline(),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">%s parcels<br />match <em>%s</em></h1>"
        % (num(total), e(q.upper())),
        "<div style=\"margin-top:1.8rem\">", lookup_form(q), "</div>",
        scope_note(),
        "</header>",
        "<section class=\"wrap band\" id=\"main\" aria-labelledby=\"res-h\" style=\"padding-top:0\">",
        "<h3 class=\"subhead\" id=\"res-h\">Pick the parcel you meant</h3>",
        "<div class=\"tablescroll\"><table>", HIT_HEAD,
        "<tbody>", rows, "</tbody></table></div>",
        "<p class=\"tblnote\">Market value is the county value on the roll, not a sale price "
        "&middot; unit counts are estimates and are not shown in this list</p>",
        pager,
        "</section>",
        footer(),
    ]
    return shell("%s parcels match %s - Landlord Mapper" % (total, q), "".join(body)), None

def page_no_hits(q):
    body = [
        "<header class=\"wrap masthead\">", topline(),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">Nothing matched<br /><em>that address</em></h1>",
        "<div style=\"margin-top:1.8rem\">", lookup_form(q), "</div>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" style=\"padding-top:0\">",
        "<div class=\"empty\"><h3>No parcel on the rolls contains %s</h3>"
        "<p>The likely cause is that the address sits outside the county rolls loaded here. "
        "Those are %s. Every parcel on them is searchable, in scope for the registry lookup or "
        "not, so being owner-occupied or small is not what keeps an address out of this list</p>"
        "<p>Try the street number on its own, or the street name on its own. The match is a "
        "plain substring on the address as the county wrote it, so BLVD and BOULEVARD are "
        "not the same string</p></div>"
        % (e(q.upper()),
           e(", ".join(sorted(STORE.stats.get("counties", {}))) or "none loaded")),
        scope_note(),
        "</section>",
        footer(),
    ]
    return shell("Nothing matched - Landlord Mapper", "".join(body)), None
