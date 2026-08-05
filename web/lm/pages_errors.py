from lm.chrome import footer, lookup_form, shell, topline
from lm.fmt import e, num
from lm.store import STORE

# ---------------------------------------------------------------------------
# page: not found
# ---------------------------------------------------------------------------
def page_404(what, ident):
    body = [
        "<header class=\"wrap masthead\">", topline(),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">No such<br /><em>%s</em></h1>" % e(what),
        "<div style=\"margin-top:1.8rem\">", lookup_form(), "</div>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" style=\"padding-top:0\">",
        "<div class=\"empty\"><h3>Nothing is loaded under %s</h3>"
        "<p>That %s is not in the data this page has in memory. Either the identifier is "
        "wrong, or it belongs to a county outside the %s parcels loaded here</p>"
        "<p>Start from an address instead. The lookup above searches the address exactly as "
        "the county wrote it</p></div>"
        % (e(ident) or "an empty identifier", e(what),
           num(STORE.stats.get("parcel_rows", 0))),
        "</section>",
        footer(),
    ]
    return shell("Not found - Landlord Mapper", "".join(body))
