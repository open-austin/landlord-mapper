import html
from lm.coerce import to_int

# ---------------------------------------------------------------------------
# formatting
# ---------------------------------------------------------------------------
def e(v):
    return html.escape("" if v is None else str(v), quote=True)

def money(n):
    return "$%s" % format(to_int(n), ",d")

def num(n):
    # values arrive as strings from CSV and R writes large ones in scientific
    # notation ("1.1e+08"), so everything goes through the float parse
    return format(to_int(n), ",d")

def dash(v):
    v = (v or "").strip()
    return v if v and v.upper() not in ("NA", "N/A", "NULL") else "not on the roll"

def datestamp(v):
    v = (v or "").strip()
    if not v or v.upper() in ("NA", "NULL"):
        return ""
    return v.split(" ")[0]

def sosdate(v):
    v = (v or "").strip()
    if "/" in v:
        parts = v.split("/")
        if len(parts) == 3:
            return "%s-%s-%s" % (parts[2], parts[0].zfill(2), parts[1].zfill(2))
    return v

def title_case(v):
    return (v or "").strip().title()
