import os
import threading

# ---------------------------------------------------------------------------
# shared page furniture
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# skins
#
# Two skins, one process, one database. A second Railway service was the obvious
# reading of "a second version of the site", and it is the wrong one here: the
# service is only a front end over a 1.7 GB read-only volume, so a second copy
# would mean seeding that volume again for a change that is entirely
# presentational. One service serving both also makes them comparable -- the same
# URL, the same row, two skins, no "is that difference real or is it stale data".
#
# The skin is per REQUEST, and page functions do not take it as an argument:
# shell() and topline() are the only two that care, they are called from ~30 page
# functions, and threading a parameter through all of them to reach two
# call sites would be worse than a thread-local. The server is
# ThreadingMixIn/thread-per-request, so a threading.local IS the request scope.
SKIN_FIELD = "field"

SKIN_DSA = "dsa"

SKINS = (SKIN_FIELD, SKIN_DSA)

SKIN_COOKIE = "lm-skin"

# The chapter skin is what a first-time visitor gets: this is Austin DSA's tool
# and it should look like it without anyone having to ask. The field-report skin
# is still whole and one click away in the footer, and LM_SKIN=field flips the
# default back without a code change.
#
# Note what this does NOT do: a browser that already chose a skin keeps it. The
# cookie wins over this default, by design -- changing the default should not
# yank the styling out from under someone mid-session.
DEFAULT_SKIN = os.environ.get("LM_SKIN", SKIN_DSA)

if DEFAULT_SKIN not in SKINS:
    DEFAULT_SKIN = SKIN_DSA

_CURRENT = threading.local()

def skin():
    """The skin for the request being served. Defaults rather than raising: a
    500 renders through page_error() -> shell() on a thread that may never have
    been through route()."""
    return getattr(_CURRENT, "skin", DEFAULT_SKIN)

def set_skin(name):
    _CURRENT.skin = name if name in SKINS else DEFAULT_SKIN

def other_skin():
    return SKIN_FIELD if skin() == SKIN_DSA else SKIN_DSA

# Austin DSA's own mark, from the chapter branding kit. The seal-with-text
# version is deliberately not used anywhere: its arched AUSTIN/DSA stops being
# legible below ~96px and nothing here renders it that big.
# bat-rose.svg, the bare mark with no disc, is deliberately NOT here. It carries
# a white knockout path, so on a near-white ground part of the mark disappears,
# and the disc version reads on paper, on the red banner and on the dark theme
# without a variant per surface.
BRAND_FILES = {
    "bat-circle-red.svg": "image/svg+xml",
    "StyreneB-Regular.otf": "font/otf",
    "ManifoldDSA-Regular.woff2": "font/woff2",
}

BRAND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brand")
