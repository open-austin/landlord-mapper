import os

DATA = os.environ.get("LM_DATA", os.path.expanduser("~/landlord-mapper-ui/data"))

PORT = int(os.environ.get("LM_PORT", "8099"))

PAGE_SIZE = 40

MAX_HITS = 400

# Filtered-browse guards.
#   RANK_LIMIT how deep the ranked owner tables go. Past this, use the export.
#              A product limit, not a technical one: nobody picks a campaign
#              target on page 24. Sorting has no cap at all any more, because
#              every sort column carries a covering index.
#   EXPORT_CAP hard row cap on /export.csv. Hitting it writes a trailing
#              comment row: silent truncation would be a lie.
RANK_LIMIT = 1000

EXPORT_CAP = 250000

# The five-county export first, the Travis-only side-effect write as a fallback.
# Order is the preference order.
PARCEL_FILES = ("parcel_roll_5county.csv", "austin_parcel_data_merged.csv")

def parcel_path():
    for name in PARCEL_FILES:
        p = os.path.join(DATA, name)
        if os.path.exists(p):
            return p
    return os.path.join(DATA, PARCEL_FILES[-1])

# Network fan-out guards. A key held by more owners than this is a hub, not a
# link, so it is reported as a count instead of drawn as edges.
HUB_OFFICER = 40

HUB_AGENT = 25

HUB_MAIL = 25

MAX_HOP1 = 6

MAX_HOP2 = 3
