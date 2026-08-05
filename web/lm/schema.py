# ---------------------------------------------------------------------------
# parcel record layout
# ---------------------------------------------------------------------------
PARCEL_COLS = [
    "situs_year", "situs_pID", "situs_address", "situs_zip",
    "totalsqftlivingarea", "property_units", "year_built", "state_code",
    "is_owner_out_of_state", "is_owner_occupied", "is_financialized",
    "is_mom_and_pop", "legallocationdesc", "owner_name", "owner_address",
    "owner_zip", "agent_name", "recent_purchase_date", "totalpropmktvalue",
    "county",
]

P = {name: i for i, name in enumerate(PARCEL_COLS)}

SCRAPE_COLS = [
    "owner_name_scraped", "owner_scraped_title", "owner_address_scraped",
    "owner_active_year", "corp_business_name", "corp_TTN", "corp_mail_address",
    "corp_right_to_transact_business_tx_status", "corp_state_of_formation",
    "corp_sos_registration_status", "corp_effective_sos_registration_date",
    "corp_tx_sos_file_num", "corp_registered_agent_name",
    "corp_registered_agent_mail_add", "scrape_status", "situs_pID",
    "situs_address",
]

S = {name: i for i, name in enumerate(SCRAPE_COLS)}

# resolved states
MATCHED = "matched"

NO_RECORD = "no_record"

NOT_RESOLVED = "not_resolved"

NOT_LOOKED_UP = "not_looked_up"

# not a scrape_status: the scrape was never going to ask about this parcel. It
# borrows the dashed and open treatment because the chain still does not end in
# an answer, and the copy says which coverage rule put it outside.
OUT_OF_SCOPE = "out_of_scope"

STATE_LABEL = {
    MATCHED: "Matched",
    NO_RECORD: "No record",
    NOT_RESOLVED: "Lookup rejected",
    NOT_LOOKED_UP: "Not looked up",
    OUT_OF_SCOPE: "Outside coverage",
}

STATE_CHIP = {
    MATCHED: "chip--matched",
    NO_RECORD: "chip--norec",
    NOT_RESOLVED: "chip--unknown",
    NOT_LOOKED_UP: "chip--unknown",
    OUT_OF_SCOPE: "chip--unknown",
}

# the run/terminator stroke that encodes the state
STATE_NODE = {
    MATCHED: "",
    NO_RECORD: " node--stop",
    NOT_RESOLVED: " node--dashed",
    NOT_LOOKED_UP: " node--dashed",
    OUT_OF_SCOPE: " node--dashed",
}

STATE_GLYPH = {
    MATCHED: "g--matched",
    NO_RECORD: "g--norec",
    NOT_RESOLVED: "g--unknown",
    NOT_LOOKED_UP: "g--unknown",
    OUT_OF_SCOPE: "g--unknown",
}
