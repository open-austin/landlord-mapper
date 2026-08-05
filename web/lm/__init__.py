#!/usr/bin/env python3
"""
Landlord Mapper web UI.

Standard library only. Reads the pipeline's CSV output from a data directory,
builds in-memory indexes at startup, and serves the ownership chain, the
landlord profile, and the address search over HTTP.

Data contract:
  <data>/parcel_roll_5county.csv         PREFERRED. The whole harmonised parcel
                                         roll, exported straight out of the
                                         pipeline's austin_parcel_data_merged
                                         target, so it carries every county roll
                                         the scrape filters, not one of them.
                                         Written with row.names = FALSE, so it
                                         has no leading row-number column.
  <data>/austin_parcel_data_merged.csv   FALLBACK, used only when the file above
                                         is absent. A Travis-only side-effect
                                         write from the pipeline, and written by
                                         R write.csv() with row names, so its
                                         column 0 is an unnamed row-number
                                         column.
  <data>/owner_data_total.csv            franchise-registry scrape output.
  <data>/owner_data_part_*.csv           same schema, unioned in.

The two parcel files differ in that leading column, which is exactly why all
field access here is by header NAME and never by position: a positional read is
correct for one of them and off by one for the other.

Join key is situs_pID plus situs_address. The scrape writes the pID zero-padded
to 12 characters and the parcel roll does not, so every comparison goes through
norm_pid(). Padding is not the only hazard: the roll is the rbind of a dozen
county rolls (_targets.R, austin_parcel_data_merged) and the counties reuse the
same numeric pID space, so a pID is not a key on its own. Roughly 468k of the
IDs loaded here are held by more than one county roll. by_pid therefore maps one
ID to every parcel carrying it, and a registry row is placed only on the
candidate whose situs_address agrees. Rows that agree with none of them are
counted and held back rather than joined to the wrong building, and a parcel URL
carries its county for the same reason.

Owner identity is the pair (owner_name, owner_address) from the parcel roll,
because that pair is what the scrape was keyed on.

The registry lookup is scoped to the rental-shaped part of the roll on purpose.
parcel_in_scope() reproduces that scope so the coverage figures are quoted
against the parcels the scrape was ever going to ask about, not against the
whole roll.

Env:
  LM_DATA        data directory (default ~/landlord-mapper-ui/data)
  LM_PORT        listen port (default 8099)
  LM_EXTRA_OWNER_CSV
                 optional extra scrape CSV, comma separated, unioned in after
                 the real files. Used to exercise scrape_status values that the
                 in-flight run has not written yet. Never set in production.
"""
