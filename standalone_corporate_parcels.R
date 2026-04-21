# standalone_corporate_parcels.R
#
# Purpose
# -------
# Downloads the TCAD Special Export (JSON) from traviscad.org and identifies
# residential parcels in Travis County that are likely owned by corporate or
# "financialized" interests.
#
# Output
# ------
# corporate_owned_parcels.csv  – one row per matching parcel, written to the
#                                 current working directory.
#
# How to run
# ----------
# 1. Open this file in RStudio (or any R console).
# 2. Run it section-by-section with Ctrl+Enter, or all at once with
#    source("standalone_corporate_parcels.R").
#
# Requirements
# ------------
# R packages (installed automatically if missing):
#   rvest, dplyr, jsonlite, readr
#
# Memory note
# -----------
# The TCAD JSON export is a large file. Parsing it with jsonlite requires
# roughly 3x the uncompressed file size in free RAM (often 8–16 GB).
# If you run out of memory, try closing other applications or running on a
# machine / cloud VM with more RAM.
#
# No Python, no Selenium, no Census API key, no ijson needed.
# ──────────────────────────────────────────────────────────────────────────────

# ── 0. Install / load required packages ──────────────────────────────────────

required_pkgs <- c("rvest", "dplyr", "jsonlite", "readr")
missing_pkgs  <- required_pkgs[!required_pkgs %in% installed.packages()[, "Package"]]
if (length(missing_pkgs)) {
  message("Installing missing packages: ", paste(missing_pkgs, collapse = ", "))
  install.packages(missing_pkgs)
}

library(rvest)
library(dplyr)
library(jsonlite)
library(readr)

# ── 1. Configuration ──────────────────────────────────────────────────────────

options(timeout = 7200)  # allow up to 2 hours for large downloads

zip_path <- "tcad_special_export.zip"   # where to save / look for the ZIP
out_path <- "corporate_owned_parcels.csv"

# ── 2. Download TCAD data ─────────────────────────────────────────────────────
# Scrapes traviscad.org/publicinformation for the latest "Special Export (JSON)"
# ZIP link and downloads it.  Skips download if the file already exists.

if (file.exists(zip_path)) {
  message("ZIP already present — skipping download: ", zip_path)
} else {
  message("Scraping download link from traviscad.org/publicinformation ...")
  page  <- rvest::read_html("https://traviscad.org/publicinformation")
  links <- page |> html_elements(".fusion-li-item-content a")
  url   <- links[grepl("Special.*export.*JSON", links, ignore.case = TRUE)] |>
             html_attr("href")

  if (!length(url) || is.na(url[1])) {
    stop(
      "Could not find the 'Special Export (JSON)' link on traviscad.org.\n",
      "  Visit https://traviscad.org/publicinformation, copy the ZIP URL,\n",
      "  download it manually, and save it as: ", zip_path
    )
  }

  # Use the first matching link
  url <- url[1]
  message("Downloading: ", url)
  message("(This may take several minutes on a slow connection.)")
  download.file(url, zip_path, mode = "wb", quiet = FALSE)
  message("Download complete.")
}

# ── 3. Inspect the ZIP and parse the JSON ─────────────────────────────────────

manifest  <- unzip(zip_path, list = TRUE)
json_name <- manifest$Name[1]
size_gb   <- round(manifest$Length[1] / 1e9, 2)

message("Parsing: ", json_name, " (", size_gb, " GB uncompressed)")
message("Memory needed: ~", round(size_gb * 3), " GB free RAM  — please wait ...")

con <- unz(zip_path, json_name)
raw <- jsonlite::fromJSON(con, simplifyDataFrame = TRUE, simplifyVector = TRUE,
                          flatten = FALSE)
message("JSON loaded successfully.")

# ── 4. Flatten nested sections ────────────────────────────────────────────────
# The top-level JSON is an array of parcel objects.  Each parcel contains nested
# arrays for owners, situses, propertyProfile, and propertyCharacteristics.
# fromJSON returns these as list columns; bind_rows collapses them to flat tables.

prefix_rename <- function(lst, prefix) {
  # Collapse a list-of-data-frames into one data frame, then prefix all column
  # names except "pID" (which becomes "<prefix>_pID").
  df <- dplyr::bind_rows(lst)
  if (nrow(df) == 0L) return(df)
  names(df) <- ifelse(names(df) == "pID",
                      paste0(prefix, "_pID"),
                      paste0(prefix, "_", names(df)))
  df
}

message("Extracting: owners ...")
owners_df <- prefix_rename(raw$owners,                 "owner")
message("Extracting: situses ...")
situs_df  <- prefix_rename(raw$situses,                "situs")
message("Extracting: propertyProfile ...")
prof_df   <- prefix_rename(raw$propertyProfile,        "propertyProf")
message("Extracting: propertyCharacteristics ...")
char_df   <- prefix_rename(raw$propertyCharacteristics, "propertyChar")

rm(raw)           # release memory as soon as possible
gc()

# ── 5. Clean and build address strings ────────────────────────────────────────
# Mirrors the address_clean() logic in target_helper_functions.R.

clean_address <- function(x) {
  x <- toupper(as.character(x))
  x <- gsub("SUITE|STE|CONDO|UNIT|APT|BLDG|[[:punct:]]", "", x)
  x <- gsub("[[:space:]]+NA[[:space:]]+|[[:space:]]+NO[[:space:]]+", " ", x)
  x <- gsub("^NA*[[:space:]]+|[[:space:]]+NA*$", "", x)
  x <- gsub("[[:space:]]{2,}", " ", x)
  x <- gsub("RANCH ROAD", "RR",   x); x <- gsub("DRIVE",    "DR",   x)
  x <- gsub("INTERSTATE", "IH",   x); x <- gsub("LANE",     "LN",   x)
  x <- gsub("ROAD",       "RD",   x); x <- gsub("TRAIL",    "TRL",  x)
  x <- gsub("STREET",     "ST",   x); x <- gsub("FREEWAY",  "FRWY", x)
  x <- gsub("AVENUE",     "AVE",  x); x <- gsub("CIRCLE",   "CIR",  x)
  x <- gsub("PARKWAY",    "PKWY", x); x <- gsub("BOULEVARD","BLVD", x)
  x <- gsub("MOUNTAIN",   "MTN",  x); x <- gsub("PLAZA",    "PLZ",  x)
  x <- gsub("NORTH(?=[[:space:]]|$)", "N", x, perl = TRUE)
  x <- gsub("SOUTH(?=[[:space:]]|$)", "S", x, perl = TRUE)
  x <- gsub("EAST(?=[[:space:]]|$)",  "E", x, perl = TRUE)
  x <- gsub("WEST(?=[[:space:]]|$)",  "W", x, perl = TRUE)
  x <- gsub("[[:space:]]{2,}", " ", x)
  trimws(x)
}

# Situs (property) address
situs_df$situs_city[is.na(situs_df$situs_city)] <- "AUSTIN"
situs_df$situs_country[is.na(situs_df$situs_country) |
                          situs_df$situs_country == ""] <- "USA"
situs_df$situs_zip <- sub("-.*", "", situs_df$situs_zip)

situs_df$situs_address <- clean_address(
  paste(situs_df$situs_streetNum,    situs_df$situs_streetPrefix,
        situs_df$situs_streetName,   situs_df$situs_streetSuffix,
        situs_df$situs_city,         situs_df$situs_state,
        situs_df$situs_zip)
)

# Owner mailing address
owners_df$owner_addrCountry[is.na(owners_df$owner_addrCountry) |
                               owners_df$owner_addrCountry == ""] <- "USA"
owners_df$owner_addrZip <- sub("-.*", "", owners_df$owner_addrZip)

owners_df$owner_address <- clean_address(
  paste(owners_df$owner_addrDeliveryLine, owners_df$owner_addrUnitDesignator,
        owners_df$owner_addrCity,         owners_df$owner_addrState,
        owners_df$owner_addrZip)
)

# Normalise owner name (remove punctuation / extra spaces)
owners_df$owner_name <- gsub(
  "[[:punct:]]", "",
  gsub("[[:space:]]{2,}", " ", as.character(owners_df$owner_name))
)

# ── 6. Merge all sections by parcel ID ────────────────────────────────────────
# Each nested section carries a pID that corresponds to the TCAD parcel ID.

message("Merging parcel sections ...")
parcels <- situs_df |>
  dplyr::left_join(char_df,   by = c("situs_pID" = "propertyChar_pID")) |>
  dplyr::left_join(owners_df, by = c("situs_pID" = "owner_pID"))        |>
  dplyr::left_join(prof_df,   by = c("situs_pID" = "propertyProf_pID"))

rm(situs_df, char_df, owners_df, prof_df)
gc()

# ── 7. Classify parcels ───────────────────────────────────────────────────────

# 7a. Residential flag
# State codes starting with A (single-family) or B (multi-family), or SF/MF zoning.
parcels$is_residential <-
  grepl("^A|^B", parcels$propertyProf_imprvStateCd, ignore.case = FALSE) |
  grepl("^A|^B", parcels$propertyProf_landStateCd,  ignore.case = FALSE) |
  grepl("SF|MF",  parcels$propertyChar_zoning,       ignore.case = FALSE)

# 7b. Owner-occupied flag
# An owner is considered occupying the property when their mailing address
# matches the situs address OR a homestead (HS) exemption is present.

# The exemptions field may arrive from JSON as a list column; convert to string
# for a simple grep.
exemptions_str <- tryCatch(
  as.character(parcels$owner_exemptions),
  error = function(e) rep(NA_character_, nrow(parcels))
)

has_hs <- grepl("HS", exemptions_str, fixed = TRUE)

addr_match <- mapply(
  function(owner_add, situs_add) {
    if (is.na(owner_add) || is.na(situs_add)) return(FALSE)
    oa <- trimws(owner_add)
    if (nchar(oa) == 0L) return(FALSE)
    grepl(oa, situs_add, fixed = TRUE)
  },
  parcels$owner_address,
  parcels$situs_address,
  USE.NAMES = FALSE
)

parcels$is_owner_occupied <- addr_match | has_hs

# 7c. Financialization / corporate-ownership flag
# Matches entity-type suffixes (LLC, LP, LTD, INC, LC) and real-estate sector
# keywords against the owner name — mirrors financial_marker_string in the
# original target_helper_functions.R.

financial_markers <- paste(
  # Formal entity type markers
  "LTD", "L T D", "L\\.?T\\.?D\\.?",
  "LLC", "L L C", "L\\.?L\\.?C\\.?",
  "LP",  "L P",   "L\\.?P\\.?",
  "LLLP","L L L P","L\\.?L\\.?L\\.?P\\.?",
  "INC", "I N C", "I\\.?N\\.?C\\.?",
  "LC",  "L C",   "L\\.?C\\.?",
  # Real-estate / corporate sector keywords
  "MORTG", "RENT",    "MARKET",  "INVEST",   "PROP",
  "MANAGE","MGT",     "MGMT",    "ASSET",    "JOINT",
  "VENTURE","VNT",    "LIMIT",   "PARTN",    "PRTN",
  "BANK",  "ASSOC",   "EQUIT",   "REALT",    "OWNER",
  "HOLDING","DEVELOP","COMP",    "CORP",     "AQUISI",
  "CONDO", "C/O",
  "[[:digit:]]",  # entities with numbers in their name (e.g. "123 HOLDINGS LLC")
  "BORROWER", "FOUNDA",
  sep = "|"
)

parcels$is_financialized <- grepl(financial_markers, parcels$owner_name)

# 7d. Primary target flag: non-owner-occupied + financialized + residential
parcels$is_target <-
  !parcels$is_owner_occupied &
  parcels$is_financialized   &
  parcels$is_residential

# ── 8. Estimate unit count (optional, but useful context) ─────────────────────
# Derived from improvement state codes and total floor area.
# 900 sq ft is used as a rough proxy for one residential unit (matches the
# original pipeline heuristic in target_helper_functions.R).
SQ_FT_PER_UNIT <- 900
parcels$property_units <- parcels$propertyProf_imprvTotalArea / SQ_FT_PER_UNIT
single_fam_codes <- c("A1", "A2", "A3")
two_unit_codes   <- "B2"
three_unit_codes <- "B3"
four_unit_codes  <- "B4"
commercial_codes <- c("C1","C2","C3","D1","D2","E1","F1","F2")

parcels$property_units[
  parcels$propertyProf_imprvStateCd %in% single_fam_codes |
  parcels$propertyProf_landStateCd  %in% single_fam_codes] <- 1
parcels$property_units[
  parcels$propertyProf_imprvStateCd %in% two_unit_codes |
  parcels$propertyProf_landStateCd  %in% two_unit_codes] <- 2
parcels$property_units[
  parcels$propertyProf_imprvStateCd %in% three_unit_codes |
  parcels$propertyProf_landStateCd  %in% three_unit_codes] <- 3
parcels$property_units[
  parcels$propertyProf_imprvStateCd %in% four_unit_codes |
  parcels$propertyProf_landStateCd  %in% four_unit_codes] <- 4
parcels$property_units[
  parcels$propertyProf_imprvStateCd %in% commercial_codes |
  parcels$propertyProf_landStateCd  %in% commercial_codes] <- 0

# ── 9. Filter and write output ─────────────────────────────────────────────────

message("Filtering for corporate-owned parcels ...")

output_cols <- c(
  "situs_pID",
  "situs_address", "situs_city", "situs_state", "situs_zip",
  "owner_name", "owner_address",
  "owner_addrCity", "owner_addrState", "owner_addrZip",
  "propertyChar_zoning",
  "propertyProf_imprvStateCd", "propertyProf_landStateCd",
  "propertyProf_imprvTotalArea", "propertyProf_imprvActualYearBuilt",
  "property_units",
  "is_residential", "is_owner_occupied", "is_financialized", "is_target"
)

# Keep only columns that actually exist after the merge (guards against
# structural changes in future TCAD exports).
output_cols <- intersect(output_cols, names(parcels))

corporate_parcels <- parcels |>
  dplyr::filter(is_target) |>
  dplyr::select(all_of(output_cols)) |>
  dplyr::rename(parcel_id = situs_pID)

message("Writing ", nrow(corporate_parcels), " rows to: ", out_path)
readr::write_csv(corporate_parcels, out_path)
message("Done! Output saved to: ", normalizePath(out_path))
