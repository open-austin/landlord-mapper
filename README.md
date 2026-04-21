# Austin Landlord Mapper

A data pipeline and interactive web application that identifies and maps residential landlords in Austin, TX—with a focus on corporate and "financialized" ownership—by integrating Travis County Appraisal District (TCAD) property records, Austin Open Data, Texas Comptroller business filings, and US Census socioeconomic data.

**Live app:** https://ontheseams.shinyapps.io/landlord_mapper_app/

**Additional project materials:** https://drive.google.com/drive/folders/1e2Ahq9sNNQ2K_Q-RuTrdkWzDL6FH_gAa?usp=sharing

---

## What this repository does

The pipeline answers the question: *who owns residential rental property in Austin, and how are those owners connected to one another?* It produces a geocoded, enriched dataset that can be explored through an interactive Shiny application allowing users to:

- Search for a property by address, owner name, corporate name, or ownership group ID
- See all properties held by a single owner or corporate family on a map
- Explore a network graph of landlord connections via shared ownership
- Download filtered tabular data for further analysis

---

## Repository structure

| File / Folder | Language | Purpose |
|---|---|---|
| `_targets.R` | R | Pipeline orchestration (via the [`targets`](https://books.ropensci.org/targets/) package). Defines every step as a reproducible target. Run `targets::tar_make()` to execute the full pipeline. |
| `TCAD_parse.py` | Python | Parses the large TCAD JSON export (a ZIP file with hundreds of thousands of records) into flat CSV files using streaming JSON parsing (`ijson`). |
| `target_helper_functions.R` | R | Core data-merging and property classification. Joins all CSV files into a single parcel dataset, derives owner-occupancy and financialization flags, and counts property units. |
| `scrape_helper_functions.R` | R | Downloads the TCAD special export from [traviscad.org](https://traviscad.org/publicinformation); scrapes Austin Open Data (code complaints) via headless Chrome + Selenium; queries the Texas Comptroller Franchise Tax API for corporate details. |
| `supplementary_scrape_helper_functions.R` | R | Downloads Census ACS data (used to build a Social Vulnerability Index), generates per-property owner "fingerprint" strings, computes pairwise cosine string-distance matrices, clusters properties into ownership groups, and geocodes parcel addresses via the Census geocoder. |
| `final_output_helper_functions.R` | R | Merges the geocoded parcel dataset with the Housing Hardship Index and SVI data to produce the final `owners_info_total` dataset consumed by the Shiny app. |
| `HHI Data 2024 United States.xlsx` | Data | Housing Hardship Index scores and ranks for US ZIP codes (2024). |
| `shinyApp/app.R` | R | Interactive Shiny dashboard for exploring the final dataset: property map (Leaflet), owner table (DT), and landlord network graph (networkD3). |

---

## Pipeline walkthrough

The pipeline is managed by the [`targets`](https://books.ropensci.org/targets/) R package, which tracks dependencies between steps and re-runs only what has changed.

### Step 1 – Download TCAD data (`tcad_data`)
`download_tcad_austin()` scrapes [traviscad.org/publicinformation](https://traviscad.org/publicinformation) for the latest "Special Export (JSON)" ZIP file and downloads it if it is new.

### Step 2 – Parse TCAD data (`tcad_parse`)
`TCAD_parseYear()` (Python, called via `reticulate`) streams through the TCAD JSON with `ijson` and writes five flat CSV files:

| Output file | Contents |
|---|---|
| `austin_propertyChar_data.csv` | Zoning codes per parcel |
| `austin_propertyProf_data.csv` | Improvement/land state codes, area, stories, year built |
| `austin_situs_data.csv` | Property street address components |
| `austin_owner_data.csv` | Owner name, mailing address, exemptions, ownership percentage |
| `austin_deeds_data.csv` | Deed history: buyer/seller names and dates |

### Step 3 – Merge and classify parcels (`austin_parcel_data_merged`)
`target_property_gen()` joins the five CSV files on parcel ID (`situs_pID`), then:

- **Filters** to residential parcels (state codes `A*`/`B*` or SF/MF zoning).
- **Estimates unit count** from improvement state codes and total floor area.
- **Flags owner-occupancy** by comparing the owner's mailing address to the situs address, or by checking for a homestead exemption (`HS`).
- **Flags financialization** by matching owner names against a list of corporate entity markers (LLC, LP, LTD, INC, etc.) and real-estate-sector keywords (INVEST, MANAGE, REALT, HOLDING, …).
- **Derives `is_target`**: non-owner-occupied residential parcels held by a financialized entity.
- **Derives `is_mom_and_pop`**: owner-occupied residential parcels held by a non-financialized entity.

### Step 4 – Append code-complaint counts (`austin_parcel_data_merged_code`)
`code_compl_merge()` downloads the Austin Code Complaint dataset from Austin Open Data via headless Selenium and counts complaints per parcel address (parallelised with `doFuture`).

### Step 5 – Scrape Texas Comptroller data (`austin_parcel_data_merged_owner`)
`owner_scrape_actual()` looks up each financialized owner's name in the Texas Comptroller Franchise Tax API, retrieving:
- Legal business name, Texas Taxpayer Number (TTN), mailing address
- State of formation, SOS registration status
- Registered agent name and address
- Officer / owner names and addresses

### Step 6 – Cluster properties into ownership groups (`situs_group_assignments`)
`situs_owner_string_gen()` builds a composite "fingerprint" string for each parcel that concatenates all known names and addresses (owner, corporation, registered agent, scraped officer names). `situs_owner_string_dist_matrix()` then computes a pairwise cosine string-distance matrix over these fingerprints for all target/large-building parcels. `situs_neighor_gen()` translates close distances plus exact-match links (shared owner name, mailing address, corporate name, registered agent) into connected ownership groups, iterating until transitive closure is reached.

### Step 7 – Geocode parcels (`owners_info_total`)
`parcel_geolocate()` sends unique situs addresses to the US Census geocoder (in batches of 10,000) and joins back latitude/longitude and census block/tract/ZCTA geography identifiers.

### Step 8 – Enrich with socioeconomic context (`owners_data_total_supp`)
`final_data_merge()` joins the parcel dataset with:
- **Housing Hardship Index** (`HHI Data 2024 United States.xlsx`) – overall score and rank by ZIP code.
- **Social Vulnerability Index** built from Census ACS 5-year estimates (via `censusapi`): poverty, education, unemployment, housing cost burden, insurance coverage, age, disability, minority population, housing type, vehicle access, and limited English proficiency, organised into four SVI themes following the CDC/ATSDR methodology.

### Step 9 – Shiny application (`shinyApp/app.R`)
Reads `owners_info_total.csv` and `owners_info_d3graph.rds` and provides three dashboard panels:

- **Property Search** – Leaflet map with cosine-matched address search; table with full parcel/owner/corporate metadata; CSV download.
- **Landlord Network Analysis** – Force-directed network graph (networkD3) showing properties connected by common ownership.
- **Tenant Stress** *(planned)* – Heatmap of rent burden and evictions.
- **Property Quality** *(planned)* – Heatmap of code violations, fines, and building age.

---

## Setup and usage

### Prerequisites

**R packages** (installed automatically by `renv` if a lockfile is present, otherwise install manually):

```r
install.packages(c(
  "targets", "tarchetypes", "crew",
  "tibble", "dplyr", "purrr", "readr", "lubridate",
  "rvest", "selenider", "selenium", "httr", "httr2",
  "reticulate", "stringdist", "tidygeocoder",
  "censusapi", "acs", "tidycensus",
  "forecast", "xgboost", "doFuture", "doRNG", "foreach", "future",
  "qs2", "readxl",
  # Shiny app
  "shiny", "shinydashboard", "leaflet", "DT", "plotly",
  "networkD3", "igraph", "sp", "stringi", "listviewer"
))
```

**Python packages** (used via `reticulate`):

```bash
pip install pandas numpy ijson
```

> `ijson` requires the `yajl2_c` backend. Install the `yajl` C library for your OS (e.g., `brew install yajl` on macOS or `apt install libyajl-dev` on Ubuntu).

**Other tools:**
- Google Chrome + ChromeDriver (for headless Selenium scraping)
- A [Census API key](https://api.census.gov/data/key_signup.html)

### Configuration

1. Open `_targets.R` and replace `YOUR OWN CENSUS API KEY GOES HERE` with your Census API key:
   ```r
   Sys.setenv(CENSUS_KEY = "your_key_here")
   ```
2. If Chrome downloads files to a non-default location, update `download_location` in `supplementary_scrape_helper_functions.R` → `austin_open_data_dl()`.

### Running the pipeline

```r
library(targets)
tar_make()        # run the full pipeline
tar_visnetwork()  # inspect the dependency graph
tar_read(owners_data_total_supp)  # inspect the final output
```

Intermediate files (CSV, RDS) are cached in the working directory and in the `_targets/` store. Only targets whose upstream inputs have changed will be re-run.

### Running the Shiny app locally

```r
shiny::runApp("shinyApp")
```

The app expects `owners_info_total.csv` and `owners_info_d3graph.rds` to be present inside `shinyApp/`. Copy or symlink these files from the pipeline output directory before launching.

---

## Key derived fields in the output dataset

| Field | Description |
|---|---|
| `is_residential` | Property has a residential improvement or zoning code |
| `is_owner_occupied` | Owner mailing address matches situs address, or homestead exemption present |
| `is_financialized` | Owner name contains a corporate entity marker or real-estate keyword |
| `is_target` | Non-owner-occupied + financialized + residential |
| `is_mom_and_pop` | Owner-occupied + non-financialized + residential |
| `property_units` | Estimated number of housing units (derived from state codes and floor area) |
| `is_owner_out_of_state` | Owner's mailing state differs from property state |
| `group_assign` | Numeric group ID linking properties to a common ownership cluster |
| `veneer_owner` | Shell entity name on the TCAD record |
| `corp_business_name` | Legal business name from Texas Comptroller |
| `corp_TTN` | Texas Taxpayer Number |
| `corp_registered_agent_name` | Registered agent name |
| `situs_lat` / `situs_long` | Geocoded coordinates (Census geocoder) |
| `HHI_score` / `HHI_rank` | Housing Hardship Index for the parcel's ZIP code |
| `rpl_themes` | Overall Social Vulnerability Index percentile rank |
