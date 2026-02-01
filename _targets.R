# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

options(timeout = max(7200, getOption("timeout")))
# Load packages required to define the pipeline:
library(targets)
library(tarchetypes) # Load other packages as needed.
library(tibble)
library(forecast)
library(doFuture)
library(doRNG)
library(future)
library(xgboost)
library(dplyr)
library(rvest)
library(selenider)
library(selenium)
library(reticulate)
library(stringdist)
library(purrr)
library(foreach)
library(doRNG)
library(readr)
library(tidygeocoder)
library(censusapi)
library(acs)
library(tidycensus)
library(httr)
library(lubridate)
library(qs2)
library(httr2)
reticulate::import('pandas')
reticulate::import('numpy')
reticulate::import('urllib')
reticulate::import('zipfile')
reticulate::import('re')
reticulate::import('ijson.backends.yajl2_c',
                   as = 'ijson')
reticulate::import('multiprocessing')

reticulate::source_python('TCAD_parse.py')
source('target_helper_functions.R')
source('scrape_helper_functions.R')
source('supplementary_scrape_helper_functions.R')
source('final_output_helper_functions.R')

data_links <- list(Code_Complaints = 'https://data.austintexas.gov/Public-Safety/Austin-Code-Complaint-Cases/6wtj-zbtb/about_data',
                   Short_Term_Rentals = 'https://data.austintexas.gov/Public-Safety/Short-Term-Rental-Locations/2fah-4p7e/about_data',
                   Zoning_Cases = 'https://data.austintexas.gov/Building-and-Development/Zoning-Cases/edir-dcnf/about_data'
                   )
base_used = c('Austin_Code_Complaint_Cases',
              'Short_Term_Rental_Locations',
              'Zoning_Cases')
Sys.setenv(CENSUS_KEY='cdd487fd377da61dfdecfbcdb620f7a94f3c5b6f')
Sys.getenv("CENSUS_KEY")

# Set target options:
tar_option_set(
  packages = c("tibble",
               "forecast",
               "doFuture",
               "future",
               "xgboost",
               "dplyr",
               "rvest",
               "selenider",
               "selenium",
               "reticulate",
               "stringdist",
               "purrr",
               "doRNG",
               "tidygeocoder",
               "readr",
               "censusapi",
               "acs",
               "tidycensus",
               "httr",
               "lubridate",
               "qs2"
               ), # Packages that your targets need for their tasks.
  format = "qs", # Optionally set the default storage format. qs is fast.
  #
  # Pipelines that take a long time to run may benefit from
  # optional distributed computing. To use this capability
  # in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller that scales up to a maximum of two workers
  # which run as local R processes. Each worker launches when there is work
  # to do and exits if 60 seconds pass with no tasks to run.
  #
    controller = crew::crew_controller_local(workers = 8,#parallel::detectCores(),
    seconds_idle = 10)
  #
  # Alternatively, if you want workers to run on a high-performance computing
  # cluster, select a controller from the {crew.cluster} package.
  # For the cloud, see plugin packages like {crew.aws.batch}.
  # The following example is a controller for Sun Grid Engine (SGE).
  #
  #   controller = crew.cluster::crew_controller_sge(
  #     # Number of workers that the pipeline can scale up to:
  #     workers = 10,
  #     # It is recommended to set an idle time so workers can shut themselves
  #     # down if they are not running tasks.
  #     seconds_idle = 120,
  #     # Many clusters install R as an environment module, and you can load it
  #     # with the script_lines argument. To select a specific verison of R,
  #     # you may need to include a version string, e.g. "module load R/4.3.2".
  #     # Check with your system administrator if you are unsure.
  #     script_lines = "module load R"
  #   )
  #
  # Set other options as needed.
  )

# Run the R scripts in the R/ folder with your custom functions:
#tar_source()
# tar_source("other_functions.R") # Source other scripts as needed.

# Replace the target list below with your own:
list(
  tar_target(
    name = tcad_data,
    command = download_tcad_austin(),
    # format = "qs" # Efficient storage for general data objects.
    ),
  tar_target(name = tcad_parse,
             TCAD_parseYear(list.files()[grepl('zip',list.files())]),
             deployment = 'main'
             ),
  tar_target(propChar_file,
             'austin_propertyChar_data.csv',
             format = 'file'),
  tar_target(propProf_file,
             'austin_propertyProf_data.csv',
             format = 'file'),
  tar_target(situs_file,
             'austin_situs_data.csv',
             format = 'file'),
  tar_target(owner_file,
             'austin_owner_data.csv',
             format = 'file'),
  tar_target(deed_file,
             'austin_deeds_data.csv',
             format = 'file'),
  tar_target(propChar_data,
             read.csv(propChar_file,
                      row.names = 'X')),
  tar_target(propProf_data,
             read.csv(propProf_file,
                      row.names = 'X')),
  tar_target(deed_data,
             read.csv(deed_file,
                      row.names = 'X')),
  tar_target(situs_data,
             read.csv(situs_file,
                      row.names = 'X')),
  tar_target(owner_data,
             read.csv(owner_file,
                      row.names = 'X')),
  tar_target(deed_summ_data,
            deed_summ_data_gen(deed_data,
                               propProf_data,
                               propChar_data),
            deployment = 'main'),
  tar_target(code_complaint_data,
             read.csv(austin_open_data_dl_shell(data_links[[1]],
                                 base_used[[1]])),
             deployment = 'main'),
  tar_target(svi_data,
             census_data_get_svi(2023),
             deployment = 'main'),
  tar_target(hhi_data,
             readxl::read_xlsx('HHI Data 2024 United States.xlsx'),
             deployment = 'main'),
  tar_target(austin_parcel_data_merged,
             target_property_gen(owner_data,
                                 propChar_data,
                                 propProf_data,
                                 situs_data,
                                 deed_summ_data
                                 ),
             deployment = 'main'),
  tar_target(austin_parcel_data_merged_code,
             code_compl_merge(austin_parcel_data_merged,
                              code_complaint_data),
             deployment = 'main'),
  tar_target(austin_parcel_data_merged_owner,
             owner_scrape_actual(austin_parcel_data_merged_code),
             deployment = 'main'
             ),
  tar_target(situs_owner_strings,
             situs_owner_string_gen(austin_parcel_data_merged_owner),
             deployment = 'main'),
  tar_target(situs_group_assignments,
             situs_owner_string_dist_matrix(situs_owner_strings,
                                            austin_parcel_data_merged_owner),
             deployment = 'main'),

  tar_target(owners_info_total,
             parcel_geolocate(austin_parcel_data_merged_owner,
                              situs_group_assignments),
             deployment = 'main'
             ),
  tar_target(owners_data_total_supp,
             final_data_merge(owners_info_total,
                              hhi_data,
                              svi_data),
             deployment = 'main')
  )

# 
# tar_plan(
#   penguins_csv_file = path_to_file("penguins_raw.csv"),
#   penguins_data_raw = read_csv(penguins_csv_file, show_col_types = FALSE),
#   penguins_data = clean_penguin_data(penguins_data_raw)
# )
