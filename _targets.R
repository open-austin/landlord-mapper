# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline
# base_path = "tcad_special_export.zip"
# file_name = 'Travis-protaxExport-20260407.json'
# tail(readLines(unz(base_path,
#                    file_name)),100)

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
library(multidplyr)
library(doRNG)
library(readr)
library(tidygeocoder)
library(censusapi)
# library(acs)
library(googleCloudStorageR)
library(tidycensus)
library(httr)
library(lubridate)
library(qs2)
library(httr2)
library(tidyr)
options(future.globals.maxSize = 1.5 * 1e9)
options(timeout = max(7200, getOption("timeout")))
reticulate::import('pandas')
reticulate::import('numpy')
reticulate::import('urllib')
reticulate::import('zipfile')
reticulate::import('re')
reticulate::import('ijson.backends.yajl2_c',
                   as = 'ijson')
reticulate::import('multiprocessing')
# reticulate::source_python('pacs.py')
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
Sys.setenv(CENSUS_KEY='YOUR CENSUS API KEY HERE')
Sys.getenv("CENSUS_KEY")
#            "GCS_AUTH_FILE" = "client_secret_970494625384-1qc5cm062uj9ljhsempgh0ririra3hoi.apps.googleusercontent.com.json")
# Set target options:
library(googleCloudStorageR)
library(gargle)

## Fetch token. See: https://developers.google.com/identity/protocols/oauth2/scopes
# scope <-c("https://www.googleapis.com/auth/cloud-platform")
# token <- token_fetch(scopes = scope)
gcs_auth(token = readRDS('token.rds'))

Sys.setenv("GCS_AUTH_FILE" = "landlord-mapper-texas-triangle-72fb0e8772e1.json")
Sys.setenv("GCS_DEFAULT_BUCKET" = "cad-data-texas-triangle")
gcs_global_bucket("cad-data-texas-triangle")

project <- "Landlord-Mapper-Texas-Triangle"
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
               "tidyr",
               "foreach",
               "censusapi",
               "tidycensus",
               "httr2",
               "lubridate",
               "multidplyr",
               "qs2"
               ), # Packages that your targets need for their tasks.
  format = "qs", # Optionally set the default storage format. qs is fast.
  debug  = 'tcad_data',
  # cue = tar_cue(mode = "never"),
  garbage_collection = 1,
  # targets::tar_make(callr_function = NULL, use_crew = FALSE, as_job = FALSE),
  #
  # Pipelines that take a long time to run may benefit from
  # optional distributed computing. To use this capability
  # in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller that scales up to a maximum of two workers
  # which run as local R processes. Each worker launches when there is work
  # to do and exits if 60 seconds pass with no tasks to run.
  # resources = tar_resources(gcp = tar_resources_gcp(bucket = gcs_get_global_bucket(),
  #                                                   prefix = "Landlord-Mapper-Texas-Triangle",
  #                                                   predefined_acl = 'bucketLevel')
                            # ),
  # repository = 'gcp',
  # repository_meta = 'gcp',
  controller = crew::crew_controller_local(workers = parallel::detectCores(),
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
    name = wcad_data,
    command = download_wcad_data(),
    deployment = 'main'),
  tar_target(name = wcad_data_parsed,
             command = parse_wcad_data(wcad_data),
             deployment = 'main'),
  tar_target(hays_data,
             command = parse_hays_cad_data(),
             deployment = 'main'),
  tar_target(pacs_data,
             command = ingest_proton_pacs_cad_data('AUSTIN–SAN ANTONIO METROPLEX (13 of 13).zip'),
             deployment = 'main'),
  tar_target(
    name = tcad_data_get,
    command = {
      download_tcad_austin()
      },
    deployment = 'main'),
  
  tar_target(
    name = propChar_data,
    command = {
      
      print('propChar')
      print(tcad_data_get)
      TCAD_parseYear_propChar(list.files()[grepl('tcad_special_export.zip',
                                                 list.files())])
      read.csv('austin_propertyChar_data.csv',
               row.names = 'X')
    },
    deployment = 'main'),
  tar_target(
    name = propProf_data,
    command = {
      
      print('propProf')
      print(tcad_data_get)
      TCAD_parseYear_propProf(list.files()[grepl('tcad_special_export.zip',
                                                         list.files())])
      read.csv('austin_propertyProf_data.csv',
               row.names = 'X')
      },
    deployment = 'main'),
  tar_target(
    name = legal_data,
    command = {
      
      print('legal')
      print(tcad_data_get)
      TCAD_parseYear_legal(list.files()[grepl('tcad_special_export.zip',list.files())])
      read.csv('austin_propertyLegal_data.csv',
               row.names = 'X')
      },
    deployment = 'main'),
  tar_target(
    name = situs_data,
    command = {
      
      print('situs')
      print(tcad_data_get)
      TCAD_parseYear_situs(list.files()[grepl('tcad_special_export.zip',list.files())])
      read.csv('austin_situs_data.csv',
               row.names = 'X')
      },
    deployment = 'main'
    ),
  tar_target(
    name = owner_data,
    command = {
      
      print('owner')
      print(tcad_data_get)
      TCAD_parseYear_owner(list.files()[grepl('tcad_special_export.zip',list.files())])
      read.csv('austin_owner_data.csv',
               row.names = 'X')
      },
    deployment = 'main'),
  tar_target(
    name = agent_data,
    command = {
      
      print('agent')
      print(tcad_data_get)
      TCAD_parseYear_agent(list.files()[grepl('tcad_special_export.zip',list.files())])
      read.csv('austin_agent_data.csv',
               row.names = 'X')
      },
    deployment = 'main'),
  tar_target(
    name = ownerValue_data,
    command = {
      
      print('ownerValue')
      print(tcad_data_get)
      TCAD_parseYear_ownerValue(list.files()[grepl('tcad_special_export.zip',
                                                           list.files())])
      read.csv('austin_ownerValue_data.csv',
               row.names = 'X')
      },
    deployment = 'main'),
  tar_target(
    name = deed_data,
    command = {
      
      print('deeds')
      print(tcad_data_get)
      
      TCAD_parseYear_deeds(list.files()[grepl('tcad_special_export.zip',
                                                      list.files())])
      read.csv('austin_deeds_data.csv',
               row.names = 'X')
      },
    deployment = 'main'),

  tar_target(deed_summ_data,
             command = {
               deed_summ_data_gen(deed_data)
               },
            deployment = 'main'),
  # tar_target(code_complaint_data,
  #            read.csv(austin_open_data_dl_shell(data_links[[1]],
  #                                base_used[[1]])),
  #            deployment = 'main'),
  tar_target(svi_data,
             census_data_get_svi(2023),
             deployment = 'main'),
  tar_target(hhi_data,
             readxl::read_xlsx('HHI Data 2024 United States.xlsx'),
             deployment = 'main'),
  tar_target(austin_parcel_data_merged_local,
             target_property_gen(propChar_data,
                                 propProf_data,
                                 situs_data,
                                 owner_data,
                                 deed_summ_data,
                                 legal_data,
                                 agent_data,
                                 ownerValue_data
                                 ),
             deployment = 'main'),
  tar_target(austin_parcel_data_merged,
             rbind(austin_parcel_data_merged_local,
                   pacs_data,
                   wcad_data_parsed,
                   hays_data
                   ),
             deployment = 'main'),
  # tar_target(austin_parcel_data_merged_code,
  #            code_compl_merge(austin_parcel_data_merged,
  #                             code_complaint_data),
  #            deployment = 'main'),
  tar_target(austin_parcel_data_merged_owner,
             owner_scrape_actual(austin_parcel_data_merged),
             deployment = 'main'
             ),
  tar_target(situs_owner_strings,
             situs_owner_string_gen(austin_parcel_data_merged_owner),
             deployment = 'main'),
  tar_target(situs_group_assignments,
             situs_owner_string_dist_matrix(situs_owner_strings,
                                            austin_parcel_data_merged_owner),
             deployment = 'main'),
  tar_target(situs_group_assignments_final,
             situs_neighor_gen(situs_group_assignments,
                               austin_parcel_data_merged_owner)
             # skip = sum(grepl('situs_group_assignments_final',
             #                  list.files("_targets\\objects")))>0
             ),

  tar_target(owners_info_total,
             parcel_geolocate(situs_group_assignments_final),
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
