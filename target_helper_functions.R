
financial_markers_base<- c('LTD',
                           'L T D',
                           'L\\.?T\\.?D\\.?',
                           'LLC',
                           'L L C',
                           'L\\.?L\\.?C\\.?',
                           'LP',
                           'L P',
                           'L\\.?P\\.?',
                           'LLLP',
                           'L L L P',
                           'L\\.?L\\.?L\\.?P\\.?',
                           'INC',
                           'I N C',
                           'I\\.?N\\.?C\\.?',
                           'LC',
                           'L C',
                           'L\\.?C\\.?')
financial_markers_supp <- c('MORTG',
                            'RENT',
                            'MARKET',
                            'INVEST',
                            'PROP',
                            'MANAGE',
                            'MGT',
                            'MGMT',
                            'ASSET',
                            'JOINT',
                            'VENTUR',
                            'VNT',
                            'LIMIT',
                            'PARTN',
                            'PRTN',
                            'BANK',
                            'ASSOC',
                            'EQUIT',
                            'REALT',
                            'OWNER',
                            'HOLDING',
                            'DEVELOP',
                            'COMP',
                            'CORP',
                            'AQUISI',
                            'CONDO',
                            'C/O',
                            '[[:digit:]]',
                            'BORROWER',
                            'FOUNDA')

financial_marker_string <- paste(paste(financial_markers_base, 
                                       collapse = '|'),
                                 paste(financial_markers_supp, 
                                       collapse = '|'),
                                 sep = '|')
financial_marker_base_string <- paste(financial_markers_base, 
                                      collapse = '|')

address_clean = function(data = austin_parcel_data_merged,
                         col = 'situs_address'){
  
  data_used <- iconv(data[,col],to='UTF-8')
  # print('1')
  data_used <-gsub('-[[:digit:]]+$',
                   '',
                   data_used,
                   useBytes = TRUE)
  data_used <- gsub('SUITE|STE|CONDO|UNIT|APT|"|BLDG|[[:punct:]]',
                    '', 
                    data_used, useBytes = TRUE)
  data_used <- gsub('P([[:space:]]|[[:punct:]])O[[:punct:]]?',
                    'PO',
                    data_used,
                    useBytes = TRUE
  )  
  data_used <- gsub('[[:space:]]+NA[[:space:]]+|[[:space:]]+NO[[:space:]]+',
                    ' ',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('^NA*[[:space:]]+|[[:space:]]+NA*$',
                    '',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('[[:space:]]{2,}',
                    ' ',
                    data_used,
                    useBytes = TRUE)
  # print('2')
  data_used <-sapply(data_used,
                     function(address){
                       regex_used <- '[[:digit:]]+TH|[[:digit:]]+RD|[[:digit:]]+ND'
                       start_ind <- regexpr(regex_used, address)
                       # print(attr(start_ind, 
                       #            'match.length'))
                       match_length_str <- attr(start_ind, 
                                                'match.length')
                       if(is.na(match_length_str)|
                          (match_length_str==(-1))){
                         return(address)
                       }
                       gsub(regex_used,
                            substr(address,(start_ind),(start_ind+match_length_str-3
                            )
                            ),
                            address,
                            useBytes = TRUE)
                     }
  )
  # print('3')
  data_used <- gsub('COUNTY ROAD',
                    'CR',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('RANCH ROAD',
                    'RR',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('DRIVE',
                    'DR',
                    data_used,
                    useBytes = TRUE)
  data_used<- gsub('INTERSTATE',
                   'IH',
                   data_used,useBytes = TRUE)
  data_used<- gsub('LANE',
                   'LN',
                   data_used,
                   useBytes = TRUE)
  data_used<- gsub('ROAD',
                   'RD',
                   data_used,
                   useBytes = TRUE)
  data_used <- gsub('TRAIL',
                    'TRL',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('STREET',
                    'ST',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('FREEWAY',
                    'FRWY',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('BLUFF',
                    'BLF',
                    data_used,
                    useBytes = TRUE)
  data_used<- gsub('FLOOR',
                   'FL',
                   data_used,
                   useBytes = TRUE)
  data_used <- gsub('PLAZA',
                    'PLZ',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('AVENUE',
                    'AVE',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('CIRCLE',
                    'CIR',  
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('LANE',
                    'LN',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('PARKWAY',
                    'PKWY',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('WAY',
                    'WY',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('COURT',
                    'CT',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('COVE',
                    'CV',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('PLACE',
                    'PL',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('POINT',
                    'PT',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('HL',
                    'HILL',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('SPGS',
                    'SPRINGS',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('BOULEVARD',
                    'BLVD',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('MOUNTAIN',
                    'MTN',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('NORTH',
                    'N',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('WEST',
                    'W',                   
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('SOUTH',
                    'S',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('EAST',
                    'E',
                    data_used,
                    useBytes = TRUE)
  # print('4')
  return(trimws(data_used))
  
}


 
target_property_gen = function(propertyChar_data,
                               propertyProf_data,
                               situs_data,
                               owner_data,
                               deeds_data,
                               legal_data,
                               agent_data,
                               ownerValue_data){
  agent_data <- agent_data %>%
    group_by(agent_pAccountID,agent_year) %>%
    summarise(agent_pID = last(agent_pID),
              agent_pAccountAgentID = last(agent_pAccountAgentID),
              companyName = last(companyName),
              effectiveDt = last(effectiveDt))
  owner_data <- left_join(owner_data,
                          agent_data,
                          by = c('owner_pAccountID'='agent_pAccountID',
                                 'owner_year'='agent_year')) %>%
    left_join(ownerValue_data,
              by = c('owner_pAccountID'='ownerValue_pAccountID',
                     'owner_year'='ownerValue_year'))
  austin_parcel_data_merged <- left_join(situs_data,
                                         propertyChar_data,
                                         by = c("situs_pID"="propertyChar_pID",
                                                'situs_year'='propertyChar_year')) %>% 
    left_join(owner_data,
              by = c('situs_pID'='owner_pID',
                     'situs_year'='owner_year')) %>%
    left_join(propertyProf_data,
              by = c('situs_pID'='propertyProf_pID',
                     'situs_year'='propertyProf_year')) %>% 
    left_join(deeds_data,
              by = c('situs_pID'='deeds_pID',
                     'situs_year'='deeds_year')) %>%
    left_join(legal_data,
              by = c('situs_pID'='propertyLegal_pID',
                     'situs_year'='propertyLegal_year'))
  # print(head(austin_parcel_data_merged))
  # print(dim(austin_parcel_data_merged))
  austin_parcel_data_merged$situs_city[is.na(austin_parcel_data_merged$situs_city)] <-'AUSTIN'
  # austin_parcel_data_merged$situs_streetNum[is.na(austin_parcel_data_merged$situs_streetNum)] <-''
  # 
  # austin_parcel_data_merged$situs_streetPrefix[is.na(austin_parcel_data_merged$situs_streetPrefix)] <-''
  austin_parcel_data_merged$situs_zip <- sapply(austin_parcel_data_merged$situs_zip,
                                                function(zip){unlist(strsplit(zip, split = '-'))[1]})
  # austin_parcel_data_merged$situs_country[is.na(austin_parcel_data_merged$situs_country)|
  #                                           austin_parcel_data_merged$situs_country==""] <-'USA'
  
  # austin_parcel_data_merged$situs_international[is.na(austin_parcel_data_merged$situs_international) |austin_parcel_data_merged$situs_international==""| grepl(0, austin_parcel_data_merged$situs_international)] <-'DOMESTIC'
  
  austin_parcel_data_merged$situs_address <- paste(austin_parcel_data_merged$situs_streetNum,
                                                   austin_parcel_data_merged$situs_streetPrefix,
                                                   austin_parcel_data_merged$situs_streetName,
                                                   austin_parcel_data_merged$situs_streetSuffix,
                                                   austin_parcel_data_merged$situs_city,
                                                   austin_parcel_data_merged$situs_state,
                                                   austin_parcel_data_merged$situs_zip)
  austin_parcel_data_merged$situs_address <- address_clean(austin_parcel_data_merged,
                                                           'situs_address')
  
  # austin_parcel_data_merged$owner_addrCountry[is.na(austin_parcel_data_merged$owner_addrCountry)|
  #                                               grepl('US',austin_parcel_data_merged$owner_addrCountry)|
  #                                               austin_parcel_data_merged$owner_addrCountry==""] <-'USA'
  # 
  # austin_parcel_data_merged$owner_addrInternational[is.na(austin_parcel_data_merged$owner_addrInternational)|austin_parcel_data_merged$owner_addrInternational=="" | grepl(0, austin_parcel_data_merged$owner_addrInternational)]  <- 'DOMESTIC'
  
  austin_parcel_data_merged$owner_addrZip <- sapply(austin_parcel_data_merged$owner_addrZip,
                                                    function(zip){unlist(strsplit(zip, split = '-'))[1]})
  
  austin_parcel_data_merged$owner_address <- paste(austin_parcel_data_merged$owner_addrDeliveryLine, 
                                                   austin_parcel_data_merged$owner_addrUnitDesignator,
                                                   austin_parcel_data_merged$owner_addrCity,
                                                   austin_parcel_data_merged$owner_addrState,
                                                   austin_parcel_data_merged$owner_addrZip)
  
  austin_parcel_data_merged$owner_address <- address_clean(austin_parcel_data_merged,
                                                           'owner_address')
  
  austin_parcel_data_merged$is_residential <- (grepl('^A|^B|^E|^F',
                                                     austin_parcel_data_merged$propertyProf_imprvStateCd)|
                                              grepl('^A|^B|^E|^F',
                                                    austin_parcel_data_merged$propertyProf_landStateCd)|
                                              grepl('SF|MF',
                                                    austin_parcel_data_merged$propertyChar_zoning))
  austin_parcel_data_merged <- dplyr::filter(austin_parcel_data_merged,
                                             is_residential==TRUE)
  # print(dim(austin_parcel_data_merged))
  austin_parcel_data_merged$is_owner_out_of_state <- as.character(austin_parcel_data_merged$situs_state)!=as.character(austin_parcel_data_merged$owner_addrState)
  austin_parcel_data_merged$is_owner_occupied <- sapply(1:nrow(austin_parcel_data_merged),
                                                        function(ind){
                                                          result <-(grepl(
                                                            austin_parcel_data_merged$owner_address[ind],
                                                            austin_parcel_data_merged$situs_address[ind]
                                                          )|grepl("'exemptionCode': '(DV)?HS'", austin_parcel_data_merged$owner_exemptions[ind]))
                                                          if(is.na(result)){
                                                            return(FALSE)
                                                          }
                                                          result
                                                        }
  )
  austin_parcel_data_merged$owner_name <- gsub('[[:punct:]]|%','',
                                               gsub('[[:space:]]{2,}',
                                                    ' ',
                                                    austin_parcel_data_merged$owner_name
                                                    )
                                               )
  
  austin_parcel_data_merged$property_units = round(austin_parcel_data_merged$propertyProf_imprvTotalArea/900)
  austin_parcel_data_merged[which((austin_parcel_data_merged$propertyProf_imprvStateCd %in%
                                     c('A1','A2','A3'))|
                                    (austin_parcel_data_merged$propertyProf_landStateCd %in%
                                       c('A1','A2','A3'))) ,'property_units']<- 1
  austin_parcel_data_merged[which((austin_parcel_data_merged$propertyProf_imprvStateCd %in%
                                     c('B2'))|
                                    (austin_parcel_data_merged$propertyProf_landStateCd %in%
                                       c('B2'))) ,'property_units']<- 2
  austin_parcel_data_merged[which((austin_parcel_data_merged$propertyProf_imprvStateCd %in%
                                     c('B3'))|
                                    (austin_parcel_data_merged$propertyProf_landStateCd %in%
                                       c('B3'))) ,'property_units']<- 3
  austin_parcel_data_merged[which((austin_parcel_data_merged$propertyProf_imprvStateCd %in%
                                     c('B4'))|
                                    (austin_parcel_data_merged$propertyProf_landStateCd %in%
                                       c('B4'))) ,'property_units']<- 4
  austin_parcel_data_merged[which((austin_parcel_data_merged$propertyProf_imprvStateCd %in%
                                      c('C1','C2','C3',
                                        'D1','D2',
                                        'E1',
                                        'F1','F2')
                                   )|
                                    (austin_parcel_data_merged$propertyProf_landStateCd %in%
                                       c('C1','C2','C3',
                                         'D1','D2',
                                         'E1',
                                         'F1','F2')
                                     )), 'property_units'] <- 0
  
  austin_parcel_data_merged$is_financialized <- grepl(financial_marker_string,  
                                                      austin_parcel_data_merged$owner_name
                                                      )
  
  austin_parcel_data_merged$is_mom_and_pop = (austin_parcel_data_merged$is_owner_occupied & 
                                                austin_parcel_data_merged$is_residential & 
                                                (austin_parcel_data_merged$is_financialized==FALSE))
  austin_parcel_data_merged$county = 'travis'
  # print(dim(austin_parcel_data_merged))
  
  austin_parcel_data_merged$agent_address = ''
  
  austin_parcel_data_merged <- austin_parcel_data_merged %>%
    rename(totalsqftlivingarea=propertyProf_imprvTotalArea,
           year_built=propertyProf_imprvActualYearBuilt,
           state_code=propertyProf_landStateCd,
           propertytypedesc=,
           legallocationdesc=propertyLegal_legalDesc,
           totalassessedvalue=ownerValue_assessedValue,
           totalpropmktvalue =ownerValue_marketValue,
           owner_zip = owner_addrZip,
           agent_name = companyName
           ) %>%
    select(situs_year,
           situs_pID,
           situs_address,
           situs_zip,
           totalsqftlivingarea,
           property_units,
           year_built,
           state_code,
           is_owner_out_of_state,
           is_owner_occupied,
           is_financialized,
           is_mom_and_pop,
           # propertytypedesc,
           legallocationdesc,
           owner_name,
           owner_address,
           owner_zip,
           agent_name,
           agent_address,
           recent_purchase_date,
           totalpropmktvalue,
           county) 
  
  write.csv(austin_parcel_data_merged,
            'austin_parcel_data_merged.csv'
            )
  austin_parcel_data_merged
}


code_compl_merge = function(austin_parcel_data_merged,
                            code_complaints){
  
  code_complaints$situs_address <- paste(code_complaints$HOUSE_NUMBER,
                                         code_complaints$STREET_NAME,
                                         code_complaints$CITY,
                                         code_complaints$STATE,
                                         code_complaints$ZIP_CODE)
  code_complaints$situs_address <- address_clean(code_complaints,
                                                 'situs_address')
  code_complaints_summ <- code_complaints %>%
    group_by(situs_address) %>%
    summarise(code_comp_num_total = n())
    
  registerDoFuture()
  plan(multisession, workers = 8)
  # situs_code_comp <- tapply(austin_parcel_data_merged$situs_address,
  #                           austin_parcel_data_merged$situs_pID,
  #                           function(add){
  #                             nrow(na.omit(code_complaints[grepl(add,
  #                                                                code_complaints$situs_address,
  #                                                                ignore.case = TRUE),]))
  #                             })
  
  # 
  austin_parcel_data_merged <- dplyr::left_join(austin_parcel_data_merged,
                                                code_complaints_summ,
                                                by = c('situs_address'))
    
  austin_parcel_data_merged
}


# BOX-PARALLEL: the eight TCAD parse targets each make their own full streaming
# pass over the same appraisal export and are mutually independent, but they were
# pinned to the main session so they ran serially at ~14 minutes apiece.
#
# They cannot be handed to a worker directly: reticulate exposes the Python
# parsers as external pointers, which do not serialise across a process boundary.
# So this wrapper takes a plain string, re-sources the Python module inside the
# worker that runs it, and returns a plain data frame. Only R objects cross.
TCAD_PARSE_OUTPUTS <- c(
  propChar   = "austin_propertyChar_data.csv",
  propProf   = "austin_propertyProf_data.csv",
  legal      = "austin_propertyLegal_data.csv",
  situs      = "austin_situs_data.csv",
  owner      = "austin_owner_data.csv",
  agent      = "austin_agent_data.csv",
  ownerValue = "austin_ownerValue_data.csv",
  deeds      = "austin_deeds_data.csv"
)

tcad_parse_dispatch <- function(kind) {
  if (!kind %in% names(TCAD_PARSE_OUTPUTS)) stop("unknown TCAD parse kind: ", kind)
  out_csv <- TCAD_PARSE_OUTPUTS[[kind]]
  py <- new.env(parent = globalenv())
  reticulate::source_python("TCAD_parse.py", envir = py)
  zips <- list.files()[grepl("tcad_special_export.zip", list.files())]
  if (!length(zips)) stop("tcad_special_export.zip not found in ", getwd())
  parser <- get(paste0("TCAD_parseYear_", kind), envir = py, inherits = FALSE)
  message("[tcad_parse_dispatch] ", kind, " on pid ", Sys.getpid())
  parser(zips[[1]])
  if (!file.exists(out_csv)) stop("parser produced no ", out_csv)
  read.csv(out_csv, row.names = "X")
}
