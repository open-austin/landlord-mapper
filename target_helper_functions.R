
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
                            'VENTURE',
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
                            'FOUNDA'
)

financial_marker_string <- paste(paste(financial_markers_base, 
                                       collapse = '|'),
                                 paste(financial_markers_supp, 
                                       collapse = '|'),
                                 sep = '|')
financial_marker_base_string <- paste(financial_markers_base, 
                                      collapse = '|')


address_clean = function(data = austin_parcel_data_merged,
                         col = 'situs_address'){
  data_used <- data[,col]
  
  data_used = gsub('SUITE|STE|CONDO|UNIT|APT|BLDG|[[:punct:]]',
                   '', 
                   data_used)
  data_used <- gsub('[[:space:]]+NA[[:space:]]+|[[:space:]]+NO[[:space:]]+',
                    ' ',
                    data_used)
  data_used <- gsub('^NA*[[:space:]]+|[[:space:]]+NA*$',
                    '',
                    data_used)
  data_used <- gsub('[[:space:]]{2,}',
                    ' ',
                    data_used)
  
  data_used <-sapply(data_used,
                     function(address){
                       regex_used <- '[[:digit:]]+TH|[[:digit:]]+RD|[[:digit:]]+ND'
                       start_ind <- regexpr(regex_used, address)
                       
                       if(start_ind<0){
                         return(address)
                       }
                       gsub(regex_used,
                            substr(address,(start_ind),(start_ind+attr(start_ind,
                                                                       'match.length')-3
                            )
                            ),
                            address)
                     }
  )
  data_used <- gsub('RANCH ROAD',
                    'RR',
                    data_used)
  data_used <- gsub('DRIVE',
                    'DR',
                    data_used)
  data_used<- gsub('INTERSTATE',
                   'IH',
                   data_used)
  data_used<- gsub('LANE',
                   'LN',
                   data_used)
  data_used<- gsub('ROAD',
                   'RD',
                   data_used)
  data_used <- gsub('TRAIL',
                    'TRL',
                    data_used)
  data_used <- gsub('STREET',
                    'ST',
                    data_used)
  data_used <- gsub('FREEWAY',
                    'FRWY',
                    data_used)
  data_used <- gsub('BLUFF',
                    'BLF',
                    data_used)
  data_used<- gsub('FLOOR',
                   'FL',
                   data_used)
  data_used <- gsub('PLAZA',
                    'PLZ',
                    data_used)
  data_used <- gsub('AVENUE',
                    'AVE',
                    data_used)
  data_used <- gsub('CIRCLE',
                    'CIR',  
                    data_used)
  data_used <- gsub('LANE',
                    'LN',
                    data_used)
  data_used <- gsub('PARKWAY',
                    'PKWY',
                    data_used)
  data_used <- gsub('WAY',
                    'WY',
                    data_used)
  data_used <- gsub('COURT',
                    'CT',
                    data_used)
  data_used <- gsub('COVE',
                    'CV',
                    data_used)
  data_used <- gsub('PLACE',
                    'PL',
                    data_used)
  data_used <- gsub('POINT',
                    'PT',
                    data_used)
  data_used <- gsub('HL',
                    'HILL',
                    data_used)
  data_used <- gsub('SPGS',
                    'SPRINGS',
                    data_used)
  data_used <- gsub('BOULEVARD',
                    'BLVD',
                    data_used)
  data_used <- gsub('MOUNTAIN',
                    'MTN',
                    data_used)
  data_used <- gsub('NORTH',
                    'N',
                    data_used)
  data_used <- gsub('WEST',
                    'W',                   
                    data_used)
  data_used <- gsub('SOUTH',
                    'S',
                    data_used)
  data_used <- gsub('EAST',
                    'E',
                    data_used)
  return(data_used)
  
}

target_property_gen = function(owner_data,
                               propertyChar_data,
                               propertyProf_data,
                               situs_data,
                               deeds_data){
  austin_parcel_data_merged <- left_join(situs_data,
                                         propertyChar_data,
                                         by = c("situs_pID"="propertyChar_pID",
                                                'situs_year'='propertyChar_year')
  ) %>%
    left_join(owner_data,
              by = c('situs_pID'='owner_pID',
                     'situs_year'='owner_year')
    ) %>%
    left_join(propertyProf_data,
              by = c('situs_pID'='propertyProf_pID',
                     'situs_year'='propertyProf_year')
    ) %>% 
    left_join(deeds_data,
              by = c('situs_pID'='deeds_pID',
                     'situs_year'='year'))
  
  austin_parcel_data_merged$situs_city[is.na(austin_parcel_data_merged$situs_city)] <-'AUSTIN'
  # austin_parcel_data_merged$situs_streetNum[is.na(austin_parcel_data_merged$situs_streetNum)] <-''
  # 
  # austin_parcel_data_merged$situs_streetPrefix[is.na(austin_parcel_data_merged$situs_streetPrefix)] <-''
  austin_parcel_data_merged$situs_zip <- sapply(austin_parcel_data_merged$situs_zip,
                                                function(zip){unlist(strsplit(zip, split = '-'))[1]})
  austin_parcel_data_merged$situs_country[is.na(austin_parcel_data_merged$situs_country)|
                                            austin_parcel_data_merged$situs_country==""] <-'USA'
  
  austin_parcel_data_merged$situs_international[is.na(austin_parcel_data_merged$situs_international) |austin_parcel_data_merged$situs_international==""| grepl(0, austin_parcel_data_merged$situs_international)] <-'DOMESTIC'
  
  austin_parcel_data_merged$situs_address <- paste(austin_parcel_data_merged$situs_streetNum,
                                                   austin_parcel_data_merged$situs_streetPrefix,
                                                   austin_parcel_data_merged$situs_streetName,
                                                   austin_parcel_data_merged$situs_streetSuffix,
                                                   austin_parcel_data_merged$situs_city,
                                                   austin_parcel_data_merged$situs_state,
                                                   austin_parcel_data_merged$situs_zip)
  austin_parcel_data_merged$situs_address <- address_clean(austin_parcel_data_merged,
                                                           'situs_address')
  
  austin_parcel_data_merged$owner_addrCountry[is.na(austin_parcel_data_merged$owner_addrCountry)|
                                                grepl('US',austin_parcel_data_merged$owner_addrCountry)|
                                                austin_parcel_data_merged$owner_addrCountry==""] <-'USA'
  
  austin_parcel_data_merged$owner_addrInternational[is.na(austin_parcel_data_merged$owner_addrInternational)|austin_parcel_data_merged$owner_addrInternational=="" | grepl(0, austin_parcel_data_merged$owner_addrInternational)]  <- 'DOMESTIC'
  
  austin_parcel_data_merged$owner_addrZip <- sapply(austin_parcel_data_merged$owner_addrZip,
                                                    function(zip){unlist(strsplit(zip, split = '-'))[1]})
  
  austin_parcel_data_merged$owner_address <- paste(austin_parcel_data_merged$owner_addrDeliveryLine, 
                                                   austin_parcel_data_merged$owner_addrUnitDesignator,
                                                   austin_parcel_data_merged$owner_addrCity,
                                                   austin_parcel_data_merged$owner_addrState,
                                                   austin_parcel_data_merged$owner_addrZip)
  
  austin_parcel_data_merged$owner_address <- address_clean(austin_parcel_data_merged,
                                                           'owner_address')
  
  austin_parcel_data_merged$is_residential <- (grepl('^A|^B',
                                                     austin_parcel_data_merged$propertyProf_imprvStateCd)|
                                              grepl('^A|^B',
                                                    austin_parcel_data_merged$propertyProf_landStateCd)|
                                              grepl('SF|MF',
                                                    austin_parcel_data_merged$propertyChar_zoning))
  austin_parcel_data_merged <- dplyr::filter(austin_parcel_data_merged,
                                             is_residential==TRUE)
  
  austin_parcel_data_merged$is_owner_out_of_state <- as.character(austin_parcel_data_merged$situs_state)!=as.character(austin_parcel_data_merged$owner_addrState)
  austin_parcel_data_merged$is_owner_occupied <- sapply(1:nrow(austin_parcel_data_merged),
                                                        function(ind){
                                                          result <-(grepl(
                                                            austin_parcel_data_merged$owner_address[ind],
                                                            austin_parcel_data_merged$situs_address[ind]
                                                          )|grepl("'exemptionCode': 'HS'", austin_parcel_data_merged$owner_exemptions[ind]))
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
  
  austin_parcel_data_merged$property_units = austin_parcel_data_merged$propertyProf_imprvTotalArea/900
  austin_parcel_data_merged[which((austin_parcel_data_merged$propertyProf_imprvStateCd %in%
                                     c('A1','A2','A3'))|
                                    (austin_parcel_data_merged$propertyProf_landStateCd %in%
                                       c('A1','A2','A3'))
  ) ,'property_units']<- 1
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
  austin_parcel_data_merged[!which((austin_parcel_data_merged$propertyProf_imprvStateCd %in%
                                      c('A1','A2','A3','A4','A5',
                                        'B1','B2','B3','B4','B5')
  )|
    (austin_parcel_data_merged$propertyProf_landStateCd %in%
       c('A1','A2','A3','A4','A5',
         'B1','B2','B3','B4','B5')
    )
  )
  ] <- 0
  
  austin_parcel_data_merged$is_financialized <- grepl(financial_marker_string,  
                                                      austin_parcel_data_merged$owner_name
                                                      )
  
  
  austin_parcel_data_merged$is_target = ((austin_parcel_data_merged$is_owner_occupied==FALSE) & 
                                           austin_parcel_data_merged$is_financialized & 
                                           austin_parcel_data_merged$is_residential)
  
  austin_parcel_data_merged$is_mom_and_pop = (austin_parcel_data_merged$is_owner_occupied & 
                                                austin_parcel_data_merged$is_residential & 
                                                (austin_parcel_data_merged$is_financialized==FALSE))
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
  austin_parcel_data_merged$code_comp_num_total <-foreach(ind = 1:nrow(austin_parcel_data_merged),
                                                          .combine = 'c') %dopar% {
                                                            year_used <- austin_parcel_data_merged$situs_year[ind]
                                                            add = austin_parcel_data_merged$situs_address[ind]
                                                            
                                                            data_used <- na.omit(code_complaints[grepl(add,
                                                                                                       code_complaints$situs_address,
                                                                                                       ignore.case = TRUE),])
                                                            
                                                            # data_used <-  dplyr::filter(data_used,
                                                            #                             lubridate::year(as.Date(OPENED_DATE,
                                                            #                                                     format = '%m/%d/%Y')
                                                            #                                             )<= as.numeric(year_used))
                                                            nrow(data_used)
                                                          }
  write.csv(austin_parcel_data_merged,
            'austin_parcel_data_merged.csv'
            )
  austin_parcel_data_merged
  
  
  
}
