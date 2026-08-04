
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
  data_used <- gsub('SUITE|STE|CONDO|UNIT|"|APT|BLDG|[[:punct:]]',
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


get_business_details = function(business_string){
  
  business_name <- regmatches(business_string,
                              regexpr("(?<=^).*(?=\\n)",
                                      business_string,
                                      perl = TRUE))
  ttn_matches <- regmatches(business_string,
                            regexpr("(?<=Texas Taxpayer Number:\\n).*(?=\\n)",
                                    business_string, 
                                    perl = TRUE))
  
  mail_add_matches <- regmatches(business_string,
                                 regexpr("(?<=Mailing Address:\\n).*(?=\\n)",
                                         business_string, 
                                         perl = TRUE))
  
  right_to_transact_status <- regmatches(business_string,
                                         regexpr("(?<=Right to Transact Business in Texas:\\n).*(?=\\n)",
                                                 business_string, perl = TRUE))
  state_of_formation <- regmatches(business_string,
                                   regexpr("(?<=State of Formation:\\n).*(?=\\n)",
                                           business_string, perl = TRUE))
  sos_registration_status <- regmatches(business_string,
                                        regexpr("(?<=updated each business day\\):\\n).*(?=\\n)",
                                                business_string, perl = TRUE))
  
  effective_sos_reg_date <- regmatches(business_string,
                                       regexpr("(?<=SOS Registration Date:\\n).*(?=\\n)",
                                               business_string, perl = TRUE))
  
  tx_sos_file_num <- regmatches(business_string,
                                regexpr("(?<=SOS File Number:\\n).*(?=\\n)",
                                        business_string, perl = TRUE))
  
  registered_agent_name <- regmatches(business_string,
                                      regexpr("(?<=Registered Agent Name:\\n).*(?=\\n)",
                                              business_string, perl = TRUE))
  
  reg_agent_add_line_1 <- regmatches(business_string,
                                     regexpr("(?<=Office Street Address:\\n).*(?=\\n)",
                                             business_string, perl = TRUE))
  reg_agent_add_line_2 <- regmatches(business_string,
                                     regexpr("(?<=\\n).*(?=$)",
                                             business_string, perl = TRUE))
  registered_agent_mail_add <- paste(reg_agent_add_line_1,
                                     reg_agent_add_line_2,
                                     sep = ', ')
  business_details_table = data.frame(corp_business_name = business_name,
                                      corp_TTN = ttn_matches,
                                      corp_mail_address = mail_add_matches,
                                      corp_right_to_transact_business_tx_status = right_to_transact_status,
                                      corp_state_of_formation = state_of_formation ,
                                      corp_sos_registration_status = sos_registration_status,
                                      corp_effective_sos_registration_date = effective_sos_reg_date,
                                      corp_tx_sos_file_num = tx_sos_file_num,
                                      corp_registered_agent_name = registered_agent_name,
                                      corp_registered_agent_add = registered_agent_mail_add)
  
  return(business_details_table)
}

type_name = function(entity_name_search_box,
                     name,
                     try = 1
                     ){
  if(try==1){
    elem_set_value(entity_name_search_box,
                   trimws(gsub(paste('([[:space:]]|[[:punct:]])?(?=',
                                     financial_marker_base_string,
                                     '.*)', sep = ''),
                               ', ',
                               name,
                               perl = TRUE)
                   )
    )
  }
  if(try==2){
    
    elem_set_value(entity_name_search_box,
                   trimws(gsub(paste('([[:space:]]|[[:punct:]])?(?=',
                                     financial_marker_base_string,
                                     '.*)', sep = ''),
                               ' ',
                               name,
                               perl = TRUE)
                   )
    )
    
  }
  if(try==3){
    
    elem_set_value(entity_name_search_box,
                   trimws(gsub(paste('([[:space:]]|[[:punct:]])?',
                                     financial_marker_base_string,
                                     sep = ''),
                               '',
                               name,
                               perl = TRUE)
                   )
    )
  }
  
}

set_name = function( name,
                     try = 1
                     ){
  if(try==1){
    return(trimws(gsub(paste('([[:space:]]|[[:punct:]])+(?=',
                                     financial_marker_base_string,
                                     '.*)', sep = ''),
                               ', ',
                               name,
                               perl = TRUE)
                   )
           )
  }
  if(try==2){
    
    return(trimws(gsub(paste('([[:space:]]|[[:punct:]])+(?=',
                                     financial_marker_base_string,
                                     '.*)', sep = ''),
                               ' ',
                               name,
                               perl = TRUE)
                   )
    )
    
  }
  if(try==3){
    
    return(trimws(gsub(paste('([[:space:]]|[[:punct:]])+',
                                     financial_marker_base_string,
                                     sep = ''),
                               '',
                               name,
                               perl = TRUE)
                   )
           )
  }
  
}

wait_to_click = function(element){
  element %>%
    elem_expect(is_present, is_visible, 
                timeout = 60) %>%
    elem_click()
  
}


officer_business_bind = function(owner_table,
                                 business_details_table_parse
){
  foreach(index = 1:nrow(owner_table),
          .combine = 'rbind') %do% {
            cbind(owner_table[index,],
                  business_details_table_parse)
          }
}
# https://guadalupead.org/wp-content/uploads/2025/07/2025-GCO-CERTIFIED-APPRAISAL-ROLL-SUPP-0-072925.zip
# https://guadalupead.org/wp-content/uploads/2026/03/2025-GCO-CERT-APPR-ROLL-SUPP70-030926.zip
library(selenider)
library(selenium)
library(rvest)
#<a href="https://traviscad.org/wp-content/largefiles/2025%20Special%20export%20Supp%207%2012032025.zip">2025 Supplemental Special Export (JSON)</a>
download_tcad_austin = function(){
  # Go to global website
  
  base_url = 'https://traviscad.org/publicinformation'
  base_html <- rvest::read_html(base_url) %>% 
    html_elements('.fusion-li-item-content') %>%
    html_elements('a')
  link_used <-tail(base_html[grepl('Special.*export.*JSON',base_html)] %>% html_attr('href'),1)
  
  old_link <- tryCatch({
    as.character(read.csv('link_used.csv',row.names = 'X'))
    }, error = function(cond){
      cond
    })
  if(is.na(link_used!=old_link)[1]|(link_used!=old_link)[1]){
    print('1')
      write.csv(link_used,
                'link_used.csv')
      download.file(link_used,
                    'tcad_special_export.zip')
  }
  else{
    print('2')
      if(any(grepl('tcad_special_export.zip',list.files()))){
        print('2.1')
        if(file.size('tcad_special_export.zip')<3e9){
            print('2.2')
            write.csv(link_used,
                      'link_used.csv')
            download.file(link_used,
                          'tcad_special_export.zip')        
        }
        
      }
    else{
      print('2.3')
      write.csv(link_used,
                'link_used.csv')
      download.file(link_used,
                    'tcad_special_export.zip')
    }
  }
  return('start')
  }

texas_zip_codes <- c(73301,75001,
                     75003:79999,
                     88510:88589)
download_wcad_data = function(){
  propertyChar  <- RSocrata::read.socrata("https://data.wcad.org/resource/cvyp-ab5t.json",
                               app_token = "JRn88zOD0OUxUOnBHmq8tFvf8",
                               email     = "kevinouyang1998@hotmail.com",
                               password  = "AttentionBandwidth2020!!"
                               )
  situs  <- RSocrata::read.socrata("https://data.wcad.org/resource/ai3c-c9pf.json",
                               app_token = "JRn88zOD0OUxUOnBHmq8tFvf8",
                               email     = "kevinouyang1998@hotmail.com",
                               password  = "AttentionBandwidth2020!!"
                         )
  owner  <- RSocrata::read.socrata("https://data.wcad.org/resource/bbia-wsxs.json",
                               app_token = "JRn88zOD0OUxUOnBHmq8tFvf8",
                               email     = "kevinouyang1998@hotmail.com",
                               password  = "AttentionBandwidth2020!!"
                         )
  arrow::write_parquet(propertyChar,
                       'wcad_propertyChar_data.parquet')
  arrow::write_parquet(situs,
                       'wcad_situs_data.parquet')
  arrow::write_parquet(owner,
                       'wcad_owner_data.parquet')
  # exemptions  <- RSocrata::read.socrata("https://data.wcad.org/resource/nbn7-h4pp.json",
  #                              app_token = "JRn88zOD0OUxUOnBHmq8tFvf8",
  #                              email     = "kevinouyang1998@hotmail.com",
  #                              password  = "AttentionBandwidth2020!!"
  #                             )
  list(propertyChar,
       situs,
       owner)
}

parse_wcad_data = function(raw_data){
  propertyChar <- raw_data[[1]]#arrow::read_parquet('wcad_propertyChar_data.parquet')
  situs <- raw_data[[2]]##arrow::read_parquet('wcad_situs_data.parquet')
  owner <- raw_data[[3]]# arrow::read_parquet('wcad_owner_data.parquet')
  # print('parquet')
  williamson_keys <- c('propertyid', 
                       'quickrefid')
  wcad_data <- dplyr::left_join(situs,
                                owner,
                                by = williamson_keys,
                                suffix = c('_situs',
                                           '_owner')) %>%
    dplyr::left_join(propertyChar,
                     by = c(williamson_keys),
                     suffix = c('_situs',
                                '_propertyChar')
                     ) %>%
    dplyr::filter(propertytypecode %in% c('RES', 'M'),
                  propertystatuskey=='A'
                  ) %>%
    select(!contains('datadate')) %>%
    rename(situs_address = situsaddress,
           owner_address = mailingaddress,
           owner_name = fullname) 
  # print('1')
  wcad_data <- wcad_data %>%
    mutate(is_owner_occupied = grepl('HS', exemptionlist),
           is_owner_out_of_state =  !grepl('TX', state_owner),
           is_financialized = grepl(financial_marker_string,  
                                    owner_name),
           is_target = !is_owner_occupied & is_financialized,
           is_mom_and_pop = is_owner_occupied & !is_financialized,
           situs_address = address_clean(wcad_data,'situs_address'),
           owner_address = address_clean(wcad_data, 'owner_address'),
           owner_name = address_clean(wcad_data, 'owner_name'),
           totalsqftlivingarea = as.numeric(totalsqftlivingarea),
           agent_name = '',
           agent_address = '',
           # full_name = paste(na.omit(nametitlekey),
           #                   na.omit(namefirst),
           #                   na.omit(namemiddle),
           #                   na.omit(namelast),
           #                   na.omit(namesuffixkey)
           #                           ),
           totalassessedvalue = as.numeric(totalassessedvalue),
           totalpropmktvalue = as.numeric(totalpropmktvalue),
           property_units =round(totalsqftlivingarea/900),
           zip_owner =unlist(lapply(strsplit(zip_owner, split = '-'),
                                    '[[',1)),
           zip_situs = unlist(lapply(strsplit(zip_situs, split = '-'),
                                     '[[',1)),
           recent_purchase_date = as.Date(deeddate),
           deeddate = NULL,
           county = 'williamson'
    )
  # print('2')
  
  wcad_data[which((wcad_data$fsptb %in%
                     c('A1','A2','A3','M1','M3'))) ,'property_units']<- 1
  wcad_data[which((wcad_data$fsptb %in%
                     c('B2'))) ,'property_units']<- 2
  wcad_data[which((wcad_data$fsptb %in%
                     c('B3'))) ,'property_units']<- 3
  wcad_data[which((wcad_data$fsptb %in%
                     c('B4'))) ,'property_units']<- 4
  
  wcad_data[which((wcad_data$fsptb %in%
                     c('C1','C2','C3',
                       'D1','D2',
                       'E1',
                       'F1','F2')
  )), 'property_units'] <- 0
  # print('3')
  wcad_data <- wcad_data %>%
    rename(situs_year = tax_year,
           situs_pID = propertyid,
           year_built = actyrbuilt,
           state_code = fsptb,
           situs_zip = zip_situs,
           owner_zip = zip_owner,
           # owner_name = full_name
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
           # totalassessedvalue,
           totalpropmktvalue,
           county) 
  # print('4')

  wcad_data
  
}

parse_hays_cad_data = function(dir =  "austin_sanantonio",
                               zipfile = "AUSTIN–SAN ANTONIO METROPLEX (13 of 13).zip",
                               county = "HAYS COUNTY APPRAISAL DISTRICT+SR(done)",
                               year_used = 2025){
  zipfile <- iconv(zipfile,to='UTF-8')
  folder <- file.path( tempdir() , gsub('[[:punct:][:space:]]|-','',
                                        zipfile) ) 
  print(folder)
  print(zipfile)
  unzip(zipfile, 
        exdir = folder )
  counties <- list.files(folder)
  
  while(length(counties)==1){
    folder <- file.path(folder, counties)
    counties <- list.files(folder)
  }
  counties <- counties[grepl('Hays', counties, ignore.case = TRUE)]
  
  data_folder <- file.path(tempdir(),
                           counties) 
  
  key_cols <- c("PropertyID","QuickRefID","PropertyNumber")
  
  # print('zip')

  
  print(data_folder)
  unzip(list.files(file.path(folder,
                             counties), full.names = TRUE), 
         exdir = file.path(tempdir(),
                           counties)) 
  # list.files( file.path( tempdir() , "unzips" ) , full.names = TRUE )
  pacs_files <- c(list.files(data_folder,
                             full.names = TRUE,
                             recursive = TRUE)[grepl('.zip',
                                                     list.files(data_folder,
                                                                full.names = TRUE,
                                                                recursive = TRUE))]
                  )
  # print(pacs_files)
  sapply(pacs_files,
         function(file){
           unzip( file, 
                  exdir = data_folder )
         })
  pacs_files <- list.files(data_folder,
                             full.names = TRUE,
                             recursive = TRUE)
  pacs_files <- pacs_files[grepl('txt$',pacs_files)]
  
  # print(pacs_files)
  hays_data <- read.csv(pacs_files[1])
  sapply(pacs_files[-1],
         function(file){
           file_data <- read.csv(file)
           hays_data <<- dplyr::left_join(hays_data,
                                         read.csv(file),
                                         by = key_cols,
                                         multiple = 'last',
                                         suffix = 
                                         )
         })
  
    hays_data <- hays_data %>% 
      dplyr::filter(Type.x %in% c('M', 'R')
                    ) %>%
      select(!contains('RecordType')) %>%
      rename(situs_address = Situs,
           owner_name = OwnerName) %>%
      mutate(owner_address = paste(replace_na(Address1,''),
                                   replace_na(Address2,''),
                                   replace_na(Address3,''),
                                   replace_na(City,''),
                                   replace_na(State,''),
                                   replace_na(Zip,'')
                                   ))
    hays_data <- hays_data %>% 
      mutate(is_owner_occupied = grepl('HS', ExemptionList),
             is_owner_out_of_state =  !grepl('TX', State),
             is_financialized = grepl(financial_marker_string,  
                                    owner_name),
           is_target = !is_owner_occupied & is_financialized,
           is_mom_and_pop = is_owner_occupied & !is_financialized,
           situs_address = address_clean(hays_data,'situs_address'),
           owner_name = address_clean(hays_data, 'owner_name'),
           totalsqftlivingarea = as.numeric(SquareFootage),
           agent_name = '',
           agent_address = '',
           owner_address = address_clean(hays_data, 'owner_address'),
           totalassessedvalue = as.numeric(AssessedValue),
           totalpropmktvalue = as.numeric(MarketValue),
           property_units =round(totalsqftlivingarea/900),
           Zip =unlist(lapply(strsplit(Zip, split = '-'),
                              function(zip) {ifelse(length(zip)==0,
                                                    '',
                                                    zip[[1]])})),
           SitusZip = unlist(lapply(strsplit(SitusZip, split = '-'),
                                    function(zip) {ifelse(length(zip)==0,
                                                          '',
                                                          zip[[1]])})),
           agent_name = '',
           agent_address='',
           recent_purchase_date = as.Date(DeedDate),
           deeddate = NULL,
           situs_year = year_used,
           county = 'hays'
           )
  
  
    hays_data[which((hays_data$LandType %in%
                     c('^A','^M[[[:digit:]]H]','^E'))) ,'property_units']<- 1
    hays_data[which((hays_data$LandType %in%
                     c('B2'))) ,'property_units']<- 2
  # hays_data[which((hays_data$fsptb %in%
  #                    c('B3'))) ,'property_units']<- 3
  # hays_data[which((hays_data$fsptb %in%
  #                    c('B4'))) ,'property_units']<- 4
  
    hays_data[which((hays_data$LandType %in%
                     c('C1','C2','C3',
                       'D1','D2',
                       'E1','E3','E5',
                       'F1','F2')
                     )), 'property_units'] <- 0
    hays_data <- hays_data %>%
      rename(situs_pID = PropertyID,
           year_built = ActYrBuilt,
           state_code = LandType,
           situs_zip = SitusZip,
           owner_zip = Zip,
           propertytypedesc = Description.y,
           legallocationdesc=LegalLocationDesc
           # owner_name = full_name
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
           property_units,
           # totalassessedvalue,
           totalpropmktvalue,
           county) 
  # arrow::write_parquet(hays_data,
  #                      'hays_county_data.parquet')
  
  # write.csv(hays_data,'hays_county_data.csv')
  hays_data
}

ingest_cad_zip_data = function(zipfile,
                               dir,
                               county){
  
  pacs_folder <- file.path( tempdir(), gsub('[["punct:]]','',
                                            county )) 
  unzip( zipfile, 
         exdir = pacs_folder )
  print('1')
  # pacs_folder <-gsub('.zip','',
  #                    zipfile )
  # list.files( file.path( tempdir() , "unzips" ) , full.names = TRUE )
  pacs_files <- c(list.files(pacs_folder,
                             full.names = TRUE,
                             recursive = TRUE),
                  # list.dirs(dir, recursive = FALSE),
                  
                  list.dirs(pacs_folder, recursive = FALSE)
                  )
  # print(pacs_files)

  iteration = 0

  
  while(iteration<10){
    appraisal_file <- pacs_files[grepl('APPRAISAL_INFO|PROP.TXT',
                                       pacs_files)]
    # print(appraisal_file)
    if(length(appraisal_file)>0){
      break
    }
    if(length(pacs_files)==0){
      pacs_folder <- dir
      pacs_files <- list.files(dir)
      break
    }
    # print(iteration)
    if(any(grepl('.zip', pacs_files))){
      print('zip')
      iteration <- iteration +1
      zipfile <- pacs_files[grepl('.zip',pacs_files)][1]
      pacs_folder <- file.path( tempdir() , gsub('[["punct:]]','',
                                                 county ) ) 
      unzip(zipfile,
            exdir = pacs_folder)

      pacs_files <- list.files(pacs_folder,
                               full.names = TRUE,
                               recursive = TRUE)
      if(length(pacs_files)==0){
        pacs_files <- list.files(dir)
      }
      appraisal_file <- pacs_files[grepl('APPRAISAL_INFO|PROP.TXT',
                                         pacs_files)]
      print(appraisal_file)
      if(length(appraisal_file)>0){
        break
      }
      # print(pacs_folder)
      # print(pacs_files)
    }
    else{
      break
    }
  }
  appraisal_file <- pacs_files[grepl('APPRAISAL_INFO|PROP.TXT',
                                     pacs_files)]
  improvement_file <- pacs_files[grepl('APPRAISAL_IMPROVEMENT_DETAIL.TXT|IMP_INFO.TXT',
                                       pacs_files)]
  land_file <- pacs_files[grepl('APPRAISAL_LAND_DETAIL.TXT|LAND_DET.TXT',
                                       pacs_files)]
  # mobile_file <- pacs_files[grepl('APPRAISAL_MOBILE_HOME_INFO.TXT|MOBILE_HOME_INFO.TXT',
  #                                      pacs_files)]
  print('1.5')
  #
  if(length(appraisal_file)==0){
    return()
  }
  
  pacs_fields <-readxl::read_xlsx("Appraisal Export Layout - 8.0.30.xlsx", skip = 54, n_max = 254)
  improvement_fields <-readxl::read_xlsx("Appraisal Export Layout - 8.0.30.xlsx", 
                                         skip = 930, n_max = 13)
  land_fields <-readxl::read_xlsx("Appraisal Export Layout - 8.0.30.xlsx", 
                                         skip = 976, n_max = 20)
  # mobile_fields <- readxl::read_xlsx("Appraisal Export Layout - 8.0.30.xlsx",
  #                                    skip = 1132, n_max = 13)
  pacs_data <- data.frame(foreach(file = appraisal_file,
                                   .combine = 'rbind') %do% {
                                     ingest_pacs_txt_data(file,
                                                          pacs_fields
                                                          )
                                   } 
                           ) %>%
    mutate(prop_val_yr = as.numeric(prop_val_yr))

  improvement_data <- data.frame(foreach(file = improvement_file,
                                   .combine = 'rbind') %do% {
                                     ingest_pacs_txt_data(file,
                                                          improvement_fields)
                                   }
                                  ) %>%
    dplyr::filter(grepl("AREA|LIVING|HOTEL|APARTMENT|CONDO|HOME|PARK|DORM|RES",
                        toupper(Imprv_det_type_desc)),
                  !Imprv_det_type_cd=='RMS')%>%
    mutate(propertyProf_imprvTotalArea = imprv_det_area,
           prop_val_yr = as.numeric(prop_val_yr)
           ) %>%
    group_by(prop_id, prop_val_yr) %>%
    summarise(imprv_det_area = sum(as.numeric(imprv_det_area)),
              yr_built = min(as.numeric(yr_built)))
              # Imprv_det_type_desc = paste(unique(Imprv_det_type_desc),
              #                             collapse = '|'))
  land_data <- data.frame(foreach(file = land_file,
                                         .combine = 'rbind') %do% {
                                           ingest_pacs_txt_data(file,
                                                                land_fields)
                                         }
                          ) %>%
    mutate(size_square_feet = as.numeric(size_square_feet),
           prop_val_yr = as.numeric(prop_val_yr)
           ) %>%
    dplyr::filter(grepl('FAM|RESID|COND|APART|HOME|PARK|DORM|APT|SINGL',toupper(land_type_desc))|
                    grepl('^M|^R|RCO|RES|CMF|RDX|RHS|RTX|RQX|RMU',land_type_cd
                    )) %>%
    group_by(prop_id,prop_val_yr) %>%
    summarise(size_square_feet = sum(size_square_feet),
              land_type_cd = paste(unique(land_type_cd),collapse = '|'),
              land_type_desc = paste(unique(land_type_desc), collapse = '|'),
              state_cd = paste(unique(state_cd), collapse = '|')
              
              )
  
  print('2')
  # print(head(pacs_data))
  # write.csv(improvement_data,
  #           'imprv_data.csv')
  # write.csv(land_data,
  #           'land_data.csv')
  # land_data
  pacs_data <- dplyr::left_join(pacs_data,
                     land_data,
                     by = c('prop_id',
                            'prop_val_yr'))
  rm(land_data)
  gc()
  pacs_data <- dplyr::left_join(pacs_data,
                                improvement_data,
                     by = c('prop_id',
                            'prop_val_yr'))  %>%
    dplyr::mutate(across(everything(),~ as.character(.x)))
  rm(improvement_data)
  gc()
  print('2.5')
  # print(head(pacs_data))
  pacs_data <- pacs_data %>%
    select(paste(unlist(read.csv('pac_cols.txt',header = FALSE)))
           ) %>%
    tidyr::fill(situs_zip,situs_city, .direction = 'updown') %>%
    mutate(across(everything(),~ enc2utf8(.x))) %>%
    mutate(situs_address = paste(replace_na(situs_num,''),
                                 replace_na(situs_street_prefx,''),
                                 replace_na(situs_street,''),
                                 replace_na(situs_street_suffix,''),
                                 replace_na(situs_unit,''),
                                 replace_na(situs_city,''),
                                 replace_na(situs_zip,'')),
           py_owner_name = paste(replace_na(py_owner_name,''),
                                 replace_na(py_addr_line1,'')),
           owner_address = paste(replace_na(py_addr_line2,''),
                                 replace_na(py_addr_line3,''),
                                 replace_na(py_addr_city,''),
                                 replace_na(py_addr_state,''),
                                 replace_na(py_addr_zip,''),
                                 replace_na(py_addr_country,'')),
           entity_agent_address = paste(replace_na(entity_agent_addr_line1,''),
                                        replace_na(entity_agent_addr_line2,''),
                                        replace_na(entity_agent_addr_line3,''),
                                        replace_na(entity_agent_city,''),
                                        replace_na(entity_agent_state,''),
                                        replace_na(entity_agent_zip,''),
                                        replace_na(entity_agent_country,'')),
           ca_agent_address = paste(replace_na(ca_agent_addr_line1,''),
                                    replace_na(ca_agent_addr_line2,''),
                                    replace_na(ca_agent_addr_line3,''),
                                    replace_na(ca_agent_city,''),
                                    replace_na(ca_agent_state,''),
                                    replace_na(ca_agent_zip,''),
                                    replace_na(ca_agent_country,'')),
           arb_agent_address = paste(replace_na(arb_agent_addr_line1,''),
                                     replace_na(arb_agent_addr_line2,''),
                                     replace_na(arb_agent_addr_line3,''),
                                     replace_na(arb_agent_city,''),
                                     replace_na(arb_agent_state,''),
                                     replace_na(arb_agent_zip,''),
                                     replace_na(arb_agent_country,''))
           ) %>% 
    rename(owner_name = py_owner_name)
    print('3')
    pacs_data <- pacs_data %>%
      mutate(
        is_owner_occupied = as.logical(hs_exempt),
        is_owner_out_of_state =  !grepl('TX', py_addr_state),
        is_financialized = grepl(financial_marker_string,
                                 owner_name),
        is_target = !is_owner_occupied & is_financialized,
        is_mom_and_pop = is_owner_occupied & !is_financialized,
        situs_address = address_clean(pacs_data,'situs_address'),
        owner_address = address_clean(pacs_data, 'owner_address'),
        owner_name = address_clean(pacs_data, 'owner_name'),
        
        entity_agent_name = address_clean(pacs_data,'entity_agent_name'),
        ca_agent_name = address_clean(pacs_data,'ca_agent_name'),
        arb_agent_name = address_clean(pacs_data,'arb_agent_name'),
        
        entity_agent_address = address_clean(pacs_data,'entity_agent_address'),
        ca_agent_address = address_clean(pacs_data,'ca_agent_address'),
        arb_agent_address = address_clean(pacs_data,'arb_agent_address'),
        
        totalsqftlivingarea = as.numeric(imprv_det_area),# size_square_feet
        legallocationdesc = paste(replace_na(legal_desc,''),
                                  replace_na(legal_desc2,'')),
        # totalassessedvalue = as.numeric(appraised_val),
        totalpropmktvalue = as.numeric(market_value),
        property_units =round(totalsqftlivingarea/900),
        owner_zip =unlist(lapply(strsplit(py_addr_zip, split = '-'),
                                    '[[',1)),
        situs_zip = unlist(lapply(strsplit(situs_zip, split = '-'),
                                     '[[',1)),
        prop_val_yr = as.numeric(prop_val_yr),
        recent_purchase_date = as.Date(deed_dt,format = '%m%d%Y')
           )
  print('4')
  pacs_data <- pacs_data %>%
    group_by(prop_id) %>%
    mutate(agent_name = trimws(paste(unique(c(replace_na(entity_agent_name,''),
                                            replace_na(ca_agent_name,''),
                                            replace_na(arb_agent_name,'')
                                            )),
                                     collapse = ' '
                                     )
                               ),
           agent_address = trimws(paste(unique(c(replace_na(entity_agent_address,''),
                                               replace_na(ca_agent_address,''),
                                               replace_na(arb_agent_address,'')
                                               )),
                                        collapse = ' '
                                        )
                                  )
           )
  
  pacs_data$county = tolower(trimws(gsub('COUNTY[[:space:]]+APPRAISAL DISTRICT+.*',
                                   '',toupper(county) )))
  pacs_data[which(grepl('^A|^M|^E[1-2]|^F',
                        pacs_data$imprv_state_cd)|
                    grepl('^A|^M|^E[1-2]|^F',
                          pacs_data$land_state_cd)
                  ) ,'property_units']<- 1
  # pacs_data[which((pacs_data$imprv_state_cd %in%
  #                    c('B2'))) ,'property_units']<- 2
  pacs_data[which(grepl('^C|^D|^G|^J|^L|^O|^S|^X',
                        pacs_data$imprv_state_cd)|
                    grepl('^C|^D|^G|^J|^L|^O|^S|^X',
                          pacs_data$land_state_cd)
                  ), 'property_units'] <- 0
  print('4.5')
  pacs_data <- pacs_data %>%
    rename(situs_year = prop_val_yr,
           situs_pID = prop_id,
           year_built = yr_built,
           state_code = state_cd,
           propertytypedesc = land_type_desc
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
           # totalassessedvalue,
           totalpropmktvalue,
           county) 
  print('4.75')
  # file.remove(list.files(tempdir(), full.names = TRUE,recursive = TRUE))
  # fs::dir_delete(file.path( tempdir(), gsub('[["punct:]]','',
  #                                           county )) )
  gc()
  # fs::dir_delete(pacs_folder)
  # write.csv(pacs_data,
  #           sprintf('%s_CAD_data.csv',
                    # county))
  # fs::dir_delete(pacs_folder)
  print('5')
  
  pacs_data
  
}

ingest_pacs_txt_data = function(appraisal_file,
                                pacs_fields){
  
  # appraisal_data_line <- readLines(appraisal_file, n = 100)
  # registerDoFuture()
  # plan(multisession)
  # appraisal_data_final <- foreach(line = appraisal_data_line,
  #                           .combine = 'rbind') %dopar% {
  #                             row <- unlist(sapply(1:nrow(pacs_fields),
  #                                                  function(field){
  #                                                    gsub('^[[:space:]0]+|[[:space:]]+$',
  #                                                         '',
  #                                                         trimws(substr(line,
  #                                                           pacs_fields$Start[field],
  #                                                           pacs_fields$End[field]))
  #                                                         )
  #                                                  }
  #                                                  ))
  #                                           
  
  #                             row
  #                           }
  appraisal_data_final <- vroom::vroom_fwf(appraisal_file,vroom::fwf_widths(pacs_fields$Length,
                                                           pacs_fields$`Field Name`)
                                           )
  colnames(appraisal_data_final) <- pacs_fields$`Field Name`
  appraisal_data_final
  
  }

ingest_proton_pacs_cad_data = function(zipfile_used){
  # zipfile_used <- 'AUSTIN–SAN ANTONIO METROPLEX (13 of 13).zip'
  folder <- file.path( gsub('.zip','',
                            zipfile_used) ) 
  unzip(zipfile_used, 
        exdir = folder )
  counties <- list.files(folder)

  while(length(counties)==1){
    folder <- file.path(folder, counties)
    counties <- list.files(folder)
  }
  counties <- counties[!grepl('Travis|Hays|Williams', counties, ignore.case = TRUE)]
  print(folder)
  print(counties)
  # BOX-PARALLEL-PACS: was %do% (serial, one core). ingest_cad_zip_data()
  # unzips each county into its own file.path(tempdir(), county), so counties
  # share no scratch path and are safe concurrently. Bounded well under
  # detectCores() because each county holds a full county of rows in memory.
  doFuture::registerDoFuture()
  future::plan(future::multisession,
               workers = max(1, min(length(counties), 8)))
  on.exit(future::plan(future::sequential), add = TRUE)
  cad_data <- foreach::foreach(county = counties,
                               .combine = 'rbind') %dopar% {
                                 gc()
                                 print(county)
                                
                                path_used <- file.path(folder,
                                                       county)
                                # print(folder)
                                print(list.files(path_used))
                               if(any(grepl('.zip',list.files(path_used)))){
                                 print('zip')
                                  return(ingest_cad_zip_data(file.path(path_used,
                                                                list.files(path_used)[grepl('.zip',list.files(path_used))][1]),
                                                      path_used, 
                                                      county))
                                }
                                if(any(fs::is_dir(list.files(path_used)))){
                                  appraisal_file <- pacs_files[grepl('APPRAISAL_INFO|PROP.TXT',
                                                                     pacs_files)]
                                  
                                }
                                else{
                                  return()
                                }
                                
                               }
  print('done')
  # fs::dir_delete(folder)
  # write.csv(cad_data,'austin_metro_pacs_data.csv')
  gc()
  cad_data
  
  
}
cpa_api_request = function(base_string,
                           input,
                           api_key_used){
  url_string <-URLencode(sprintf(base_string,
                                        input))
  used_request <- request(url_string) %>%
    httr2::req_headers('x-api-key' = api_key_used) %>%
    httr2::req_perform() %>%
    httr2::resp_body_json()# 
}


# BOX-SPEED: classify a failed lookup so the retry policy can stop treating
# "your request was wrong" as "the server is busy".
#
# cpa_api_request() ends in httr2::req_perform(), which throws on any non-2xx
# status as well as on transport failure, so by the time a condition reaches
# the retry wrapper the two cases are indistinguishable unless we look. The
# 360-owner probe saw 60 x HTTP 400 and 5 x HTTP 413 and zero 429/503/
# Retry-After, and re-asking a 400 five times rescued none of the 39 affected
# owners -- see the block at insist_scrape_owner.
#
# Returns exactly one of:
#   "transient" - worth the full backoff: 408, 425, 429, any 5xx, or a curl
#                 transport failure (no response at all).
#   "permanent" - any other HTTP status the server actually returned. A 4xx is
#                 a statement about our request, and we would resend the same
#                 bytes, so more attempts cannot change the answer.
#   "unknown"   - no status to read, and not a recognised transport failure:
#                 an error raised in our own R parsing code. Deliberately NOT
#                 called permanent, because a finicky response shape is one way
#                 those arise, and the caller keeps retrying these.
#
# Never throws. A classifier that can error would turn one odd condition object
# into a dead worker.
cpa_error_is_transient <- function(cnd) {
  status <- NA_integer_
  s <- tryCatch(cnd$resp$status_code, error = function(e) NULL)
  if (is.numeric(s) && length(s) == 1L && !is.na(s)) status <- as.integer(s)
  if (is.na(status)) {
    # httr2 also encodes the status in the condition class (httr2_http_400).
    cl <- class(cnd)
    hit <- cl[grepl('^httr2_http_[0-9]{3}$', cl)]
    if (length(hit)) status <- as.integer(sub('^httr2_http_', '', hit[1]))
  }
  if (!is.na(status)) {
    if (status >= 500L || status %in% c(408L, 425L, 429L)) return('transient')
    return('permanent')
  }
  if (inherits(cnd, 'httr2_failure') || inherits(cnd, 'curl_error')) {
    return('transient')
  }
  'unknown'
}

# BOX-SPEED: the retry policy for one owner lookup. See the long block at the
# call site in owner_scrape_actual() for the measurements that motivate it.
#
# A factory, not a closure defined at the call site, so that the returned
# function's enclosing environment is THIS frame -- three small closures --
# rather than owner_scrape_actual()'s frame, which holds the ~128 MB parcel
# frame that future would then ship to every worker on every chunk.
#
# Attempt budget, compared with the purrr:::insistently() call this replaces:
#   transient failure : 1 + up to 5 = up to 6 attempts, 2-4-8-16 s backoff.
#                       Strictly MORE retrying than before, never less, so a
#                       real throttle is handled at least as well as it was.
#   unknown failure   : 1 + up to 3 = up to 4 attempts, 1-2 s backoff. Still
#                       re-asks, but ~3 s of sleep instead of ~30 s.
#   permanent failure : 1 attempt. The condition is re-signalled unchanged, so
#                       the worker still records the owner as not_resolved and
#                       the pass loop still re-asks it next pass.
make_insist_scrape_owner <- function() {
  # Captured BY VALUE, both of them, so that the closure this returns carries
  # everything it needs and a future worker never has to resolve either name.
  # purrr:::insistently(scrape_owner_api, ...) captured the function by value
  # too -- it becomes insistently's `f` -- so keeping that property means this
  # rewrite adds no new export dependency for the %dopar% body to satisfy.
  ask <- scrape_owner_api
  classify <- cpa_error_is_transient

  # VERBATIM the wrapper this replaces -- same function, same rate_backoff
  # arguments -- reached only when the failure is classified transient.
  slow_retry <- purrr:::insistently(scrape_owner_api,
                                    rate =purrr::rate_backoff(pause_base = 2,
                                                        pause_cap = 30,
                                                        pause_min = 2,
                                                        max_times = 5,
                                                        jitter = TRUE
                                    ))

  cheap_retry <- purrr:::insistently(scrape_owner_api,
                                     rate =purrr::rate_backoff(pause_base = 1,
                                                         pause_cap = 4,
                                                         pause_min = 1,
                                                         max_times = 3,
                                                         jitter = TRUE
                                     ))

  function(...) {
    # The first attempt is identical to insistently()'s first attempt:
    # rate_sleep() does not sleep at i = 0, so nothing is being skipped here.
    first <- tryCatch(ask(...), error = function(cnd) cnd)
    if (!inherits(first, 'error')) {
      return(first)
    }
    kind <- classify(first)
    if (identical(kind, 'transient')) {
      return(slow_retry(...))
    }
    if (identical(kind, 'unknown')) {
      return(cheap_retry(...))
    }
    # Permanent. Re-signal the ORIGINAL condition, unchanged, so the worker's
    # `'error' %in% class(answer)` branch sees exactly what it saw before.
    stop(first)
  }
}

cpa_franchise_get = function(taxId,
                             api_key_used){
  franchise_info <- cpa_api_request("https://comptroller.texas.gov/data-search/franchise-tax/%s",
                  taxId,
                  api_key_used)$data
  
  
  corp_add <- paste(franchise_info$mailingAddressStreet,
                    franchise_info$mailingAddressCity,
                    franchise_info$mailingAddressState,
                    franchise_info$mailingAddressZip
                        )
  registered_agent_add <- paste(franchise_info$registeredOfficeAddressStreet,
                                franchise_info$registeredOfficeAddressCity,
                                franchise_info$registeredOfficeAddressState,
                                franchise_info$registeredOfficeAddressZip
                                )
  
  business_details_df <- data.frame(corp_business_name = franchise_info$name,
                                    corp_TTN = taxId,
                                    corp_mail_address = corp_add,
                                    corp_right_to_transact_business_tx_status = franchise_info$rightToTransactTX,
                                    corp_state_of_formation = franchise_info$stateOfFormation,
                                    corp_sos_registration_status =franchise_info$sosRegistrationStatus,
                                    corp_effective_sos_registration_date = franchise_info$effectiveSosRegistrationDate,
                                    corp_tx_sos_file_num = franchise_info$sosFileNumber,
                                    corp_registered_agent_name = franchise_info$registeredAgentName,
                                    corp_registered_agent_add = registered_agent_add)
  
  officerInfo <- franchise_info$officerInfo
  owner_address <- unlist(lapply(officerInfo,
                                 function(officer){
                                   add_used <- paste(officer$AD_STR_POB_TX,
                                         officer$CITY_NM,
                                         officer$ST_CD,
                                         officer$AD_ZP)
                                   ifelse(length(add_used)==0,
                                          NA,
                                          add_used)
                                 })
                          )
  owner_address[which(is.null(owner_address)|
                        is.na(owner_address))] <- NA
  officer_details_df <- data.frame(owner_name = unlist(lapply(officerInfo,
                                                             '[[',1)),
                                   owner_title = unlist(lapply(officerInfo,
                                                               '[[',2)),
                                   owner_active_year = unlist(lapply(officerInfo,
                                                                '[[',3)),
                                   owner_mail_address = owner_address
                                   )
                                   
  return(list(business_details_df,
              officer_details_df))
}
scrape_owner_api = function(owner_name,
                        situs_pID,
                        situs_address,
                        veneer_owner,
                        veneer_owner_mail_address,
                        depth = 3,
                        owner_title = NA,
                        owner_mail_address = NA,
                        owner_active_year = NA,
                        business_details_table = NA){
  # print(depth)
  # print(owner_name) 
  # print(owner_mail_address)
  api_key = readLines('cpa_key.txt', warn = FALSE)
  # payers_response <- cpa_api_request("https://api.comptroller.texas.gov/public-data/v1/public/sales-tax-payer?searchType=legalName&BUSINESS_NAME=%s",
  #                                 owner_name,
  #                                 api_key)
  # tax_payer_ids = unlist(lapply(payers_response$data,
  #                               function(entity){unlist(entity$taxpayerId)}))
  # cpa_api_request("https://api.comptroller.texas.gov/public-data/v1/public/franchise-tax/%s",
  #                 owner_name,
  #                 api_key)
  # print('0')
  payers_response <- cpa_api_request("https://comptroller.texas.gov/data-search/franchise-tax?name=%s",
                                     owner_name,
                                     api_key)
  # print('initial')
  try_used = 1

  # BOX-SPEED: skip a spelling attempt whose QUERY STRING is byte-identical to
  # one already sent for this owner. Measured, not guessed.
  #
  # All three set_name() branches only rewrite whitespace/punctuation sitting
  # immediately before a financial marker (LLC / LTD / ...). For a name where
  # that regex does not fire, set_name() returns trimws(name) -- i.e. exactly
  # the string we already asked about. Over all 94,954 real owner keys:
  #
  #   attempt 2 byte-identical to attempt 1  : 87,921 of 89,408 names (98.3%)
  #   attempt 3 byte-identical to 1 or 2     : 21,900 of 89,408 names (24.5%)
  #   distinct queries the 3 attempts produce: mean 1.78 of 3 per owner key
  #   => 115,858 of 284,862 name searches (40.7%) are exact repeats
  #
  # And the repeats never pay off. Over 360 probed owners the winning spelling
  # was attempt 1 for 101, attempt 3 for 48, and attempt 2 for ZERO of them --
  # which is what you would expect when attempt 2 is usually the same question.
  #
  # So this does NOT reduce the number of attempts and does NOT drop attempt 3:
  # attempt 3 is load-bearing (52% of successful matches came from it). The
  # bound is still "three spellings, then NULL"; try_used still increments on
  # every iteration, so the loop is still capped at 3 iterations and cannot
  # spin. Only the duplicate HTTP request is dropped.
  #
  # Answer-preserving, with one judgement call stated plainly: re-sending an
  # identical query did act as an accidental retry of the count == 0 case, and
  # the API owner warns a no-result is not durable. That durability concern is
  # already handled one level up and better: a key that comes back no_record is
  # re-asked on the next pass (SCRAPE_RETRY_PASSES = 4), minutes later rather
  # than milliseconds later. Asking the identical question twice in the same
  # second was never the mechanism protecting us.
  asked_names <- owner_name

  while(payers_response$count==0){
    # print(try_used)
    try_used = try_used+1
    if(try_used>3){
      return(NULL)
    }
    next_name <- set_name(owner_name,
                          try_used
                          )
    # length check first: if set_name() ever returns something other than a
    # single string, fall through and ask exactly as this code always has,
    # rather than letting `if (logical(0))` throw.
    if(length(next_name)==1L && next_name %in% asked_names){
      next
    }
    asked_names <- c(asked_names, next_name)
    payers_response <- cpa_api_request("https://comptroller.texas.gov/data-search/franchise-tax?name=%s",
                                       next_name,
                                       api_key)
    }
  try_used = 1
  taxId <- unlist(lapply(payers_response$data,
                                function(entity){
                                  unlist(entity$taxpayerId)
                                  
                                }))[1]
  
  # print(taxId)
  franchise_info <- cpa_franchise_get(taxId,
                                      api_key)
  
  business_details_table_parse = franchise_info[[1]]
  owner_details_table_parse = franchise_info[[2]]
  # print(owner_details_table_parse)
  if(nrow(owner_details_table_parse)==0){
    # print('1')
    #no results on a recursive owner search
    if(depth>0 ){
      
      # print('1.1')
      owner_table = data.frame(owner_name = owner_name,
                               owner_title = owner_title,
                               owner_address = owner_mail_address,
                               owner_active_year = owner_active_year)
      
      # BOX-FIX: this branch used to pass `business_details_table` -- the
      # function's own NA-valued default argument -- where it meant to pass
      # `business_details_table_parse`, the franchise record this very call just
      # fetched. That is an UPSTREAM bug, not one introduced by the box patches;
      # it is present in scrape_owner_api() as written, and the legacy
      # Selenium-era scrape_owner() below correctly uses the _parse table in BOTH
      # the depth > 0 and depth == 0 arms of this same if/else, which is what
      # marks this as a porting typo rather than intent.
      #
      # Why it fires at the top level at all: the comment above says "no results
      # on a recursive owner search", implying depth > 0 only happens under
      # recursion (where the caller does pass its parsed table down, so the bug
      # is invisible). But owner_scrape_actual()'s worker calls this with the
      # default depth = 3, so EVERY top-level owner whose franchise record exists
      # but lists zero officers lands here with business_details_table == NA.
      #
      # What that produced, measured: officer_business_bind() cbinds a 4-column
      # owner_table to the scalar NA, giving 5 columns, and situs_pID /
      # situs_address bring it to 7 -- against the 16 the schema expects. Those 7
      # columns do not include corp_registered_agent_add, so the address_clean()
      # calls at the end of this function hit `data[, col]` on a missing column
      # and throw "undefined columns selected". The throw is inside the worker's
      # tryCatch, so it did not kill the run, but it did mean: the owner was
      # retried five times by purrr::insistently with backoff, then re-asked on
      # the next pass, and finally recorded as `not_resolved` -- i.e. "the API
      # never answered us" -- when in fact Texas answered clearly and the answer
      # was a real franchise filing with no officers listed. Wasted API calls,
      # and a genuine finding misfiled as an unknown, which is exactly the
      # matched / no_record / not_resolved distinction the previous commit exists
      # to protect.
      #
      # Fixing the argument makes this branch return the same 16 columns as its
      # depth == 0 sibling: the entity's own franchise record with an empty
      # officer row. It also removes those wasted retries, which is a reduction
      # in API traffic for the affected owners, not a change to the retry policy.
      results = officer_business_bind(owner_table,
                                      business_details_table_parse)
      results$situs_pID <- situs_pID
      results$situs_address <- situs_address
    }
    #no results on base owner search
    if(depth==0 ){
      # print('1.2')
      owner_table = data.frame(owner_name = NA,
                               owner_title = NA,
                               owner_address = NA,
                               owner_active_year = NA)
      
      results = officer_business_bind(owner_table,
                                      business_details_table_parse)
      results$situs_pID <- situs_pID
      results$situs_address <- situs_address
    }
    
  }
  #found results
  else{
    # print('2')
    finance_inds <- grepl(financial_marker_string,
                          owner_details_table_parse$owner_name)
    # repeat_inds <- which(owner_details_table_parse$owner_name==owner_name)
    #if owner has financial markers, do a recursive search on it
    if(depth>=0){
      if(sum(finance_inds)>0){
        # print('2.1')
        owners_fin = foreach(ind = which(finance_inds),
                             .combine = 'rbind') %do% {
                               fin_owner_scrape = tryCatch({
                                 if(owner_details_table_parse$owner_name[ind]==owner_name){
                                   owner_fin = data.frame(owner_name= owner_details_table_parse$owner_name[ind],
                                                          owner_title = owner_details_table_parse$owner_title[ind],
                                                          owner_address = owner_details_table_parse$owner_mail_address[ind],
                                                          owner_active_year = owner_details_table_parse$owner_active_year[ind]
                                                          )
                                   officer_business_bind(owner_fin,
                                                         business_details_table_parse)
                                 }
                                 else{
                                   scrape_owner_api( owner_details_table_parse$owner_name[ind],
                                                     situs_pID = situs_pID , 
                                                     situs_address = situs_address,
                                                     veneer_owner = veneer_owner,
                                                     veneer_owner_mail_address = veneer_owner_mail_address,
                                                     depth = depth-1,
                                                     owner_title = owner_details_table_parse$owner_title[ind],
                                                     owner_mail_address = owner_details_table_parse$owner_mail_address[ind],
                                                     owner_active_year = owner_details_table_parse$owner_active_year,
                                                     business_details_table = business_details_table_parse )
                                 }
                                 
                               },error = function(cond){
                                 cond
                               })
                               
                               if('error' %in% class(fin_owner_scrape)){
                                 
                                 # print('error')
                                 owner_fin = data.frame(owner_name = owner_details_table_parse$owner_name[ind],
                                                        owner_title = owner_details_table_parse$owner_title[ind],
                                                        owner_address = owner_details_table_parse$owner_mail_address[ind],
                                                        owner_active_year = owner_details_table_parse$owner_active_year[ind]
                                                        )
                                 
                                 fin_owner_scrape = officer_business_bind(owner_fin,
                                                                          business_details_table_parse)
                               }
                               fin_owner_scrape
                               
                             }
        # print(depth)
        # print('fin')
        owners_fin$situs_pID <- situs_pID
        owners_fin$situs_address <- situs_address
        # print(owners_fin)
        if(sum(!finance_inds)>0){
          # print('2.2')
          owners_non_fin = data.frame(owner_name = owner_details_table_parse$owner_name,
                                      owner_title = owner_details_table_parse$owner_title,
                                      owner_address = owner_details_table_parse$owner_mail_address,
                                      owner_active_year = owner_details_table_parse$owner_active_year)[!finance_inds,]
          
          
          owners_non_fin =officer_business_bind(owners_non_fin,
                                                business_details_table_parse)
          # print(depth)
          # print('nonfin')
          owners_non_fin$situs_pID <- situs_pID
          owners_non_fin$situs_address <- situs_address
          # print(owners_non_fin)
          results = data.frame(rbind(owners_fin,
                                     owners_non_fin))
          # print(results)
        }
        else{
          # print('2.3')
          results = data.frame(owners_fin)
          # print(results)
        }
      }
      else{
        # print('3')
        # print(owner_details_table_parse)
        owner_table = data.frame(owner_name= owner_details_table_parse$owner_name,
                                 owner_title = owner_details_table_parse$owner_title,
                                 owner_address = owner_details_table_parse$owner_mail_address,
                                 owner_active_year = owner_details_table_parse$owner_active_year)
        # print(owner_table)
        # print(business_details_table_parse)
        results <- officer_business_bind(owner_table,
                                         business_details_table_parse)
        results$situs_pID <- situs_pID
        results$situs_address <- situs_address
        # print(results)
        
      }
      
    }
    else{
      # print('4')
      owner_table = data.frame(owner_name = owner_details_table_parse$owner_name,
                               owner_title = owner_details_table_parse$owner_title,
                               owner_address = owner_details_table_parse$owner_mail_address,
                               owner_active_year = owner_details_table_parse$owner_active_year)
      # print(owner_table)
      # print(business_details_table_parse)
      results <- officer_business_bind(owner_table,
                                       business_details_table_parse)
      results$situs_pID <- situs_pID
      results$situs_address <- situs_address
      # print(results)

      
    }
    
    
  }
  # print(depth)
  # print('out')
  # print(results)
  results$owner_address <- address_clean(results,
                                             'owner_address')
  results$corp_registered_agent_add  <- address_clean(results,
                                              'corp_registered_agent_add')
  results$owner_name <- address_clean(results,
                                      'owner_name')
  results$corp_registered_agent_name  <- address_clean(results,
                                                      'corp_registered_agent_name')
  # results$owner_mail_address <- address_clean(results,
  #                                            'owner_mail_address')
  results$situs_pID <- situs_pID
  results$situs_address <- situs_address
  # print(results)
  # print('results')
  # print(results)
  return(results)
}



colnames_used <- c('owner_name_scraped',
                   'owner_scraped_title',
                   'owner_address_scraped',
                   'owner_active_year',
                   'corp_business_name',
                   'corp_TTN',
                   'corp_mail_address',
                   'corp_right_to_transact_business_tx_status',
                   'corp_state_of_formation',
                   'corp_sos_registration_status',
                   'corp_effective_sos_registration_date',
                   'corp_tx_sos_file_num',
                   'corp_registered_agent_name',
                   'corp_registered_agent_mail_add',
                   # BOX-FIX: resolution status. The old 16-column schema could
                   # not tell "Texas genuinely has no franchise filing under this
                   # name" apart from "the registry never answered us". Both
                   # landed as byte-identical all-NA rows, which makes the
                   # headline "~41% of owners have no Texas registration"
                   # unverifiable -- every transient API refusal was silently
                   # counted as a real absence, so a UI built on it either
                   # overclaims or looks broken. The information already existed
                   # at runtime (the retry loop knows which keys resolved), it
                   # was simply never written down.
                   #
                   # Deliberately placed BEFORE situs_* so those two stay last.
                   'scrape_status',
                   'situs_pID',
                   'situs_address')

# BOX-FIX: the columns the CPA answer itself fills, in the order
# scrape_owner_api() returns them. scrape_status is derived by the pass loop
# rather than scraped, so it is NOT part of that shape. Keeping the two vectors
# separate is load-bearing: assigning the 17-name vector onto the 16-column
# answer would not error, it would shift every corp_* label by one and corrupt
# the table silently.
colnames_scraped <- setdiff(colnames_used, 'scrape_status')



#

# BOX-SPEED: owner-scrape concurrency. Was 12, chosen while we were spending the
# upstream owner's API quota and politeness was the binding constraint. He has since
# confirmed the key has no cost ceiling for this volume, so the only limit left is
# the API's rate ceiling. This is network-bound HTTP, so it can exceed core count
# freely.
SCRAPE_WORKERS <- 64
# BOX-SPEED: lowered from 128. The previous entry here argued that once a
# non-answer is recorded as `not_resolved` instead of a fake all-NA row,
# over-driving the API costs time instead of corrupting output, so the ceiling
# could be found empirically. That argument stands -- and it has now been
# spent. We ran the experiment and the answer is that the API is the ceiling,
# not us:
#
#    48 workers   ~700-870 rows/min
#   128 workers   ~894 rows/min
#
# 2.7x the workers bought under 30% throughput, and CPU sat at 140-220% of a
# possible 12800% (1.5-2 cores out of 128), so none of this is compute-bound --
# we are queued behind the CPA API's own latency and rate limits and more
# clients do not move that.
#
# Memory, meanwhile, scales linearly in workers, because each `multisession`
# worker is a persistent R process whose RSS only ever ratchets upward (R does
# not return freed memory to the OS). The 128-worker run measured:
#
#   ~10 min    71.8 GiB container RSS    79 GB host free
#   ~17 min   114.2 GiB                  37 GB
#   ~18 min   120.7 GiB                  30 GB
#   ~19 min   127.3 GiB                  23 GB
#
# ~6.5 GB/min of monotonic growth on a 188 GB host. It was minutes from OOM and
# was killed by hand. So a high worker count buys nothing and costs everything;
# 64 halves the ratchet rate for no measurable throughput loss. This is only the
# cheap half of the fix -- the structural half is SCRAPE_CHUNK below.

# BOX-FIX: how many owner keys to process before the worker pool is destroyed
# and rebuilt. This is the actual fix for the growth measured above.
#
# The leak is not a bug that can be found and deleted. R does not hand freed
# memory back to the OS, so a worker that processes thousands of owners in
# sequence simply ratchets its RSS up to its own high-water mark and holds it.
# The pool used to be established once, before the pass loop, and stay up for an
# entire pass (~95k keys), so nothing reclaimed any of it until the target
# finished. Killing the process is the only reclaim available, so the pass is
# now chunked and the pool is recycled between chunks.
#
# Sizing. The measured ~6.5 GB/min at ~894 rows/min is ~550 distinct owners/min
# (~95k owners expand to ~153k parcel rows), i.e. roughly 12 MB retained per
# owner processed. If the ratchet tracks owners processed rather than worker
# count -- and the near-flat CPU says it is the work that allocates, not the
# pool -- then CHUNK SIZE, not SCRAPE_WORKERS, is what sets the peak. 4000 keys
# x ~12 MB is a ~48 GB ceiling per chunk, about a quarter of the 188 GB host,
# leaving room for the UI container and the rest of the targets pipeline. It is
# also ~7 minutes of work per chunk at the measured rate, so the few seconds
# spent spinning up 64 PSOCK nodes is under 2% overhead. Each chunk logs RSS
# before the pool comes up and after it is torn down: turn this DOWN if that
# number still climbs into the tens of GB, and up only once the log shows the
# post-teardown floor staying flat across chunks.
SCRAPE_CHUNK <- 1000
# BOX-SPEED: was 4000. A single 4000-key chunk never finished: 90.3 GiB of RSS
# at 14 minutes with only ~70% of the chunk done, i.e. ~30 MB retained per owner
# processed, so peak memory is set by keys-per-chunk and 4000 overshot by 2x.
# 1000 projects to ~30 GiB of pool over a ~10 GiB baseline. It should also be
# FASTER, not just safer: observed throughput was under 285 owners/min at 64
# workers versus 353 owners/min at 16 clients in a short-lived probe, while HTTP
# measured 0.26 s/call with zero throttling and CPU sat at 125-150%. That gap is
# GC pressure from ~1.4 GiB worker heaps, and small chunks keep heaps small.
# More pool restarts (91 vs 23) is a cheap trade now that parcel_groups is no
# longer in the workers' export set.

# BOX-SPEED: how many times to ask about an owner that did not resolve. The CPA API
# is finicky (owner's words): the same property can return a response or no response
# depending on when you ask, so a no-result is not a durable fact and must not be
# cached. Two passes, because ~41% of owners genuinely have no franchise record and
# each extra pass re-asks ~39k questions that will keep answering "no" -- a third
# pass would cost more lookups than skipping the dedup entirely.
SCRAPE_RETRY_PASSES <- 4
# BOX-SPEED: raised from 2, which is only affordable because passes 3+ re-ask
# ONLY the keys the API never answered (see the pass loop). Every pass used to
# re-ask everything still pending, including the ~39k owners that genuinely
# have no filing, so a third pass cost ~39k lookups to recover almost nothing.
# Passes 1 and 2 still ask everything, which keeps the repo owner's point that
# even a count==0 answer can be transient.

# BOX-FIX: top level ON PURPOSE. This used to be defined inside
# owner_scrape_actual(), where its enclosing environment was that call frame --
# which holds the full target_properties. future then measured the closure at
# 1.00 GiB and aborted the whole target with a future.globals.maxSize error
# before dispatching a single worker. Keep this at file scope.
#
# Expand one owner's answer out to every parcel that owner holds. A lookup can
# legitimately return several rows (one per officer), so this crosses
# answer-rows by parcels.
expand_to_parcels <- function(answer, parcels) {
  do.call(rbind, lapply(seq_len(nrow(parcels)), function(p) {
    row <- answer
    row$situs_pID     <- parcels$situs_pID[p]
    row$situs_address <- parcels$situs_address[p]
    row
  }))
}

# BOX-FIX: report memory so a run's own log answers "is the ratchet bounded?"
# instead of requiring someone to sit watching `docker stats` for 20 minutes --
# which is how the 128-worker blowup was caught, by hand, minutes from an OOM.
#
# Sums resident pages over every process in this PID namespace, which inside the
# container is the main R session plus whatever worker pool is currently up, so
# the number is directly comparable to the `docker stats` figures quoted at
# SCRAPE_WORKERS. It over-counts pages shared between processes (cgroup
# accounting charges those once), but `multisession` workers are spawned PSOCK
# nodes rather than forks, so the overlap is small. Linux-only by construction;
# returns NA anywhere /proc is absent, and never throws -- a logging helper must
# not be able to kill a six-hour scrape.
container_rss_gb <- function() {
  tryCatch({
    pids <- list.files('/proc', pattern = '^[0-9]+$')
    pages <- vapply(pids, function(p) {
      v <- tryCatch(scan(file.path('/proc', p, 'statm'), what = 0, n = 2,
                         quiet = TRUE),
                    error = function(e) numeric(0),
                    warning = function(e) numeric(0))
      if (length(v) < 2) 0 else v[2]
    }, numeric(1), USE.NAMES = FALSE)
    round(sum(pages) * 4096 / 1024^3, 1)
  }, error = function(e) NA_real_)
}

# BOX-FIX: fold per-worker scrape part files into owner_data_total.csv.
#
# The scrape loops used to have every parallel worker append to one shared
# owner_data_total.csv, which races. Workers now each append to their own
# owner_data_part_<pid>.csv and this collapses them afterwards.
#
# Called at owner_scrape_actual() entry as well as after the loop, so parts left
# by a crashed run are recovered into owner_data_total.csv before the resume
# check reads it -- otherwise a crash would silently re-scrape everything.
consolidate_owner_parts = function() {
  parts <- list.files(pattern = '^owner_data_part_[0-9]+[.]csv$')
  if (!length(parts)) {
    return(invisible(FALSE))
  }
  read_one <- function(p) {
    tryCatch(data.table::fread(p, colClasses = 'character', showProgress = FALSE),
             error = function(e) NULL)
  }
  chunks <- Filter(Negate(is.null), lapply(parts, read_one))
  chunks <- Filter(function(d) nrow(d) > 0, chunks)
  if (!length(chunks)) {
    file.remove(parts)
    return(invisible(FALSE))
  }
  combined <- data.table::rbindlist(chunks, use.names = TRUE, fill = TRUE)
  if (file.exists('owner_data_total.csv')) {
    prev <- read_one('owner_data_total.csv')
    if (!is.null(prev) && nrow(prev)) {
      combined <- data.table::rbindlist(list(prev, combined),
                                        use.names = TRUE, fill = TRUE)
    }
  }
  # A property can legitimately be attempted twice across a crash/resume; keep
  # the last write for each parcel.
  combined <- unique(combined, by = c('situs_pID', 'situs_address'),
                     fromLast = TRUE)
  data.table::fwrite(combined, 'owner_data_total.csv', sep = ',')
  file.remove(parts)
  message('[consolidate_owner_parts] folded ', length(parts), ' part file(s) -> ',
          nrow(combined), ' rows')
  invisible(TRUE)
}

owner_scrape_actual = function(austin_parcel_data_merged
                              ){
  # BOX-SPEED: stop paying 30 seconds of exponential backoff for an answer the
  # API already gave us definitively. This is THE cost of the scrape.
  #
  # The previous entry here assumed the retries were absorbing rate-limit
  # rejections at SCRAPE_WORKERS clients. Measured over 360 real owner lookups
  # against the live registry, that assumption is simply false:
  #
  #   HTTP 429 seen              : 0
  #   HTTP 503 seen              : 0
  #   Retry-After header seen    : 0
  #   non-200 responses          : 60 x HTTP 400, 5 x HTTP 413
  #
  # We are not being throttled. We are retrying HTTP 400 Bad Request -- which
  # is deterministic, because the request is malformed, so asking again with a
  # byte-identical request cannot produce a different answer. And it never did:
  # of the 39 owners that errored across both probes, every single one burned
  # all 5 attempts and still ended as an error. Retries rescued ZERO of them.
  #
  # What that costs, from the 200-owner sequential probe:
  #
  #   200 owners, 808.5 s total, mean 4.04 s/owner, MEDIAN 0.79 s/owner
  #     81 matched   : mean 0.96 s
  #    100 no_record : mean 0.78 s
  #     19 error     : mean 34.33 s   <-- 652 s, i.e. 80.7% of all wall time
  #   time split     : 29.1% HTTP, 70.9% NOT HTTP
  #   non-HTTP time, owners that never retried : 0.005 s/owner (R work is free)
  #   non-HTTP time, owners that retried       : 30.09 s/owner (pure Sys.sleep)
  #
  # So ~70% of this scrape is 64 workers sleeping, in lockstep 2-4-8-16 second
  # steps, waiting to re-ask a question that was already answered "no". It is
  # also the straggler generator: foreach hands each worker a static slice, so
  # one worker that draws several 30-second owners holds up the whole chunk.
  #
  # The fix classifies the failure instead of blanket-retrying it. See
  # make_insist_scrape_owner() / cpa_error_is_transient(): a genuinely transient
  # failure (429, 408, 425, 5xx, transport error) still gets the ORIGINAL
  # backoff, unchanged, so if Texas ever does start throttling us the old
  # behaviour is what happens. A deterministic 4xx fails fast. An error we
  # cannot classify -- one raised in our own parsing code rather than by the
  # transport -- keeps retrying, but on a cheap 1-2 second ladder instead of a
  # 2-4-8-16 one, because the API being finicky is exactly how those arise and
  # we do not want to stop asking.
  #
  # Every path still ends in the same label: an owner that ultimately errors is
  # returned as an error, which the worker records as `not_resolved` and the
  # pass loop re-asks, exactly as before. Nothing about the matched / no_record
  # / not_resolved distinction or the tiered passes moves.
  #
  # Built by a top-level factory rather than inline, on purpose. A plain
  # `function(...)` defined here would close over owner_scrape_actual()'s
  # frame, and future would then serialise austin_parcel_data_merged (~128 MB)
  # to every worker on every chunk -- the accidental frame capture that crashed
  # this run once already. The factory's frame holds three small closures and
  # nothing else.
  insist_scrape_owner = make_insist_scrape_owner()
  # BOX-FIX: recover any part files from a previous interrupted run before the
  # size check below decides between the resume and fresh-start branches.
  consolidate_owner_parts()
  if(is.na(file.size('owner_data_total.csv'))|
     file.size('owner_data_total.csv')<40000000){
    # print('1')
    
    target_properties = dplyr::filter(austin_parcel_data_merged,
                                      ((is_financialized ==TRUE)&
                                         (is_owner_occupied==FALSE))|
                                        (property_units>5),
                                      property_units!=0)
    # Resume: drop parcels already recorded. consolidate_owner_parts() above has
    # already folded in part files from an interrupted run, so this sees them.
    if(!is.na(file.size('owner_data_total.csv'))){
      target_owner_info <- read.csv('owner_data_total.csv')
      target_properties <- dplyr::filter(target_properties,
                                         (as.numeric(situs_pID) %in%
                                            as.numeric(unique(target_owner_info$situs_pID))==FALSE ))
      print(dim(target_properties))
    }

    if(nrow(target_properties) > 0){
      # BOX-SPEED: one lookup per distinct owner, expanded back out to that owner's
      # parcels inside the worker. (owner_name, owner_address) is the complete and
      # correct key -- see this file's patch header for why.
      #
      # Slimmed to the four columns the loop needs before fanning out: the full
      # frame is 21 columns and each of SCRAPE_WORKERS sessions gets its own copy.
      tp_slim <- target_properties[, c('owner_name', 'owner_address',
                                       'situs_pID', 'situs_address')]
      owner_keys <- unique(tp_slim[, c('owner_name', 'owner_address')])

      # BOX-FIX: group each owner's parcels ONCE here on the main session, instead
      # of having the worker rescan all of tp_slim per key (~95.8k linear scans
      # over ~153k rows). parcel_groups[[i]] is owner_keys row i's parcels, so the
      # worker does an O(1) list lookup. Built on a factor whose levels ARE the
      # owner_keys order, which is what keeps the two aligned.
      key_sep <- '\u0001'   # cannot occur in a name or address
      key_of_row <- paste(tp_slim$owner_name, tp_slim$owner_address, sep = key_sep)
      key_levels <- paste(owner_keys$owner_name, owner_keys$owner_address,
                          sep = key_sep)
      parcel_groups <- split(tp_slim[, c('situs_pID', 'situs_address')],
                             factor(key_of_row, levels = key_levels))
      stopifnot(length(parcel_groups) == nrow(owner_keys))

      doFuture::registerDoFuture()

      # BOX-FIX: never ask for more workers than this process can open sockets
      # for. Each multisession worker costs one connection, and R's ceiling is
      # per process -- the container raises it for the main session only, so a
      # target running anywhere else silently has far less headroom. Asking for
      # too many is a hard startup failure, not a slowdown, which is how this
      # scrape died once already. Clamp instead, and say so in the log: a wrong
      # SCRAPE_WORKERS should cost throughput, never the whole run.
      scrape_workers_used <- SCRAPE_WORKERS
      .free_conn <- tryCatch(parallelly::freeConnections(),
                             error = function(e) NA_integer_)
      if (!is.na(.free_conn)) {
        # Leave a margin: fwrite, the API client and targets itself all need
        # connections of their own while the pool is up.
        scrape_workers_used <- max(1L, min(SCRAPE_WORKERS, .free_conn - 12L))
      }
      if (scrape_workers_used < SCRAPE_WORKERS) {
        message('[owner_scrape] clamped workers ', SCRAPE_WORKERS, ' -> ',
                scrape_workers_used, ' (', .free_conn,
                ' R connections free in this process)')
      }

      # BOX-FIX: the pool is NOT established here any more. It is established
      # and destroyed once per chunk inside the pass loop below, because that
      # teardown is the only thing that ever reclaims worker RSS -- see
      # SCRAPE_CHUNK. This on.exit stays purely as a safety net, so that an
      # error anywhere below cannot leave 64 orphaned R processes sitting on the
      # host's memory.
      on.exit(future::plan(future::sequential), add = TRUE)

      # BOX-FIX: future's cautious 500 MiB globals ceiling is too low for this
      # loop's exported set, and re-planning per chunk re-exports that set on
      # every chunk, so the headroom is wanted more than once. Raise it for this
      # call only. This is a deliberate export, not the accidental frame capture
      # that crashed the run.
      #
      # parcel_groups (~34 MiB) and owner_keys are deliberately NOT in that set
      # any more. Exporting them wholesale would have cost 34 MiB x workers on
      # EVERY chunk; the chunk loop below instead zips each key's parcels and its
      # owner name/address in as foreach ITERATION values, so a worker receives
      # only the rows it will actually process.
      .old_max <- getOption('future.globals.maxSize')
      options(future.globals.maxSize = 2 * 1024^3)
      on.exit(options(future.globals.maxSize = .old_max), add = TRUE)

      message('[owner_scrape] ', nrow(tp_slim), ' parcels -> ',
              nrow(owner_keys), ' distinct owners, ', scrape_workers_used,
              ' workers')

      # Keys still to resolve. A key leaves this set only by producing a real
      # answer; an error or a NULL leaves it pending for the next pass, because a
      # no-response from this API is not durable.
      pending <- seq_len(nrow(owner_keys))

      # BOX-FIX: the sidecar has to report how many times we actually asked,
      # which is not always SCRAPE_RETRY_PASSES -- this loop breaks early once
      # nothing is pending, and "unknown after 1 pass" is a weaker claim than
      # "unknown after 2".
      passes_used <- 0L
      for (pass in seq_len(SCRAPE_RETRY_PASSES)) {
        if (!length(pending)) break
        passes_used <- pass

        # BOX-SPEED: from pass 3 on, only re-ask keys the API never answered.
        # `no_record` means the registry replied that nothing is filed under
        # the name, three times, across three spellings. Passes 1 and 2 re-ask
        # those anyway because the repo owner warned a no-response can be
        # transient -- but re-asking ~39k genuine no_records on every further
        # pass would cost far more than it recovers. `not_resolved` is the set
        # worth hammering, and higher concurrency is what produces it.
        held <- pending[0]
        ask_now <- pending
        if (pass >= 3) {
          held    <- pending[names(pending) == 'no_record']
          ask_now <- pending[names(pending) != 'no_record']
        }
        if (!length(ask_now)) break

        message('[owner_scrape] pass ', pass, '/', SCRAPE_RETRY_PASSES,
                ': ', length(ask_now), ' owners to look up',
                if (length(held)) paste0(' (holding ', length(held),
                                         ' no_record)') else '')

        # BOX-FIX: chunk the pass and recycle the worker pool between chunks.
        #
        # This was one foreach over the whole of `ask_now`, with the
        # `multisession` pool established once before the pass loop and torn
        # down only by on.exit when the target finished. That is what produced
        # the monotonic ~6.5 GB/min growth quoted at SCRAPE_WORKERS -- 127.3 GiB
        # of container RSS at 19 minutes with 23 GB of host memory left, killed
        # by hand. Each persistent worker's RSS ratchets to its own high-water
        # mark and R never gives it back, so 128 workers each grinding through
        # ~750 owners in sequence multiplied one leak by 128 with nothing in the
        # pass ever reclaiming any of it.
        #
        # Process death is the only reclaim mechanism R offers here, so: plan,
        # run a bounded slice, plan(sequential) to stop every node, gc(), repeat.
        # Verified in this container (future 1.70.0) that plan(sequential)
        # genuinely stops the PSOCK nodes -- the worker process count and
        # parallelly::freeConnections() both return to their pre-plan values, and
        # the next plan() call yields fresh PIDs. That last part is the whole
        # fix: if future merely re-used a cached pool, this would be a no-op.
        #
        # API behaviour is deliberately untouched. Chunking changes only WHICH
        # process asks. Every key in `ask_now` is still asked exactly once per
        # pass, in the same order, through the same insistently() backoff, and
        # the tiered pass logic above still decides what is in `ask_now` at all.
        chunk_starts <- seq(1L, length(ask_now), by = SCRAPE_CHUNK)
        n_chunks <- length(chunk_starts)
        # Filled with [<- and list(), NEVER [[<-: assigning NULL via [[<- would
        # DELETE the element and shift every later chunk into the wrong slot. A
        # chunk whose every key resolved legitimately returns NULL.
        still_pending_parts <- vector('list', n_chunks)

        for (chunk_i in seq_len(n_chunks)) {
          chunk_lo <- chunk_starts[chunk_i]
          chunk_hi <- min(chunk_lo + SCRAPE_CHUNK - 1L, length(ask_now))
          # `[` keeps the names, and the names are load-bearing: they carry each
          # key's matched / no_record / not_resolved label, which the tiered
          # pass filter and the post-loop sweep both read back.
          chunk_keys <- ask_now[chunk_lo:chunk_hi]

          # BOX-SPEED: ship the slice, not the whole world. Re-planning per
          # chunk re-exports every detected global to every worker, and
          # parcel_groups is a ~34 MiB list, so leaving the worker to index into
          # it would cost 34 MiB x workers on every single chunk. Passing the
          # parcels as a foreach ITERATION value instead means foreach hands each
          # worker only the elements it will actually process.
          #
          # And there is no index remap to get wrong, because the worker no
          # longer indexes anything at all: parcels, owner_name and owner_address
          # arrive already paired with their own key_index. These three vectors
          # are built by the SAME global index in the same order, so element j of
          # each is owner_keys row chunk_keys[j] by construction. key_index
          # itself stays GLOBAL, so `pending`, the unresolved sweep and
          # owner_scrape_unresolved.csv keep indexing owner_keys and
          # parcel_groups exactly as they did before.
          chunk_parcels     <- parcel_groups[unname(chunk_keys)]
          chunk_owner_names <- owner_keys$owner_name[unname(chunk_keys)]
          chunk_owner_addrs <- owner_keys$owner_address[unname(chunk_keys)]
          stopifnot(length(chunk_parcels) == length(chunk_keys),
                    length(chunk_owner_names) == length(chunk_keys),
                    length(chunk_owner_addrs) == length(chunk_keys))

          message('[owner_scrape] pass ', pass, ' chunk ', chunk_i, '/',
                  n_chunks, ': ', length(chunk_keys), ' owners to look up, ',
                  scrape_workers_used, ' fresh workers (RSS ',
                  container_rss_gb(), ' GiB before pool)')

          future::plan(future::multisession, workers = scrape_workers_used)

          chunk_result <- foreach(key_index = chunk_keys,
                                  owner_name = chunk_owner_names,
                                  owner_address = chunk_owner_addrs,
                                  parcels = chunk_parcels,
                                  .combine = 'c',
                                  .options.RNG = 8989,
                                  .export = financial_marker_string) %dopar% {
            # BOX-FIX: pending carries each key's status in its names, so an
            # iterated element arrives named. Strip it here rather than trust
            # that `[[` on a named integer stays positional.
            #
            # owner_name, owner_address and parcels used to be looked up here out
            # of the exported owner_keys / parcel_groups. They are now zipped in
            # as iteration values (see above), which is both cheaper per chunk
            # and impossible to misalign. situs_* are still pass-through labels
            # for the lookup, so the first parcel's are fine; every parcel gets
            # its own stamped copy in expand_to_parcels().
            key_index <- unname(key_index)
            answer <- tryCatch({
              insist_scrape_owner(owner_name,
                                  situs_pID = parcels$situs_pID[1],
                                  situs_address = parcels$situs_address[1],
                                  veneer_owner = owner_name,
                                  veneer_owner_mail_address = owner_address)
            }, error = function(cond){ cond })

            # BOX-FIX: NULL and an error condition are NOT the same outcome, and
            # this branch used to collapse them. cpa_api_request() ends in
            # httr2::req_perform(), which throws on any transport failure or
            # non-2xx status, so every genuine failure -- including the rate-limit
            # rejections the backoff above exists for -- arrives here as a
            # condition object. A bare NULL can only come from one place:
            # scrape_owner_api()'s name-search loop giving up after three
            # SUCCESSFUL responses that each reported count == 0. That is the
            # registry telling us nothing is filed under the name, which is a
            # finding, not a failure.
            #
            # The distinction rides back on the NAME of the returned index;
            # `.combine = 'c'` preserves names, so the sweep below can read it.
            # Control flow is deliberately unchanged -- a no_record key is still
            # re-asked next pass, because the API owner's warning that a no-result
            # is not durable applies to count == 0 as much as to a refusal. Only
            # the label is new, and it is always the label from the LAST pass that
            # asked, so an eventual answer overwrites an earlier no.
            if ('error' %in% class(answer)) {
              return(stats::setNames(key_index, 'not_resolved'))
            }
            if (is.null(answer)) {
              return(stats::setNames(key_index, 'no_record'))
            }

            # BOX-FIX: everything from here down is width-sensitive, and it used to
            # sit OUTSIDE the tryCatch above -- which only ever guarded the API
            # call. `colnames(answer) <- colnames_scraped` errors outright when
            # ncol(answer) != 16 ("'names' attribute [16] must be the same length
            # as the vector [7]"), and an error raised in a %dopar% body that no
            # handler catches propagates out of foreach and terminates the ENTIRE
            # loop. One malformed owner would therefore abandon all ~95.8k
            # lookups. That is the wrong blast radius no matter what causes the
            # malformed answer -- the argument bug fixed above in scrape_owner_api()
            # is one cause, but any future shape drift in the recursive rbind paths
            # would do the same -- so the guard belongs here regardless.
            #
            # The width is now checked explicitly and loudly rather than being
            # discovered by an assignment failing. Under doFuture/future the
            # worker's message() conditions are relayed to the main session when
            # the future is collected, so this lands in the run log with the owner
            # name attached instead of vanishing in a worker.
            #
            # The downgrade is to `not_resolved`, never `no_record`: no_record is a
            # positive finding ("Texas has nothing filed under this name") and must
            # not be allowed to absorb our own processing failures. The key is
            # returned still-pending, exactly as an API error would be, so the
            # two-pass retry semantics are untouched -- it is re-asked next pass and
            # takes the label of the last pass that asked. No extra API calls
            # happen: this runs strictly after the single lookup has returned.
            write_failure <- tryCatch({
              if (ncol(answer) != length(colnames_scraped)) {
                stop(sprintf(
                  'answer for owner "%s" has %d columns, expected %d -- refusing to label it; downgrading this owner to not_resolved. columns seen: %s',
                  owner_name,
                  ncol(answer),
                  length(colnames_scraped),
                  paste(colnames(answer), collapse = ', ')))
              }
              colnames(answer) <- colnames_scraped
              answer$scrape_status <- 'matched'
              # Reorder to the canonical schema so every part file appends with an
              # identical header; fwrite(append = TRUE) does not reconcile columns.
              answer <- answer[, colnames_used, drop = FALSE]
              .pf <- sprintf('owner_data_part_%d.csv', Sys.getpid())
              data.table::fwrite(expand_to_parcels(answer, parcels),
                                 .pf,
                                 append = file.exists(.pf),
                                 sep = ',')
              NULL
            }, error = function(cond){ cond })

            if (!is.null(write_failure)) {
              message('[owner_scrape] ', conditionMessage(write_failure))
              return(stats::setNames(key_index, 'not_resolved'))
            }
            return(NULL)
          }

          # Destroy every worker process. THIS is the reclaim -- gc() inside a
          # living worker would free R objects without returning the pages, which
          # is exactly why the unchunked version grew without bound.
          future::plan(future::sequential)
          gc()

          still_pending_parts[chunk_i] <- list(chunk_result)
          message('[owner_scrape] pass ', pass, ' chunk ', chunk_i, '/',
                  n_chunks, ': resolved ',
                  length(chunk_keys) - length(chunk_result),
                  ', still pending ', length(chunk_result),
                  ' (RSS ', container_rss_gb(), ' GiB after teardown)')
        }

        # Recombine to exactly what the single unchunked foreach produced.
        # unlist() over an UNNAMED list of named integer vectors concatenates the
        # inner names unprefixed and in order, so this is name-for-name and
        # order-for-order what `.combine = 'c'` over all of ask_now would have
        # returned. NULL chunks contribute nothing, as they should.
        still_pending <- unlist(still_pending_parts, use.names = TRUE)
        # unlist() of an all-NULL list returns NULL and of an empty list returns
        # list(); c(held, list()) would silently turn `pending` into a LIST and
        # corrupt every downstream index. ask_now[0] is a zero-length integer
        # with the right names attribute, which c() absorbs harmlessly.
        if (!length(still_pending)) {
          still_pending <- ask_now[0]
        }

        resolved_this_pass <- length(ask_now) - length(still_pending)
        message('[owner_scrape] pass ', pass, ': resolved ', resolved_this_pass,
                ', still pending ', length(still_pending))
        # Keys held back this pass keep their existing label and stay pending,
        # so the sweep still records them and the counts stay complete.
        pending <- c(held, still_pending)
      }

      # Whatever never resolved is recorded as empty, same as the original
      # behaviour -- but only after being asked SCRAPE_RETRY_PASSES times rather
      # than once, so a transient refusal is not mistaken for "no such entity".
      #
      # BOX-FIX: these empty rows now say WHY they are empty. `no_record` is the
      # registry answering that nothing is filed under the name; `not_resolved`
      # is the API never giving a usable answer across all passes. Anything that
      # quotes a "share of owners with no Texas registration" must exclude
      # not_resolved from the denominator, which was impossible before.
      sweep_status <- names(pending)
      if (is.null(sweep_status)) {
        sweep_status <- rep(NA_character_, length(pending))
      }
      # A missing label can only come from a code path that predates the
      # tagging. Call that unknown; never let it become a finding by default.
      sweep_status[is.na(sweep_status) | sweep_status == ''] <- 'not_resolved'
      if (length(pending)) {
        message('[owner_scrape] writing ', length(pending),
                ' unresolved owners as empty rows')
        # BOX-FIX: built from length(colnames_used) rather than the old
        # `t(c(rep(NA, 14), NA, NA))`, which hardcoded the column count. That
        # form had already drifted once (14 + 2 spelled out separately) and
        # would now emit a 16-wide row against a 17-wide schema.
        empty <- data.frame(matrix(NA,
                                   nrow = 1,
                                   ncol = length(colnames_used)))
        colnames(empty) <- colnames_used
        stopifnot(ncol(empty) == length(colnames_used))
        .pf <- sprintf('owner_data_part_%d.csv', Sys.getpid())
        for (i in seq_along(pending)) {
          key_index <- unname(pending[i])
          parcels <- parcel_groups[[key_index]]
          empty$scrape_status <- sweep_status[i]
          data.table::fwrite(expand_to_parcels(empty, parcels),
                             .pf,
                             append = file.exists(.pf),
                             sep = ',')
        }

        # BOX-FIX: sidecar audit trail. owner_data_total.csv is joined and
        # reshaped downstream and is keyed by parcel, so the unresolved set is
        # far easier to trust when it also exists standalone and keyed by owner.
        # Nothing reads this file -- it exists so a human can check the split by
        # hand, independently of the main table.
        unresolved_log <- data.frame(
          owner_name       = owner_keys$owner_name[unname(pending)],
          owner_address    = owner_keys$owner_address[unname(pending)],
          scrape_status    = sweep_status,
          passes_attempted = passes_used,
          n_parcels        = vapply(unname(pending),
                                    function(k) nrow(parcel_groups[[k]]),
                                    integer(1)),
          stringsAsFactors = FALSE)
        data.table::fwrite(unresolved_log, 'owner_scrape_unresolved.csv',
                           sep = ',')
      }

      # BOX-FIX: state the split in the log. The whole point of the change is
      # that this ratio is now knowable, so the run should assert it rather than
      # make the next person re-read 150k rows to find out.
      n_matched      <- nrow(owner_keys) - length(pending)
      n_no_record    <- sum(sweep_status == 'no_record')
      n_not_resolved <- sum(sweep_status == 'not_resolved')
      message('[owner_scrape] status split over ', nrow(owner_keys),
              ' owner keys after ', passes_used, ' pass(es): matched ',
              n_matched, ', no_record ', n_no_record,
              ', not_resolved ', n_not_resolved)
    }
  }
  print('done')
  # BOX-FIX: collapse this run's per-worker part files before reading the total.
  consolidate_owner_parts()
  target_owner_info <- read.csv('owner_data_total.csv')
  
  # austin_parcel_data_merged <- qs2::qs_read("_targets\\objects\\austin_parcel_data_merged_code")
    # mutate(situs_pID = as.numeric(situs_pID))
  # print('2')
  # austin_parcel_data_merged <- austin_parcel_data_merged %>%
  #   mutate(situs_pID=as.integer(situs_pID))
  austin_parcel_data_merged <- dplyr::left_join(austin_parcel_data_merged,
                                                  target_owner_info,
                                                  by = c('situs_pID',
                                                         'situs_address'
                                                         ))
  
  austin_parcel_data_merged$owner_address_scraped <- address_clean(austin_parcel_data_merged,
                                                                            'owner_address_scraped')
  austin_parcel_data_merged$corp_registered_agent_mail_add <- address_clean(austin_parcel_data_merged,
                                                                            'corp_registered_agent_mail_add')
  austin_parcel_data_merged$corp_mail_address <- address_clean(austin_parcel_data_merged,
                                                               'corp_mail_address')
  austin_parcel_data_merged$owner_address <- address_clean(austin_parcel_data_merged,
                                                           'owner_address')
  # print('3')
  # write.csv(austin_parcel_data_merged,
  #           'austin_parcel_data_merged.csv'
  #           )
  
    # print(Sys.time())
    austin_parcel_data_merged
  }



deed_summ_data_gen = function(deeds_data){
  print(tail(deeds_data))
  print(dim(py_to_r(deeds_data)))
  deed_summ <- py_to_r(deeds_data) %>%
    # dplyr::filter(deeds_pID %in% residential_pIDs) %>%
    group_by(deeds_pID,
             deeds_year) %>%
    summarise(recent_purchase_date = max(deeds_deedDt,
                                         na.rm = TRUE))
 
  # print('e')
  # print(head(deed_summ))
  # print(dim(deed_summ))
  deed_summ <- as.data.frame(deed_summ)
  write.csv(deed_summ,
            'deed_summ_total.csv')
  deed_summ
}




scrape_owner = function(owner_name,
                        situs_pID,
                        situs_address,
                        veneer_owner,
                        veneer_owner_mail_address,
                        depth = 0,
                        owner_title = NA,
                        owner_mail_address = NA,
                        business_details_table = NA){
  session <- selenider_session(session = "selenium",
                               browser = 'chrome',
                               # options = chromote_options(headless = FALSE),
                               driver = list(selenium_server(version = '4.29.0',
                                                             interactive = FALSE,
                                                             path = getwd(), temp = TRUE),
                                             SeleniumSession$new(browser = 'chrome',
                                                                 capabilities = selenium::chrome_options(args=c('--headless',
                                                                                                                '--no-sandbox',
                                                                                                                '--disable-extensions',
                                                                                                                '--disable-browser-side-navigation',
                                                                                                                '--disable-dev-shm-usage',
                                                                                                                "--disable-gpu",
                                                                                                                "--proxy-server='direct://'",
                                                                                                                '--proxy-bypass-list=*')),
                                                                 timeout = 30)),
                               timeout = 30
  )
  # Go to global website
  
  base_url = 'https://comptroller.texas.gov/'
  open_url("https://mycpa.cpa.state.tx.us/coa/")
  entity_name_search_box <- s("#name")
  search_button <- s("#submitBtn")
  
  type_name(entity_name_search_box,
            owner_name,
            try = 1)
  wait_to_click(search_button)
  # message('1')
  search_result <-  s('#resultTable') %>%
    elem_expect(is_present, is_visible, 
                timeout = 10)  %>% 
    elem_text()
  # message('2')
  if(search_result %in% c("","No data available in table")){
    type_name(entity_name_search_box,
              owner_name,
              try = 2)
    wait_to_click(search_button)
    search_result <-  s('#resultTable') %>% 
      elem_expect(is_present, is_visible, 
                  timeout = 10) %>%
      elem_text()
    
  }
  if(search_result %in% c("","No data available in table")){
    type_name(entity_name_search_box,
              owner_name,
              try = 3)
    wait_to_click(search_button)
    Sys.sleep(1)
    search_result <-  s('#resultTable') %>%
      elem_expect(is_present, is_visible, 
                  timeout = 10) %>%
      elem_text()
  }
  if(search_result %in% c("","No data available in table")){
    return(NULL)
  }
  link_addend <- s('#resultTable') %>% 
    find_element('a') %>%
    elem_attr('href')
  
  open_url(paste(base_url,
                 link_addend,
                 sep = ''))
  
  business_string <- s('#content') %>% 
    elem_expect(is_present, is_visible, 
                timeout = 60) %>%
    elem_text()
  
  
  business_details_table_parse = get_business_details(business_string)
  
  #officer button appears after search
  owner_titles <- lapply(s('#table') %>% 
                           find_element('tbody') %>%
                           find_elements('th'),
                         elem_text)
  owners <- lapply(s('#table') %>% 
                     find_element('tbody') %>%
                     find_elements('.left-align'),
                   elem_text)
  # print(business_details_table_parse)
  if(length(owners)==0){
    # print('1.0')
    #no results on a recursive owner search
    if(depth>0 ){
      
      # print('1.1')
      owner_table = data.frame(owner_name_scraped = owner_name,
                               owner_title = owner_title,
                               owner_mail_address = owner_address)
      
      results = officer_business_bind(owner_table,
                                      business_details_table_parse)
      
    }
    #no results on base owner search
    if(depth==0 ){
      # print('1.2')
      owner_table = data.frame(owner_name_scraped = NA,
                               owner_title = NA,
                               owner_mail_address = NA)
      
      results = officer_business_bind(owner_table,
                                      business_details_table_parse)
      
    }
    
  }
  #found results
  else{
    # print('officer')
    owner_name_parse <- unlist(lapply(owners, 
                                      function(owner){
                                        regmatches(owner,
                                                   regexpr("(?<=^).*(?=\\n)",
                                                           owner, 
                                                           perl = TRUE))
                                      }) )
    owner_title_parse <- unlist(owner_titles)
    owner_address_parse <-unlist(lapply(owners, 
                                        function(owner){
                                          paste(strsplit(owner,
                                                         split = '\\n')[[1]][2:3],
                                                collapse = ', ')
                                          
                                        }) )
    # selenider::close_session(session)
    finance_inds <- grepl(financial_marker_string,
                          owner_name_parse)
    #if owner has financial markers, do a recursive search on it
    if(sum(finance_inds)>0){
      # print('2.1')
      owners_fin = foreach(ind = which(finance_inds),
                           .combine = 'rbind') %do% {
                             fin_owner_scrape = tryCatch({
                               scrape_owner( owner_name_parse[ind],
                                             situs_pID = situs_pID , 
                                             situs_address = situs_address,
                                             veneer_owner = veneer_owner,
                                             veneer_owner_mail_address = veneer_owner_mail_address,
                                             depth = depth+1,
                                             owner_title = owner_title_parse[ind],
                                             owner_mail_address = owner_address_parse[ind],
                                             business_details_table = business_details_table_parse )
                             },error = function(cond){
                               cond
                             })
                             
                             if('error' %in% class(fin_owner_scrape)){
                               # print('error')
                               owner_fin = data.frame(owner_name = owner_name_parse[ind],
                                                      owner_title = owner_title_parse[ind],
                                                      owner_address = owner_address_parse[ind])
                               
                               fin_owner_scrape = officer_business_bind(owner_fin,
                                                                        business_details_table_parse)
                             }
                             fin_owner_scrape
                             
                           }
      
      if(sum(!finance_inds)>0){
        # print('2.2')
        owners_non_fin = data.frame(owner_name = owner_name_parse,
                                    owner_title = owner_title_parse,
                                    owner_address = owner_address_parse)[!finance_inds,]
        
        
        owners_non_fin =officer_business_bind(owners_non_fin,
                                              business_details_table_parse)
        
        results = data.frame(rbind(owners_fin,
                                   owners_non_fin))
      }
      else{
        # print('2.3')
        results = data.frame(owners_fin)
      }
    }
    else{
      # print('3')
      owner_table = data.frame(owner_name = owner_name_parse,
                               owner_title = owner_title_parse,
                               owner_mail_address = owner_address_parse)
      
      results <- officer_business_bind(owner_table,
                                       business_details_table_parse)
      
    }
  }
  
  
  results$situs_pID <- situs_pID
  results$depth <- depth
  # print(results)
  return(results)
}


