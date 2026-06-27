
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

parse_hays_cad_data = function(dir =  "AUSTIN–SAN ANTONIO METROPLEX (13 of 13)\\HAYS COUNTY APPRAISAL DISTRICT+SR(done)",
                               zipfile = "AUSTIN–SAN ANTONIO METROPLEX (13 of 13).zip",
                               county = "HAYS COUNTY APPRAISAL DISTRICT+SR(done)",
                               year_used = 2025){
  
  folder <- file.path( tempdir() , gsub('[[:punct:]]','',
                                        zipfile) ) 
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
  cad_data <- foreach::foreach(county = counties,
                               .combine = 'rbind') %do% {
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

  while(payers_response$count==0){
    # print(try_used)
    try_used = try_used+1
    if(try_used>3){
      return(NULL)
    }
    payers_response <- cpa_api_request("https://comptroller.texas.gov/data-search/franchise-tax?name=%s",
                                       set_name(owner_name,
                                                try_used
                                                ),
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
      
      results = officer_business_bind(owner_table,
                                      business_details_table)
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
        print(owners_fin)
        if(sum(!finance_inds)>0){
          print('2.2')
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
          print('2.3')
          results = data.frame(owners_fin)
          # print(results)
        }
      }
      else{
        print('3')
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
                   'situs_pID',
                   'situs_address')



#

owner_scrape_actual = function(austin_parcel_data_merged
                              ){
  insist_scrape_owner = purrr:::insistently(scrape_owner_api,
                                            rate =purrr::rate_backoff(pause_base = 2,#5,
                                                                pause_cap = 5,#30,
                                                                pause_min = 2,
                                                                max_times = 2,
                                                                jitter = TRUE
                                            ))
  if(is.na(file.size('owner_data_total.csv'))|
     file.size('owner_data_total.csv')<18000000){
    # print('1')
    # registerDoFuture()
    # plan(multisession, workers = parallel::detectCores()-1)
    target_properties = dplyr::filter(austin_parcel_data_merged,
                                      ((is_financialized ==TRUE)&
                                         (is_owner_occupied==FALSE))|
                                        (property_units>5),
                                      property_units!=0)
    if(!is.na(file.size('owner_data_total.csv'))){
      target_owner_info <- read.csv('owner_data_total.csv')
      target_properties <- dplyr::filter(target_properties,
                                         (as.numeric(situs_pID) %in% 
                                            as.numeric(unique(target_owner_info$situs_pID))==FALSE ))
      print(dim(target_properties))
      target_owner_info = foreach(index =1:nrow(target_properties),
                                  .combine = 'rbind',
                                  .options.RNG = 8989,
                                  .export = financial_marker_string) %do% {
                                    # print(index)
                                    owner_name =target_properties$owner_name[index]
                                    owner_address = target_properties$owner_address[index]
                                    situs_pID = target_properties$situs_pID[index]
                                    situs_address = target_properties$situs_address[index]
                                  
                                    # print(situs_pID)
                                    # print(situs_address)
                                    # print(owner_name)
                                    # print(owner_address)
                                    property_owner_info <- tryCatch({insist_scrape_owner(owner_name,
                                                                                         situs_pID = situs_pID ,
                                                                                         situs_address = situs_address,
                                                                                         veneer_owner = owner_name,
                                                                                         veneer_owner_mail_address = owner_address)},
                                                                    error=function(cond){
                                                                      cond}
                                                                    )
                                    print(property_owner_info)
                                    if((is.null(property_owner_info)) |
                                       ('error' %in% class(property_owner_info))){
                                      # print('not found')
                                      property_owner_info <- data.frame(t(c(rep(NA, 14),
                                                                            situs_pID,
                                                                            situs_address)
                                      ))
                                    }
                                    
                                    # print(dim(data.frame(as.matrix(property_owner_info))))
                                    # print(data.frame(as.matrix(property_owner_info)))
                                    colnames(property_owner_info) <- colnames_used
                                    print(property_owner_info)
                                    # print('write')
                                    data.table::fwrite(property_owner_info,

                                                       'owner_data_total.csv',
                                                       append = TRUE,
                                                       sep = ','
                                                       )
                                    return(NULL)
                                  }
    }
    else{
      target_owner_info = foreach(index =1:nrow(target_properties),
                                  .combine = 'rbind',
                                  .options.RNG = 8989,
                                  .export = financial_marker_string) %dopar% {
                                    # print(index)
                                    owner_name =target_properties$owner_name[index]
                                    owner_address = target_properties$owner_address[index]
                                    situs_pID = target_properties$situs_pID[index]
                                    situs_address = target_properties$situs_address[index]
                                    
                                    
                                    property_owner_info <- tryCatch({insist_scrape_owner(owner_name,
                                                                                         situs_pID = situs_pID ,
                                                                                         situs_address = situs_address,
                                                                                         veneer_owner = owner_name,
                                                                                         veneer_owner_mail_address = owner_address)},
                                                                    error=function(cond){
                                                                      cond}
                                    )
                                    # print(property_owner_info)
                                    if((is.null(property_owner_info)) |
                                       ('error' %in% class(property_owner_info))){
                                      property_owner_info <- data.frame(t(c(rep(NA, 14),
                                                                            situs_pID,
                                                                            situs_address)
                                      ))
                                    }
                                    first_prop <- index!=1
                                    colnames(property_owner_info) <- colnames_used
                                    data.table::fwrite(property_owner_info,
                                                       
                                                       'owner_data_total.csv',
                                                       append = first_prop,
                                                       sep = ','
                                    )
                                    return(NULL)
                                  }
    }
    print(dim(target_properties))
    
    
  }
  print('done')
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


