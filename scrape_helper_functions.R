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
    return(trimws(gsub(paste('([[:space:]]|[[:punct:]])?(?=',
                                     financial_marker_base_string,
                                     '.*)', sep = ''),
                               ', ',
                               name,
                               perl = TRUE)
                   )
           )
  }
  if(try==2){
    
    return(trimws(gsub(paste('([[:space:]]|[[:punct:]])?(?=',
                                     financial_marker_base_string,
                                     '.*)', sep = ''),
                               ' ',
                               name,
                               perl = TRUE)
                   )
    )
    
  }
  if(try==3){
    
    return(trimws(gsub(paste('([[:space:]]|[[:punct:]])?',
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
  link_used <- base_html[grepl('Special.*export.*JSON',base_html)] %>% html_attr('href')
  
  old_link <- readLines('link_used.csv',
                        
                        warn = FALSE
                        )
  if(sum(grepl('zip',list.files()))>0){
    download.file(link_used,
                  'tcad_special_export.zip')
  }
  if(link_used!=old_link){
    writeLines(link_used,
              'link_used.csv')
    download.file(link_used,
                  'tcad_special_export.zip')
  }
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
                        depth = 0,
                        owner_title = NA,
                        owner_mail_address = NA,
                        owner_active_year = NA,
                        business_details_table = NA){
  
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
  
  # print('payers_response')
  # print(str(payers_response))
  taxId <- unlist(lapply(payers_response$data,
                                function(entity){
                                  unlist(entity$taxpayerId)
                                }))[1]
  
  # print(taxId)
  franchise_info <- cpa_franchise_get(taxId,
                                      api_key)
  
  business_details_table_parse = franchise_info[[1]]
  owner_details_table_parse = franchise_info[[2]]
  # print(str(franchise_info[[1]]))
  # print(str(franchise_info[[2]]))
  # print('data_get')
  #officer button appears after search
  # print(business_details_table_parse)
  if(nrow(owner_details_table_parse)==0){
    # print('1.0')
    #no results on a recursive owner search
    if(depth>0 ){
      
      # print('1.1')
      owner_table = data.frame(owner_name_scraped = owner_name,
                               owner_title = owner_title,
                               owner_mail_address = owner_mail_address,
                               owner_active_year = owner_active_year)
      
      results = officer_business_bind(owner_table,
                                      business_details_table)
    }
    #no results on base owner search
    if(depth==0 ){
      # print('1.2')
      owner_table = data.frame(owner_name_scraped = NA,
                               owner_title = NA,
                               owner_mail_address = NA,
                               owner_active_year = NA)
      
      results = officer_business_bind(owner_table,
                                      business_details_table_parse)
    }
    
  }
  #found results
  else{
    # print('2')
    finance_inds <- grepl(financial_marker_string,
                          owner_details_table_parse$owner_name)
    #if owner has financial markers, do a recursive search on it
    if(sum(finance_inds)>0){
      # print('2.1')
      owners_fin = foreach(ind = which(finance_inds),
                           .combine = 'rbind') %do% {
                             fin_owner_scrape = tryCatch({
                               scrape_owner_api( owner_details_table_parse$owner_name[ind],
                                             situs_pID = situs_pID , 
                                             situs_address = situs_address,
                                             veneer_owner = veneer_owner,
                                             veneer_owner_mail_address = veneer_owner_mail_address,
                                             depth = depth+1,
                                             owner_title = owner_details_table_parse$owner_title[ind],
                                             owner_mail_address = owner_details_table_parse$owner_mail_address[ind],
                                             owner_active_year = owner_details_table_parse$owner_active_year,
                                             business_details_table = business_details_table_parse )
                             },error = function(cond){
                               cond
                             })
                             
                             if('error' %in% class(fin_owner_scrape)){
                               print('error')
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
      
      if(sum(!finance_inds)>0){
        # print('2.2')
        owners_non_fin = data.frame(owner_name = owner_details_table_parse$owner_name,
                                    owner_title = owner_details_table_parse$owner_title,
                                    owner_address = owner_details_table_parse$owner_mail_address,
                                    owner_active_year = owner_details_table_parse$owner_active_year)[!finance_inds,]
        
        
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
      # print(owner_details_table_parse)
      owner_table = data.frame(owner_name = owner_details_table_parse$owner_name,
                               owner_title = owner_details_table_parse$owner_title,
                               owner_mail_address = owner_details_table_parse$owner_mail_address,
                               owner_active_year = owner_details_table_parse$owner_active_year)
      # print(owner_table)
      # print(business_details_table_parse)
      results <- officer_business_bind(owner_table,
                                       business_details_table_parse)
      # print(results)
      results
      
    }
  }
  
  result$owner_mail_address <- address_clean(result,
                                             'owner_mail_address')
  result$owner_mail_address <- address_clean(result,
                                             'owner_mail_address')
  results$situs_pID <- situs_pID
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
                   'situs_pID')



#

owner_scrape_actual = function(austin_parcel_data_merged
                              ){
  
  print(Sys.time())
  insist_scrape_owner = purrr:::insistently(scrape_owner_api,
                                            rate =purrr::rate_backoff(pause_base = 2,
                                                                pause_cap = 30,
                                                                pause_min = 1,
                                                                max_times = 3,
                                                                jitter = TRUE
                                            ))
  registerDoFuture()
  plan(multisession, workers = 6)
  target_properties = dplyr::filter(austin_parcel_data_merged, is_target==TRUE)
  target_owner_info = foreach(index =1:nrow(target_properties),
                              .combine = 'rbind',
                              .options.RNG = 8989,
                              .export = financial_marker_string) %dopar% {
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
                                                    situs_pID)
                                                  )
              )
              }
            first_prop <- index!=1
            colnames(property_owner_info) <- colnames_used
            data.table::fwrite(property_owner_info,

                               'owner_data_total.csv',
                               append = first_prop,
                               sep = ','
                               )
            property_owner_info
          }
  # target_owner_info <- target_owner_info %>%
  #   dplyr::group_by(situs_pID) %>%
  #   dplyr::summarise(owners_name_scraped = paste(unique(owner_name_scraped),
  #                                                collapse = ', '),
  #             owner_mail_address_scraped = paste(unique(owner_mail_address),
  #                                        collapse = ', '),
  #             owner_active_year = paste(unique(owner_active_year),
  #                                       collapse = ', '),
  #             corp_business_name = paste(unique(corp_business_name),
  #                                        collapse = ', '),
  #             corp_TTN = paste(unique(corp_TTN),
  #                              collapse = ', '),
  #             corp_mail_address = paste(unique(corp_mail_address),
  #                                       collapse = ', '),
  #             corp_right_to_transact_business_tx_status =paste(unique(corp_right_to_transact_business_tx_status),
  #                                                              collapse = ', '),
  #             corp_state_of_formation = paste(unique(corp_state_of_formation),
  #                                             collapse = ', '),
  #             
  #             corp_sos_registration_status = paste(unique(corp_sos_registration_status),
  #                                                  collapse = ', '),
  #             corp_effective_sos_registration_date =paste(unique(corp_effective_sos_registration_date),
  #                                                         collapse = ', '),
  #             corp_tx_sos_file_num = paste(unique(corp_tx_sos_file_num),
  #                                          collapse = ', '),
  #             corp_registered_agent_name = paste(unique(corp_registered_agent_name),
  #                                                collapse = ', '),
  #             corp_registered_agent_mail_add = paste(unique(corp_registered_agent_mail_add),
  #                                                    collapse = ', ')
  #             )
  target_owner_info <- read.csv('owner_data_total.csv')
  
  austin_parcel_data_merged <- dplyr::left_join(austin_parcel_data_merged,
                                                  target_owner_info,
                                                  by = 'situs_pID')
  write.csv(austin_parcel_data_merged,
            'austin_parcel_data_merged.csv'
            )
  
    print(Sys.time())
    austin_parcel_data_merged
  }



deed_summ_data_gen = function(deeds_data, 
                              propertyProf_data, 
                              propertyChar_data){
  residential_pIDs <-unique(unlist(propertyProf_data$propertyProf_pID[grepl('^A|^B',
                                                                            propertyProf_data$propertyProf_imprvStateCd)|
                                                                        grepl('^A|^B',
                                                                              propertyProf_data$propertyProf_landStateCd)]),
                            unlist(propertyChar_data$propertyChar_pID[grepl('SF|MF',
                                                                            propertyChar_data$propertyChar_zoning)]
                                   ))
  registerDoFuture()
  plan(multisession, workers = 8)
  
  deed_summ <- foreach(pid = residential_pIDs,
                       .combine = 'rbind') %dopar% {
                         foreach(year = unique(deeds_data$deeds_year),
                                 .combine = 'rbind') %do% {
                                   # message(pid)
                                   # message(year)
                                   data_used <- dplyr::filter(deeds_data,
                                                              deeds_pID == pid,
                                                              deeds_year == year)
                                   dates <- as.Date(data_used$deeds_deedDt)
                                   #message(dates)
                                   data_used <- data_used[order(dates),]
                                   if(nrow(data_used)==0){
                                     
                                     print('5')
                                     return(data.frame(year = NA,
                                                       deeds_pID = NA,
                                                       owner_num = NA,
                                                       recent_purchase_date = NA,
                                                       recent_owner_name = NA,
                                                       avg_owner_tenure_months =NA))
                                   }
                                   dates <- unique(dates[order(dates)])
                                   if(length(dates)>1){
                                     tenure <- as.numeric(unlist(sapply(2:length(dates),
                                                                        function(index){
                                                                          date1 = lubridate::ymd(dates[index-1])
                                                                          # print(date1)
                                                                          date2 = lubridate::ymd(dates[(index)])
                                                                          # print(date2)
                                                                          interval_used = interval(date1,
                                                                                                   date2)
                                                                          # print(interval_used)
                                                                          return(interval_used/months(1))
                                                                        })))
                                     tenure = c(tenure,
                                                interval(max(dates),
                                                         lubridate::ymd(as.Date(Sys.time()
                                                         )
                                                         )
                                                )/months(1)
                                     )
                                   }
                                   else{
                                     date1 = lubridate::ymd(dates)
                                     interval_used <- interval(date1,
                                                               lubridate::ymd(as.Date(Sys.time())))
                                     tenure = interval_used/months(1)
                                     
                                   }
                                   data.frame(year = year,
                                              deeds_pID = unique(data_used$deeds_pID),
                                              owner_num = length(data_used$deeds_buyerline),
                                              recent_purchase_date = max(data_used$deeds_deedDt,na.rm = TRUE),
                                              recent_owner_name = unique(data_used$deeds_buyerline[which(dates==max(dates,
                                                                                                                    na.rm = TRUE))])[1],
                                              avg_owner_tenure_months = mean(tenure,na.rm=TRUE))
                                 }
                       }
 
  # print('e')
  print(head(deed_summ))
  print(dim(deed_summ))
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
                               print('error')
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


