
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

financial_marker_base_string <- paste(financial_markers_base, 
                                      collapse = '|')

situs_owner_string_gen = function(owner_data){
  owner_data$owner_address <- address_clean(owner_data,
                                                 'owner_address')
  owner_data$corp_mail_address <- address_clean(owner_data,
                                                     'corp_mail_address')
  owner_data$owner_address_scraped <- address_clean(owner_data,
                                                         'owner_address_scraped')
  owner_data$corp_registered_agent_mail_add <- address_clean(owner_data,
                                                                  'corp_registered_agent_mail_add')
  registered_agent_add_string <-paste(unique(owner_data[grepl('COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST',
                                                              owner_data$corp_registered_agent_name,
                                                              ignore.case = TRUE),
                                                        'corp_registered_agent_mail_add']),
                                      collapse = '|')
  registered_agent_name_string <-paste(unique(owner_data[grepl('COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST',
                                                               owner_data$corp_registered_agent_name,
                                                               ignore.case = TRUE),
                                                         'corp_registered_agent_name']),
                                       collapse = '|')
  # owner_data <- head(owner_data,100)
  situs_pIDs <- unique(owner_data$situs_pID)
  
  registerDoFuture()
  # 3312
  plan(multisession, workers = 6)
  situs_owner_strings <-foreach(pID = situs_pIDs) %dopar% {
    data_used <- dplyr::filter(owner_data,
                               situs_pID==pID)
    unique_owners <- toupper(unique(data_used$owner_name))
    unique_owners_secondary_name <- toupper(unique(gsub('%[[:space:]]?',
                                                        '',
                                                        na.omit(data_used$owner_nameSecondary)
                                                        )
                                                   )
                                            )
    unique_owner_add <- toupper(unique(data_used$owner_address))
    corp_name <- toupper(unique(data_used$corp_business_name))
    corp_address <- toupper(unique(address_clean(data_used,
                                         'corp_mail_address')))
    registered_agent <- toupper(unique(data_used$corp_registered_agent_name))
    registered_agent_add <-toupper(unique( address_clean(data_used,
                                                'corp_registered_agent_mail_add')))
    
    scraped_owner_address <- toupper(unique(address_clean(data_used,
                                                  'owner_address_scraped')))
    scraped_owner <- toupper(unique(data_used$owner_name_scraped))
    
    unique_entities <- na.omit(unique(c(corp_name,
                                scraped_owner)))
    unique_entities <- paste(unique_entities[order(unique_entities)],
                             collapse = ' ')
    # print(unique_entities)
    unique_addresses <- na.omit(unique(unlist(c(unique_owner_add,
                                 corp_address,
                                 # registered_agent_add,
                                 scraped_owner_address))))
    unique_addresses <- paste(unique_addresses[order(unique_addresses)],
                              collapse = ' ')
    # print(unique_addresses)
    unique_individuals <- na.omit(unique(c(unique_owners,
                                 unique_owners_secondary_name,
                                 registered_agent
                                 )))
    
    unique_individuals <- paste(unique_individuals[order(unique_individuals)],
                                collapse = ' ')
    # print(unique_individuals)
    result <- paste(unique_entities,
                    unique_addresses,
                    unique_individuals,
                    sep = ' ')
    result <- gsub(registered_agent_add_string,
                   '',
                   result)
    result <- gsub(registered_agent_name_string,
                   '',
                   result)
    result <-gsub(financial_marker_base_string,'',result)
    
    
    result <- gsub('[[:punct:]]',
                   '',
                   result)
    result <- trimws(gsub('[[:space:]]{2,}',
                   ' ',
                   result
                   ))
    
    result[length(result)]
    
  }
  
  
  situs_owner_strings <- trimws(unlist(situs_owner_strings))
  
  readr::write_rds(situs_owner_strings,
                   'situs_owner_strings.rds')
  situs_owner_strings
  
  
}

situs_owner_string_dist_matrix = function(situs_owner_strings, 
                                          austin_parcel_data_merged){
  strings_used <- which((tapply(austin_parcel_data_merged$is_target,
                                austin_parcel_data_merged$situs_pID, 
                                function(x){
                                  sum(as.numeric(x))>0
                                  }
                                )
                         )|
                          (tapply(austin_parcel_data_merged$property_units,
                                  austin_parcel_data_merged$situs_pID,
                                  function(units){
                                    sum(as.numeric(units)>5)>0
                                  })))
  readr::write_rds(strings_used,
                   'strings_used.rds')
  situs_owner_cosine_dist_matrix <- as.matrix(stringdist::stringdistmatrix(unlist(situs_owner_strings[strings_used]),
                                                                           q = 2,
                                                                           method = 'cosine',
                                                                           useName = 'strings'))
  situs_owner_cosine_dist_matrix
}

situs_neighbor_cov = function(situs_owner_cosine_dist_matrix){
  Rfast::cova(q3_dist_matrix, large = TRUE)
}

situs_neighor_gen = function(situs_owner_cosine_dist_matrix,
                             owner_data){
  owner_data_used <- owner_data

  owner_data_used$owner_address <- address_clean(owner_data_used,
                                                'owner_address')
  owner_data_used$corp_mail_address <- address_clean(owner_data_used,
                                                'corp_mail_address')
  owner_data_used$owner_address_scraped <- address_clean(owner_data_used,
                                                'owner_address_scraped')
  owner_data_used$corp_registered_agent_mail_add <- address_clean(owner_data_used,
                                                         'corp_registered_agent_mail_add')
  registered_agent_add_string <-paste(paste(unique(owner_data_used[grepl('COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST',
                                                                    owner_data_used$corp_registered_agent_name,
                                                                    ignore.case = TRUE),
                                                              'corp_registered_agent_mail_add']),
                                            collapse = '|'),
                                      '815 BRAZOS ST 500 AUSTIN TX 78701',
                                      '2595 DALLAS PKWY STE 350 FRISCO TX 75034',
                                      sep = '|')
  
  readr::write_rds(registered_agent_add_string,
                   'registered_agent_add_string.rds')
  registered_agent_name_string <-paste(unique(owner_data_used[grepl('COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST',
                                                               owner_data_used$corp_registered_agent_name,
                                                               ignore.case = TRUE),
                                                         'corp_registered_agent_name']),
                                       collapse = '|')
  readr::write_rds(registered_agent_name_string,
                   'registered_agent_name_string.rds')
  owner_data_used$owner_address <- toupper(gsub(registered_agent_add_string,
                                        NA,
                                        owner_data_used$owner_address))
  
  owner_data_used$corp_mail_address <-toupper( gsub(registered_agent_add_string,
                                            NA,
                                            owner_data_used$corp_mail_address))
  
  owner_data_used$owner_address_scraped <- toupper(gsub(registered_agent_add_string,
                                                NA,
                                                owner_data_used$owner_address_scraped))
  
  owner_data_used$corp_registered_agent_mail_add <- toupper(gsub(registered_agent_add_string,
                                                         NA,
                                                         owner_data_used$corp_registered_agent_mail_add))
  
  owner_data_used$corp_business_name <- toupper(gsub(registered_agent_name_string,
                                             NA,
                                             owner_data_used$corp_business_name))
  
  owner_data_used$owner_name <-toupper(gsub(registered_agent_name_string,
                                            NA,
                                            owner_data_used$owner_name))
  owner_data_used$owner_name_scraped <- toupper(gsub(registered_agent_name_string,
                                                     NA,
                                                     owner_data_used$owner_name_scraped))
  owner_data_used$corp_registered_agent_name <-toupper(gsub(registered_agent_name_string,
                                                            NA,
                                                            owner_data_used$corp_registered_agent_name))
  
  write.csv(owner_data_used,
            'owner_data_sample.csv')
  # print(owner_data)

  situs_pIDs <-names(which((tapply(owner_data_used$is_target,
                                   owner_data_used$situs_pID, 
                                    function(x){
                                      sum(as.numeric(x))>0
                                    }
                                    )
                             )|
                              (tapply(owner_data_used$property_units,
                                      owner_data_used$situs_pID,
                                      function(units){
                                        sum(as.numeric(units)>5)>0
                                        }))))
  
  situs_neighbors <- sapply(1:nrow(situs_owner_cosine_dist_matrix),
                            function(index){
                              print('index')
                              print(index)
                              situs_pID_used <- situs_pIDs[index]
                              data_used <- dplyr::filter(owner_data_used,
                                                         situs_pID==situs_pID_used)
                              owner_name_scrape_inds <-which(situs_pIDs %in%
                                                               owner_data_used$situs_pID[which(owner_data_used$owner_name_scraped %in%
                                                                 na.omit(gsub("^$",
                                                                              NA,
                                                                              unique(data_used$owner_name_scraped)
                                                                              )
                                                                         )
                                                                 )])
                              
                              owner_name_inds <-which(situs_pIDs %in%
                                                        owner_data_used$situs_pID[which(owner_data_used$owner_name %in%
                                                                                     na.omit(gsub("^$",
                                                                                                  NA,
                                                                                                  unique(data_used$owner_name)
                                                                                     )
                                                                                     )
                                                        )])
                              
                              owner_addr_scrape_inds <- which(situs_pIDs %in%
                                                                owner_data_used$situs_pID[which(owner_data_used$owner_address_scraped %in%
                                                                                             na.omit(gsub("^$",
                                                                                                          NA,
                                                                                                          unique(data_used$owner_address_scraped)
                                                                                             )
                                                                                             )
                                                                )])
                              owner_addr_inds <-which(situs_pIDs %in%
                                                        owner_data_used$situs_pID[which(owner_data_used$owner_address %in%
                                                                                     na.omit(gsub("^$",
                                                                                                  NA,
                                                                                                  unique(data_used$owner_address)
                                                                                     )
                                                                                     )
                                                        )]) 
                              corp_addr_inds <- which(situs_pIDs %in%
                                                        owner_data_used$situs_pID[which(owner_data_used$corp_mail_address %in%
                                                                                     na.omit(gsub("^$",
                                                                                                  NA,
                                                                                                  unique(data_used$corp_mail_address)
                                                                                     )
                                                                                     )
                                                        )])
                              
                              corp_bus_inds <- which(situs_pIDs %in%
                                                       owner_data_used$situs_pID[which(owner_data_used$corp_business_name %in%
                                                                                    na.omit(gsub("^$",
                                                                                                 NA,
                                                                                                 unique(data_used$corp_business_name)
                                                                                    )
                                                                                    )
                                                       )])
                              reg_agent_name_inds <- which(situs_pIDs %in%
                                                             owner_data_used$situs_pID[which(owner_data_used$corp_registered_agent_name %in%
                                                                                    na.omit(gsub("^$",
                                                                                                 NA,
                                                                                                 unique(data_used$corp_registered_agent_name)
                                                                                    )
                                                                                    ) 
                                                                                  )])
                              reg_agent_add_inds <- which(situs_pIDs %in%
                                                            owner_data_used$situs_pID[which(owner_data_used$corp_registered_agent_mail_add %in%
                                                                                    na.omit(gsub("^$",
                                                                                                 NA,
                                                                                                 unique(data_used$corp_registered_agent_mail_add)
                                                                                    )
                                                                                    )
                                                       )])
                              
                                                     
                              dist_inds <- unique(c(which(situs_owner_cosine_dist_matrix[index,]<0.15),
                                                    which(situs_owner_cosine_dist_matrix[,index]<0.15)
                                                    ))
                              neighbor_inds <- unique(c(owner_name_scrape_inds,
                                                 owner_name_inds,
                                                 owner_addr_scrape_inds,
                                                 owner_addr_inds,
                                                 corp_addr_inds,
                                                 corp_bus_inds,
                                                 reg_agent_name_inds,
                                                 reg_agent_add_inds,
                                                 dist_inds))
                              print(neighbor_inds)
                              
                              return(neighbor_inds[order(neighbor_inds)])
                            })
  print(situs_neighbors)
  
  readr::write_rds(situs_neighbors,
                   'situs_neighbors.rds')
  print('neigh')
  iterative_add = function(inds, neighbors){
    result <-unique(as.numeric(  
      unlist(sapply(inds,
                    function(ind){
                      inner_result <-neighbors[[ind]]
                      inner_result <- append(inner_result,
                                             sapply(inner_result,
                                                    function(ind){
                                                      # print(ind)
                                                      neighbors[[ind]]
                                                    }))
                      inner_result
                    })))
      )
    result
  }

  matched_owners_inds_uniq<-unique(foreach(inds = situs_neighbors) %do% {
                                            result <-iterative_add(inds = inds,
                                                                   situs_neighbors)
                                            base_length = length(result)
                                            new_length = 0
                                            while(new_length!=base_length){
                                              base_length <- length(result)
                                              result <- iterative_add(result,
                                                                      situs_neighbors)
                                              new_length <- length(result)
                                            }
                                            
                                            result <- unique(result[order(result)])
                                            
                                            # rem_inds <- which(situs_owner_cosine_dist_matrix[inds[1],result]>0.6)
                                            # rem_inds <- unique(unlist(apply(situs_owner_cosine_dist_matrix[inds,result],2,
                                            #                         function(col){which(col>0.6)})))
                                            # rem_inds <- rem_inds[!(rem_inds %in% inds)]
                                            # if(length(rem_inds)>0){
                                            #   result <- result[-rem_inds]
                                            # }
                                            result
                                          })
  
  readr::write_rds(matched_owners_inds_uniq,
                   'matched_owners_inds_uniq.rds'
                   )
  situs_group_assignment <- data.frame(situs_pID = situs_pIDs)
  print(head(situs_group_assignment))

  situs_group_assignment$group_assign <- foreach(index = 1:length(situs_pIDs),
                                                 .combine = 'c') %do% {
                                                  results <- which(unlist(lapply(matched_owners_inds_uniq,
                                                                      function(set){index %in% set})))
                                                  paste(results,
                                                        collapse = ',')
                                                  # results
                                                 }
  # print(situs_group_assignment)
  situs_group_assignment
}

parcel_geolocate = function(owner_data,
                            situs_group_assignment){
  owner_data$situs_pID <- as.character(owner_data$situs_pID )
  owner_data <-dplyr::left_join(owner_data,
                                situs_group_assignment,
                                by = c('situs_pID'= 'situs_pID'))
  
  unique_situs_addr <- data.frame(situs_addr=unique(owner_data$situs_address))
  
  start_inds <- seq(1,nrow(unique_situs_addr),10000)
  end_inds <-c(seq(10000,nrow(unique_situs_addr),10000),
               nrow(unique_situs_addr))

  inds_used <- list(start = start_inds,end = end_inds)
  
  insist_geocode = purrr::insistently(geocode,
                                      rate =purrr::rate_backoff(pause_base = 5,
                                                                pause_cap = 30,
                                                                max_times = 3,
                                                                jitter = TRUE)
                                      )
  registerDoFuture()
  plan(multisession, workers = 6)
  owners_info_scraped_coords <- foreach(index = 1:length(inds_used$start),
                                        .combine = 'rbind') %dopar% {
                                          start_ind = inds_used$start[index]
                                          end_ind = inds_used$end[index]
                                          owner_coords <- data.frame(situs_addr = unique_situs_addr[start_ind:end_ind,]) %>%
                                            insist_geocode(situs_addr,
                                                    full_results = TRUE, 
                                                    method = 'census',
                                                    api_options = list(census_return_type = 'geographies'))
                                          owner_coords
                                        }
  
  
  owners_info_scraped_coords$id <- NULL
  owners_info_scraped_coords$input_address <- NULL
  owners_info_scraped_coords$matched_address <- NULL  
  owners_info_total <- left_join(owner_data,
                                 owners_info_scraped_coords,
                                 by = c('situs_address'='situs_addr'))

  
  owners_info_total <- owners_info_total %>% 
    rename(situs_lat = lat,
           situs_long = long)
  owners_info_total
  
  
  
}


final_data_merge = function(owners_data_total,
                           hhi_data,
                           svi_data){
  hhi_data <- hhi_data %>%
    dplyr::rename(HHI_score = OVERALL_SCORE,
                  HHI_rank = OVERALL_RANK)
  svi_data <- dplyr::filter(svi_data,
                            year == max(year)
  )
  svi_data$zip_code_tabulation_area <- as.character(svi_data$zip_code_tabulation_area)
  
  
  owners_data_total_supp <- dplyr::left_join(owners_data_total,
                                             hhi_data[,c('ZCTA',
                                                         'HHI_score',
                                                         'HHI_rank')],
                                             by = c('situs_zip'='ZCTA')) %>%
    dplyr::left_join(svi_data[,c('zip_code_tabulation_area',
                                 'total_population',
                                 # 'below_150_pov_cnt',
                                 'below_150_pov_perc',
                                 # 'uninsured_cnt',
                                 'uninsured_perc',
                                 'below_150_pov_perc',
                                 'no_hs_dip_perc',
                                 # 'disability_cnt',
                                 'disability_perc',
                                 # 'single_parent_cnt',
                                 'single_parent_perc',
                                 # 'unemp_cnt',
                                 'unemp_rate_perc',
                                 'minority_perc',
                                 # 'crowded_housing_cnt',
                                 'crowded_housing_perc',
                                 # 'no_vehicle_cnt',
                                 'no_vehicle_perc',
                                 # 'limited_eng',
                                 'limited_eng_perc',
                                 'group_quarter_perc',
                                 'rpl_theme1',
                                 'rpl_theme2',
                                 'rpl_theme3',
                                 'rpl_theme4',
                                 'spl_themes',
                                 'rpl_themes'
    )
    ],
    by = c('situs_zip'='zip_code_tabulation_area')
    ) %>%
    relocate(situs_address,
             .before = situs_pID)
  
  write.csv(owners_data_total_supp,
            'owners_data_total.csv')
  owners_data_total_supp
  
  
}



