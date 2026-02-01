
situs_owner_string_gen = function(owner_data){
  # owner_data <- head(owner_data,100)
  situs_pIDs <- unique(owner_data$situs_pID)
  
  registerDoFuture()
  # 
  plan(multisession, workers = 6)
  situs_owner_strings <-foreach(pID = situs_pIDs) %dopar% {
    data_used <- dplyr::filter(owner_data,
                               situs_pID==pID)
    unique_owners <- unique(data_used$owner_name)
    unique_owner_add <- unique(data_used$owner_address)
    corp_name <- unique(data_used$corp_business_name)
    
    corp_address <- unique(data_used$corp_mail_address)
    registered_agent <- unique(data_used$corp_registered_agent_name)
    registered_agent_add <-unique(data_used$corp_registered_agent_mail_add)
    scraped_owner_address <- unique(data_used$owner_address_scraped)
    
    scraped_owner <- unique(data_used$owner_name_scraped)
    
    unique_entities <- na.omit(unique(c(corp_name,
                                scraped_owner)))
    unique_entities <- paste(unique_entities[order(unique_entities)],
                             collapse = ' ')
    
    unique_addresses <- na.omit(unique(unlist(c(unique_owner_add,
                                 corp_address,
                                 registered_agent_add,
                                 scraped_owner_address))))
    unique_addresses <- paste(unique_addresses[order(unique_addresses)],
                              collapse = ' ')
    unique_individuals <- na.omit(unique(unique_owners,
                                 # unique_owners_secondary_name,
                                 registered_agent
                                 ))
    unique_individuals <- paste(unique_individuals[order(unique_individuals)],
                                collapse = ' ')
    
    result <- paste(unique_entities,
                    unique_addresses,
                    unique_individuals,
                    sep = ' ')
    result <- gsub('[[:space:]]{2,}',
                   ' ',
                   result
                   )
    result <- gsub('[[:punct:]]',
                   '',
                   result)
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
                                    sum(as.numeric(units)>10)>0
                                  })))
  readr::write_rds(strings_used,
                   'strings_used.rds')
  print(head(strings_used))
  print(length(strings_used))
  situs_owner_cosine_dist_matrix <- as.matrix(stringdist::stringdistmatrix(unlist(situs_owner_strings[strings_used]),
                                                                           q = 2,
                                                                           method = 'cosine',
                                                                           useName = 'strings'))
  readr::write_rds(situs_owner_cosine_dist_matrix,
                   sprintf("situs_owner_cosine_dist_matrix_q%s.rds",
                           q),
                   version = 3,
                   compress = 'gz')
  situs_owner_cosine_dist_matrix
}

situs_neighor_gen = function(situs_owner_cosine_dist_matrix,
                             owner_data){
  situs_pIDs <- unique(owner_data$situs_pID)
  situs_owner_cosine_sim_matrix <- 1 - situs_owner_cosine_dist_matrix
  situs_neighbors <- sapply(1:nrow(situs_owner_cosine_sim_matrix),
                            function(index){
                              unique(c(which(situs_owner_cosine_sim_matrix[index,]>0.9),
                                       which(situs_owner_cosine_sim_matrix[,index]>0.9)
                              ))
                            })
  matched_owners_inds_uniq<-unique(lapply(situs_neighbors,
                                          function(inds){
                                            # print(inds)
                                            result <-unique(as.numeric(  
                                              unlist(sapply(inds,
                                                            function(ind){
                                                              inner_result <-c(ind, situs_neighbors[[ind]])
                                                              inner_result <- append(inner_result,
                                                                                     sapply(inner_result,
                                                                                            function(ind){
                                                                                              situs_neighbors[[ind]]
                                                                                            }))
                                                              inner_result
                                                            }
                                              )
                                              )))
                                            result = union(result,inds)
                                            result <- result[order(result)]
                                            
                                            result
                                          }))
  situs_group_assignment <- data.frame(situs_pIDs)
  
  situs_group_assignment$group_assign <- sapply(1:length(situs_pIDs), 
                                                function(index){
                                                  which(unlist(lapply(matched_owners_inds_uniq,
                                                                      function(set){index %in% set})))[[1]]
                                                  
                                                })
  situs_group_assignment$group_assign <- as.numeric(situs_group_assignment[,2])
  situs_group_assignment
}

parcel_geolocate = function(owner_data,
                            situs_group_assignment){
  
  owner_data <-dplyr::left_join(owner_data,
                                situs_group_assignment,
                                by = c('situs_pID'= 'situs_pIDs'))
  
  unique_situs_addr <- data.frame(situs_addr=unique(owner_data$situs_address))
  
  start_inds <- seq(1,nrow(unique_situs_addr),10000)
  end_inds <-c(seq(10000,nrow(unique_situs_addr),10000),
               nrow(unique_situs_addr))

  inds_used <- list(start = start_inds,end = end_inds)

  owners_info_scraped_coords <- foreach(index = 1:length(inds_used$start),
                                        .combine = 'rbind') %do% {
                                          start_ind = inds_used$start[index]
                                          end_ind = inds_used$end[index]
                                          owner_coords <- data.frame(situs_addr = unique_situs_addr[start_ind:end_ind,]) %>%
                                            geocode(situs_addr,
                                                    full_results = TRUE, 
                                                    method = 'census',
                                                    api_options = list(census_return_type = 'geographies'))
                                          owner_coords
                                        }
  
  
  owners_info_scraped_coords$id <- NULL
  
  owners_info_total <- left_join(owner_data,
                                 owners_info_scraped_coords,
                                 by = c('situs_address'='situs_addr'))
  owners_info_total$input_address <- NULL
  owners_info_total$matched_address <- NULL
  
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
    )
  
  write.csv(owners_data_total_supp,
            'owners_data_total.csv')
  owners_data_total_supp
  
  
}



