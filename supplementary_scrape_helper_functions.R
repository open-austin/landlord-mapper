austin_open_data_dl_shell = function(link_used,
                                     base_used,
                                     download_location = 'C:/Users/kevin/Downloads'){
  initial_try = tryCatch({
    austin_open_data_dl(link_used,
                      base_used,
                      download_location)
    },error = function(cond){
      cond
    })
  if('error' %in% class(initial_try)){
    return(list.files()[grepl('Code_Complaint', list.files())])
  }
  
}
austin_open_data_dl = function(link_used,
                               base_used,
                               download_location = 'C:/Users/kevin/Downloads'){
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
                                                                 timeout = 60))
                               ,
                               timeout = 60
  )
  base_url = link_used
  open_url(base_url)
  
  export_button <- s(".export-button")
  
  wait_to_click(export_button)
  download_button <- s("forge-button[data-testid=export-download-button]")
  
  wait_to_click(download_button)
  Sys.sleep(20)
  
  name_used = sprintf('%s_%s.csv',
                      base_used,
                      gsub('-','',lubridate::ymd(as.Date(Sys.time())))
  )
  file_used = paste(download_location,
                    name_used,
                    sep = '/')
  
  new_loc <-  paste(getwd(),
                    name_used,sep = '/')
  fs::file_copy(file_used,
                new_loc,
                overwrite = TRUE)
  if(getwd()!=download_location){  
    fs::file_delete(file_used)
    
  }
  return(name_used)
  
}



census_data_get_svi = function(years_used){
  registerDoFuture()
  plan(multisession)
  Sys.setenv(CENSUS_KEY='cdd487fd377da61dfdecfbcdb620f7a94f3c5b6f')
  Sys.getenv("CENSUS_KEY")
  
  
  svi_data <- foreach(year=years_used,
                      .combine = 'rbind') %dopar% {
                        print(year)
                        subject_vars <- getCensus('acs/acs5/subject',
                                                  year,
                                                  vars = c('S0601_C01_001E',#total population
                                                           'S0601_C01_033E', #no hs dip perc
                                                           'S0701_C01_001E',
                                                           'S0701_C01_025E',#foreign
                                                           'S1701_C01_001E',#
                                                           'S1701_C01_036E',
                                                           'S1701_C01_040E',#pop below 150 pov level cnt (2014, 36)
                                                           'S2503_C01_001E',
                                                           'S2503_C01_028E',#housing burden <75k/yr(2014, start 32)
                                                           'S2503_C01_032E',
                                                           
                                                           'S2503_C01_036E',
                                                           'S2503_C01_040E',
                                                           'S2503_C01_044E',
                                                           'S2701_C02_001E',
                                                           
                                                           # uninsured cnt (2014, S2701_C04_062E; 2012 S2701_C02_001E)
                                                           #S2701_C04_001E
                                                           ifelse(year<2015,
                                                                  'S2701_C04_062E',
                                                                  'S2701_C04_001E'),
                                                           'S2701_C05_001E', #uninsured perc (2014, S2701_C05_062E; 2012 S2701_C03_001E)
                                                           ifelse(year<2015,
                                                                  'S2701_C05_062E',
                                                                  'S2701_C01_002E'
                                                           ),
                                                           'S0101_C01_001E',
                                                           'S0101_C01_030E',#pop 65+ cnt (2016, S0101_C01_028E)
                                                           'S0101_C01_028E',
                                                           'S0101_C02_030E'#pop 65+ perc (2016, /S0101_C01_001E)
                                                  ),
                                                  region = 'zip code tabulation area:*')
                        if(year>2016){
                          subject_vars <- subject_vars %>%
                            dplyr::mutate(house_burden_less75k_cnt=(S2503_C01_028E + S2503_C01_032E +
                                                                      S2503_C01_036E + S2503_C01_040E),
                                          house_burden_less75k_perc = (house_burden_less75k_cnt/S2503_C01_001E),
                                          house_burden_less75k_percRank = dplyr::percent_rank(house_burden_less75k_perc),
                                          below_150_pov_perc = (S1701_C01_040E)/S1701_C01_001E,
                                          below_150_pov_percRank = dplyr::percent_rank(below_150_pov_perc),
                                          no_hs_dip_percRank = dplyr::percent_rank(S0601_C01_033E),
                                          uninsured_percRank = dplyr::percent_rank(S2701_C05_001E),
                                          over65_percRank = dplyr::percent_rank(S0101_C02_030E),
                                          foreign_born_prop = S0701_C01_025E/S0701_C01_001E) %>%
                            dplyr::rename(total_population = S0601_C01_001E,
                                          no_hs_dip_perc = S0601_C01_033E,
                                          below_150_pov_cnt = S1701_C01_040E,
                                          uninsured_cnt = S2701_C04_001E,
                                          uninsured_perc = S2701_C05_001E,
                                          over65_cnt = S0101_C01_030E,
                                          over65_perc = S0101_C02_030E,
                                          foreign_born_cnt = S0701_C01_025E
                            )
                        }
                        if(year %in% c(2015, 2016)){
                          
                          subject_vars <- subject_vars %>%
                            dplyr::mutate(house_burden_less75k_cnt=(S2503_C01_028E + S2503_C01_032E +
                                                                      S2503_C01_036E + S2503_C01_040E),
                                          house_burden_less75k_perc = (house_burden_less75k_cnt/S2503_C01_001E),
                                          house_burden_less75k_percRank = dplyr::percent_rank(house_burden_less75k_perc),
                                          below_150_pov_perc = (S1701_C01_040E)/S1701_C01_001E,
                                          below_150_pov_percRank = dplyr::percent_rank(below_150_pov_perc),
                                          
                                          no_hs_dip_percRank = dplyr::percent_rank(S0601_C01_033E),
                                          uninsured_percRank = dplyr::percent_rank(S2701_C05_001E),
                                          over65_perc = S0101_C01_028E/S0101_C01_001E,
                                          over65_percRank = dplyr::percent_rank(S0101_C02_030E)) %>%
                            dplyr::rename(total_population = S0601_C01_001E,
                                          no_hs_dip_perc = S0601_C01_033E,
                                          below_150_pov_cnt = S1701_C01_040E,
                                          uninsured_cnt = S2701_C04_001E,
                                          uninsured_perc = S2701_C05_001E,
                                          over65_cnt = S0101_C01_028E
                            )
                          
                        }
                        if(year<2015){
                          subject_vars <- subject_vars %>%
                            dplyr::mutate(house_burden_less75k_cnt=(S2503_C01_032E + S2503_C01_036E +
                                                                      S2503_C01_040E + S2503_C01_044E),
                                          house_burden_less75k_perc = (house_burden_less75k_cnt/S2503_C01_001E),
                                          house_burden_less75k_percRank = dplyr::percent_rank(house_burden_less75k_perc),
                                          below_150_pov_perc = (S1701_C01_036E)/S1701_C01_001E,
                                          below_150_pov_percRank = dplyr::percent_rank(below_150_pov_perc),
                                          
                                          no_hs_dip_percRank = dplyr::percent_rank(S0601_C01_033E),
                                          uninsured_percRank = dplyr::percent_rank(S2701_C05_062E),
                                          over65_percRank = dplyr::percent_rank(S0101_C02_030E),
                                          over65_perc = S0101_C01_028E/S0101_C01_001E) %>%
                            dplyr::rename(total_population = S0601_C01_001E,
                                          no_hs_dip_perc = S0601_C01_033E,
                                          below_150_pov_cnt = S1701_C01_036E,
                                          uninsured_cnt = S2701_C04_062E,
                                          uninsured_perc = S2701_C05_062E,
                                          over65_cnt = S0101_C01_028E,
                            )
                          
                          
                        }
                        if(year==2023){
                          white_cnt_var = 'DP05_0082E'#white cnt 2023
                          
                        }
                        if(year==2022){
                          white_cnt_var ='DP05_0079E'#white cnt 2022
                        }
                        
                        if((year<=2021) & (year>2016)){
                          white_cnt_var = 'DP05_0077E'#white cnt 2021
                        }
                        if(year<=2016){
                          white_cnt_var = 'DP05_0072E'#white cnt 2016
                        } 
                        profile_vars <- getCensus('acs/acs5/profile',
                                                  year,
                                                  vars = c('DP04_0001E',#total housing units
                                                           'DP02_0001E',#total households
                                                           'DP05_0018E',
                                                           'DP05_0001E',
                                                           'DP05_0019E',#pop under 18 (2016 total -DP05_0018E)
                                                           'DP02_0071E',
                                                           'DP02_0072E',#pop with disabilities (2018, DP02_0071E)
                                                           'DP02_0007E',#single parents 
                                                           'DP02_0008E',
                                                           'DP02_0011E',#(2018,DP02_0008E )
                                                           'DP03_0001E',#total inds
                                                           'DP03_0005E',#unemployment count, 16+
                                                           'DP04_0012E',#10-19 unit housing
                                                           'DP04_0013E',#20+ unit housing,
                                                           'DP04_0014E', #mobile housing,
                                                           'DP04_0078E',#crowded homes
                                                           'DP04_0079E',
                                                           'DP04_0057E',
                                                           'DP04_0058E',#houses with no vehicle (2014, DP04_0057E)
                                                           'DP05_0001E',#total eth cnt,
                                                           white_cnt_var
                                                  ),
                                                  region = 'zip code tabulation area:*')
                        
                        if(year>2018){
                          profile_vars$minority_cnt <- profile_vars$DP05_0001E - profile_vars[,white_cnt_var]
                          
                          
                          profile_vars <- profile_vars %>%
                            dplyr::mutate(single_parent_cnt=DP02_0007E + DP02_0011E,
                                          single_parent_perc = single_parent_cnt/DP02_0001E,
                                          single_parent_percRank = dplyr::percent_rank(single_parent_perc),
                                          unemp_rate_perc = DP03_0005E/DP03_0001E,
                                          unemp_rate_percRank = dplyr::percent_rank(unemp_rate_perc),
                                          under18_perc = DP05_0019E/DP05_0001E,
                                          under18_percRank = dplyr::percent_rank(under18_perc),
                                          disability_perc = DP02_0072E/DP02_0001E,
                                          disability_percRank = dplyr::percent_rank(disability_perc),
                                          minority_perc = minority_cnt/DP05_0001E,
                                          minority_percRank = dplyr::percent_rank(minority_perc),
                                          large_unit_housing_cnt = DP04_0012E+DP04_0013E,
                                          large_unit_housing_perc = large_unit_housing_cnt/DP04_0001E,
                                          large_unit_housing_percRank = dplyr::percent_rank(large_unit_housing_perc),
                                          mobile_housing_perc = DP04_0014E/DP04_0001E,
                                          mobile_housing_percRank = dplyr::percent_rank(mobile_housing_perc),
                                          crowded_housing_cnt = DP04_0078E + DP04_0079E,
                                          crowded_housing_perc = crowded_housing_cnt/DP04_0001E,
                                          crowded_housing_percRank= dplyr::percent_rank(crowded_housing_perc),
                                          no_vehicle_perc = DP04_0058E/DP04_0001E,
                                          no_vehicle_percRank = dplyr::percent_rank(no_vehicle_perc)
                            ) %>% 
                            dplyr::rename(total_houseunits = DP04_0001E,
                                          total_households = DP02_0001E,
                                          total_inds = DP03_0001E,
                                          unemp_cnt = DP03_0005E,
                                          under18_cnt = DP05_0019E,
                                          disability_cnt = DP02_0072E,
                                          no_vehicle_cnt = DP04_0058E
                            )
                          
                        }
                        if(year %in% c(2017,2018)){
                          profile_vars$minority_cnt <- profile_vars$DP05_0001E -profile_vars[,white_cnt_var]
                          profile_vars <- profile_vars %>%
                            dplyr::mutate(single_parent_cnt=DP02_0007E + DP02_0008E,
                                          single_parent_perc = single_parent_cnt/DP02_0001E,
                                          single_parent_percRank = dplyr::percent_rank(single_parent_perc),
                                          unemp_rate_perc = DP03_0005E/DP03_0001E,
                                          unemp_rate_percRank = dplyr::percent_rank(unemp_rate_perc),
                                          under18_perc = DP05_0019E/DP05_0001E,
                                          under18_percRank = dplyr::percent_rank(under18_perc),
                                          disability_perc = DP02_0071E/DP02_0001E,
                                          disability_percRank = dplyr::percent_rank(disability_perc),
                                          minority_perc = minority_cnt/DP05_0001E,
                                          minority_percRank = dplyr::percent_rank(minority_perc),
                                          large_unit_housing_cnt = DP04_0012E+DP04_0013E,
                                          large_unit_housing_perc = large_unit_housing_cnt/DP04_0001E,
                                          large_unit_housing_percRank = dplyr::percent_rank(large_unit_housing_perc),
                                          mobile_housing_perc = DP04_0014E/DP04_0001E,
                                          mobile_housing_percRank = dplyr::percent_rank(mobile_housing_perc),
                                          crowded_housing_cnt = DP04_0078E + DP04_0079E,
                                          crowded_housing_perc = crowded_housing_cnt/DP04_0001E,
                                          crowded_housing_percRank= dplyr::percent_rank(crowded_housing_perc),
                                          no_vehicle_perc = DP04_0058E/DP04_0001E,
                                          no_vehicle_percRank = dplyr::percent_rank(no_vehicle_perc)
                            ) %>% 
                            dplyr::rename(total_houseunits = DP04_0001E,
                                          total_households = DP02_0001E,
                                          total_inds = DP03_0001E,
                                          unemp_cnt = DP03_0005E,
                                          under18_cnt = DP05_0019E,
                                          disability_cnt = DP02_0071E,
                                          no_vehicle_cnt = DP04_0058E
                            )
                        }
                        if(year %in% c(2015,2016)){
                          profile_vars$minority_cnt <- profile_vars$DP05_0001E - profile_vars[,white_cnt_var]
                          profile_vars <- profile_vars %>%
                            dplyr::mutate(single_parent_cnt=DP02_0007E + DP02_0008E,
                                          single_parent_perc = single_parent_cnt/DP02_0001E,
                                          single_parent_percRank = dplyr::percent_rank(single_parent_perc),
                                          unemp_rate_perc = DP03_0005E/DP03_0001E,
                                          unemp_rate_percRank = dplyr::percent_rank(unemp_rate_perc),
                                          under18_perc = (DP05_0001E-DP05_0018E)/DP05_0001E,
                                          under18_percRank = dplyr::percent_rank(under18_perc),
                                          disability_perc = DP02_0071E/DP02_0001E,
                                          disability_percRank = dplyr::percent_rank(disability_perc),
                                          
                                          
                                          minority_perc = minority_cnt/DP05_0001E,
                                          minority_percRank = dplyr::percent_rank(minority_perc),
                                          
                                          large_unit_housing_cnt = DP04_0012E+DP04_0013E,
                                          large_unit_housing_perc = large_unit_housing_cnt/DP04_0001E,
                                          large_unit_housing_percRank = dplyr::percent_rank(large_unit_housing_perc),
                                          
                                          mobile_housing_perc = DP04_0014E/DP04_0001E,
                                          mobile_housing_percRank = dplyr::percent_rank(mobile_housing_perc),
                                          crowded_housing_cnt = DP04_0078E + DP04_0079E,
                                          crowded_housing_perc = crowded_housing_cnt/DP04_0001E,
                                          crowded_housing_percRank= dplyr::percent_rank(crowded_housing_perc),
                                          no_vehicle_perc = DP04_0058E/DP04_0001E,
                                          no_vehicle_percRank = dplyr::percent_rank(no_vehicle_perc),
                                          under18_cnt = DP05_0001E-DP05_0018E
                            ) %>% 
                            dplyr::rename(total_houseunits = DP04_0001E,
                                          total_households = DP02_0001E,
                                          total_inds = DP03_0001E,
                                          unemp_cnt = DP03_0005E,
                                          
                                          disability_cnt = DP02_0071E,
                                          no_vehicle_cnt = DP04_0058E
                            )
                          
                        }
                        if(year<2015){
                          profile_vars$minority_cnt <- profile_vars$DP05_0001E - profile_vars[,white_cnt_var]
                          profile_vars <- profile_vars %>%
                            dplyr::mutate(single_parent_cnt=DP02_0007E + DP02_0008E,
                                          single_parent_perc = single_parent_cnt/DP02_0001E,
                                          single_parent_percRank = dplyr::percent_rank(single_parent_perc),
                                          unemp_rate_perc = DP03_0005E/DP03_0001E,
                                          unemp_rate_percRank = dplyr::percent_rank(unemp_rate_perc),
                                          under18_perc = (DP05_0001E-DP05_0018E)/DP05_0001E,
                                          under18_percRank = dplyr::percent_rank(under18_perc),
                                          disability_perc = DP02_0071E/DP02_0001E,
                                          disability_percRank = dplyr::percent_rank(disability_perc),
                                          
                                          
                                          minority_perc = minority_cnt/DP05_0001E,
                                          minority_percRank = dplyr::percent_rank(minority_perc),
                                          
                                          large_unit_housing_cnt = DP04_0012E+DP04_0013E,
                                          large_unit_housing_perc = large_unit_housing_cnt/DP04_0001E,
                                          large_unit_housing_percRank = dplyr::percent_rank(large_unit_housing_perc),
                                          
                                          mobile_housing_perc = DP04_0014E/DP04_0001E,
                                          mobile_housing_percRank = dplyr::percent_rank(mobile_housing_perc),
                                          crowded_housing_cnt = DP04_0078E + DP04_0079E,
                                          crowded_housing_perc = crowded_housing_cnt/DP04_0001E,
                                          crowded_housing_percRank= dplyr::percent_rank(crowded_housing_perc),
                                          no_vehicle_perc = DP04_0057E/DP04_0001E,
                                          no_vehicle_percRank = dplyr::percent_rank(no_vehicle_perc),
                                          under18_cnt = DP05_0001E-DP05_0018E
                            ) %>% 
                            dplyr::rename(total_houseunits = DP04_0001E,
                                          total_households = DP02_0001E,
                                          total_inds = DP03_0001E,
                                          unemp_cnt = DP03_0005E,
                                          disability_cnt = DP02_0071E,
                                          no_vehicle_cnt = DP04_0057E
                            )
                        }
                        
                        
                        detailed_vars <- getCensus('acs/acs5/',
                                                   year,
                                                   vars = c('B06009_002E',#no hs dip
                                                            'B16005_001E',
                                                            'B16005_007E',#limited english 5+
                                                            'B16005_008E',
                                                            'B16005_012E',
                                                            'B16005_013E',
                                                            'B16005_017E',
                                                            'B16005_018E', 
                                                            'B16005_022E',
                                                            'B16005_023E',
                                                            'B16005_029E',
                                                            'B16005_030E',
                                                            'B16005_034E',
                                                            'B16005_035E',
                                                            'B16005_039E',
                                                            'B16005_040E',
                                                            'B16005_044E',
                                                            'B16005_045E',
                                                            'B26001_001E' #group quarter_cnt
                                                   ),
                                                   region = 'zip code tabulation area:*') %>%
                          dplyr::mutate(limited_eng=(B16005_007E + B16005_008E +
                                                       B16005_012E + B16005_013E +
                                                       B16005_017E + B16005_018E +
                                                       B16005_022E + B16005_023E +
                                                       B16005_029E + B16005_030E +
                                                       B16005_034E + B16005_035E +
                                                       B16005_039E + B16005_040E +
                                                       B16005_044E + B16005_045E),
                                        limited_eng_perc = (limited_eng/B16005_001E),
                                        limited_eng_percRank = dplyr::percent_rank(limited_eng_perc)) %>%
                          dplyr::rename(no_hs_dip_cnt = B06009_002E,
                                        group_quarter_cnt = B26001_001E
                          )
                        
                        full_data_set <- dplyr::left_join(subject_vars,
                                                          profile_vars,
                                                          by = 'zip_code_tabulation_area') %>%
                          dplyr::left_join(
                            detailed_vars,
                            by = 'zip_code_tabulation_area') %>%
                          
                          dplyr::mutate(group_quarter_perc = group_quarter_cnt/total_population,
                                        group_quarter_percRank = dplyr::percent_rank(group_quarter_perc),
                                        spl_theme1 = below_150_pov_percRank+no_hs_dip_percRank+unemp_rate_percRank+
                                          house_burden_less75k_percRank+uninsured_percRank,
                                        rpl_theme1 = dplyr::percent_rank(spl_theme1),
                                        spl_theme2 = limited_eng_percRank+over65_percRank+under18_percRank+
                                          single_parent_percRank+disability_percRank,
                                        rpl_theme2 = dplyr::percent_rank(spl_theme2),
                                        spl_theme3 = minority_percRank,
                                        rpl_theme3 = dplyr::percent_rank(spl_theme3),
                                        spl_theme4 = group_quarter_percRank+large_unit_housing_percRank+
                                          mobile_housing_percRank+crowded_housing_percRank+no_vehicle_percRank,
                                        rpl_theme4 = dplyr::percent_rank(spl_theme4),
                                        spl_themes = spl_theme1+spl_theme2+spl_theme3+spl_theme4,
                                        rpl_themes = dplyr::percent_rank(spl_themes)
                          )
                        full_data_set[,grepl('^B2|^B1|^B0|^DP|^S1|^S0|^S2|state',
                                             colnames(full_data_set))] <- NULL
                        data.frame(year = year,
                                   full_data_set)
                        
                      }
  write.csv(svi_data,
            'svi_data_total.csv')
  svi_data
}







