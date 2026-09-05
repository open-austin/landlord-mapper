
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

gcs_save_file_upload = function(file_name,
                                object_used){
  
  if(grepl('.rds',file_name)){
    readr::write_rds(object_used,
                     file_name
    )
  }
  if(grepl('.csv', file_name)){
    write.csv(object_used,
              file_name)
  }
  
  old_models<-gcs_list_objects(prefix =  file_name,
                               detail = 'summary')
  if(nrow(old_models)>0){
    delete_old_files <- sapply(old_models$name, 
                               function(object_used){gcs_delete_object(object_used)})
  }
  upload_new_files <- gcs_upload(file_name,
                                 name = file_name,
                                 predefinedAcl = 'bucketLevel')
}

name_clean = function(data_used){
  data_used <- toupper(iconv(data_used,to='UTF-8',
                             sub = 'byte'))
  
  data_used <-gsub(paste(sapply(c(financial_markers_base,'THE','AND','IN','ON','OF','CO','US','TRUE','FALSE'#,'[[:digit:]]{1}'
                                  ), 
                                function(s){sprintf('(^|[^[:alnum:]])%s($|[^[:alnum:]])',s)}),
                         collapse = '|'),
                   ' ',
                   data_used,
                   useBytes = TRUE)
  data_used <- gsub('[[:punct:]]',
                        '',
                        data_used,
                        useBytes = TRUE)
  data_used <- gsub('([[:space:]])[[:digit:]]{7,}([[:space:]])',
                    ' ',
                    data_used,
                    useBytes = TRUE)
  data_used <- gsub('LIFE ESTATE|ESTATE|REAL ESTATE|SERVICE(S)?|RODRIGUEZ|ATTN|AD VALOREM|TAX DEPT|PROPERTY TAX|TAX DEPARTMENT|TAX|DBA|FBO|FKA|LIVING|TRUST|REVOCABLE|CORP|CORPORATION|INCORPORATE(D)?|LIMITED',
                    '',
                    data_used,
                    useBytes = TRUE)
  #INVESTMENT(S)?
  # ENTERPRISE(S)?
  #HOLDING(S)?
  #GROUP(S)?
  #PROPERT(IES|Y)
  #COMPANY
  #ASSOCIATION ASSN
  print('sub1')
  # data_used <- gsub('TRST',
  #                   'TRUST',
  #                   data_used,
  #                   useBytes = TRUE)
  
  data_used <- gsub('MANAGE((R(S)?)|MENT)?',
                      'MGT',
                      data_used,
                      useBytes = TRUE)
  data_used <- gsub('PARTNER(S(HIP)?)?',
                    'PTSHP',
                    data_used,
                    useBytes = TRUE)
    
  data_used <- gsub('ASSOCIATION|ASSN',
                    'ASSOC',
                    data_used,
                    useBytes = TRUE)
  print('sub2')
  data_used <-gsub(paste(sapply(c('I','II','III','IV','V','VI','VII','VIII','IX','X',
                                  1,2,3,4,5,6,7,8,9,10,
                                  'ONE','TWO','THREE','FOUR','FIVE','SIX','SEVEN','EIGHT','NINE','TEN'
                                  # 'A','B','C','D','F','G',
                                  # 'H','I','J','K','L','M','O','P','Q','R','T',
                                  # 'U','X','Y','Z'
                                  ),
                                function(s){sprintf('(^|[^[:alnum:]])%s($|[^[:alnum:]])',
                                                    s)
                                  }
                                ),
                         collapse = '|'),
                   ' ',
                   data_used,
                   useBytes = TRUE)
  print('sub3')
  data_used <- trimws(gsub('[[:space:]]{2,}',
                               ' ',
                               data_used,
                               useBytes = TRUE
                           ))
  data_used
}
address_clean = function(data = austin_parcel_data_merged,
                         col = 'situs_address'){
  # print(col)

  data_used <- toupper(iconv(data[,col],to='UTF-8'))
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

agent_string_sub = function(result_string, string_list){
  lapply(string_list,
         function(agent_string){
           # if(nchar(agent_string)==0|agent_string==""){
           #   return()
           # }
           result_string <<- gsub(agent_string,
                                  '',
                                  result_string,
                                  useBytes = TRUE)
         })
  result_string
}
#"CORPORATION SERVICE COMPANY D/B/A CSC-LAWYERS INCO"  
reg_agent_string_gen = function(data_used,
                                cuts_used){
  
  high_own_num_addrs <- tapply(data_used$owner_name,
                               data_used$owner_address,
                               function(names){length(unique(names))})
  
  high_own_num_addrs <- high_own_num_addrs[order(high_own_num_addrs,
                                                 decreasing = TRUE)]
  high_own_num_addrs_ret <- c(names(high_own_num_addrs)[(grepl('PO BOX',
                                                              names(high_own_num_addrs))) & 
                                                          (high_own_num_addrs>60)],
                              names(high_own_num_addrs)[(nchar(names(high_own_num_addrs))>25) &
                                                          (high_own_num_addrs>90)]
                              )
  # trust_addr <- unique(data_used$owner_address[which(grepl('TRUST',
  #                                                    data_used$owner_name))])
  # 
  # trust_addr_cuts <-  cut(1:length(trust_addr),
  #                         cuts_used)
  # print('trust')
  # trust_addr_string <- lapply(levels(trust_addr_cuts),
  #                                       function(level_used){
  #                                         results <-trust_addr[trust_addr_cuts==level_used]
  #                                         results <- results[which(sapply(results,nchar)>22)]
  #                                         results <- paste(results[which(results!="")],
  #                                                          collapse = '|')
  #                                         results
  #                                       })
  
  # print('trust')
  # print(trust_addr_string)
  # trust_prop <- tapply(owner_data_total_supp$owner_name,
  #                      owner_data_total_supp$owner_address,
  #                      function(names){sum(grepl('TRUST',
  #                                                toupper(names)))/
  #                          length(names)}
  #                      )
  # trust_row <- sapply(names(trust_prop[trust_prop>0.8]),
  #                     function(name){nrow(dplyr::filter(owner_data_total_supp,
  #                                                       owner_address==name))})
  
  registered_agent_inds <- which(c(grepl('PLLC|P([[:punct:]]|[[:space:]])+L([[:punct:]]|[[:space:]])+L([[:punct:]]|[[:space:]])+C|RYAN LLC|ASSOC|CONSULT|COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST|LAW|ADVIS|REGIRED',
                                         data_used$corp_registered_agent_name,
                                         ignore.case = TRUE)
  ))
  # print(registered_agent_inds)
  # liminal_chars <- ''
  agent_inds <- which(grepl('PLLC|P([[:punct:]]|[[:space:]])+L([[:punct:]]|[[:space:]])+L([[:punct:]]|[[:space:]])+C|RYAN LLC|ASSOC|CONSULT|COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST|LAW|ADVIS|REGIRED',
                            data_used$agent_name,
                            ignore.case = TRUE))
  
  registered_agent_inds_cuts <- cut(1:length(registered_agent_inds),
                                    cuts_used)
  # print(registered_agent_inds_cuts)
  agent_inds_cuts <- cut(1:length(agent_inds),
                         cuts_used)
  # print(agent_inds_cuts)
  
  # print('reg')
  registered_agent_add_string <- lapply(levels(registered_agent_inds_cuts),
                                        function(level_used){
                                          results <- unlist(unique(data_used[registered_agent_inds[registered_agent_inds_cuts==level_used],
                                                                      'corp_registered_agent_mail_add']))
                                          # results <- results[which(sapply(results,nchar)>22)]
                                          results <- paste(results[which(results!="")],
                                                           collapse = '|')
                                        })
  # print('agent')c
  # print('reg agent add')
  # print(registered_agent_add_string)
  agent_add_string <- lapply(levels(agent_inds_cuts),
                             function(level_used){
                               results <- unlist(unique(data_used[agent_inds[agent_inds_cuts==level_used],
                                                           'agent_address']))
                               # ret_inds <- which(sapply(results,nchar)>20)
                               # if(length(ret_inds)>0){
                               results <- results[which(sapply(results,nchar)>22)]
                               # }

                               results <- paste(results[which(results!="")],
                                                collapse = '|')
                             })
  
  # print('agent add')
  # print(agent_add_string[[1]])
  registered_agent_name_string <- lapply(levels(registered_agent_inds_cuts),
                                         function(level_used){
                                           results <-unlist(unique(data_used[registered_agent_inds[registered_agent_inds_cuts==level_used],
                                                                       'corp_registered_agent_name']))
                                           results <- results[which(sapply(results,nchar)>3)]
                                           results <- paste(results[which(results!="")],
                                                            collapse = '|')
                                         })
  # print(' reg agent name')
  # print(registered_agent_name_string[[1]])
  agent_name_string <- lapply(levels(agent_inds_cuts),
                              function(level_used){
                                results <- trimws(unlist(unique(data_used[agent_inds[agent_inds_cuts==level_used],
                                                            'agent_name'])))
                                # print(results)
                                results <- results[which(sapply(results,nchar)>3)]
                                results <- paste(results[which(results!="")],
                                                 collapse = '|')
                              })
  
  # print('agent name')
  # print(agent_name_string[[1]])
  misc_name_string <-list(paste(c('D3 REAL ESTATE CONSULTANTS',
                                  'GILL, DENSON & COMPANY',
                                  'L L CASEY & CO',
                                  '^US$',
                                  'KE ANDREWS',
                                  'COMMERCIAL',
                                  'UNAVAILABLE',
                                  'FBO',
                                  'EQUITY TRUST COMPANY',
                                  'TAX EXEMPT',
                                  'NONE',
                                  '00000',
                                  'UNKNOWN',
                                  'OWNER',
                                  'DBA',
                                  'ADDRESS',
                                  'CUSTODIAN',
                                  'UNKNOWN CITY',
                                  'UNKNOWN STATE',
                                  'ZIP',
                                  'PROPERTY TAX (DEPARTMENT|DEPT)',
                                  'ATTN',
                                  'AVAILABLE UPON.+REQUEST',
                                  'MICHEL.+ROGERS.+MALONEY.+'),
                                collapse = '|'))
  # print('misc name')
  # print(misc_name_string)
  misc_add_string <- list(paste(c('815 BRAZOS.+AUSTIN TX 78701',
                                  '2595 DALLAS P.+FRISCO TX 75034',
                                  '401 TOM LANDRY H.+DALLAS TX 75266',
                                  '5900 BALCONES.+AUSTIN.+',
                                  'PO BOX 4090.+SCOTTSDALE AZ 85261',
                                  'PO BOX 592226.+SAN ANTONIO TX 78259',
                                  '901.+MOPAC.+AUSTIN TX 78746',
                                  '901.+MO PAC.+AUSTIN TX 78746',
                                  '3225 MCLEOD DR.+LAS VEGAS NV 89121',
                                  '17350 STATE H.+HOUSTON TX 77064',
                                  '304 S JONES.+LAS VEGAS NV 89107',
                                  '3839 BEE CAVE.+AUSTIN TX 78746 US',
                                  high_own_num_addrs_ret),
                                collapse = '|'))
  
  
  # print('misc add')
  # print(misc_add_string)
  result <- list(addresses = c(#trust_addr_string,
                             registered_agent_add_string,
                           agent_add_string,
                           misc_add_string),
               names = c( registered_agent_name_string,
                          agent_name_string,
                          
                          misc_name_string))
  result$addresses <- result$addresses[which(unlist(lapply(result$addresses, nchar))>0)]
  result$names <- result$names[which(unlist(lapply(result$names, nchar))>0)]
  return(result)
}


#row.names(d)[[28]]
# [1] "906 W JAMES ST LLC GRANT MCGREGOR 3267 BEE CAVES RD 107151 AUSTIN TX 78746 906 W JAMES ST LLC TEXAN"
situs_owner_string_gen = function(owner_data){
  
  owner_data <-dplyr::filter(owner_data,
                             ((is_financialized ==TRUE) &
                                (is_owner_occupied==FALSE))|
                               (property_units>4),
                             property_units!=0,
                             # nchar(owner_address)>20,
                             !is.na(property_units))
  
  print(dim(owner_data))
  # owner_data <- head(owner_data,20000)
  registered_agent_string_list <- reg_agent_string_gen(owner_data,
                                                       10)
  shared_owner_data <- mori::share(owner_data)
  # owner_data <- head(owner_data,100)
  # situs_pIDs <- unique(owner_data$situs_pID)
  cl <- new_cluster(parallel::detectCores())
  cluster_assign(cl,
                 registered_agent_string_list = registered_agent_string_list,
                 agent_string_sub = agent_string_sub,
                 name_clean = name_clean,
                 reg_agent_string_gen = reg_agent_string_gen,
                 financial_markers_base = financial_markers_base,
                 financial_marker_base_string = financial_marker_base_string)
  print(Sys.time())
  # registerDoFuture()
  # 3312
  # plan(multisession, workers =parallel::detectCores() )
  print('clean')
  situs_owner_strings <- shared_owner_data %>%
    group_by(situs_pID,
             situs_address) %>%
    partition(cl) %>%
    summarise(strings_used = {
      # print(unique(situs_pID))
      # print(unique(situs_address))
      unique_owners <- toupper(unique(owner_name))
      unique_owner_add <- toupper(unique(owner_address))
      corp_name <- toupper(unique(corp_business_name))
      corp_address <- toupper(unique(corp_mail_address))
      registered_agent <- toupper(unique(corp_registered_agent_name))
      registered_agent_add <- toupper(unique(corp_registered_agent_mail_add))
      message('upper')
      # agent_name <- toupper(unique(agent_name))
      # agent_address <- toupper(unique(agent_address))
      scraped_owner_address = toupper(unique(owner_address_scraped))
      scraped_owner = toupper(unique(owner_name_scraped))
      
      unique_entities <- na.omit(unique(c(unique_owners,
                                          unique_owner_add,
                                          corp_name,
                                          corp_address,
                                          registered_agent,
                                          registered_agent_add,
                                          # agent_name,
                                          # agent_address,
                                          scraped_owner_address,
                                          scraped_owner)
      )
      )
      
      result_string <- paste(unique_entities,
                             collapse = ' ')
      
      # result_string <-gsub(financial_marker_base_string,
      #                      '',
      #                      result_string)
      message('string')
      result_string <- agent_string_sub(result_string,
                                        registered_agent_string_list$addresses)
      message('reg add sub')
      result_string <- agent_string_sub(result_string,
                                        registered_agent_string_list$names)
      message('reg name sub')
      result_string <- name_clean(result_string)
      message('name clean')
      result_string[length(result_string)]
      
    }) %>%
    collect()
  
  print('done')
  return(situs_owner_strings)
}

situs_owner_string_dist_matrix = function(situs_owner_strings, 
                                          owner_data){
  # owner_data <- head(owner_data,
  #                    20000)
  pIDs_used <- unique(dplyr::filter(owner_data, 
                                    ((is_financialized ==TRUE) & 
                                       (is_owner_occupied==FALSE))|
                                      (property_units>4),
                                    property_units!=0,
                                    # nchar(owner_address)>20,
                                    !is.na(property_units))$situs_pID)
  print(length(pIDs_used))
  strings_used <- which(situs_owner_strings$situs_pID %in%
                          pIDs_used)
  strings_used_final <- situs_owner_strings$strings_used[strings_used]
  
  
  names(strings_used_final) <- paste(situs_owner_strings$situs_pID[strings_used],
                                     situs_owner_strings$situs_address[strings_used],
                                     sep = '|')
  
  print(length(strings_used_final))
  # readr::write_rds(strings_used_final,
  #                  'strings_used_final.rds')
  print(Sys.time())
  registerDoFuture()
  plan(multisession)
  # mirai::daemons(parallel::detectCores()-1)
  # daemons(parallel::detectCores())
  # mirai::mirai_map(1:100,#length(strings_used_final),
  #                  function(ind) {
  #                    string = strings_used_final[ind]
  #                    dist_vals <- stringdist::stringdist(string,
  #                                                        strings_used_final,
  #                                                        useBytes =TRUE,
  #                                                        method = 'cosine',
  #                                                        q=1)
  #                    neighbors <- which(dist_vals<0.02)
  #                    neighbors <- neighbors[which(neighbors>ind)]
  #                    rowInds <<- append(rowInds,
  #                                      rep(ind,
  #                                          length(neighbors)
  #                                      ))
  #                    colInds <<- append(colInds,
  #                                      c(neighbors))
  #                    return(NULL)
  #                  },
  #                  strings_used_final = strings_used_final)[.progress]
  # mirai::daemons(0)
  inds_found <- foreach(ind = 1:length(strings_used_final)
  ) %dopar% {
    string = strings_used_final[ind]
    dist_vals <- stringdist::stringdist(string,
                                        strings_used_final,
                                        useBytes =TRUE,
                                        method = 'cosine',
                                        q=2)
    neighbors <- which(dist_vals<0.25)
    neighbors <- neighbors[which(neighbors>ind)]
    rowInds_used <- rep(ind,
                        length(neighbors)
    )
    colInds_used <-  c(neighbors)
    return(list(rowInds = rowInds_used,
                colInds = colInds_used))
  }
  # print(head(inds_found))
  rowInds <- unlist(lapply(inds_found, '[[',1))
  colInds <- unlist(lapply(inds_found, '[[',2))
  print(head(rowInds,100))
  print(head(colInds,100))
  # readr::write_rds(rowInds,'rowInds.rds')
  # readr::write_rds(colInds,'colInds.rds')
  
  
  situs_owner_cosine_dist_matrix <- Matrix::sparseMatrix(i = rowInds,
                                                         j = colInds,
                                                         x = 1L,
                                                         dims = c(length(strings_used_final),
                                                                  length(strings_used_final)),
                                                         dimnames = list(names(strings_used_final),
                                                                         names(strings_used_final)),
                                                         symmetric = TRUE
  )
  # situs_owner_cosine_dist_matrix <- as.matrix(stringdist::stringdistmatrix(unlist(strings_used_final),
  #                                          q = 2,
  #                                          method = 'cosine',
  #                                          useName = 'names'))
  
  situs_owner_cosine_dist_matrix
}

# situs_neighbor_cov = function(situs_owner_cosine_dist_matrix){
#   Rfast::cova(q3_dist_matrix, large = TRUE)
# }

situs_neighor_gen_clean = function(owner_data_used){
  
  # owner_data_used <- head(owner_data_used,
  #                         20000)
  registered_agent_string_list <- reg_agent_string_gen(owner_data_used,
                                                       100)
  # readr::write_rds(registered_agent_string_list,'registered_agent_string_list.rds')
  # print(registered_agent_string_list)
  pIDs_used <- unique(dplyr::filter(owner_data_used, 
                                    ((is_financialized ==TRUE) & 
                                       (is_owner_occupied==FALSE))|
                                      (property_units>4),
                                    property_units!=0,
                                    # nchar(owner_address)>20,
                                    !is.na(property_units))$situs_pID)
  
  addresses_used <- unique(dplyr::filter(owner_data_used, 
                                         ((is_financialized ==TRUE) & 
                                            (is_owner_occupied==FALSE))|
                                           (property_units>4),
                                         property_units!=0,
                                         # nchar(owner_address)>20,
                                         !is.na(property_units))$situs_address)
  # print(registered_agent_name_string_2)
  
  # if(any(grepl('owner_data_used_proc.rds', list.files()))){
  #   
  #   owner_data_used <- readRDS('owner_data_used_proc.rds')
  # }
  print(Sys.time())
  # else{
  cl <- multidplyr::new_cluster(parallel::detectCores())
  owner_data_used$legallocationdesc <- NULL
  
  
  print(dim(owner_data_used))
  
  cluster_assign(cl,
                 registered_agent_string_list = registered_agent_string_list,
                 name_clean = name_clean,
                 financial_markers_base = financial_markers_base,
                 agent_string_sub = agent_string_sub,
                 reg_agent_string_gen = reg_agent_string_gen,
                 financial_marker_base_string = financial_marker_base_string)
  
  owner_data_used$row_num <- 1:nrow(owner_data_used)
  
  owner_data_used_share <- mori::share(owner_data_used)
  owner_data_used <- owner_data_used_share %>%
    partition(cl) %>%
    mutate(owner_address = name_clean(agent_string_sub(toupper(owner_address),
                                            registered_agent_string_list$addresses)),
           corp_mail_address = name_clean(agent_string_sub(toupper(corp_mail_address),
                                                registered_agent_string_list$addresses)),
           owner_address_scraped = name_clean(agent_string_sub(toupper(owner_address_scraped),
                                                    registered_agent_string_list$addresses)),
           
           corp_registered_agent_mail_add = name_clean(agent_string_sub(toupper(corp_registered_agent_mail_add),
                                                              registered_agent_string_list$addresses)),
           # agent_address = agent_string_sub(toupper(agent_address),
           #                                  registered_agent_string_list),
           
           corp_business_name = name_clean(agent_string_sub(toupper(corp_business_name),
                                                 registered_agent_string_list$names)),
           owner_name = name_clean(agent_string_sub(toupper(owner_name),
                                         registered_agent_string_list$names)),
           owner_name_scraped = name_clean(agent_string_sub(toupper(owner_name_scraped),
                                                 registered_agent_string_list$names)),
           corp_registered_agent_name =name_clean(agent_string_sub(toupper(corp_registered_agent_name),
                                                         registered_agent_string_list$names))
           # agent_name = agent_string_sub(toupper(agent_name),
           #                               registered_agent_string_list),
    ) %>%
    collect()
  # print(dim(owner_data_used))
  owner_data_used <- owner_data_used[order(owner_data_used$row_num,
                                           decreasing = FALSE),]
  
  owner_data_used$row_num <- NULL
  # readr::write_rds(owner_data_used,
  #                  'owner_data_used_proc.rds')
  # parallel::stopCluster(cl)
  # }
  
  # 
  print(Sys.time())
  owner_data_used
}

situs_neighor_gen = function(situs_owner_cosine_dist_matrix,
                             owner_data_used){
  
  owner_data_used <- data.frame(owner_data_used)
  # owner_data_used$row_num <- 1:nrow(owner_data_used)
  print(dim(owner_data_used))
  # registered_agent_string_list <- reg_agent_string_gen(owner_data_used,
  #                                                      100)
  print(Sys.time())
  pIDs_used <- unique(dplyr::filter(owner_data_used, 
                                    ((is_financialized ==TRUE) & 
                                       (is_owner_occupied==FALSE))|
                                      (property_units>4),
                                    property_units!=0,
                                    # nchar(owner_address)>20,
                                    !is.na(property_units))$situs_pID)
  print(Sys.time())
  addresses_used <- unique(dplyr::filter(owner_data_used, 
                                         ((is_financialized ==TRUE) & 
                                            (is_owner_occupied==FALSE))|
                                           (property_units>4),
                                         property_units!=0,
                                         # nchar(owner_address)>20,
                                         !is.na(property_units))$situs_address)
  print(dim(owner_data_used %>%
              filter((situs_pID %in% pIDs_used),
                     (situs_address %in% addresses_used))))
  print(Sys.time())
  # readr::write_rds(owner_data_used,'owner_data_used_proc.rds')
  # 
  gc()
  cl <- multidplyr::new_cluster(round(3*parallel::detectCores()/4))
  # 
  
  # valid_owner_address <- nchar(owner_data_used$owner_address)>20
  owner_data_used_names <-paste(' ',
                                paste(owner_data_used$owner_name_scraped,
                                                  owner_data_used$owner_name,
                                                  owner_data_used$corp_business_name,
                                                  owner_data_used$corp_registered_agent_name
                                                  ),
                                ' ',
                                sep = '')
  owner_data_used_addrs <-paste(' ',
                                paste(owner_data_used$owner_address_scraped,
                                                  owner_data_used$owner_address,
                                                  owner_data_used$corp_mail_address,
                                                  owner_data_used$corp_registered_agent_mail_add
                                                  ),
                                ' ',
                                sep = '')
  multidplyr::cluster_assign(cl,
                             pIDs_used = pIDs_used,
                             addresses_used = addresses_used,
                             situs_owner_cosine_dist_matrix = situs_owner_cosine_dist_matrix,
                             owner_data_used = owner_data_used
                             # owner_data_used_names = owner_data_used_names,
                             # owner_data_used_addrs = owner_data_used_addrs
                             )
  # readr::write_rds(owner_data_used_addrs,
  #                  'owner_data_used_addrs.rds' )
  # readr::write_rds(owner_data_used_names,
  #                  'owner_data_used_names.rds')
  owner_data_used_share <- mori::share(owner_data_used)
  
  situs_neighbor_ind <- owner_data_used_share %>%
    filter((situs_pID %in% pIDs_used),
             (situs_address %in% addresses_used)) %>%
    group_by(situs_pID,
             situs_address) %>%
    multidplyr::partition(cl) %>%
    summarise(indexes_used =paste(unique(row_num),collapse = ' '),
              situs_neighbors = {
                # (grepl(gsub("^$",
                #             NA,
                #             paste(unique(owner_address),
                #                   collapse = '|'),
                #             useBytes = TRUE
                # ),
                # owner_data_used_addrs
                # ))
      names_used <- unique(na.omit(c(owner_name_scraped,
                                   owner_name,
                                   corp_business_name,
                                   corp_registered_agent_name)))
    
      names_used <- names_used[which(nchar(names_used)>8)]
      if(length(names_used)>0){
        name_string_used <- sprintf(' %s ',
                                    paste(names_used,
                                          collapse = ' | '))
      }
      else{
        name_string_used <- character()
      }
      
      
      # readr::write_rds(name_string_used,'name_string_used.rds')
      
      addrs_used <- unique(na.omit(c(owner_address_scraped,
                                     owner_address,
                                     corp_mail_address,
                                     corp_registered_agent_mail_add
                                     )))
      
      # print('addr_used')
      # print(addrs_used)
      addrs_used <- addrs_used[which(nchar(addrs_used)>22)]
      # print(addrs_used)
      if(length(addrs_used)>0){
        addr_string_used <- sprintf(' %s ',
                                    paste(addrs_used,
                                          collapse = ' | '))
      }
      else{
        addr_string_used <- character()
      }
      
      
      # readr::write_rds(addr_string_used,
      #                  'addr_string_used.rds')
      
      
      name_neigh_matches <- stringi::stri_count_regex(owner_data_used_names,
                                                      name_string_used,
                                                      opts_regex = stringi::stri_opts_regex(case_insensitive = TRUE)
                                                      )
      name_neigh_inds  <- which(name_neigh_matches>0)
      name_neighs <-name_neigh_matches[name_neigh_inds]
      names(name_neighs) <- name_neigh_inds
      # 
      # readr::write_rds(name_neighs,
      #                  'name_neighs.rds')
      
      # print( 'name')
      # print(name_neighs)
      addr_neigh_matches <- stringi::stri_count_regex(owner_data_used_addrs,
                                                      addr_string_used,
                                                      opts_regex = stringi::stri_opts_regex(case_insensitive = TRUE)
                                                     )
      addr_neigh_inds  <- which(addr_neigh_matches>0)
      
      addr_neighs  <- addr_neigh_matches[addr_neigh_inds]
      names(addr_neighs) <- addr_neigh_inds
      # print('addr')
      # print(addr_neighs)
      
      # readr::write_rds(addr_neighs,
      #                  'addr_neighs.rds')
      
      # reg_agent_addr_neighs_uniq <- sprintf( ' %s ',
      #                                   paste(unique(na.omit(corp_registered_agent_mail_add)),
      #                                         collapse = '|')
      #                                   )
      # 
      # if(nchar(reg_agent_addr_neighs_uniq)>2){
      #   
      #   reg_agent_addr_neighs <- which(( stringi::stri_detect_regex(owner_data_used_addrs,
      #                                                               reg_agent_addr_neighs_uniq,
      #                                                               opts_regex = stringi::stri_opts_regex(case_insensitive = TRUE))) &
      #                                    (nchar(corp_registered_agent_mail_add)>22)
      #                                  )
      # }
      # else{
      #   reg_agent_addr_neighs <- integer()
      # }
      
      # print('8')
      # print(reg_agent_add_neighs)
      # agent_name_neighs <- which(owner_data_used$agent_name %in%
      #                              na.omit(gsub("^$",
      #                                           NA,
      #                                           unique(agent_name)
      #                              )
      #                              )
      # )
      # print('9')
      # print(agent_name_neighs)
      # agent_add_neighs <- which(owner_data_used$agent_address %in%
      #                             na.omit(gsub("^$",
      #                                          NA,
      #                                          unique(agent_address)
      #                             )
      #                             )
      # )
      
      # print('exact matches done')
      # print(agent_add_neighs)
      if(unique(situs_pID) %in% pIDs_used){
        # print(paste(unique(situs_pID),
        #             unique(situs_address),
        #             sep = '\\|'))
        situs_dist_ind <- which(grepl(paste(unique(situs_pID),
                                            unique(situs_address),
                                            sep = '\\|'),
                                      colnames(situs_owner_cosine_dist_matrix)
                                      ))
        # print('situs_dist')
        # print(situs_dist_ind)
        dist_inds <- tryCatch({
          unique(unlist(sapply(situs_dist_ind,
                               function(ind){
                                 c(which(situs_owner_cosine_dist_matrix[ind,]==1),
                                   which(situs_owner_cosine_dist_matrix[,ind]==1))
                               })))
          # unique(c(which(situs_owner_cosine_dist_matrix[situs_dist_ind,]==1),
          #          which(situs_owner_cosine_dist_matrix[,situs_dist_ind]==1))
          #        )
        },
        error = function(cond){
          cond
        })
        # print(dist_inds)
        if('error' %in% class(dist_inds)){
          # print('error')
          dist_inds <- unique(c(unlist(apply(as.data.frame.matrix(situs_owner_cosine_dist_matrix[situs_dist_ind,]),1,
                                             function(row){which(row==1)})),
                                unlist(apply(as.data.frame.matrix(situs_owner_cosine_dist_matrix[,situs_dist_ind]),2,
                                             function(col){which(col==1)}))
          ))
          # print(dist_inds)
        }
        
        
        
        dist_neigh_pID <- sapply(colnames(situs_owner_cosine_dist_matrix)[dist_inds],
                                 function(col){strsplit(col, 
                                                        split = '|',
                                                        fixed = TRUE)[[1]][1]})
        # print('dist neigh pid')
        dist_neigh_address <- sapply(colnames(situs_owner_cosine_dist_matrix)[dist_inds],
                                     function(col){strsplit(col, 
                                                            split = '|',
                                                            fixed = TRUE)[[1]][2]})
        # print(dist_neigh_pID)
        # print(dist_neigh_address)
        # print('dist neigh')
        dist_neighs <- which((owner_data_used$situs_pID %in% dist_neigh_pID) &
                               owner_data_used$situs_address %in% dist_neigh_address)
        # readr::write_rds(dist_neighs,
        #                  'dist_neighs.rds')
      }
      else{
        dist_neighs <- integer()
      }
      # print('dist inds')
      # print(dist_neighs)
      # print('total neighbors done')
      neighbors <- na.omit(c(names(name_neighs),
                             names(addr_neighs),
                             # agent_name_neighs,
                             # agent_add_neighs,
                             dist_neighs))
      
      
      # readr::write_rds(neighbors,
      #                  'neighbors.rds')
      # print(neighbors)
      if(length(neighbors)>0){
        neighbors <- t(neighbors[order(neighbors)])
        neighbors <- unlist(neighbors[!is.na(neighbors)])
        n_uniq <- unique(neighbors)
        # print('neigh')
        # print(neighbors)
        # print('uniq')
        # print(n_uniq)
        neighbors_final <- n_uniq[sapply(n_uniq,
                                         function(n_used){sum(c(as.numeric(name_neighs[which(names(name_neighs)==n_used)]),
                                                                as.numeric(addr_neighs[which(names(addr_neighs)==n_used)]),
                                                                as.numeric(dist_neighs) %in% 
                                                                  n_used))>1}
                                         )]
        neighbors_final_edge_weights <- sapply(neighbors_final,
                                                               function(n_used){
                                                                 
                                                                 sum(c(as.numeric(name_neighs[which(names(name_neighs)==n_used)]),
                                                                       as.numeric(addr_neighs[which(names(addr_neighs)==n_used)]),
                                                                       as.numeric(dist_neighs) %in% 
                                                                         n_used))
                                                                 })
        }
      else{
        # print(neighbors)
        neighbors_final <- integer()
        neighbors_final_edge_weights <- integer()
      }
      
      
      # print(neighbors_final)
      c(paste(paste(neighbors_final,
                    neighbors_final_edge_weights,
                    sep='-'),
              collapse = ' ')
        )
    }) %>%
    collect()
  # parallel::stopCluster(cl)
  print(Sys.time())
  
  situs_neighbor_ind
  
}
# second_inds <- c(-1)
situs_neighor_gen_final = function(owner_data_used,
                                   situs_neighbor_ind){
  # readr::write_rds(situs_neighbor_ind,
  #                  'situs_neighbor_ind.rds')
  # print(Sys.time())
  print(dim(owner_data_used))
  print(dim(situs_neighbor_ind))
  # print('neigh'
  # iterative_add = function(inds, 
  #                          neighbors,
  #                          situs_neighbors,
  #                          situs_neighbors_padded,
                           # situs_neighbor_ind = situs_neighbor_ind,
                           # depth = 2 ){
    # neighbors <- as.character(neighbors)
    # print(depth)
    
    # print(inds)
    # print(neighbors)
    # print(depth)
    # print(length(inds))
    # print(length(neighbors))
    # dup_inds <- stringi::stri_detect_regex(inds,
    #                                        sprintf('^%s$',
    #                                                paste(neighbors,collapse = '$|^')
    #                                                ))
    # if(length(dup_inds)>0){
    #   inds <- inds[!dup_inds]
    # }
    
    
    # if(length(inds)==0){
    #   return(NA)
    # }
    # if(length(inds)>100){
    #   return(inds)
    # }
    # result <-unique(as.numeric(  
    #   c(unlist(sapply(inds,
    #                 
    #                 function(ind){
    #                   if(Rfast::is_element(neighbors, ind)){#ind %in% neighbors){
    #                     if(depth!=2){
    #                       return(NULL)
    #                     }
                        # # break
                      #   return(ind)
                      # }
                      # inner_result_inds <- which(stringi::stri_detect_fixed(situs_neighbors_padded,
                      #                                                       sprintf( ' %s ',
                      #                                                                ind),
                      #                                                       # max_count = length(inds)*2,
                      #                                                       opts_fixed = stringi::stri_opts_fixed(case_insensitive = TRUE))
                      #                            )
                      # 
                      # 
                      # inner_result <-unlist(situs_neighbors[inner_result_inds])
                      # inner_result <- inner_result[!(inner_result %in% inds)]
                      # if((length(inds>500)) & (depth!=1)){
                      # 
                      # neighbors <<- unique(c(neighbors,
                      #                          inner_result[inner_result == ind]))
                      # }
                      
               #        return(inner_result)
               #      })
               # ))))
    # print('mid')
    # print(result)
    
    
    # if(depth!=0){
    #   sec_run <- result[!(sapply(result, function(result_used){ Rfast::is_element(neighbors,
    #                                                                               result_used)}))]
      # [sapply(result,
      #                          function(result_used){
      #                            Rfast::is_element(c(inds,
      #                                                neighbors),
      #                                              result_used
      #                                              )##
      #                          }) ]#
      # print(sec_run)
      
      # if(length(sec_run)>0){
      #   result <- unique(c(result,
      #                      iterative_add(sec_run,
      #                                    c(inds,
      #                                      neighbors),
                                         
                                         # paste('',
                                         #       paste(inds,
                                         #         collapse = ' '),
                                         #       neighbors),
                                         # situs_neighbors,
                                         # situs_neighbors_padded,
                                         # depth = depth-1)))
    #   }
    #   
    # }
    # result <- unique(c(result,
    #                    inds))
    # result[order(result)]
  # }
  
  # registerDoFuture()
  # plan(multisession)
  
  print(Sys.time())
  # indexes_used <-  foreach(index = 1:nrow(situs_neighbor_ind)
  #                                   ) %dopar% {
  #                                       as.numeric(which(
  #     (owner_data_used$situs_pID==situs_neighbor_ind$situs_pID[index]) & 
  #       (owner_data_used$situs_address==situs_neighbor_ind$situs_address[index])
  #                                       ))
  #                                       }
  # print(Sys.time())
  # gcs_save_file_upload('indexes_used.rds',
  #                      indexes_used)
  # owner_data_used <- mori::share(owner_data_used)
  # situs_neighbor_ind <- mori::share(situs_neighbor_ind)
  situs_neighbors <-mori::share(lapply(strsplit(situs_neighbor_ind$situs_neighbors, split = ' '),
                                       function(neighbors){
                                         sapply(neighbors,
                                                function(n_used){
                                                  strsplit(n_used,split='-')[[1]][1]})
                                       }) 
                                )
  
  situs_weights <-mori::share(lapply(strsplit(situs_neighbor_ind$situs_neighbors, split = ' '),
                                     function(neighbors){
                                       sapply(neighbors,
                                              function(n_used){
                                                strsplit(n_used,split='-')[[1]][2]})
                                     })
  )
  
  situs_indexes <- mori::share(strsplit(situs_neighbor_ind$indexes_used, split = ' '))
  
  print(head(situs_neighbors))
  print(head(situs_weights))
  print(head(situs_indexes))
  # plan(sequential)
  # library(doSNOW)
  # cl <- makeCluster(8, type="SOCK")
  # registerDoSNOW(cl)
  # registerDoParallel(core = 8)
  options(future.globals.maxSize = 4e9)
  registerDoFuture()
  plan(multisession,
       workers = round(3*parallel::detectCores()/4),
       maxSizeOfObjects =4e9)
  # registerDoSNOW(cl)
  # src_nodes <- c()
  # dest_nodes <- c()
  # adj_list <- list()
  # blocs <-
  #   df %>%
  #   mutate(batch = row_number() %% ncores) %>%
  #   nest(-batch) %>%
  #   pull(data)
  # split(1:nrow(situs_neighbor_ind),
  #       seq_len(nrow(situs_neighbor_ind)) %% 
  #         parallel::detectCores() +1), 
  print(Sys.time())
  edge_list <- do.call(rbind,
                       future.apply::future_sapply(1:nrow(situs_neighbor_ind),
                       # .options.future = list(chunk.size = 10000,
                       #                        scheduling = 1),
                       # .combine = 'rbind',
                       # .inorder = FALSE
                       function(index){

                         # print(index)
                         # print(length(adj_list))
                         # print(tail(adj_list,1))
                          nodes_used <- situs_indexes[[index]]
                          
                          neighbors_used <- situs_neighbors[[index]]
                          weights_used <- situs_weights[[index]]
                          # print(nodes_used)
                          if(length(nodes_used)==0){
                            return()
                          }
                          
                          # src_nodes <<- append(src_nodes,
                          #                      rep(nodes_used,
                          #                          rep(length(neighbors_used),
                          #                              length(nodes_used)))
                          #                      )
                          # dest_nodes <<-append(dest_nodes,
                          #                      rep(neighbors_used,
                          #                          length(nodes_used))
                          #                      )
                          data.frame(src = rep(nodes_used,
                                               rep(length(neighbors_used),
                                                   length(nodes_used))),
                                     dest = rep(neighbors_used,
                                                length(nodes_used)),
                                     weight = rep(weights_used,
                                                  length(nodes_used)),
                                     address = rep(situs_neighbor_ind$situs_address[[index]],
                                                   length(neighbors_used)*length(nodes_used)),
                                     pID = rep(situs_neighbor_ind$situs_pID[[index]],
                                               length(neighbors_used)*length(nodes_used))
                                     )
                                   
                          # sapply(nodes_used,
                          #          function(node){
                          #            node_list <- situs_neighbors[index]
                          #            names(node_list) <- node
                          #            
                          #            adj_list <<- append(adj_list,
                          #                                node_list)})
                          
                          #return(data.frame(source = rep(src_node,
                                #                  length(dest_nodes)),
                                #     dest = dest_nodes
                         #            )

                       })
                       )#|> futurize()
  
  # edge_list <- foreach(chunk_used = edge_list,
  #                            .combine = 'rbind',
  #                            .inorder = FALSE) %dopar%{
  #                              chunk_used
  #                            }
  print(Sys.time())
  # stopCluster(cl)
  

  readr::write_rds(edge_list,
                   'edge_list.rds')
  plan(sequential)
  # plan(multisession)
  # edge_list <- data.frame(src = src_nodes,
  #                         dest = dest_nodes)
  
  
  # stopCluster(cl)
  # src_nodes <- c()
  # dest_nodes <- c()
  # vertex_names <- unique(as.character(unlist(indexes_used)))
  # df_col <- foreach(index=1:length(indexes_used)) %dopar% {
  #   neighbors_used <- situs_neighbors[[index]]
  #   src_nodes <<- append(src_nodes,
  #                        rep(indexes_used[[index]],
  
  #                            length(neighbors_used)))
  #   dest_nodes <<- append(dest_nodes,
  #                         neighbors_used)
  #   
  # }
  # edges_df <- data.frame(src = scr_nodes,
  #            dest = dest_nodes)
  # vertex_df <- data.frame(name = vertex_names)
  # situs_graph <- igraph::graph_from_data_frame(edges_df,
  #                                              vertices = vertex_df)
  
  # placeholder <- foreach(index = 1:length(indexes_used),.combine = 'rbind') %dopar% {
  #   nodes_used <- indexes_used[[index]]
  #   place <- sapply(nodes_used,
  #                   function(node){
  #                     node_list <- situs_neighbors[index]
  #                     names(node_list) <- node
  #                     adj_list <<- append(adj_list,
  #                                         node_list)
  #                   })
  #   return(NULL)
  # }
  # gcs_save_file_upload('edge_list.rds',
  #                      edge_list)
  print(head(edge_list))
  situs_graph <- igraph::graph_from_edgelist(trimws(as.matrix(edge_list[,c('src',
                                                                           'dest')
                                                                        ]
                                                              )
                                                    ), directed = TRUE)
  
  E(situs_graph)$weight <- edge_list$weight
  rm(edge_list)
  gc()
  # situs_graph_undir <- igraph::graph_from_edgelist(trimws(as.matrix(edge_list[,c('src',
  #                                                                          'dest')
  # ]
  # )
  # ), directed = FALSE)
  # 
  # E(situs_graph_undir)$weight <- edge_list$weight
  # situs_adj_matrix <- igraph::as_adjacency_matrix(situs_graph)
  # readr::write_rds(situs_graph, 
  #                  'situs_graph.rds')
  # gcs_save_file_upload('situs_graph.rds',
  #                      situs_graph)
  labelProp_clusters <- igraph::cluster_label_prop(situs_graph)
  # louvain_clusters <- igraph::cluster_louvain(situs_graph_undir, 
  #                                             weights = E(situs_graph)$weight
  #                                             )
  # 
  print('labelprop')
  
  # situs_graph_comps <-igraph::components(situs_graph)
  # situs_membership <- igraph::components(situs_graph)$membership
  # gsbm_model <- greed(situs_adj_matrix, model =DcSbm())
  situs_membership <- membership(labelProp_clusters)
  situs_membership_size <- tapply(situs_membership,
                                  as.numeric(situs_membership),
                                  function(mems){
                                    length(unique(paste(owner_data_used[as.numeric(names(mems)),
                                                                  'situs_address'],
                                                        owner_data_used[as.numeric(names(mems)),
                                                                        'situs_pID'])
                                                  )
                                    )}
                                  )
  
  print('size')
  situs_membership_size <- situs_membership_size[order(situs_membership_size,
                                                       decreasing = TRUE)]
  # readr::write_rds(situs_membership_size,
  #                  'situs_membership_size.rds')
  # owner_data_used$group_assign <- 0zZ
  # plan(multisession)
  # q<-sapply(1:100,#length(situs_membership_size), 
  #                 function(index){
  #                   austin_parcel_data_merged_owner[names(situs_membership)[which(situs_membership==as.numeric(names(situs_membership_size)[index]))],
  #                                   'group_assign'] <<- index
  #                   
  #                 }) #%>% futurize()
  # 9635 31763    91  9524  1415  1466  8607  9109  1293  3298    49  4107  4042  3435    75  8269  7017  9672    10  6604    32 
  # 4532  4052  2373  2232  1853  1690  1607  1574  1454  1298  1296  1146  1070  1064   901   882   867   831   820   785   772 
  # 7998 16017  6535  4428    64  6721  7230  1018  7166  9253  9061  4128  6464  7123  6164  6903  7274  1225  1064  6724 31900 
  # 760   662   621   594   592   588   588   584   576   573   571   563   542   532   525   523   512   497   476   474   456 
  # 31641  6810  9769  5746  6653 16018  1130  5673 31844  6016 31894  4583 28774  5317 15475  5348 29101  1035  7151 31754  5910 
  # 455   434   410   404   400   395   374   371   368   344   340   330   324   320   318   308   306   305   304   304   301 
  # 9725 15430  1364  3511  6746 31899  9634  6213  8995  9229  9804  5350  8558   990  5856  6256  7265  7215  5578 31897 31898 
  # 289   285   282   280   276   276   273   270   268   267   267   263   260   257   257   255   242   241   240   230   230 
  # 6634  7150  9230  9783  6685  6785  6510  7010 24141  9706  5263  6607 29892  6303  6958  5471 
  # 229   227   227   226   223   223   222   222   221   217   213   213   212   211   211   210
  
  print(head(situs_membership_size))
  options(future.globals.maxSize = 4e9)
  registerDoFuture()
  plan(multisession,
       maxSizeOfObjects = 4e9)
  
  print(Sys.time())
  owner_data_used_final <- foreach(index=1:length(situs_membership_size),
                                             .combine = 'rbind') %dopar%{
                                               group_inds <- which(situs_membership==as.numeric(names(situs_membership_size)[index]))
                                               data.frame(owner_data_used[names(situs_membership)[group_inds],],
                                                    group_assign = rep(index,length(group_inds)))
                                             }
  # owner_data_used_final <- do.call(rbind, 
  #                                  future.apply::future_sapply(1:length(situs_membership_size),
  #                                     # .options.future = list(chunk.size = 10000,
  #                                     #                        scheduling = 1),
  #                                     # .combine = 'rbind',
  #                                     # .inorder = FALSE
  #                                     function(index){
  #                                       group_inds <- which(situs_membership==as.numeric(names(situs_membership_size)[index]))
  #                                       data.frame(owner_data_used[names(situs_membership)[group_inds],],
  #                                                  group_assign = rep(index,length(group_inds)))
  #                                       
                                        # print(index)
                                        # print(length(adj_list))
                                        # print(tail(adj_list,1))
                                       
                                        # }))
  print(Sys.time())
  print('assign')
  # owner_data_used[as.numeric(names(situs_membership)),'group_assign'] <- as.numeric(situs_membership)
  
  owner_data_used_final
  # situs_cliques <- igraph::max_cliques(situs_graph, max = 2000,
  #                                      file = 'situs_cliques.csv')
  
  # situs_neighbors_padded <- paste(' ', situs_neighbor_ind$situs_neighbors, ' ',
  #                                 sep = '')
  #   
  # situs_neighbors_shared <- mori::share(situs_neighbors)
  
  
  # options(future.globals.maxSize = 4e9)
  # registerDoFuture()
  # plan(multisession,
  #      maxSizeOfObjects = 4e9)
  # )
  # print(Sys.time())
  # matched_owners_inds_uniq<-unique(foreach(inds =head(situs_neighbors,1000)) %dopar% {
    # print(inds)
    # print('start')
    # print(Sys.time())
    # readr::write_rds(second_inds,'second_inds.rds')
    # result <-na.omit(iterative_add(inds = as.numeric(inds),
    #                                as.numeric(second_inds),
    #                                situs_neighbors,
    #                                situs_neighbors_padded
    #                                ))
    # 
    # second_inds <<- unique(c(second_inds,
    #                          result))
    
    # base_length <- length(result)
    # new_length = 0
    # while(new_length!=base_length){
    #   base_length <- length(result)
    #   result <- na.omit(c(result,
    #                       iterative_add(result,
    #                                   na.omit(second_inds),
    #                                   depth = 0)))
    #   second_inds <- c(second_inds,
    #                    result)
    #   
    #   new_length <- length(result)
    # }
    
    
    # rem_inds <- which(situs_owner_cosine_dist_matrix[inds[1],result]>0.6)
    # rem_inds <- unique(unlist(apply(situs_owner_cosine_dist_matrix[inds,result],2,
    #                         function(col){which(col>0.6)})))
    # rem_inds <- rem_inds[!(rem_inds %in% inds)]
    # if(length(rem_inds)>0){
    #   result <- result[-rem_inds]
    # }
    # result <- na.omit(append(result,
    #                          iterative_add(result,
    #                                        second_inds,
    #                                        depth = 0)))
    # second_inds <- unique(c(second_inds,
    #                         result))
    
    # print('done')
    # print(Sys.time())
    #   # print('sec')
    # result <- unique(result[order(result)])
    # print(second_inds)
    # print(length(second_inds))
    # print('done')
    # print(result)
    # print(length(result))
    # second_inds <<- unique(c(second_inds,
    #                          result))
    # result
  # })
  #   
  # matched_owners_inds_uniq <- matched_owners_inds_uniq[order(sapply(matched_owners_inds_uniq,
  #                                                                   length),
  #                                                            decreasing = TRUE)]
  # daemons(parallel::detectCores())
  # mirai::mirai_map(1:length(matched_owners_inds_uniq),
  #                  function(index) {
  #                    indexes = as.numeric(matched_owners_inds_uniq[[index]])
  #                    # print(indexes)
  #                    owner_data_used$group_assign[indexes] <<- index
  #                    })[.progress]
  # daemons(0)
  # sapply(1:length(matched_owners_inds_uniq),
  #        function(index){
  #          # print(index)
  #          indexes = as.numeric(matched_owners_inds_uniq[[index]])
  #          # print(indexes)
  #          owner_data_used$group_assign[indexes] <- index
  #        })
  
  # print(situs_group_assignment)
  
}
#PO BOX 4090 SCOTTSDALE AZ 85261', 



parcel_geolocate = function(owner_data){
  
  # owner_data <- head(owner_data,
  #                    20000)
  
  # print(dim(owner_data))
  owner_data$situs_pID <- as.character(owner_data$situs_pID )
  
  situs_addrs_used <- dplyr::filter(owner_data, 
                                    ((is_financialized ==TRUE) & 
                                       (is_owner_occupied==FALSE))|
                                      (property_units>4),
                                    property_units!=0,
                                    nchar(situs_address)>22,
                                    !is.na(property_units))$situs_address
  # print(dim(situs_addrs_used))
  # print(length(unique(situs_addrs_used)))
  unique_situs_addr <- data.frame(situs_addr=unique(situs_addrs_used))
  start_inds <- seq(1,nrow(unique_situs_addr),1000)
  end_inds <-c(seq(1000,nrow(unique_situs_addr),1000),
               nrow(unique_situs_addr))
  
  # print(start_inds)
  inds_used <- list(start = start_inds,end = end_inds)
  insist_geocode = purrr::insistently(geocode,
                                      rate =purrr::rate_backoff(pause_base = 5,
                                                                pause_cap = 30,
                                                                max_times = 3,
                                                                jitter = TRUE)
                                      )
  options(future.globals.maxSize = 4e9)
  registerDoFuture()
  plan(multisession,
       maxSizeOfObjects = 4e9
       )
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
  # print('out')
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
