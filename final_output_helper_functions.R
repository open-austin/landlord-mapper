
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

address_clean = function(data = austin_parcel_data_merged,
                         col = 'situs_address'){
  
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
           result_string <<- gsub(agent_string,
                                  '',
                                  result_string)
         })
  result_string
}
#"CORPORATION SERVICE COMPANY D/B/A CSC-LAWYERS INCO"  
reg_agent_string_gen = function(data_used,
                                cuts_used){
  registered_agent_inds <- which(c(grepl('RYAN LLC|ASSOC|CONSULT|COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST|LAW|ADVIS',
                                         data_used$corp_registered_agent_name,
                                         ignore.case = TRUE)
  ))
  # print(registered_agent_inds)
  agent_inds <- which(grepl('RYAN LLC|ASSOC|CONSULT|COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST|LAW|ADVIS',
                            data_used$agent_name,
                            ignore.case = TRUE))
  
  registered_agent_inds_cuts <- cut(1:length(registered_agent_inds),
                                    cuts_used)
  # print(registered_agent_inds_cuts)
  agent_inds_cuts <- cut(1:length(registered_agent_inds),
                         cuts_used)
  # print(agent_inds_cuts)
  
  registered_agent_add_string <- lapply(levels(registered_agent_inds_cuts),
                                        function(level_used){
                                          results <- unique(data_used[registered_agent_inds[registered_agent_inds_cuts==level_used],
                                                                      'corp_registered_agent_mail_add'])
                                          results <- results[which(sapply(results,nchar)>22)]
                                          results <- paste(results[which(results!="")],
                                                           collapse = '|')
                                        })
  agent_add_string <- lapply(levels(agent_inds_cuts),
                             function(level_used){
                               results <- unique(data_used[agent_inds[agent_inds_cuts==level_used],
                                                           'agent_address'])
                               # ret_inds <- which(sapply(results,nchar)>20)
                               # if(length(ret_inds)>0){
                               results <- results[which(sapply(results,nchar)>22)]  
                               # }
                               
                               results <- paste(results[which(results!="")],
                                                collapse = '|')
                             })
  registered_agent_name_string <- lapply(levels(registered_agent_inds_cuts),
                                         function(level_used){
                                           results <- unique(data_used[registered_agent_inds[registered_agent_inds_cuts==level_used],
                                                                       'corp_registered_agent_name'])
                                           results <- results[which(sapply(results,nchar)>3)]
                                           results <- paste(results[which(results!="")],
                                                            collapse = '|')
                                         })
  agent_name_string <- lapply(levels(registered_agent_inds_cuts),
                              function(level_used){
                                results <- unique(data_used[agent_inds[agent_inds_cuts==level_used],
                                                            'agent_name'])
                                results <- results[which(sapply(results,nchar)>3)]
                                results <- paste(results[which(results!="")],
                                                 collapse = '|')
                              })
  
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
                                  'ADDRESS',
                                  'CUSTODIAN',
                                  'UNKNOWN CITY',
                                  'UNKNOWN STATE',
                                  'ZIP',
                                  'PROPERTY TAX DEPARTMENT',
                                  'ATTN',
                                  'AVAILABLE UPON REQUEST',
                                  'MICHEL ROGERS & MALONEY, PC'),
                                collapse = '|'))
  
  misc_add_string <- list(paste(c('815 BRAZOS.+AUSTIN TX 78701',
                                  '2595 DALLAS PKWY.+FRISCO TX 75034',
                                  '401 TOM LANDRY HWY.+DALLAS TX 75266',
                                  'PO BOX 4090 SCOTTSDALE AZ 85261',
                                  'PO BOX 592226 SAN ANTONIO TX 78259',
                                  '901.+MOPAC.+AUSTIN TX 78746',
                                  '901.+MO PAC.+AUSTIN TX 78746',
                                  '3225 MCLEOD DR.+LAS VEGAS NV 89121',
                                  '17350 STATE H.+HOUSTON TX 77064'),
                                collapse = '|'))
  
  return( list(addresses = c(registered_agent_add_string,
                           agent_add_string,
                           misc_add_string),
               names = c( registered_agent_name_string,
                          agent_name_string,
                          misc_name_string))
          )
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
  
  # print(dim(owner_data))
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
                 reg_agent_string_gen = reg_agent_string_gen,
                 financial_markers_base = financial_markers_base,
                 financial_marker_base_string = financial_marker_base_string)
  # print(Sys.time())
  # registerDoFuture()
  # 3312
  # plan(multisession, workers =parallel::detectCores() )
  # print('clean')
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
      
      result_string <- agent_string_sub(result_string,
                                        registered_agent_string_list$addresses)
      result_string <- agent_string_sub(result_string,
                                        registered_agent_string_list$names)
      result_string <-gsub(paste(sapply(financial_markers_base, function(s){sprintf('[^[:alnum:]]%s[^[:alnum:]]',s)}),
                                 collapse = '|'),
                           ' ',
                           result_string)
      result_string <- gsub('[[:punct:]]',
                            '',
                            result_string)
      result_string <- trimws(gsub('[[:space:]]{2,}',
                                   ' ',
                                   result_string
      ))
      
      result_string[length(result_string)]
      
    }) %>%
    collect()
  
  # print('done')
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
  # BOX-FIX: cap the pool. A bare plan(multisession) takes
  # future::availableCores(), which on the box is 128 -- and this container has
  # no cpu limit for that call to read, so nothing was holding it down. Each of
  # those sessions is a full R process with this project's package set loaded
  # (~2.1 GiB RSS measured), so the pool alone wants ~270 GiB on a 188 GiB
  # machine. It got there: the target ran 49 minutes, climbed 35 -> 177 GiB, and
  # the container was OOM-killed (exit 137) with situs_group_assignments still
  # dispatched. Nothing about the computation is wrong, only the fan-out.
  #
  # 24 is deliberately conservative rather than tuned. This loop is
  # CPU-saturating (stringdist rebuilds the q-gram profile of all ~162k strings
  # on every one of ~162k iterations), so throughput is not what is scarce --
  # headroom is. 24 sessions is ~50 GiB of baseline, which leaves room for the
  # main session's copy of strings_used_final, the inds_found accumulation, and
  # the sparseMatrix built from it. Raise it only against a fresh measurement,
  # the same way SCRAPE_WORKERS in scrape_helper_functions.R was sized.
  DIST_MATRIX_WORKERS <- 24L
  plan(multisession,
       workers = min(DIST_MATRIX_WORKERS,
                     max(1L, future::availableCores() - 1L)))
  # Hand the sessions back before building the matrix. Without this the pool
  # stays parked for the rest of the target, holding its whole baseline while
  # sparseMatrix() allocates.
  on.exit(plan(sequential), add = TRUE)
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
                                        q=1)
    neighbors <- which(dist_vals<0.02)
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
                                                       10)
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
                 agent_string_sub = agent_string_sub,
                 reg_agent_string_gen = reg_agent_string_gen,
                 financial_marker_base_string = financial_marker_base_string)
  
  owner_data_used_share <- mori::share(owner_data_used)
  owner_data_used <- owner_data_used_share %>%
    partition(cl) %>%
    mutate(owner_address = agent_string_sub(toupper(owner_address),
                                            registered_agent_string_list$addresses),
           
           corp_mail_address = agent_string_sub(toupper(corp_mail_address),
                                                registered_agent_string_list$addresses),
           owner_address_scraped = agent_string_sub(toupper(owner_address_scraped),
                                                    registered_agent_string_list$addresses),
           
           corp_registered_agent_mail_add =  agent_string_sub(toupper(corp_registered_agent_mail_add),
                                                              registered_agent_string_list$addresses),
           # agent_address = agent_string_sub(toupper(agent_address),
           #                                  registered_agent_string_list),
           
           corp_business_name = agent_string_sub(toupper(corp_business_name),
                                                 registered_agent_string_list$names),
           owner_name = agent_string_sub(toupper(owner_name),
                                         registered_agent_string_list$names),
           owner_name_scraped = agent_string_sub(toupper(owner_name_scraped),
                                                 registered_agent_string_list$names),
           corp_registered_agent_name = agent_string_sub(toupper(corp_registered_agent_name),
                                                         registered_agent_string_list$names)
           # agent_name = agent_string_sub(toupper(agent_name),
           #                               registered_agent_string_list),
    ) %>%
    collect()
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
  print(dim(owner_data_used))
  registered_agent_string_list <- reg_agent_string_gen(owner_data_used,
                                                       10)
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
  print(Sys.time())
  # readr::write_rds(owner_data_used,'owner_data_used_proc.rds')
  
  cl <- multidplyr::new_cluster(parallel::detectCores())
  
  multidplyr::cluster_assign(cl,
                             pIDs_used = pIDs_used,
                             situs_owner_cosine_dist_matrix = situs_owner_cosine_dist_matrix,
                             owner_data_used = owner_data_used)
  # valid_owner_address <- nchar(owner_data_used$owner_address)>20
  
  owner_data_used_share <- mori::share(owner_data_used)
  situs_neighbor_ind <- owner_data_used_share %>%
    filter((situs_pID %in% pIDs_used)|
             (situs_address %in% addresses_used)) %>%
    group_by(situs_pID,
             situs_address) %>%
    multidplyr::partition(cl) %>%
    summarise(situs_neighbors = {
      owner_name_scrape_neighs <-which((owner_data_used$owner_name_scraped %in%
                                         na.omit(gsub("^$",
                                                      NA,
                                                      unique(owner_name_scraped)
                                         )
                                         ))
                                       )
      
      owner_name_neighs <- which((owner_data_used$owner_name %in%
                                   na.omit(gsub("^$",
                                                NA,
                                                unique(owner_name)
                                   )
                                   ))
      )
      # print(owner_name_neighs)
      owner_addr_scrape_neighs <- which((owner_data_used$owner_address_scraped %in%
                                          na.omit(gsub("^$",
                                                       NA,
                                                       unique(owner_address_scraped)
                                          )
                                          )) & (nchar(owner_address_scraped)>22)
                                        )
      # print('3')
      # print(owner_addr_scrape_neighs)
      owner_addr_neighs <- which((owner_data_used$owner_address %in%
                                   na.omit(gsub("^$",
                                                NA,
                                                unique(owner_address)
                                                )
                                           )) & (nchar(owner_address)>22)
                                 )
      # print('4')
      # print(owner_addr_neighs)
      corp_addr_neighs <- which((owner_data_used$corp_mail_address %in%
                                  na.omit(gsub("^$",
                                               NA,
                                               unique(corp_mail_address)
                                               
                                  )
                                  )) & (nchar(corp_mail_address)>22)
                                )
      # print('5')
      # print(corp_addr_neighs)
      corp_bus_neighs <- which((owner_data_used$corp_business_name %in%
                                 na.omit(gsub("^$",
                                              NA,
                                              unique(corp_business_name)
                                              )
                                         ))
                               )
      # print('6')
      # print(corp_bus_neighs)
      reg_agent_name_neighs <- which((owner_data_used$corp_registered_agent_name %in%
                                       na.omit(gsub("^$",
                                                    NA,
                                                    unique(corp_registered_agent_name)
                                                    )
                                               )) 
                                     )
      # print('7')
      # print(reg_agent_name_neighs)
      reg_agent_add_neighs <- which((owner_data_used$corp_registered_agent_mail_add %in%
                                      na.omit(gsub("^$",
                                                   NA,
                                                   unique(corp_registered_agent_mail_add)
                                      )
                                      )) & (nchar(corp_registered_agent_mail_add)>22)
                                    )
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
        
        # print(dist_inds)
        # print('dist inds')
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
        # print(dist_neighs)
      }
      else{
        dist_neighs <- NA
      }
      # print('total neighbors done')
      neighbors <- unique(c(
        owner_name_scrape_neighs,
        owner_name_neighs,
        owner_addr_scrape_neighs,
        owner_addr_neighs,
        corp_addr_neighs,
        corp_bus_neighs,
        reg_agent_name_neighs,
        reg_agent_add_neighs,
        # agent_name_neighs,
        # agent_add_neighs,
        dist_neighs))
      # print(neighbors)
      if(length(neighbors)>0){
        neighbors <- t(neighbors[order(neighbors)])
      }
      
      neighbors <- paste(unlist(neighbors[!is.na(neighbors)]),
                         collapse = ' ')
      # print(neighbors)
      neighbors
    }) %>%
    collect()
  # parallel::stopCluster(cl)
  print(Sys.time())
  
  situs_neighbor_ind
  
}
second_inds <- c(-1)
situs_neighor_gen_final = function(owner_data_used,
                                   situs_neighbor_ind){
  
  
  # readr::write_rds(situs_neighbor_ind,
  #                  'situs_neighbor_ind.rds')  
  # print(Sys.time())
  print(dim(owner_data_used))
  print(dim(situs_neighbor_ind))
  # print('neigh'
  iterative_add = function(inds, 
                           neighbors,
                           situs_neighbors,
                           situs_neighbors_padded,
                           # situs_neighbor_ind = situs_neighbor_ind,
                           depth = 2 ){
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
    
    
    if(length(inds)==0){
      return(NA)
    }
    # if(length(inds)>100){
    #   return(inds)
    # }
    result <-unique(as.numeric(  
      c(unlist(sapply(inds,
                    
                    function(ind){
                      if(Rfast::is_element(neighbors, ind)){#ind %in% neighbors){
                        if(depth!=2){
                          return(NULL)
                        }
                        # # break
                        return(ind)
                      }
                      inner_result_inds <- which(stringi::stri_detect_fixed(situs_neighbors_padded,
                                                                            sprintf( ' %s ',
                                                                                     ind),
                                                                            # max_count = length(inds)*2,
                                                                            opts_fixed = stringi::stri_opts_fixed(case_insensitive = TRUE))
                                                 )
                      
                      
                      inner_result <-unlist(situs_neighbors[inner_result_inds])
                      inner_result <- inner_result[!(inner_result %in% inds)]
                      # if((length(inds>500)) & (depth!=1)){
                      # 
                      # neighbors <<- unique(c(neighbors,
                      #                          inner_result[inner_result == ind]))
                      # }
                      
                      return(inner_result)
                    })
               ))))
    # print('mid')
    # print(result)
    
    
    if(depth!=0){
      sec_run <- result[!(sapply(result, function(result_used){ Rfast::is_element(neighbors,
                                                                                  result_used)}))]
      # [sapply(result,
      #                          function(result_used){
      #                            Rfast::is_element(c(inds,
      #                                                neighbors),
      #                                              result_used
      #                                              )##
      #                          }) ]#
      # print(sec_run)
      
      if(length(sec_run)>0){
        result <- unique(c(result,
                           iterative_add(sec_run,
                                         c(inds,
                                           neighbors),
                                         
                                         # paste('',
                                         #       paste(inds,
                                         #         collapse = ' '),
                                         #       neighbors),
                                         situs_neighbors,
                                         situs_neighbors_padded,
                                         depth = depth-1)))
      }
      
    }
    result <- unique(c(result,
                       inds))
    # result[order(result)]
  }
  situs_neighbors <- strsplit(situs_neighbor_ind$situs_neighbors, split = ' ')
  situs_neighbors_padded <- paste(' ', situs_neighbor_ind$situs_neighbors, ' ',
                                  sep = '')
  #   
  situs_neighbors_shared <- mori::share(situs_neighbors)
  
  
  options(future.globals.maxSize = 4e9)
  registerDoFuture()
  plan(multisession,
       maxSizeOfObjects = 4e9)
  # )
  print(Sys.time())
  matched_owners_inds_uniq<-unique(foreach(inds =situs_neighbors_shared) %dopar% {
    # print(inds)
    # print('start')
    # print(Sys.time())
    # readr::write_rds(second_inds,'second_inds.rds')
    result <-na.omit(iterative_add(inds = as.numeric(inds),
                                   as.numeric(second_inds),
                                   situs_neighbors,
                                   situs_neighbors_padded
                                   ))
    
    second_inds <<- unique(c(second_inds,
                             result))
    
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
    result <- unique(result[order(result)])
    # print(second_inds)
    # print(length(second_inds))
    # print('done')
    # print(result)
    # print(length(result))
    # second_inds <<- unique(c(second_inds,
    #                          result))
    result
  })
  #   
  
  
  owner_data_used$group_assign <- 0
  matched_owners_inds_uniq <- matched_owners_inds_uniq[order(sapply(matched_owners_inds_uniq,
                                                                    length),
                                                             decreasing = TRUE)]
  # daemons(parallel::detectCores())
  # mirai::mirai_map(1:length(matched_owners_inds_uniq),
  #                  function(index) {
  #                    indexes = as.numeric(matched_owners_inds_uniq[[index]])
  #                    # print(indexes)
  #                    owner_data_used$group_assign[indexes] <<- index
  #                    })[.progress]
  # daemons(0)
  sapply(1:length(matched_owners_inds_uniq),
         function(index){
           # print(index)
           indexes = as.numeric(matched_owners_inds_uniq[[index]])
           # print(indexes)
           owner_data_used$group_assign[indexes] <- index
         })
  
  # print(situs_group_assignment)
  print(Sys.time())
  owner_data_used
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
                                    nchar(situs_address)>20,
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
