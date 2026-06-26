
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


#row.names(d)[[28]]
# [1] "906 W JAMES ST LLC GRANT MCGREGOR 3267 BEE CAVES RD 107151 AUSTIN TX 78746 906 W JAMES ST LLC TEXAN"
situs_owner_string_gen = function(owner_data){
  if(is.na(file.size('_targets/objects/situs_owner_strings'))#|
     # (file.size('_targets/objects/situs_owner_strings')<30000000)
     ){
    
    registered_agent_inds <- which(c(grepl('RYAN LLC|ASSOC|CONSULT|INC|COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST|LAW|ADVIS',
                                           owner_data$corp_registered_agent_name,
                                           ignore.case = TRUE)
                                     ))
    agent_inds <- which(grepl('RYAN LLC|ASSOC|CONSULT|INC|COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST|LAW|ADVIS',
                              owner_data$agent_name,
                              ignore.case = TRUE))
    registered_agent_add_string_1 <-paste(paste(unique(owner_data[registered_agent_inds,
                                                            'corp_registered_agent_mail_add']),
                                          collapse = '|'),
                                          '815 BRAZOS.+500 AUSTIN TX 78701',
                                          '2595 DALLAS PKWY.+350 FRISCO TX 75034',
                                          'PO BOX 592226 SAN ANTONIO TX 78259 US',
                                          '901 S MOPAC.+410.+AUSTIN TX 78746',
                                          '3225 MCLEOD DR.+LAS VEGAS NV 89121',
                                          '17350 STATE HIGHWY 249 220 HOUSTON TX 77064',
                                          sep = '|')
    registered_agent_add_string_2 <-paste(unique(owner_data[agent_inds,
                                                            'agent_address']),
                                          collapse = '|')
    
    registered_agent_name_string_1 <-paste(paste(unique(owner_data[registered_agent_inds,
                                                                   'corp_registered_agent_name']),
                                                 collapse = '|'),
                                           'D3 REAL ESTATE CONSULTANTS',
                                           'GILL, DENSON & COMPANY',
                                           'L L CASEY & CO',
                                           'KE ANDREWS',
                                           'MICHEL ROGERS & MALONEY, PC',
                                           sep = '|')
    registered_agent_name_string_2 <- paste(unique(owner_data[agent_inds,
                                                              'agent_name']),
                                            collapse = '|')
    # owner_data <- head(owner_data,100)
    # situs_pIDs <- unique(owner_data$situs_pID)
    cl <- new_cluster(parallel::detectCores())
    cluster_assign(cl,
                   registered_agent_add_string_1 = registered_agent_add_string_1,
                   registered_agent_add_string_2 = registered_agent_add_string_2,
                   registered_agent_name_string_1 = registered_agent_name_string_1,
                   registered_agent_name_string_2 = registered_agent_name_string_2,
                   financial_marker_base_string = financial_marker_base_string)
    print(Sys.time())
    # registerDoFuture()
    # 3312
    # plan(multisession, workers = 2)
    # print('clean')
    situs_owner_strings <- owner_data %>%
      group_by(situs_pID,
               situs_address) %>%
      partition(cl) %>%
      summarise(strings_used = {
        unique_owners <- toupper(unique(owner_name))
        unique_owner_add <- toupper(unique(owner_address))
        corp_name <- toupper(unique(corp_business_name))
        corp_address <- toupper(unique(corp_mail_address))
        registered_agent <- toupper(unique(corp_registered_agent_name))
        registered_agent_add <- toupper(unique(corp_registered_agent_mail_add))
        
        agent_name <- toupper(unique(agent_name))
        agent_address <- toupper(unique(agent_address))
        scraped_owner_address = toupper(unique(owner_address_scraped))
        scraped_owner = toupper(unique(owner_name_scraped))
        
        unique_entities <- na.omit(unique(c(unique_owners,
                                            unique_owner_add,
                                            corp_name,
                                            corp_address,
                                            registered_agent,
                                            registered_agent_add,
                                            agent_name,
                                            agent_address,
                                            scraped_owner_address,
                                            scraped_owner)
        )
        )
        result_string <- paste(unique_entities,
                               collapse = ' ')
        result_string <- gsub(registered_agent_add_string_1,
                              '',
                              result_string)
        result_string <- gsub(registered_agent_add_string_2,
                              '',
                              result_string)
        result_string <- gsub(registered_agent_name_string_1,
                              '',
                              result_string)
        result_string <- gsub(registered_agent_name_string_2,
                              '',
                              result_string)
        result_string <-gsub(financial_marker_base_string,
                             '',
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
    
  }
  else{
    situs_owner_strings <- qs2::qs_read('_targets/objects/situs_owner_strings')
  }
  
  return(situs_owner_strings)
}

situs_owner_string_dist_matrix = function(situs_owner_strings, 
                                          owner_data){
  if(is.na(file.size('_targets/objects/situs_group_assignments'))|
     (file.size('_targets/objects/situs_group_assignments')<5000000)){
    
  pIDs_used <- unique(dplyr::filter(owner_data, 
                             ((is_financialized ==TRUE) & 
                               (is_owner_occupied==FALSE))|
                               (property_units>4),
                             property_units!=0,
                             !is.na(property_units))$situs_pID)
  
  strings_used <- which(situs_owner_strings$situs_pID %in%
                          pIDs_used)
  strings_used_final <- situs_owner_strings$strings_used[strings_used]
  
  names(strings_used_final) <- paste(situs_owner_strings$situs_pID[strings_used],
                                     situs_owner_strings$situs_address[strings_used],
                                     sep = '|')
  
  # print(length(strings_used_final))
  readr::write_rds(strings_used_final,
                   'strings_used_final.rds')
  # print(Sys.time())
  registerDoFuture()
  plan(multisession)

  rowInds <- c()
  colInds <- c()
  foreach(ind = 1:length(strings_used_final),
          .combine = 'rbind') %do% {
            string = strings_used_final[ind]
            dist_vals <- stringdist::stringdist(string,
                                                strings_used_final,
                                                useBytes =TRUE,
                                                method = 'cosine',
                                                q=1)
            neighbors <- which(dist_vals<0.02)
            neighbors <- neighbors[which(neighbors>ind)]
            rowInds <- append(rowInds,
                               rep(ind,
                                   length(neighbors)
                                   ))
            colInds <- append(colInds,
                               c(neighbors))
            return(NULL)
          }
  readr::write_rds(rowInds,'rowInds.rds')
  readr::write_rds(colInds,'colInds.rds')
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
  
  
  
  }
  else{
    situs_owner_cosine_dist_matrix <- qs2::qs_read('_targets/objects/situs_group_assignments')
  }
  situs_owner_cosine_dist_matrix
}

situs_neighbor_cov = function(situs_owner_cosine_dist_matrix){
  Rfast::cova(q3_dist_matrix, large = TRUE)
}

situs_neighor_gen = function(situs_owner_cosine_dist_matrix,
                             owner_data_used){
  registered_agent_inds <- which(c(grepl('RYAN LLC|ASSOC|CONSULT|INC|COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST|LAW|ADVIS',
                                         owner_data_used$corp_registered_agent_name,
                                          ignore.case = TRUE)
                                   ))
  agent_inds <- which(grepl('RYAN LLC|ASSOC|CONSULT|INC|COGENCY|REGISTER|(IN)?CORPORAT(E|ION)?|SERVICE|LAWYER|CSC|SOLUTION|AGENT|AGENC|LEGAL|BUSINESS|TAX|MAIL|POST|LAW|ADVIS',
                            owner_data_used$agent_name,
                             ignore.case = TRUE))


  registered_agent_add_string_1 <-paste(paste(unique(owner_data_used[registered_agent_inds,
                                                          'corp_registered_agent_mail_add']),
                                        collapse = '|'),
                                        '815 BRAZOS.+500 AUSTIN TX 78701',
                                        '2595 DALLAS PKWY.+350 FRISCO TX 75034',
                                        'PO BOX 592226 SAN ANTONIO TX 78259 US',
                                        '901 S MOPAC.+410.+AUSTIN TX 78746',
                                        '3225 MCLEOD DR.+LAS VEGAS NV 89121',
                                        '17350 STATE HIGHWY 249 220 HOUSTON TX 77064',
                                        sep = '|')
  
  registered_agent_add_string_2 <-paste(unique(owner_data_used[agent_inds,
                                                          'agent_address']),
                                        collapse = '|')
  
  registered_agent_name_string_1 <-paste(paste(unique(owner_data_used[registered_agent_inds,
                                                                 'corp_registered_agent_name']),
                                               collapse = '|'),
                                         'D3 REAL ESTATE CONSULTANTS',
                                         'GILL, DENSON & COMPANY',
                                         'L L CASEY & CO',
                                         'KE ANDREWS',
                                         'MICHEL ROGERS & MALONEY, PC',
                                         sep = '|')
  registered_agent_name_string_2 <- paste(unique(owner_data_used[agent_inds,
                                                            'agent_name']),
                                          collapse = '|')
  pIDs_used <- unique(dplyr::filter(owner_data_used, 
                                    ((is_financialized ==TRUE) & 
                                       (is_owner_occupied==FALSE))|
                                      (property_units>4),
                                    property_units!=0,
                                    !is.na(property_units))$situs_pID)
  
  addresses_used <- unique(dplyr::filter(owner_data_used, 
                                    ((is_financialized ==TRUE) & 
                                       (is_owner_occupied==FALSE))|
                                      (property_units>4),
                                    property_units!=0,
                                    !is.na(property_units))$situs_address)
  # print(registered_agent_name_string_2)
  # print('string')
  if(any(grepl('owner_data_used_proc.rds', list.files()))){
    
    owner_data_used <- readRDS('owner_data_used_proc.rds')
  }
  else{
    cl <- multidplyr::new_cluster(parallel::detectCores())
    multidplyr::cluster_assign(cl,
                               registered_agent_add_string_1 = registered_agent_add_string_1,
                               registered_agent_add_string_2 = registered_agent_add_string_2,
                               registered_agent_name_string_1 = registered_agent_name_string_1,
                               registered_agent_name_string_2 = registered_agent_name_string_2
    )
    owner_data_used <- owner_data_used %>%
      partition(cl) %>%
      mutate(owner_address = toupper(gsub(registered_agent_add_string_1,
                                          '',
                                          owner_address)),
             owner_address = toupper(gsub(registered_agent_add_string_2,
                                          '',
                                          owner_address)),
             corp_mail_address = toupper( gsub(registered_agent_add_string_1,
                                               '',
                                               corp_mail_address)),
             corp_mail_address = toupper( gsub(registered_agent_add_string_2,
                                               '',
                                               corp_mail_address)),
             owner_address_scraped = toupper(gsub(registered_agent_add_string_1,
                                                  '',
                                                  owner_address_scraped)),
             owner_address_scraped = toupper(gsub(registered_agent_add_string_2,
                                                  '',
                                                  owner_address_scraped)),
             
             corp_registered_agent_mail_add = toupper(gsub(registered_agent_add_string_1,
                                                           '',
                                                           corp_registered_agent_mail_add)),
             corp_registered_agent_mail_add = toupper(gsub(registered_agent_add_string_2,
                                                           '',
                                                           corp_registered_agent_mail_add)),
             # agent_address = toupper(gsub(registered_agent_add_string_1,
             #                              '',
             #                              agent_address)),
             # agent_address = toupper(gsub(registered_agent_add_string_2,
             #                              '',
             #                              agent_address)),
             
             corp_business_name = toupper(gsub(registered_agent_name_string_1,
                                               '',
                                               corp_business_name)),
             corp_business_name = toupper(gsub(registered_agent_name_string_2,
                                               '',
                                               corp_business_name)),
             owner_name = toupper(gsub(registered_agent_name_string_1,
                                       '',
                                       owner_name)),
             owner_name = toupper(gsub(registered_agent_name_string_2,
                                       '',
                                       owner_name)),
             owner_name_scraped = toupper(gsub(registered_agent_name_string_1,
                                               '',
                                               owner_name_scraped)),
             owner_name_scraped = toupper(gsub(registered_agent_name_string_2,
                                               '',
                                               owner_name_scraped)),
             corp_registered_agent_name = toupper(gsub(registered_agent_name_string_1,
                                                       '',
                                                       corp_registered_agent_name)),
             
             corp_registered_agent_name = toupper(gsub(registered_agent_name_string_2,
                                                       '',
                                                       corp_registered_agent_name))
             # agent_name = toupper(gsub(registered_agent_name_string_1,
             #                           '',
             #                           agent_name)),
             # agent_name = toupper(gsub(registered_agent_name_string_2,
             #                           '',
             #                           agent_name))
      ) %>%
      collect()
    readr::write_rds(owner_data_used,
                     'owner_data_used_proc.rds')
  }
  
    # 
  # readr::write_rds(owner_data_used,'owner_data_used_proc.rds')
  # print('2')
  cl <- multidplyr::new_cluster(parallel::detectCores())
  multidplyr::cluster_assign(cl,
                             pIDs_used = pIDs_used,
                             situs_owner_cosine_dist_matrix = situs_owner_cosine_dist_matrix,
                             owner_data_used = owner_data_used)
  print(Sys.time())
  situs_neighbor_ind <- owner_data_used %>%
    filter((situs_pID %in% pIDs_used)|
           (situs_address %in% addresses_used)) %>%
    group_by(situs_pID,
             situs_address) %>%
    multidplyr::partition(cl) %>%
    summarise(situs_neighbors = {
      # print(unique(situs_pID))
      # print(unique(situs_address))
      owner_name_scrape_neighs <-which(owner_data_used$owner_name_scraped %in%
                                         na.omit(gsub("^$",
                                                      NA,
                                                      unique(owner_name_scraped)
                                         )
                                         )
      )
    
      owner_name_neighs <- which(owner_data_used$owner_name %in%
                                   na.omit(gsub("^$",
                                                NA,
                                                unique(owner_name)
                                   )
                                   )
      )
      # print('2')
      # print(owner_name_neighs)
      owner_addr_scrape_neighs <- which(owner_data_used$owner_address_scraped %in%
                                          na.omit(gsub("^$",
                                                       NA,
                                                       unique(owner_address_scraped)
                                                       )
                                                  )
      )
      # print('3')
      # print(owner_addr_scrape_neighs)
      owner_addr_neighs <- which(owner_data_used$owner_address %in%
                                   na.omit(gsub("^$",
                                                NA,
                                                unique(owner_address)
                                   )
                                   )
      )
      # print('4')
      # print(owner_addr_neighs)
      corp_addr_neighs <- which(owner_data_used$corp_mail_address %in%
                                  na.omit(gsub("^$",
                                               NA,
                                               unique(corp_mail_address)
                                  )
                                  )
      )
      # print('5')
      # print(corp_addr_neighs)
      corp_bus_neighs <- which(owner_data_used$corp_business_name %in%
                                 na.omit(gsub("^$",
                                              NA,
                                              unique(corp_business_name)
                                 )
                                 )
      )
      # print('6')
      # print(corp_bus_neighs)
      reg_agent_name_neighs <- which(owner_data_used$corp_registered_agent_name %in%
                                       na.omit(gsub("^$",
                                                    NA,
                                                    unique(corp_registered_agent_name)
                                       )
                                       )
      )
      # print('7')
      # print(reg_agent_name_neighs)
      reg_agent_add_neighs <- which(owner_data_used$corp_registered_agent_mail_add %in%
                                      na.omit(gsub("^$",
                                                   NA,
                                                   unique(corp_registered_agent_mail_add)
                                      )
                                      )
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
      
      print('exact matches done')
      # print(agent_add_neighs)
      if(unique(situs_pID) %in% pIDs_used){
        print(paste(unique(situs_pID),
                    unique(situs_address),
                    sep = '\\|'))
        situs_dist_ind <- which(grepl(paste(unique(situs_pID),
                                      unique(situs_address),
                                      sep = '\\|'),
                                colnames(situs_owner_cosine_dist_matrix)
                                ))
        # print('situs_dist')
        print(situs_dist_ind)
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
        print(dist_inds)
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
  # print(Sys.time())
  
       readr::write_rds(situs_neighbor_ind,
                   'situs_neighbor_ind.rds')
    # print('neigh')
  iterative_add = function(inds, neighbors, depth = 2 ){
    # print(depth)
    if(length(inds)==0){
      return(NA)
    }
    if(length(inds)>500){
      return(inds)
    }
    result <-unique(as.numeric(  
      unlist(sapply(inds,
                    
                    function(ind){
                      if(ind %in% neighbors){
                        if(depth!=2){
                          return(NULL)
                        }
                        # break
                          return(ind)
                        }
                      inner_result <-unique(unlist(strsplit(situs_neighbor_ind$situs_neighbors[grepl(sprintf('([[:space:]]|^)%s([[:space:]]|$)',
                                                                                      ind),
                                                                              situs_neighbor_ind$situs_neighbors)],
                                                     split = ' ')
                                            ))
                      
                      
                      inner_result
                    })))
      )
    # print(result)
    if(depth!=0){
      result <- unique(append(result,
                              iterative_add(result,
                                            c(inds,
                                              neighbors),
                                            depth = depth-1)))
                              }

    result[order(result)]
  }
  situs_neighbors <- strsplit(situs_neighbor_ind$situs_neighbors, split = ' ')
  # names(situs_neighbors) <- paste(situs_neighbor_ind$situs_pID,
  #                                 situs_neighbor_ind$situs_address,
  #                                 sep = '_')
  # options(future.globals.maxSize = 2 * 1e9)
  # registerDoFuture()
  # plan(multisession,
  #      maxSizeOfObjects = 2e9
  #      )
  second_inds <- c()
  # print(Sys.time())
  matched_owners_inds_uniq<-unique(foreach(inds = situs_neighbors) %do% {
                                            # print(inds)
                                            result <-na.omit(iterative_add(inds = inds,
                                                                   second_inds))
                                            second_inds <- append(second_inds,
                                                                  result)
                                            # print('init')
                                            # print(result)
                                            base_length = length(result)
                                            new_length = 0
                                            while(new_length!=base_length){
                                              base_length <- length(result)
                                              result <- na.omit(iterative_add(result,
                                                                      na.omit(second_inds)))
                                              second_inds <- append(second_inds,
                                                                    result)

                                              new_length <- length(result)
                                            }
                                            result <- na.omit(append(result,
                                                                     iterative_add(result,
                                                                            second_inds,
                                                                            depth = 1)))
                                            # print('sec')
                                            result <- unique(result[order(result)])
                                            # print('done')
                                            # print(result)
                                            # rem_inds <- which(situs_owner_cosine_dist_matrix[inds[1],result]>0.6)
                                            # rem_inds <- unique(unlist(apply(situs_owner_cosine_dist_matrix[inds,result],2,
                                            #                         function(col){which(col>0.6)})))
                                            # rem_inds <- rem_inds[!(rem_inds %in% inds)]
                                            # if(length(rem_inds)>0){
                                            #   result <- result[-rem_inds]
                                            # }
                                            result
                                          })
  readr::write_rds(second_inds,
        'second_inds.rds')
  print(Sys.time())
  readr::write_rds(matched_owners_inds_uniq,
                   'matched_owners_inds_uniq.rds'
                   )
  
  owner_data_used$group_assign <- 0
  
  sapply(1:length(matched_owners_inds_uniq),
         function(index){
           # print(index)
           indexes = as.numeric(matched_owners_inds_uniq[[index]])
           # print(indexes)
           owner_data_used$group_assign[indexes] <<- index
         })
            
  # print(situs_group_assignment)
  print(Sys.time())
  owner_data_used
}

parcel_geolocate = function(owner_data){
  
  owner_data <- head(owner_data,
                     20000)
  owner_data$situs_pID <- as.character(owner_data$situs_pID )
  
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
#367751
# d <- dplyr::filter(q, situs_pID %in% situs_pIDs[m[[4562]]])

#s <- unlist(lapply(m, length))

#order(s,decreasing = TRUE)

#s[order(s,decreasing = TRUE)]
