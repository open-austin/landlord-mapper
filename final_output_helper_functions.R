
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
  # BOX-FIX: the ~162k-iteration stringdist fan-out is replaced by the same
  # computation written as one blocked matrix product. The worker pool is gone
  # with it, so the 24-session cap this block used to hold is no longer needed.
  #
  # method='cosine' with q=1 and useBytes=TRUE is cosine distance between
  # single-BYTE count vectors: 1 - (v.w)/(|v||w|) over a fixed 256-dimensional
  # space. The whole distance matrix is therefore a normalised Gram matrix, and
  # the loop was paying stringdist to rebuild the byte profile of all 161,831
  # strings on every one of 161,831 iterations to recover it one row at a time --
  # O(n^2) work done n times over, and 24 R sessions each spawning a full OpenMP
  # team inside stringdist on top of that (12,500% CPU measured, on 128 cores).
  #
  # Measured on the box, real situs_owner_strings input, n = 161,831:
  #   shipped loop, 24 multisession workers : 56.0 min wall, 4.3 GiB peak container
  #   blocked BLAS, single process          : 11.1 min wall, 4.8 GiB peak container
  # Both produce the same 2,480,561 pairs, and the two sets are identical() --
  # every pair, both directions checked, at full scale, not on a sample.
  #
  # The count matrix is n x 256 doubles = 331 MB. The n x n product is never
  # materialised: DIST_MATRIX_BLOCK rows at a time gives one block that is
  # thresholded and dropped, so the ceiling is bounded and predictable instead of
  # proportional to worker count. OpenBLAS (openblas-pthread in this image)
  # threads the product, so there is no R-level pool to size, register, or hand
  # back -- hence no registerDoFuture()/plan() here any more. The two
  # plan(multisession, ...) call sites further down this file are untouched.
  #
  # Three reference behaviours are NOT float noise and are reproduced on purpose,
  # because the count-vector form loses them:
  #   * stringdist('','') is 0, not NaN. The 14 zero-byte strings in this input
  #     are mutual neighbours in the shipped loop, contributing choose(14,2) = 91
  #     pairs. Their count vectors are all-zero so the normalised product gives
  #     0/0; they are held out of the product and their 91 pairs added back.
  #     Omitting this was the only discrepancy the equivalence check ever found,
  #     and it accounted for it exactly (91 of 91, then 105 of 105 on a variant
  #     with one more empty string appended).
  #   * stringdist('', nonempty) is NaN and stringdist(NA, x) is NA. Both are
  #     dropped by which(dist_vals < 0.02), so a zero-profile or NA string is
  #     never a neighbour of a non-empty one.
  #   * the threshold is written (1 - sim) < 0.02, not sim > 0.98, so it is the
  #     same floating-point expression the loop evaluated. Nothing in this input
  #     comes near the boundary anyway: over 400 query rows x 161,831 the closest
  #     any distance got to 0.02 was 1.6e-05, and zero pairs fell within 1e-06.
  n_strings <- length(strings_used_final)
  string_nbytes <- nchar(strings_used_final, type = 'bytes')
  is_na_string <- is.na(strings_used_final)
  is_zero_profile <- !is_na_string & (string_nbytes == 0L)

  byte_counts <- matrix(0, nrow = n_strings, ncol = 256L)
  for (string_ind in seq_len(n_strings)) {
    if (is_na_string[string_ind] || string_nbytes[string_ind] == 0L) next
    byte_counts[string_ind, ] <- tabulate(
      as.integer(charToRaw(strings_used_final[string_ind])) + 1L,
      256L)
  }
  byte_row_norms <- sqrt(rowSums(byte_counts * byte_counts))
  degenerate_rows <- (byte_row_norms == 0) | !is.finite(byte_row_norms)
  byte_row_norms[degenerate_rows] <- 1
  byte_counts <- byte_counts / byte_row_norms
  # A degenerate row must not become NaN: NaN would poison the whole block it
  # appears in. Zeroed, it scores similarity 0 against everything, i.e. distance
  # 1, i.e. never a neighbour -- which is what the NaN/NA drop above does.
  byte_counts[degenerate_rows, ] <- 0
  byte_counts_t <- t(byte_counts)

  # 2000 rows -> a 2000 x 161,831 block, 2.6 GB, 8.9s measured. This block is the
  # only large transient in the target; shrink it first if n grows.
  DIST_MATRIX_BLOCK <- 2000L
  pair_blocks <- vector('list', ceiling(n_strings / DIST_MATRIX_BLOCK) + 1L)
  n_pair_blocks <- 0L
  for (block_start in seq(1L, n_strings, by = DIST_MATRIX_BLOCK)) {
    block_rows <- block_start:min(block_start + DIST_MATRIX_BLOCK - 1L, n_strings)
    dist_block <- 1 - (byte_counts[block_rows, , drop = FALSE] %*% byte_counts_t)
    hits <- which(dist_block < 0.02, arr.ind = TRUE)
    rm(dist_block)
    if (nrow(hits) > 0L) {
      hit_rows <- block_rows[hits[, 1L]]
      hit_cols <- hits[, 2L]
      # neighbors[which(neighbors > ind)] in the original, plus the degenerate
      # rows which the zeroing above already excludes -- kept explicit so the
      # exclusion does not depend on a floating-point accident.
      keep <- (hit_cols > hit_rows) &
        !is_zero_profile[hit_rows] & !is_zero_profile[hit_cols] &
        !is_na_string[hit_rows] & !is_na_string[hit_cols]
      if (any(keep)) {
        n_pair_blocks <- n_pair_blocks + 1L
        pair_blocks[[n_pair_blocks]] <- cbind(hit_rows[keep], hit_cols[keep])
      }
    }
    rm(hits)
  }
  zero_profile_inds <- which(is_zero_profile)
  if (length(zero_profile_inds) > 1L) {
    n_pair_blocks <- n_pair_blocks + 1L
    pair_blocks[[n_pair_blocks]] <- t(utils::combn(zero_profile_inds, 2L))
  }
  pairs_found <- do.call(rbind, pair_blocks[seq_len(n_pair_blocks)])
  rowInds <- as.integer(pairs_found[, 1L])
  colInds <- as.integer(pairs_found[, 2L])
  rm(pairs_found, pair_blocks, byte_counts, byte_counts_t)
  print(Sys.time())
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

# BOX-FIX: bound the fan-out in the final-output stage.
#
# Four call sites here sized their worker pool from the machine:
# multidplyr::new_cluster(parallel::detectCores()) in situs_neighor_gen_clean and
# situs_neighor_gen, and a bare plan(multisession, maxSizeOfObjects = 4e9) in
# situs_neighor_gen and parcel_geolocate. On this box that is 128 workers, and
# each one is a full R process that ends up holding its own copy of the
# 2,133,448 x 35 owner frame.
#
# Measured on the box: austin_parcel_data_merged_owner_clean was run with the
# call site untouched and killed deliberately at 60.6 GiB. ps inside the
# container showed 130 R processes -- the main session, one crew worker at
# 4.35 GiB, and 128 multidplyr session workers at 1.40 GiB each and still
# climbing, on track for roughly 180 GiB against 188 GiB of RAM. The run before
# that one was allowed to continue and died at 118.8 GiB against a 120 GiB
# container cap, surfacing as "error writing to connection" -- a worker being
# killed mid-write, not a serialisation bug. serialize(owner_data_used) is
# 1014 MB, which is exactly the size of the 77 callr-fun-* spill files the first
# attempt left in the container tempdir, so the deaths appear to have triggered a
# relaunch storm that re-serialised the frame once per retry and filled the root
# filesystem as a second-order effect.
#
# 24 is the same bound, chosen the same way, as DIST_MATRIX_WORKERS was in
# 6f426d6 and SCRAPE_WORKERS in scrape_helper_functions.R: headroom rather than
# throughput. 24 x 1.6 GiB is about 38 GiB of worker baseline, which leaves the
# main session, the crew worker and the 1.4 GiB mori shared region room to fit.
# Raise it only against a fresh measurement.
#
# Deliberately NOT applied to the new_cluster(parallel::detectCores()) in
# situs_owner_string_gen above: that target has already completed at 128 workers,
# it is cached, and touching the function would invalidate it for no reason.
FINAL_OUTPUT_WORKERS <- 24L

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
  cl <- multidplyr::new_cluster(FINAL_OUTPUT_WORKERS)
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

# ---------------------------------------------------------------------------
# BOX-FIX: situs_neighor_gen rebuilt on inverted indexes instead of per-group
# full-column scans.
#
# WHY: the original body ran, for every one of the 429,591 (situs_pID,
# situs_address) groups, nine `which(owner_data_used$<col> %in% <group values>)`
# scans over the FULL 2,133,448-row owner frame. That is ~19.2 billion string
# comparisons plus 24 multidplyr workers each holding a copy of the frame, and
# it measured 13,908 s wall clock. The work is a many-to-many join, so it wants
# a join index: build the value -> row-indices map ONCE per column, then each
# group is a handful of O(1) slices. Measured single-threaded on the box at
# 31.5 s end to end, 441x faster, with peak RSS 4.0 GiB.
#
# Where that 31.5 s actually goes, measured with per-phase timestamps by an
# independent reviewer (an earlier version of this comment had the last two
# phases inverted, so trust these): filter + grouping 1.7 s; the eight inverted
# index blocks 7.9 s; triplet conversion + suffix/pID indexes 3.3 s; the cosine
# loop 7.1 s; output assembly 15.1 s, of which split() + paste0() is 12.0 s.
# Output assembly is therefore the largest phase AND the one that sets the
# memory peak (+766 MiB across the radix sort and the split/paste). Tune there
# first, not in the index blocks.
#
# WHY single-threaded: every out-of-memory death in this stage came from fanning
# the 1 GB owner frame out to workers (see the FINAL_OUTPUT_WORKERS comment
# above -- 128 workers x 1.4 GiB, and a 145 GB cgroup OOM caused by a closure
# defined inside the exported function capturing its whole enclosing frame and
# being serialised to every worker). The indexed version does not need
# parallelism to be fast, so it takes none, and every NAMED helper below is
# defined at TOP LEVEL rather than nested inside situs_neighor_gen, so that a
# future `future`/`multidplyr` call site cannot serialise this function's frame
# along with a helper.
#
# That protection is NOT total, and the earlier absolute wording here was wrong.
# Two anonymous callbacks are still defined inside this function and therefore
# still capture its frame, which holds the 1.4 GB owner frame:
#   * the `function(z) paste0(unique(z), collapse = " ")` passed to vapply in
#     the output-assembly block, and
#   * the `function(z) length(unique(z))` passed to tapply in the gate audit.
# Nothing serialises either one today, so measured peak RSS is unaffected. But
# the 145 GB trap is latent in exactly those two spots: wrapping either in
# future or multidplyr would ship the whole frame to every worker, and
# object.size() on the closure would report a few hundred bytes while doing it.
# Lift them to top level before parallelising anything here.
#
# WHAT IS DELIBERATELY DIFFERENT (one behaviour change, everything else is
# byte-identical -- see verify_neigh.R):
#
#   The `nchar(<col>) > 22` gate. The original wrote
#       which((owner_data_used$col %in% S) & (nchar(GROUP$col) > 22))
#   where the second operand has length = group size and was therefore RECYCLED
#   against the 2,133,448-element `%in%` logical. That is a bug: for a group of
#   two rows it would have kept only every other matching row. It is inert on
#   today's data because the gate verdict is uniform inside all 3,018 multi-row
#   groups (and 99.3% of groups are a single row, where recycling a length-1
#   vector is a scalar). It is fixed here as a PER-VALUE gate: a distinct value
#   from the group's column participates in matching only if that value itself
#   has nchar > 22. That is what the >22 threshold means everywhere else in this
#   file -- reg_agent_string_gen uses the same number to throw away address
#   fragments too short to be discriminating -- and it is identical to the
#   original on every single-row group. The number of groups whose verdict is
#   NOT uniform across their rows is counted and logged below; it is 0 today,
#   and if a future extract makes it non-zero that counter is the only signal
#   that this rewrite has started to diverge from the recycled original.
#
# WHAT IS PRESERVED EXACTLY:
#   * Neighbour tokens are row indices into the FULL owner frame, not into the
#     432,788-row filtered subset that defines the groups.
#   * NA and "" are never members of a group's match set (the original's
#     na.omit(gsub("^$", NA, unique(x))) idiom), so the ~1.98M NA rows and the
#     ""-valued rows in those columns can never be emitted as neighbours.
#   * The cosine block's UNANCHORED substring match on colnames. `grepl("<pID>\\|
#     <address>", colnames)` on colnames that contain exactly one pipe is
#     equivalent to "colname pID ends with the query pID AND colname address
#     starts with the query address" (verified: 0 of the 269,612 query addresses
#     contain a regex metacharacter, all pIDs are digits). That equivalence is
#     what lets a suffix index replace 269,612 x 161,831 regex calls, and it
#     keeps the 6 groups that legitimately match more than one column.
#   * NA situs_address in the cosine query becoming the literal string "NA",
#     because paste() coerces it.
#   * strsplit(colname, "|")[[1]][2] returning NA -- not "" -- for the 9
#     colnames with an empty address, which is what lets those columns match
#     rows whose situs_address is NA.
#   * The cross-product form of dist_neighs: (pID %in% P) & (address %in% A),
#     not a paired-key match.
#   * Output format: union -> ascending -> deduplicated -> single-space join,
#     and exactly "" for a group with no neighbours.
#   * Column names, group_by(situs_pID) structure, and the row order of the
#     returned frame, which is the multidplyr partition-then-collect permutation
#     (greedy load-balance of groups across FINAL_OUTPUT_WORKERS shards, then
#     shards concatenated in worker order). Row order is semantically irrelevant
#     downstream -- situs_neighor_gen_final treats each row position as an
#     opaque graph vertex id -- but reproducing it makes the rewrite diffable
#     against the cached target with no reordering step.
# ---------------------------------------------------------------------------

# The >22 threshold, named once. Same number, same intent, as the
# sapply(results, nchar) > 22 filter in reg_agent_string_gen.
NEIGH_ADDR_MIN_CHARS <- 22L


# Columns that propagate a neighbour link when two rows share a value, in the
# order the original listed them. Order is irrelevant to the result (the tokens
# are unioned and sorted) but is kept for readability against the old body.
NEIGH_MATCH_COLS <- c("owner_name_scraped",
                      "owner_name",
                      "owner_address_scraped",
                      "owner_address",
                      "corp_mail_address",
                      "corp_business_name",
                      "corp_registered_agent_name",
                      "corp_registered_agent_mail_add")

# The four address-shaped columns carrying the nchar > NEIGH_ADDR_MIN_CHARS gate.
NEIGH_GATED_COLS <- c("owner_address_scraped",
                      "owner_address",
                      "corp_mail_address",
                      "corp_registered_agent_mail_add")

# Value -> row-indices index in CSR form.
#
# WHY not split(seq_len(n), col): split would materialise a list with one R
# vector per distinct value -- 1,639,464 of them for owner_name, ~90 MB of
# SEXP headers before any data, per column. CSR is two integer vectors plus an
# offset vector, and it makes the lookup vectorisable with sequence(), so a
# whole column's worth of group lookups is one call instead of 429,591.
neigh_index <- function(x) {
  uv  <- unique(x)
  vid <- match(x, uv)
  list(uv   = uv,
       # rows[ off[j]+1 : off[j+1] ] are the rows holding value uv[j]
       rows = order(vid, method = "radix"),
       off  = c(0L, cumsum(tabulate(vid, nbins = length(uv)))))
}

# Rows holding any of the value ids in vsel, flattened, with a parallel vector
# saying how many rows each vsel entry contributed (so the caller can attach
# group ids with rep.int).
neigh_rows_for <- function(ix, vsel) {
  len <- ix$off[vsel + 1L] - ix$off[vsel]
  list(rows = ix$rows[sequence(len, from = ix$off[vsel] + 1L)],
       len  = len)
}

# Suffix index over the cosine matrix's colname pIDs.
#
# WHY a suffix index: the original matched columns with an unanchored regex, so
# a query pID matches any colname pID that ENDS with it. pIDs are not uniformly
# zero-padded (nchar 2,4,5,6,7,12), so "49659" really does match "000001349659".
# Dropping that and doing an exact (pID,address) key lookup would silently lose
# columns on the 6 groups that currently multi-match. Indexing every suffix of
# every colname pID (~1.5M keys) keeps the original semantics at O(1) per query.
neigh_pid_suffix_index <- function(col_pID) {
  nc   <- length(col_pID)
  wid  <- nchar(col_pID)
  keys <- vector("list", max(wid))
  owns <- vector("list", max(wid))
  for (k in seq_len(max(wid))) {
    w <- which(wid >= k)
    keys[[k]] <- substring(col_pID[w], wid[w] - k + 1L)
    owns[[k]] <- w
  }
  key <- unlist(keys, use.names = FALSE)
  own <- unlist(owns, use.names = FALSE)
  uk  <- unique(key)
  kid <- match(key, uk)
  o   <- order(kid, own, method = "radix")
  list(uk   = uk,
       cols = own[o],
       off  = c(0L, cumsum(tabulate(kid[o], nbins = length(uk)))))
}

situs_neighor_gen = function(situs_owner_cosine_dist_matrix,
                             owner_data_used){

  owner_data_used <- data.frame(owner_data_used)
  print(dim(owner_data_used))
  n_rows <- nrow(owner_data_used)

  # The original also computed reg_agent_string_gen(owner_data_used, 10) here
  # and never referenced the result -- the scrubbing it feeds happens in
  # situs_neighor_gen_clean, one target upstream. Dropped: it is pure, so
  # dropping it cannot change the output, and it is not free.

  print(Sys.time())
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
  print(Sys.time())

  # The group set, and the map from each kept row back to its group. Note that
  # `keep` is the original's filter verbatim, including the fact that %in% makes
  # an NA situs_address match the single NA in addresses_used, which is why 9
  # NA-address groups exist.
  keep      <- (owner_data_used$situs_pID %in% pIDs_used) |
               (owner_data_used$situs_address %in% addresses_used)
  kept_rows <- which(keep)
  gd <- dplyr::group_data(dplyr::group_by(owner_data_used[kept_rows,
                                                          c("situs_pID",
                                                            "situs_address")],
                                          situs_pID,
                                          situs_address))
  n_groups <- nrow(gd)
  gid <- integer(length(kept_rows))
  gid[unlist(gd$.rows, use.names = FALSE)] <- rep.int(seq_len(n_groups),
                                                      lengths(gd$.rows))
  cat("situs_neighor_gen:", length(kept_rows), "rows kept,",
      n_groups, "groups\n")

  # Rows of multi-row groups only, used for the gate-uniformity audit below.
  multi      <- which(lengths(gd$.rows) > 1L)
  multi_rows <- unlist(gd$.rows[multi], use.names = FALSE)
  multi_gid  <- rep.int(seq_along(multi), lengths(gd$.rows[multi]))
  mixed_gate <- integer(0)

  # ---- the eight shared-value blocks --------------------------------------
  edge_g <- vector("list", length(NEIGH_MATCH_COLS) + 1L)
  edge_t <- vector("list", length(NEIGH_MATCH_COLS) + 1L)

  for (k in seq_along(NEIGH_MATCH_COLS)) {
    cname <- NEIGH_MATCH_COLS[[k]]
    full  <- owner_data_used[[cname]]
    ix    <- neigh_index(full)
    v     <- full[kept_rows]

    # na.omit(gsub("^$", NA, unique(v))) in the original: NA and "" are never
    # members of the match set, so they can never link rows.
    ok <- !is.na(v) & nzchar(v)
    if (cname %in% NEIGH_GATED_COLS) {
      ok <- ok & (nchar(v) > NEIGH_ADDR_MIN_CHARS)
      # Audit the one deliberate behaviour change: how many multi-row groups
      # disagree with themselves about the gate? 0 means this per-value gate is
      # indistinguishable from the original's recycled vector gate.
      verdict <- nchar(v[multi_rows]) > NEIGH_ADDR_MIN_CHARS
      # NA (an NA column value) is its own verdict level, because the original
      # dropped those rows via which() rather than treating them as FALSE.
      code <- ifelse(is.na(verdict), 2L, as.integer(verdict))
      spread <- tapply(code, multi_gid, function(z) length(unique(z)))
      mixed_gate <- c(mixed_gate,
                      stats::setNames(sum(spread > 1L), cname))
    }

    vsel <- match(v[ok], ix$uv)
    hit  <- neigh_rows_for(ix, vsel)
    edge_t[[k]] <- hit$rows
    edge_g[[k]] <- rep.int(gid[ok], hit$len)
    rm(full, ix, v, ok, vsel, hit)
  }
  print(Sys.time())

  # ---- the cosine-distance block ------------------------------------------
  cn       <- colnames(situs_owner_cosine_dist_matrix)
  col_pID  <- sub("\\|.*$", "", cn)
  col_addr <- substring(cn, nchar(col_pID) + 2L)
  # strsplit("123|", "|", fixed = TRUE)[[1]][2] is NA, not "", and the original
  # fed exactly that into `situs_address %in% dist_neigh_address`. An NA in that
  # set matches rows whose situs_address is NA, so the distinction is load
  # bearing for the 9 colnames with an empty address.
  col_addr_key <- ifelse(nzchar(col_addr), col_addr, NA_character_)

  # Adjacency of the 0/1 symmetric matrix. m[ind,] and m[,ind] are the same
  # vector for a symmetric matrix, so the original's union of the two is just
  # the neighbour set; == 1 is still applied rather than assumed.
  tm  <- methods::as(methods::as(situs_owner_cosine_dist_matrix,
                                 "generalMatrix"),
                     "TsparseMatrix")
  one <- tm@x == 1
  adj_i <- tm@i[one] + 1L
  adj_j <- tm@j[one] + 1L
  rm(tm, one)
  o     <- order(adj_j, method = "radix")
  adj_i <- adj_i[o]
  adj_off <- c(0L, cumsum(tabulate(adj_j[o], nbins = length(cn))))
  rm(adj_j, o)

  sufix   <- neigh_pid_suffix_index(col_pID)
  pid_ix  <- neigh_index(owner_data_used$situs_pID)
  # colname pID -> value id in the owner frame, so the per-group loop never has
  # to hash strings again. NA means that pID is absent from the frame, which
  # contributes no rows -- exactly what %in% did.
  col_pid_vid <- match(col_pID, pid_ix$uv)
  full_addr   <- owner_data_used$situs_address

  in_block <- gd$situs_pID %in% pIDs_used
  # paste() stringifies NA, so an NA situs_address queries for the literal "NA".
  q_addr <- gd$situs_address
  q_addr[is.na(q_addr)] <- "NA"
  q_key  <- match(gd$situs_pID, sufix$uk)

  todo    <- which(in_block)
  cos_g   <- vector("list", length(todo))
  cos_t   <- vector("list", length(todo))
  n_multi_col <- 0L   # groups matching >1 colname (the substring-vs-exact delta)
  n_hit_col   <- 0L
  for (p in seq_along(todo)) {
    i <- todo[[p]]
    kk <- q_key[[i]]
    if (is.na(kk)) next
    cols <- sufix$cols[sequence(sufix$off[kk + 1L] - sufix$off[kk],
                                from = sufix$off[kk] + 1L)]
    a <- q_addr[[i]]
    if (nzchar(a)) cols <- cols[startsWith(col_addr[cols], a)]
    if (!length(cols)) next
    n_hit_col <- n_hit_col + 1L
    if (length(cols) > 1L) n_multi_col <- n_multi_col + 1L

    len <- adj_off[cols + 1L] - adj_off[cols]
    if (!sum(len)) next
    dist_inds <- unique(adj_i[sequence(len, from = adj_off[cols] + 1L)])

    pv <- unique(col_pid_vid[dist_inds])
    pv <- pv[!is.na(pv)]
    if (!length(pv)) next
    plen <- pid_ix$off[pv + 1L] - pid_ix$off[pv]
    cand <- pid_ix$rows[sequence(plen, from = pid_ix$off[pv] + 1L)]
    dn   <- cand[full_addr[cand] %in% unique(col_addr_key[dist_inds])]
    if (!length(dn)) next
    cos_t[[p]] <- dn
    cos_g[[p]] <- rep.int(i, length(dn))
  }
  edge_t[[length(NEIGH_MATCH_COLS) + 1L]] <- unlist(cos_t, use.names = FALSE)
  edge_g[[length(NEIGH_MATCH_COLS) + 1L]] <- unlist(cos_g, use.names = FALSE)
  rm(cos_t, cos_g, adj_i, adj_off, sufix, pid_ix)
  print(Sys.time())

  # ---- union -> ascending -> dedup -> space join ---------------------------
  eg <- unlist(edge_g, use.names = FALSE)
  et <- unlist(edge_t, use.names = FALSE)
  rm(edge_g, edge_t)
  o  <- order(eg, et, method = "radix")
  eg <- eg[o]; et <- et[o]; rm(o)
  neighbors <- character(n_groups)          # groups with no neighbour stay ""
  parts <- split(et, eg)
  # et is already ascending inside each group, so unique() dedups adjacently and
  # preserves the ascending order the original got from neighbors[order(...)].
  neighbors[as.integer(names(parts))] <- vapply(parts,
                                                function(z) paste0(unique(z),
                                                                   collapse = " "),
                                                character(1))
  rm(parts, eg, et)

  # ---- row order ----------------------------------------------------------
  # Reproduce multidplyr's permutation: groups are dealt to FINAL_OUTPUT_WORKERS
  # shards by greedy least-loaded-first (ties to the lowest worker index), then
  # collect() concatenates the shards in worker order.
  counts <- lengths(gd$.rows)
  load   <- integer(FINAL_OUTPUT_WORKERS)
  worker <- integer(n_groups)
  for (i in seq_len(n_groups)) {
    j <- which.min(load)
    worker[[i]] <- j
    load[[j]] <- load[[j]] + counts[[i]]
  }
  ord <- order(worker, seq_len(n_groups), method = "radix")

  situs_neighbor_ind <- dplyr::group_by(
    tibble::tibble(situs_pID     = gd$situs_pID[ord],
                   situs_address = gd$situs_address[ord],
                   situs_neighbors = neighbors[ord]),
    situs_pID)

  cat("situs_neighor_gen: cosine block matched",
      n_hit_col, "groups to >=1 colname,", n_multi_col, "to more than one\n")
  cat("situs_neighor_gen: groups with a non-uniform nchar >",
      NEIGH_ADDR_MIN_CHARS, "verdict:",
      paste(names(mixed_gate), mixed_gate, sep = "=", collapse = " "), "\n")
  if (any(mixed_gate > 0L)) {
    warning("situs_neighor_gen: the per-value address gate now differs from the ",
            "original recycled gate on the groups counted above")
  }
  print(Sys.time())

  situs_neighbor_ind
}

situs_neighor_gen_final = function(owner_data_used,
                                   situs_neighbor_ind){

  # Assign every parcel a landlord-portfolio group id.
  #
  # situs_neighbor_ind$situs_neighbors holds, per situs row, a space-separated
  # list of owner_data_used ROW INDICES that share that row's situs address. Two
  # parcels belong to the same portfolio when they are reachable from each other
  # through those shared rows, so the grouping is the transitive closure of
  # "co-listed in some situs row" -- exactly the connected components of the
  # bipartite incidence graph parcel <-> situs row. igraph computes that exactly,
  # in one process, in seconds.
  #
  # This replaces a foreach(%dopar%) depth-capped BFS (iterative_add) that ran
  # 24,596 s and was then cgroup-OOM-killed at 145 GB. Three separate defects
  # made that version not worth repairing in place:
  #
  #   1. iterative_add was defined INSIDE this function, so future serialised its
  #      enclosing frame to all 24 workers: owner_data_used (1321 MB),
  #      situs_neighbor_ind (112 MB) and the strsplit token list (1439 MB) -- the
  #      list twice over, because R's serialiser ref-tracks environments but not
  #      vectors. About 4.4 GB per worker, about 105 GB in total. The
  #      future.globals.maxSize = 4e9 guard could not catch it, because
  #      object.size() on a closure reports 560 bytes: it does not follow the
  #      environment.
  #
  #   2. second_inds was meant to be a global visited-set, but under multisession
  #      "<<-" only ever mutated each worker's private copy, so the output
  #      depended on FINAL_OUTPUT_WORKERS. Worse, the prune it fed made the
  #      result incoherent rather than merely approximate: once a component had
  #      been touched, every later row in it returned a small stub, and the
  #      decreasing-length sort assigned stubs LAST, overwriting each full
  #      component with a fragment of itself.
  #
  #   3. The consumer wrote the answer into a discarded copy:
  #        sapply(..., function(index){ owner_data_used$group_assign[i] <- index })
  #      Plain "<-" inside that closure creates a local binding, so the enclosing
  #      frame was never touched -- verified in this image: the same pattern with
  #      "<-" yields 0,0,0,0,0 where "<<-" yields 1,1,2,2,0. The author's
  #      commented-out mirai version did use "<<-". This target therefore
  #      returned group_assign == 0 for every parcel, so there is no prior output
  #      to stay compatible with, and it also spent the run copying the 1.3 GB
  #      frame once per group.
  #
  # Because the old output was a constant, exact components are a strict
  # improvement rather than a behaviour change. They are also what the author was
  # reaching for: the commented-out "while (new_length != base_length)" loop is
  # full closure, and with the prune removed every row of a component returns the
  # identical vector, which the outer unique() collapses to one entry per
  # component.
  #
  # KNOWN DATA ARTEFACT, deliberately not papered over here: the largest
  # component is 65,878 parcels spanning 30,250 situs rows, and its widest rows
  # are street-only addresses carrying no house number ("GUILBEAU RD SAN ANTONIO
  # 78250", "S LAREDO ST SAN ANTONIO 78207"). Parcels missing a house number
  # normalise onto a bare street name, which chains unrelated owners together.
  # That is an upstream address-normalisation problem in situs_neighor_gen; it is
  # not fixed by capping component size here, because a cap would invent a
  # grouping policy this function has no basis to choose. The size distribution
  # is printed below so the artefact stays visible in the run log.

  print(dim(owner_data_used))
  print(dim(situs_neighbor_ind))
  print(Sys.time())

  n_parcels <- nrow(owner_data_used)
  n_situs   <- nrow(situs_neighbor_ind)

  # Same strsplit the old code did; only the consumer of it changed.
  neighbor_tokens <- strsplit(situs_neighbor_ind$situs_neighbors, split = ' ')
  token_counts    <- lengths(neighbor_tokens)
  nonempty_rows   <- which(token_counts > 0L)

  parcel_ids <- suppressWarnings(as.integer(unlist(neighbor_tokens[nonempty_rows],
                                                   use.names = FALSE)))
  situs_ids  <- rep.int(nonempty_rows, token_counts[nonempty_rows])
  rm(neighbor_tokens)
  invisible(gc())

  # Guard the join instead of trusting it. A token that is not a usable row index
  # of owner_data_used would otherwise create a phantom vertex and silently merge
  # unrelated portfolios through it.
  usable <- !is.na(parcel_ids) & parcel_ids >= 1L & parcel_ids <= n_parcels
  if (any(!usable)) {
    warning(sprintf('situs_neighor_gen_final: dropped %d of %d neighbour tokens outside 1..%d',
                    sum(!usable), length(parcel_ids), n_parcels))
    parcel_ids <- parcel_ids[usable]
    situs_ids  <- situs_ids[usable]
  }
  rm(usable)

  if (length(parcel_ids) == 0L) {
    warning('situs_neighor_gen_final: no usable neighbour tokens; group_assign left all 0')
    owner_data_used$group_assign <- integer(n_parcels)
    print(Sys.time())
    return(owner_data_used)
  }

  # Parcel vertices occupy 1..n_parcels and situs vertices sit above them, so the
  # two id spaces cannot collide and a situs row can never be read as a parcel.
  graph_used <- igraph::make_graph(c(rbind(parcel_ids, n_parcels + situs_ids)),
                                   n = n_parcels + n_situs,
                                   directed = FALSE)
  membership <- igraph::components(graph_used)$membership
  rm(graph_used)
  invisible(gc())

  # Only parcels that appear in at least one neighbour list get a group; the rest
  # keep 0, so 0 keeps meaning "not grouped". Numbering runs largest component
  # first, preserving the old code's intent that group 1 is the biggest
  # portfolio.
  #
  # Residual behaviour worth knowing before touching shinyApp/app.R: 1,443,757
  # parcels are named in no neighbour list at all and so all share label 0.
  # app.R filters with group_assign %in% group_assign_used, so selecting an
  # ungrouped parcel matches all 1.44M of them. That is still far better than
  # today, where the column is uniformly 0 and any selection matches every
  # row, and it keeps the author's group_assign <- 0 convention. The clean fix
  # belongs in app.R: drop 0 from group_assign_used.
  parcel_membership <- membership[seq_len(n_parcels)]
  is_referenced     <- tabulate(parcel_ids, nbins = n_parcels) > 0L
  parcels_per_comp  <- tabulate(parcel_membership[is_referenced],
                                nbins = max(membership))

  occupied          <- which(parcels_per_comp > 0L)
  # Largest first, ties broken by component id, so the labelling is
  # byte-identical across reruns by construction rather than by relying on
  # order() happening to be stable for the sort method in play.
  by_size           <- occupied[order(-parcels_per_comp[occupied], occupied)]
  group_of          <- integer(length(parcels_per_comp))
  group_of[by_size] <- seq_along(by_size)

  group_assign <- integer(n_parcels)
  group_assign[is_referenced] <- group_of[parcel_membership[is_referenced]]
  owner_data_used$group_assign <- group_assign

  sizes <- parcels_per_comp[by_size]
  cat(sprintf('group_assign: %d of %d parcels grouped into %d groups, %d of them with 2+ parcels, largest %d\n',
              sum(is_referenced), n_parcels, length(by_size),
              sum(sizes >= 2L), sizes[1]))
  cat('group size quantiles:\n')
  print(quantile(sizes, c(0.5, 0.9, 0.99, 0.999, 1)))
  cat('largest 10 groups: ', paste(utils::head(sizes, 10), collapse = ', '), '\n', sep = '')

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
       workers = FINAL_OUTPUT_WORKERS,
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
