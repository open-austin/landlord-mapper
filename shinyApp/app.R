#reading in libraries
library(DT)
library(leaflet)
library(stringi)
library(readxl)
library(tidyverse)
library(shiny)
# library(rgdal)
library(readr)
library(plotly)
library(sp)
library(shinydashboard)
library(listviewer)


library(networkD3)
library(igraph)
library(stringdist)
library(tidygeocoder)
#data source
owners_info_total <- read.csv('owners_info_total.csv',
                              row.names = 'X')

owners_info_total$group_assign <- as.character(owners_info_total$group_assign)


owners_info_situs_cols <- grepl('situs',colnames(owners_info_total))
                                
owners_info_total <- cbind(owners_info_total[,owners_info_situs_cols],
                           owners_info_total[,!owners_info_situs_cols])


owners_info_d3graph <- readRDS('owners_info_d3graph.rds')


address_clean = function(situs_address){
  
  situs_address <- gsub('SUITE|STE|CONDO|UNIT|APT|BLDG|[[:punct:]]',
                        '',
                        situs_address)
  
  situs_address <- gsub('[[:space:]]+NA[[:space:]]+|[[:space:]]+NO[[:space:]]+',
                        ' ',    
                        situs_address)
  situs_address <- gsub('^NA*[[:space:]]+|[[:space:]]+NA*$',
                        '', 
                        situs_address)
  situs_address <- gsub('[[:space:]]{2,}',
                        ' ',   
                        situs_address)
  situs_address <-sapply(situs_address,
                         function(address){
                           regex_used <- '[[:digit:]]+TH|[[:digit:]]+RD|[[:digit:]]+ND'
                           start_ind <- regexpr(regex_used, address)
                           if(start_ind<0){
                             return(address)
                           }
                           gsub(regex_used,
                                substr(address,(start_ind),
                                       (start_ind+attr(start_ind,
                                                       'match.length')-3
                                        )
                                       ),
                                address)
                           }
                         )
  situs_address <- gsub('RANCH ROAD',
                        'RR',        
                        situs_address)
  
  situs_address <- gsub('DRIVE',
                        'DR',
                        situs_address)
  
  situs_address <- gsub('INTERSTATE',
                        'IH',
                        situs_address)
  situs_address <- gsub('LANE',
                        'LN', 
                        situs_address)
  situs_address <- gsub('ROAD',
                        'RD', 
                        situs_address)
  situs_address <- gsub('TRAIL',
                        'TRL',
                        situs_address)
  situs_address <- gsub('STREET',
                        'ST',   
                        situs_address)
  
  situs_address <- gsub('FREEWAY',
                        'FRWY',   
                        situs_address)
  
  situs_address <- gsub('BLUFF',
                        'BLF',    
                        situs_address)
  situs_address <- gsub('FLOOR',
                        'FL',   
                        situs_address)
  situs_address <- gsub('PLAZA',
                        'PLZ',
                        situs_address)
  situs_address <- gsub('AVENUE',
                        'AVE',
                        situs_address)
  situs_address <- gsub('CIRCLE',
                        'CIR',
                        situs_address)
  situs_address <- gsub('LANE',
                        'LN',  
                        situs_address)
  situs_address <- gsub('PARKWAY',
                        'PKWY',                   
                        situs_address)
  situs_address <- gsub('WAY',
                        'WY',
                        situs_address)
  situs_address <- gsub('COURT',
                        'CT',
                        situs_address)
  situs_address <- gsub('COVE',
                        'CV',
                        situs_address)
  situs_address <- gsub('PLACE',
                        'PL',
                        situs_address)
  situs_address <- gsub('POINT',
                        'PT',
                        situs_address)
  situs_address <- gsub('HL',
                        'HILL',                                               
                        situs_address)
  situs_address <- gsub('SPGS',
                        'SPRINGS',
                        situs_address)
  
  situs_address <- gsub('BOULEVARD',
                        'BLVD',
                        situs_address)
  situs_address <- gsub('MOUNTAIN',
                        'MTN',
                        situs_address)
  
  situs_address <- gsub('NORTH',
                        'N',
                        situs_address)
  
  situs_address <- gsub('WEST',
                        'W',
                        situs_address)
  situs_address <- gsub('SOUTH',
                        'S',
                        situs_address)
  situs_address <- gsub('EAST',
                        'E',
                        situs_address)
  situs_address
  
}

#UI Interface
ui <- fluidPage(
  # Application titles
  
  titlePanel(strong("Austin Landlord Mapper")),
  navbarPage(strong('Maps'),
             tabPanel('Property Search',
                      class = 'leftAlign',
                      fluidPage(
                        fluidRow(textInput("propertyAddress", 
                                           h4("Address Search"),
                                           '1000 S 8 ST AUSTIN TX 78704'
                                           ) ),
                        radioButtons('propertyFilter1',
                                     label = NULL,
                                     inline = TRUE,
                                     choices = c('Search by Property'='1',
                                                 'Search by Owner'='2',
                                                 'Search by Corporation'='3',
                                                 'Search by Group ID'='4')
                        ),
                        
                        br(),
                        leafletOutput('propertyMap',
                                        height = 600),
                        br(),
                        br(),
                        h4(strong('Owner Table Lookup')),
                        radioButtons('propertyFilter2',
                                     label = NULL,
                                     inline = TRUE,
                                     choices = c('Search by Property'='1',
                                                 'Search by Owner'='2',
                                                 'Search by Corporation'='3')
                        ),
                        fluidRow(
                          DT::DTOutput('propertyTable',
                                       width = '100%',
                                       fill = TRUE)),
                        downloadButton("downloadTabularData",
                                       "Download Tablular Data"),
                        
                        p("Made with",
                          a("Shiny",
                            href = "http://shiny.rstudio.com"),
                          ".")
                         )),
             tabPanel('Landlord Network Analysis',
                      class = 'leftAlign',
                      fluidPage(textInput('ownerInput',
                                          h4('Owner Search'),
                                          'Powell'
                                          ),
                                networkD3::simpleNetworkOutput('D3_graph')
                                )
                      ), #network graph of landlord connections by common ownership
             tabPanel('Tenant Stress'),#heat map of rent burden, evictions weighted by renter population
             tabPanel('Property Quality') #heatmap of code violations, fines, age
  )
)


# Define server logic required to draw a histogram
server <- function(input, output, session) {
  primary_address_lat = c()
  primary_address_long = c()
  map_previous = c()
  props_inds_used <-c()
  data <- reactive({
    
    if(input$propertyFilter1!='4'){
      address_used <- address_clean(toupper(input$propertyAddress))
    }
    else{
      address_used <- sprintf('^%s$',
                              input$propertyAddress)
    }
    
    
    col_used <- switch(input$propertyFilter1,
                       '1'='situs_address',
                       '2'='owner_name',
                       '3'='corp_business_name',
                       '4'='group_assign'
                       )
    
    print(col_used)
    if(input$propertyFilter1=='1'){
      props_inds_used <-  which(ain(owners_info_total[,col_used],
                                    address_used,
                                    method = 'cosine',
                                    q = 3,
                                    maxDist = 0.05))
      }
    print(input$propertyFilter1)
    
    if(input$propertyFilter1=='4'){
      props_inds_used <- which(grepl(sprintf('^%s$',
                                             address_used),
                                     owners_info_total[,col_used]
      )
      )
    }
    if(((input$propertyFilter1 %in% c('1','4'))==FALSE)|
       (length(props_inds_used)==0)
       ){
      props_inds_used <- which(grepl(address_used,
                                     owners_info_total[,col_used])
                               )
    }
    print(props_inds_used)
    if(length(props_inds_used)==0){
      return()
    }
    print(props_inds_used)
    group_assign_used <- unique(owners_info_total$group_assign[props_inds_used])
    print(group_assign_used)
    data_used <- dplyr::filter(owners_info_total,
                  group_assign %in% group_assign_used)
    data_used$owners_used <- gsub('<br>$',
                                  '',
                                  paste(as.character(unique(data_used$owner_name)),
                                   collapse = '<br>'))
    data_used$tcad_link <- sapply(data_used$situs_pID,
                                  function(pID){
                                    
                                      link_used <- paste('https://travis.prodigycad.com/property-detail',
                                                        as.character(pID),
                                                        lubridate::year(Sys.time()),
                                                        sep = '/')
                                     as.character(a(link_used,
                                                    href = link_used))
                                  }) 
    data_used$cpa_link <- sapply(data_used$corp_TTN,
                                 function(TTN){
                                   link_used <- sprintf('https://comptroller.texas.gov/taxes/franchise/account-status/search/%s',
                                                        TTN)
                                   as.character(a(link_used,
                                                  href = link_used))
                                 })
    data_used$is_primary_address <-switch(input$propertyFilter1,
                                          '1'= sapply(data_used$situs_address,
                                                      function(add){
                                                        ifelse(grepl(address_used,
                                                                     add),
                                                               'red',
                                                               'teal')
                                                        }
                                                      ),
                                          '2'='red',
                                          '3'='red',
                                          '4'='red'
                                          )
   
    primary_address_long <<-median(unique(dplyr::filter(data_used,
                                                    is_primary_address=='red')$situs_long),
                                 na.rm=TRUE)
    primary_address_lat <<-median(unique(dplyr::filter(data_used,
                                                 is_primary_address=='red')$situs_lat),
                                na.rm=TRUE)
    print(primary_address_lat)
    print(primary_address_long)
    
    data_used$popup_label <- paste(paste(h4('Site Address:'),
                                         data_used$situs_address),
                                   paste(h4('Group ID:'),
                                         data_used$group_assign),
                                   paste(h4('Shell Owner:'),
                                         data_used$veneer_owner),
                                   paste(h4('Texas Taxpayer #:'),
                                         data_used$corp_TTN),
                                   paste(h4('Parent Entity Name:'),
                                         data_used$corp_business_name),
                                   paste(h4('Parent Entity Owner(s)'),
                                         na.omit(data_used$owners_used)),
                                   paste(h4('Parent Entity Mail Address:'),
                                         data_used$corp_mail_address),
                                   paste(h4('Registered Agent:'),
                                         data_used$corp_registered_agent_namena.omi),
                                   paste(h4('Registered Agent Address:'),
                                         data_used$corp_registered_agent_mail_address),
                                   paste(h4('Travis County Property Appraisal Info:'),
                                         data_used$tcad_link),
                                   paste(h4('Texas Comptroller Business Filing:'),
                                         data_used$cpa_link),
                                   paste(h4('Census Block:'),
                                         data_used$census_block),
                                   sep = '<br>')
    data_used
  })
  
  red_path <- "markers_red.png"
  teal_path <- "markers_teal.png"
  iconColors <- iconList(red = makeIcon(red_path, iconWidth = 32, iconHeight =46),
           teal = makeIcon(teal_path, iconWidth = 32, iconHeight =46))
  
  output$propertyMap <- renderLeaflet(({
    
    owner_data <- data()
    if(length(owner_data)==0){
      print('data')
      return(map_previous)
    }
    groups_fnd <- unique(owner_data$group_assign)
    print(groups_fnd)
    if(input$propertyFilter1=='1'){
      if(length(groups_fnd)>1){
        print('groups')
        return(map_previous)
      }
    }
    else{
      if(length(groups_fnd)>20){
        print('groups')
        return(map_previous)
      }
    }
    
    
    # function to generate exact color scheme from bin_breaks and pal_used

    m<- leaflet(owner_data) %>%
      addTiles() %>%
      addMarkers(lng = ~situs_long,
                 lat = ~situs_lat,
                 popup =~popup_label,
                 label = ~situs_address,
                 icon = iconColors[as.character(owner_data$is_primary_address)],
                 popupOptions = popupOptions(closeOnClick = TRUE,
                                             maxHeight = 300,
                                             maxWidth = 1000)) %>%
      addLegend(colors = c('red','teal'),
                label =c('Primary Address',
                         'Related Addresses') )# %>%
      # setView(lng = primary_address_long,
      #         lat = primary_address_lat,
      #         zoom = 20)
    
    
    map_previous <<- m
    
    return(m)
    }))
  
  
  
  dt_colnames<- c()
  dt_situs_cols <- c()
  dt_numeric_cols <- c()
  dt_used <- c()
  cols_used <- c()
  dt_nonvis_cols <-c()
  output$propertyTable <- renderDT({
    
    data_used = owners_info_total
    # data_used$group_assign<- as.character(data_used$group_assign)
    dt_colnames <<- colnames(data_used)
    dt_situs_cols <<-grepl('situs',dt_colnames )
    dt_owner_cols <<-grepl('^owner_n',dt_colnames )
    dt_corp_cols <<-grepl('^corp_re|^corp_bu',dt_colnames )
    dt_group_cols <<-grepl('^group_assign',dt_colnames )
    dt_nonvis_cols <<- grepl('match|tiger|corp_right|corp_state|corp_sos|corp_eff|corp_tx',
                             dt_colnames)
    cols_used <<- switch(input$propertyFilter2,
                       '1'=dt_situs_cols,
                       '2'=dt_owner_cols,
                       '3'=dt_corp_cols
                       )
    dt_numeric_cols <<- sapply(dt_colnames,
                               function(col){ grepl('_fips$|^census|group', 
                                                    col
                                                    )
                                 }
                               )
    if(input$propertyFilter2=='4'){
      props_inds_used <- which(grepl(sprintf('^%s$',
                                             input$propertyTable_search),
                                     data_used[,cols_used]
      )
      )
      if((length(props_inds_used)==0)){
        props_inds_used <- which(grepl(input$propertyTable_search,
                                       data_used[,cols_used])
        )
      }
      print(input$propertyTable_search)
      print(head(data_used[props_inds_used,]))
      print(length(props_inds_used))
      if(length(props_inds_used)==0){
        print('nope')
        dt_used <<- data_used
        return(data_used)
        
      }
      else{
        dt_used <<- data_used[props_inds_used,]
        return(data_used[props_inds_used,])
        
      }
    }
    print('10101')
    dt_used <<- data_used
    data_used
    },
    server = TRUE,
    # extensions = c("SearchBuilder", "DateTime"),
    options = list(pageLength = 5,  
                   search = list(regex=TRUE,
                                 smart=FALSE
                                 #search =input$propertyAddress
                                 ),
                   language = list(search = 'Lookup:'),
                   dom = "Qlfrtip",
                   # searchBuilder = TRUE,
                   searching = TRUE,
                   responsive = TRUE,
                   autowidth = TRUE,
                   scrollX = TRUE,
                   height = '400px',
                   columnDefs = list(
                     list(className = 'dt-left',
                          targets = (which(!dt_numeric_cols))),
                     list(className = 'dt-right',
                          targets = (which(dt_numeric_cols))),
                     list(
                       targets = '_all',
                       width = '25px'
                         ),
                     list(targets=(which(cols_used)
                     ),
                     searchable=TRUE,
                     visible = TRUE
                     ),
                     list(targets=(which(dt_nonvis_cols)
                                   ),
                          searchable=FALSE,
                          visible = FALSE
                          )
                          ,
                   list(targets=(which(c(!cols_used) & c(!dt_nonvis_cols))
                                 ),
                        searchable=FALSE,
                        visible = TRUE)
                   )
                   )
    )
  
  output$downloadTabularData <- downloadHandler(
    filename = function() {
      paste0(input$propertyTable_search, ".csv")
    },
    content = function(file) {

      write.csv(dt_used[input$propertyTable_rows_all,], file)
    }
  )
}

# Run the application 
shinyApp(ui = ui, server = server)

