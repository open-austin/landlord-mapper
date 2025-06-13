#reading in libraries

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

library(stringdist)

#data source
owners_info_total <- read.csv('owners_info_total.csv',
                              row.names = 'X')


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
                      sidebarLayout(
                        sidebarPanel(
                         
                          #select Opportunity SDG
                          textInput("propertyAddress", 
                                    h4("Address"),
                                    '1000 S 8 ST AUSTIN TX 78704'
                                    ),
                          br(),
                          #select sub indicator, see server code
                          br(),
                          #interactive histogram for selected Vibrancy area, SDG, sub-ind
                          p("Made with", a("Shiny", href = "http://shiny.rstudio.com"), ".")
                        ),
                        # Show a plot of the generated distribution
                        mainPanel(
                          #main map display
                          leafletOutput('propertyMap',
                                        height = 640)
                        )
                      )
             ),
             tabPanel('Landlord Network Analysis', #network graph of landlord connections by common ownership
                      ), 
             tabPanel('Tenant Stress'),#heat map of rent burden, evictions weighted by renter population
             tabPanel('Property Quality') #heatmap of code violations, fines, age
  )
)


# Define server logic required to draw a histogram
server <- function(input, output) {
  primary_address_lat = c()
  primary_address_long = c()
  map_previous = c()
  data <- reactive({
    
    address_used <- address_clean(toupper(input$propertyAddress))
    
    props_inds_used <-  which(ain(owners_info_total$situs_address,
                            address_used,
                            method = 'cosine',
                            q = 3,
                            maxDist = 0.05))
   
    if(length(props_inds_used)==0){
      print('b')
      props_inds_used <- which(grepl(address_used,
                                     owners_info_total$situs_address
                                     )
                               )
      
    }
    
    group_assign_used <- unique(owners_info_total$group_assign[props_inds_used])
    data_used <- dplyr::filter(owners_info_total,
                  group_assign %in% group_assign_used)
    data_used$owners_used <- gsub('<br>$',
                                  '',
                                  paste(as.character(unique(data_used$owner_name)),
                                   collapse = '<br>'))
    data_used$tcad_link <- sapply(data_used$situs_pID,
                                  function(pID){
                                   as.character(
                                     a(paste('https://travis.prodigycad.com/property-detail',
                                          as.character(pID),
                                          lubridate::year(Sys.time()),
                                          sep = '/')))
                                  }) 
    
    data_used$is_primary_address <- sapply(data_used$situs_address,
                                        function(add){
                                          ifelse(grepl(address_used,
                                                       add),
                                                 'red',
                                                 'teal')
                                        })
   
    primary_address_long <<-median(unique(dplyr::filter(data_used,
                                                    is_primary_address=='red')$situs_long),
                                 na.rm=TRUE)
    primary_address_lat <<-median(unique(dplyr::filter(data_used,
                                                 is_primary_address=='red')$situs_lat),
                                na.rm=TRUE)
    
    data_used$popup_label <- paste(paste(h4('Shell Owner:'),
                                         data_used$veneer_owner),
                                   paste(h4('Texas Taxpayer #:'),
                                         data_used$corp_TTN),
                                   paste(h4('Parent Entity Name:'),
                                         data_used$corp_business_name),
                                   paste(h4('Parent Entity Owner(s)'),
                                         data_used$owners_used),
                                   paste(h4('Parent Entity Mail Address:'),
                                         data_used$corp_mail_address),
                                   paste(h4('Registered Agent:'),
                                         data_used$corp_registered_agent_name),
                                   paste(h4('Registered Agent Address:'),
                                         data_used$corp_registered_agent_mail_address),
                                   paste(h4('Travis County Property Appraisal Info:'),
                                         data_used$tcad_link),
                                   paste(h4('Census Block:'),
                                         data_used$census_block),
                                   sep = '<br>')
    data_used
  })
  
  red_path <- "leaf-red.png"
  teal_path <- "leaf-blue.png"
  iconColors <- iconList(red = makeIcon(red_path, iconWidth = 75, iconHeight =100),
           teal = makeIcon(teal_path, iconWidth = 75, iconHeight =100))
  
  output$propertyMap <- renderLeaflet(({
    #get data
    owner_data <- data()
    
    groups_fnd <- unique(owner_data$group_assign)
    print(groups_fnd)
    print(dim(owner_data))
   
    if(length(groups_fnd)>1){
      return(map_previous)
    }
    if(nrow(owner_data)==0){
      print('1')
      return()
    }
    # function to generate exact color scheme from bin_breaks and pal_used
   

    m<- leaflet(owner_data) %>%
      addTiles() %>%
      addMarkers(lng = ~situs_long,
                 lat = ~situs_lat,
                
                 popup =~popup_label,
                 label = ~situs_address,
                 icon = ~iconColors[as.character(~is_primary_address)]) %>%
      addLegend(colors = c('red','teal'),
                label =c('Primary Address',
                         'Related Addresses') ) %>%
      setView(lng = primary_address_long,
              lat = primary_address_lat,
              zoom = 20)
    print(class(m))
    print(m)
    
    map_previous <<- m
    
    return(m)
    }))
}

# Run the application 
shinyApp(ui = ui, server = server)

