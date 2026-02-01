import pandas as pd
import numpy as np
import urllib
import zipfile
import re
import ijson
import multiprocessing
from multiprocessing import Pool

situs_year = list()
situs_pID = list()
situs_streetNum = list()
situs_streetPrefix = list()
situs_streetName = list()
situs_streetSuffix = list()
situs_city = list()
situs_state = list()
situs_zip = list()
situs_country = list()
situs_international = list()
owner_year = list()
owner_ID = list()
owner_name = list()
owner_nameSecondary = list()
owner_addrDeliveryLine = list()
owner_addrUnitDesignator = list()
owner_addrCity = list()
owner_addrZip = list()
owner_addrState = list()
owner_addrCountry = list()
owner_addrInternational = list()
owner_ownerPct = list()
owner_exemptions = list()
owner_pID = list()
propertyChar_year =list()

propertyChar_marketArea = list()
propertyChar_region = list()
propertyChar_zoning = list()
propertyChar_pID = list()

pID_list = list()
year_list = list()
situs_coord = list()

# deeds_deedDt=list()
deeds_pID=list()
deeds_deedID=list()
deeds_sellerLine=list()
deeds_buyerLine=list()
deeds_deedDt=list()
deeds_year = list()

propertyProf_year = list()
propertyProf_pID = list()
propertyProf_imprvStateCd = list()
propertyProf_AirHeat = list()
propertyProf_imprvTotalArea = list()
propertyProf_landStateCd = list()
propertyProf_landTotalLots = list()
propertyProf_landTotalSqft = list()
propertyProf_imprvStories = list()
propertyProf_imprvUnits = list()
propertyProf_imprvActualYearBuilt = list()
propertyProf_imprvEffYearBuilt = list()
propertyProf_fieldInsDt = list()
propertyProf_landHomesitePct = list()
propertyProf_mobileHomeNumbers = list()

def propChar_add(key, value):
  if(key=='pID'):
    if(value is None):
      propertyChar_pID.append('NA')
    else:
      propertyChar_pID.append(value)
              
  if(key=='zoning'):
    if(value is None):
      propertyChar_zoning.append('NA')
    else:
              
      propertyChar_zoning.append(value)
  


def propProf_add(key,value):
  if(key=='pID'):
    if(value is None):
      propertyProf_pID.append('NA')
    else:
      propertyProf_pID.append(value)
  if(key=='centralAirHeat'):
    if(value is None):
      propertyProf_AirHeat.append('NA')
    else:
      propertyProf_AirHeat.append(value)
  if(key=='imprvStateCd'):
    if(value is None):
      propertyProf_imprvStateCd.append('NA')
    else:
      propertyProf_imprvStateCd.append(value)

  if(key=='imprvTotalArea'):
    if(value is None):
      propertyProf_imprvTotalArea.append('NA')
    else:
      propertyProf_imprvTotalArea.append(value)

  if(key=='imprvStories'):
    if(value is None):
      propertyProf_imprvStories.append('NA')
    else:
      propertyProf_imprvStories.append(value)
  if(key=='imprvActualYearBuilt'):
    if(value is None):
      propertyProf_imprvActualYearBuilt.append('NA')
    else:
      propertyProf_imprvActualYearBuilt.append(value)
  if(key=='imprvEffYearBuilt'):
    if(value is None):
      propertyProf_imprvEffYearBuilt.append('NA')
    else:
      propertyProf_imprvEffYearBuilt.append(value)
  if(key=='landStateCd'):
    if(value is None):
      propertyProf_landStateCd.append('NA')
    else:
      propertyProf_landStateCd.append(value)
  if(key=='landTotalLots'):
    if(value is None):
      propertyProf_landTotalLots.append('NA')
    else:
      propertyProf_landTotalLots.append(value)

  
def situsChar_add(key,value):
  if(key=='pID'):
    if(value is None):
      situs_pID.append('NA')  
    else:
      situs_pID.append(value)
  if(key=='streetNum'):
    if(value is None):
      situs_streetNum.append('NA')
    else:
      situs_streetNum.append(value)
  if(key=='streetPrefix'):
    if(value is None):
      situs_streetPrefix.append('NA')
    else:
      situs_streetPrefix.append(value)
  if(key=='streetName'):
    if(value is None):
      situs_streetName.append('NA')
    else:
      situs_streetName.append(value)
  if(key=='streetSuffix'):
    if(value is None):
      situs_streetSuffix.append('NA')
    else:
      situs_streetSuffix.append(value)
  if(key=='city'):
    if(value is None):
      situs_city.append('NA')
    else:
      situs_city.append(value)
  if(key=='state'):
    if(value is None):
      situs_state.append('NA')
    else:
      situs_state.append(value)
  if(key=='zip'):
    if(value is None):
      situs_zip.append('NA')
    else:
      situs_zip.append(value)
  if(key=='country'):
    if(value is None):
      situs_country.append('NA')
    else:
      situs_country.append(value)
  if(key=='international'):
    if(value is None):
      situs_international.append('NA')
    else:
      situs_international.append(value)
    
   
def ownerChar_add(key,value):
  if(key=='ownerID'):
    if(value is None):
      owner_ID.append('NA')
    else:
      owner_ID.append(value)
  if(key=='pID'):
    if(value is None):
      owner_pID.append('NA')
    else:
      owner_pID.append(value)
  if(key=='ownerPct'):
    if(value is None):
      owner_ownerPct.append('NA')
    else:
      owner_ownerPct.append(value)
  if(key=='name'):
    if(value is None):
      owner_name.append('NA')
    else:
      owner_name.append(value)
  if(key=='nameSecondary'):
    if(value is None):
      owner_nameSecondary.append('NA')
    else:
      owner_nameSecondary.append(value)
  if(key=='addrDeliveryLine'):
    if(value is None):
      owner_addrDeliveryLine.append('NA')
    else:
      owner_addrDeliveryLine.append(value)
  if(key=='addrUnitDesignator'):
    if(value is None):
      owner_addrUnitDesignator.append('NA')
    else:
      owner_addrUnitDesignator.append(value)
  if(key=='addrCity'):
    if(value is None):
      owner_addrCity.append('NA')
    else:
      owner_addrCity.append(value)
  if(key=='addrZip'):
    if(value is None):
      owner_addrZip.append('NA')
    else:
      owner_addrZip.append(value)
  if(key=='addrState'):
    if(value is None):
      owner_addrState.append('NA')
    else:
      owner_addrState.append(value)
  if(key=='addrCountry'):
    if(value is None):
      owner_addrCountry.append('NA')
    else:
      owner_addrCountry.append(value)
  if(key=='addrInternational'):
    if(value is None):
      owner_addrInternational.append('NA')
    else:
      owner_addrInternational.append(value)
  if(key=='exemptions'):
    if(value is None):
      owner_exemptions.append('NA')
    else:
      owner_exemptions.append(value)


def deedChar_add(key,value):
  if(key=='pID'):
    if(value is None):
      deeds_pID.append('NA')
    else:
      deeds_pID.append(value)
  if(key=='buyerLine'):
    if(value is None):
      deeds_buyerLine.append('NA')
    else:
      deeds_buyerLine.append(value)
  if(key=='sellerLine'):
    if(value is None):
      deeds_sellerLine.append('NA')
    else:
      deeds_sellerLine.append(value)
  if(key=='deedDt'):
    if(value is None):
      deeds_deedDt.append('NA')
    else:
      deeds_deedDt.append(value)
  if(key=='deedID'):
    if(value is None):
      deeds_deedID.append('NA')
    else:
      deeds_deedID.append(value)
          
  
# if __name__ == "__main__":
def TCAD_parseYear(TCAD_special_export_file_name= "TCAD_special_export.zip",year = 2025):
  
  
  with zipfile.ZipFile(TCAD_special_export_file_name,'r') as austin_parcel_data_zip:
    parcel_file_name = austin_parcel_data_zip.namelist()[0]
    print('profile')
    # propertyProf_yearLenOld = len(propertyProf_pID)
    with austin_parcel_data_zip.open(parcel_file_name) as austin_parcel_data:
      parser = ijson.parse(austin_parcel_data)
      propProf = ijson.kvitems(parser, 'item.propertyProfile.item')
      for key, value in propProf:
        propProf_add(key,value)
    # propertyProf_yearLen = len(propertyProf_pID)-propertyProf_yearLenOld
    # propertyProf_year.append(np.repeat(str(year),propertyProf_yearLen,axis=0))

    propertyProf_data = pd.DataFrame({'propertyProf_year':year,
                                      'propertyProf_pID':propertyProf_pID,

                                  'propertyProf_imprvStateCd': propertyProf_imprvStateCd,
                                  'propertyProf_imprvTotalArea': propertyProf_imprvTotalArea,
                                  'propertyProf_landStateCd':propertyProf_landStateCd,

                                  'propertyProf_imprvStories':propertyProf_imprvStories,

                                  'propertyProf_imprvActualYearBuilt':propertyProf_imprvActualYearBuilt,
                                  'propertyProf_imprvEffYearBuilt':propertyProf_imprvEffYearBuilt
                                  }
                                  )

    propertyProf_data.to_csv('austin_propertyProf_data.csv')
  #
  with zipfile.ZipFile(TCAD_special_export_file_name,'r') as austin_parcel_data_zip:
    parcel_file_name = austin_parcel_data_zip.namelist()[0]
    print('situs')
    # situs_yearLenOld = len(situs_pID)
    with austin_parcel_data_zip.open(parcel_file_name) as austin_parcel_data:
      parser = ijson.parse(austin_parcel_data)
      situsChars = ijson.kvitems(parser, 'item.situses.item')
      for key, value in situsChars:
        situsChar_add(key, value)
    # situs_yearLen = len(situs_pID) - situs_yearLenOld
    # situs_year.append(np.repeat(str(year),situs_yearLen,axis=0))
    situs_data = pd.DataFrame({'situs_year':year,
                                  'situs_pID': situs_pID,
                                  'situs_streetNum': situs_streetNum,
                                  'situs_streetPrefix': situs_streetPrefix,
                                  'situs_streetName': situs_streetName,
                                  'situs_streetSuffix': situs_streetSuffix,
                                  'situs_city': situs_city,
                                  'situs_state': situs_state,
                                  'situs_zip': situs_zip,
                                  'situs_country': situs_country,
                                  'situs_international': situs_international})

    situs_data.to_csv('austin_situs_data.csv')

  with zipfile.ZipFile(TCAD_special_export_file_name,'r') as austin_parcel_data_zip:
    parcel_file_name = austin_parcel_data_zip.namelist()[0]
    print('owner')
    # owner_yearLenOld = len(owner_pID)
    with austin_parcel_data_zip.open(parcel_file_name) as austin_parcel_data:
      parser = ijson.parse(austin_parcel_data)
      ownerChars = ijson.kvitems(parser, 'item.owners.item')
      for key, value in ownerChars:
        ownerChar_add(key, value)
    # owner_yearLen = len(owner_pID)-owner_yearLenOld
    # owner_year.append(np.repeat(str(year),owner_yearLen,axis=0))
    owner_data =  pd.DataFrame({'owner_year':year,
                              'owner_ID': owner_ID,
                                  'owner_pID': owner_pID,
                                  'owner_ownerPct':owner_ownerPct,
                                  'owner_name': owner_name,
                                  'owner_nameSecondary': owner_nameSecondary,
                                  'owner_addrDeliveryLine': owner_addrDeliveryLine,
                                  'owner_addrUnitDesignator': owner_addrUnitDesignator,
                                  'owner_addrCity': owner_addrCity,
                                  'owner_addrZip': owner_addrZip,
                                  'owner_addrState': owner_addrState,
                                  'owner_addrCountry': owner_addrCountry,
                                  'owner_addrInternational': owner_addrInternational,
                                  'owner_exemptions':owner_exemptions
                                  }
                                  )

    owner_data.to_csv('austin_owner_data.csv')

  with zipfile.ZipFile(TCAD_special_export_file_name,'r') as austin_parcel_data_zip:
    parcel_file_name = austin_parcel_data_zip.namelist()[0]
    print('propChar')
    # propertyChar_yearLenOld = len(propertyChar_pID)
    with austin_parcel_data_zip.open(parcel_file_name) as austin_parcel_data:
      parser = ijson.parse(austin_parcel_data)
      propChar = ijson.kvitems(parser, 'item.propertyCharacteristics.item')
      for key, value in propChar:
        propChar_add(key,value)
    # propertyChar_yearLen = len(propertyChar_pID)-propertyChar_yearLenOld
    # propertyChar_year.append(np.repeat(str(year),propertyChar_yearLen,axis=0))

    propertyChar_data = pd.DataFrame({'propertyChar_year':year,
                                      'propertyChar_pID':propertyChar_pID,

                                  'propertyChar_zoning': propertyChar_zoning})

    propertyChar_data.to_csv('austin_propertyChar_data.csv')
  #   
  with zipfile.ZipFile(TCAD_special_export_file_name,'r') as austin_parcel_data_zip:
    parcel_file_name = austin_parcel_data_zip.namelist()[0]
    print('deed')
    # deeds_yearLenOld = len(deeds_pID)
    with austin_parcel_data_zip.open(parcel_file_name) as austin_parcel_data:
      parser = ijson.parse(austin_parcel_data)
      deedsChar = ijson.kvitems(parser, 'item.deeds.item')
      for key, value in deedsChar:
        deedChar_add(key,value)
    # deeds_yearLen = len(deeds_pID)-deeds_yearLenOld
    # deeds_year.append(np.repeat(str(year),deeds_yearLen,axis=0))
    print(len(deeds_year))
    print(len(deeds_pID))
    # print(len(deeds_buyerline))
    print(len(deeds_sellerLine))
    print(len(deeds_deedDt))
    print(len(deeds_deedID))
    deeds_data = pd.DataFrame({'deeds_year':year,
                                      'deeds_pID':deeds_pID,
                                      'deeds_buyerline': deeds_buyerLine,
                                  'deeds_sellerLine': deeds_sellerLine,
                                  'deeds_deedDt': deeds_deedDt,
                                  'deeds_deedID':deeds_deedID})
    
    deeds_data.to_csv('austin_deeds_data.csv')
    
    
