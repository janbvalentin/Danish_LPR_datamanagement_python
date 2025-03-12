import pandas as pd
import numpy as np
import os
import re
import datetime as dt
from pandas.core.common import flatten



# Import excel file of Regex codes for diagnosis, medicine and procedure codes
# Expected columns: Regex, Name and Weight (all other columns will be ignored)
def read_code_list(phile):
  code_list = pd.read_excel(phile)
  if not 'Weight' in code_list:
    code_list['Weight'] = 1
  code_list['Weight'] = code_list['Weight'].apply(lambda x: 1 if np.isnan(x) else x)
  code_list = code_list[['Name','Regex','Weight']]
  return(code_list)


##############################
# Filter functions
#

# Filter primary and secondary diagnoses using list of diagnosis categories
def filter_diag_codes(data,filter_list):
  
  fdata = data[data['DIAGNOSETYPE'].str.match(r'^[ABCG]')].copy()
  
  for j,row in filter_list.iterrows():
    fdata.loc[:,row['Name']] = fdata['DIAGNOSEKODE'].str.match(row['Regex'])*row['Weight']
  
  fdata = fdata[fdata[filter_list['Name']].any(axis=1)]
  fdata = fdata[['DW_EK_KONTAKT','PNR','DATO_START']+list(filter_list['Name'])].drop_duplicates()
  
  return(fdata)


def first_non_zero(s): # Returns zero if all zero
  return(s[((s!=0) & ~(s.isnull()))].reset_index(drop=True).get(0,0))

# Filter procedures using list of procedure categories
def filter_proc_codes(data,filter_list,codename='PROCEDUREKODE',by='DW_EK_KONTAKT'):
  
  fdata = data[data[by].str.len() > 0].copy()
  
  for j,row in filter_list.iterrows():
    fdata.loc[:,row['Name']] = fdata[codename].str.match(row['Regex'])*row['Weight']
  
  fdata = fdata[fdata[filter_list['Name']].any(axis=1)]
  fdata = fdata[list(flatten([by]))+['PNR','DATO_START']+list(filter_list['Name'])].drop_duplicates() #.groupby(by).agg(first_non_zero).reset_index()
  
  return(fdata)
  

# Filter ATC data using list of ATC categories
def filter_atc_codes(data,filter_list,codename='ATC'):
  
  fdata = data[['PNR','eksd','apk','Volume',codename]].copy()
  
  for j,row in filter_list.iterrows():
    fdata.loc[:,row['Name']] = fdata[codename].str.match(row['Regex'])*row['Weight']
  
  fdata = fdata[fdata[filter_list['Name']].any(axis=1)]
  fdata = fdata[['PNR','eksd','apk','Volume']+list(filter_list['Name'])]
  
  return(fdata)
  

def date_from_week(x,century="20"):
  
  return(
    dt.datetime.strptime(century + x[0:2] + "-W" + x[2:4] + "-1","%Y-W%W-%w")
  )


# Filter acute contacts in the primary sector
def filter_ssr_codes(data):
  
  if 'KONTAKT' in data.columns:
    fdata = data[data['SPECIALE'].astype('str').str.match(r'^8[1-59]') | (data['KONTAKT'].astype('str').astype('str').str.match(r'^1($|.0$)') & data['YDLTID'].astype('str').str.match(r'^[2-9]$'))].copy()
  else:
    fdata = data[data['SPECIALE'].astype('str').str.match(r'^8[1-59]')].copy()
  
  fdata.loc[:,'AKUT_DATO'] = fdata['HONUGE'].apply(lambda x: date_from_week(x) if int(x[0:2])<40 else date_from_week(x,century="19"))
  
  return(fdata[['PNR','AKUT_DATO']])


# Filter acute hospital contacts 
def filter_acute_codes(data):
  
  if 'C_INDM' in data.columns and 'C_PATTYPE' in data.columns:
    fdata = data[data[['C_INDM','C_PATTYPE']].apply(lambda x: 
      bool(re.match(r'^1$',x['C_INDM'])) or bool(re.match(r'^3$',x['C_PATTYPE'])),
      axis=1)].copy()

  elif 'PRIORITET' in data.columns and 'KONTAKTTYPE' in data.columns:
    fdata = data[data[['PRIORITET','KONTAKTTYPE']].apply(lambda x: 
      bool(re.match(r'^ATA1$',x['PRIORITET'])) and bool(re.match(r'^ALCA00$',x['KONTAKTTYPE'])),
      axis=1)].copy()
  
  return(fdata[['PNR','DATO_START']].drop_duplicates().rename(columns={'DATO_START':'AKUT_DATO'}))


# Filter children age 2-18 from population register
def filter_bef_by_age(data,start,end):
  
  keep = ["PNR","MOR_ID","FAR_ID","FAMILIE_ID","ANTBOERNF","ANTBOERNH","ANTPERSF","ANTPERSH","CIVST","CIV_VFRA","FAMILIE_TYPE","FM_MARK","FOED_DAG","HUSTYPE","KOEN","KOM","REFERENCETID"]
  fdata = data[data.columns.intersection(keep)].copy()
  
  return(fdata[(((start-pd.to_datetime(fdata["FOED_DAG"])).dt.days < 18*365.25) & ((end-pd.to_datetime(fdata["FOED_DAG"])).dt.days > 2*365.25))])
  
  

  




if __name__ == "__main__":
  print("There is no main!")
  
