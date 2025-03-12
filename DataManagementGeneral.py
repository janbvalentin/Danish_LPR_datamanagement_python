import pandas as pd
import numpy as np
import os
import sys
import pyreadstat as prs

import FilterFunctions as dmf


class DataManagementGeneral:
  
  # Dictionary used to standardise variable names
  names_dict = {
    "PNR":           "PNR",
    "RECNUM":        "DW_EK_KONTAKT",
    "DW_EK_KONTAKT": "DW_EK_KONTAKT",
    "C_DIAG":        "DIAGNOSEKODE",
    "DIAGNOSEKODE":  "DIAGNOSEKODE",
    "C_DIAGTYPE":    "DIAGNOSETYPE",
    "DIAGNOSETYPE":  "DIAGNOSETYPE",
    "DATO_START":    "DATO_START",
    "D_INDDTO":      "DATO_START",
    "D_ODTO":        "DATO_START",
    "PROCEDUREKODE": "PROCEDUREKODE",
    "C_OPR":         "PROCEDUREKODE",
    "PROCEDURETYPE": "PROCEDURETYPE",
    "C_OPRART":      "PROCEDURETYPE"
  }
  
  def __init__(self,rawg,rawe,tmp,res):
    self.rawg = rawg
    self.rawe = rawe
    self.tmp = tmp
    self.res = res

  # Import sas file
  # Use filter_fct to extract specific diagnoses, procedures etc.
  # Use lookup to standardize variable names
  # Use unique_id_df to filter population.
  def read_lpr_sas(self,
    phile,
    lookup       = None, #self.names_dict
    unique_id_df = None,
    match_by     = "PNR",
    path         = None, #self.rawg
    filter_fct   = None,
    keep         = None,
    add_filname  = False,
    slice_s      = 50000000,
    **kwargs):
    
    # Defaults:
    lookup = lookup or self.names_dict
    path = path or self.rawg
    
    offset = 0
    s = 1
    df = pd.DataFrame()
    while True:
      df_chunk,_ = prs.pyreadstat.read_sas7bdat(path + phile,row_limit=slice_s,row_offset=offset)
      if df_chunk.empty: break
      
      df_chunk.rename(columns=lookup,inplace=True)
      
      if isinstance(unique_id_df,pd.DataFrame) or isinstance(unique_id_df,pd.Series):
        df_chunk = df_chunk.join(pd.DataFrame(unique_id_df).set_index(match_by),on=match_by,how='inner',validate='m:1')
      
      if filter_fct is not None:
        df_chunk = filter_fct(df_chunk,**kwargs)
      
      if keep is not None:
        df_chunk = df_chunk[df_chunk.columns.intersection(keep)]
      
      if add_filname:
        df_chunk['filename'] = phile
      
      df = pd.concat([df,df_chunk])
      print(". " + phile + ", slice: " + str(s))
      offset += slice_s
      s += 1
    
    return(df)
    
  # Merge two data frames
  # Use filter_fct_diag and filter_fct_adm to extract specific diagnoses, procedures etc.
  # Use lookup to standardize variable names
  def join_lpr(self,
    code_file,
    adm,
    lookup     = None, #self.names_dict
    join_by    = "DW_EK_KONTAKT",
    path       = None, #self.rawg
    filter_fct = None,
    keep       = None,
    **kwargs):

    # Defaults:
    lookup = lookup or self.names_dict
    path = path or self.rawg
    
    code_data = self.read_lpr_sas(
      code_file,
      lookup       = lookup,
      path         = path,
      filter_fct   = filter_fct,
      keep         = None,
      add_filname  = False,
      **kwargs)
    
    code_data = code_data.join(adm.set_index(join_by),on=join_by,how='inner',validate='m:1')
    
    if keep is not None:
      code_data = code_data[code_data.columns.intersection(keep)]
    
    return(code_data)
  
if __name__ == "__main__":
  print("There is no main!")