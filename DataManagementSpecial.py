import pandas as pd
import numpy as np
import os
import re
import datetime as dt
import pyreadstat as prs
from itertools import compress
import random as rd

import FilterFunctions as dmf
import DataManagementGeneral as dmg


class DataManagementSpecial(dmg.DataManagementGeneral):
  
  def __init__(self,rawg,rawe,tmp,res,justtest=False):
    super().__init__(rawg,rawe,tmp,res)
    self.diag_codes     = dmf.read_code_list("diag_regex.xlsx")
    self.nmi_diag_codes = dmf.read_code_list("nmi_diag_regex.xlsx")
    self.atc_codes      = dmf.read_code_list("atc_regex.xlsx")
    self.nmi_atc_codes  = dmf.read_code_list("nmi_atc_regex.xlsx")
    self.proc_codes     = dmf.read_code_list("proc_regex.xlsx")
    
    self.list_adm_files  = list(filter(lambda x: re.match(r'^lpr_(adm|f_(kontakter|forloeb2022))',x),os.listdir(rawg)))
    self.list_diag_files = list(filter(lambda x: re.match(r'^lpr_(diag|f_diagnoser)',x),os.listdir(rawg)))
    self.list_ube_files  = list(filter(lambda x: re.match(r'^lpr_(sksube|f_procedurer_andre)',x),os.listdir(rawg)))
    self.list_ube_files  = self.list_ube_files[1:] + [self.list_ube_files[0]]
    self.list_opr_files  = list(filter(lambda x: re.match(r'^lpr_(sksopr|f_procedurer_kirurgi)',x),os.listdir(rawg)))
    self.list_opr_files  = self.list_opr_files[1:] + [self.list_opr_files[0]]
    self.list_ssr_files  = list(filter(lambda x: re.match(r'^s(ysi|ssy)',x),os.listdir(rawg)))
    self.list_lmdb_files = list(filter(lambda x: re.match(r'^lmdb',x),os.listdir(rawg)))
    self.list_bef_files  = list(filter(lambda x: re.match(r'^bef20(1[0-9]|2[0-3])',x),os.listdir(rawg)))
    
    if justtest:
      self.list_adm_files  = self.list_adm_files[-4:]
      self.list_diag_files = self.list_diag_files[-3:]
      self.list_ube_files  = self.list_ube_files[-3:]
      self.list_opr_files  = self.list_opr_files[-3:]
      self.list_ssr_files  = self.list_ssr_files[-3:]
      self.list_lmdb_files = self.list_lmdb_files[-3:]
      self.list_bef_files  = self.list_bef_files[-3:]
    
  
  
  def map_adm_list(self,list_of_files,unique_subjects):
    
    adm_list = list(map(lambda x: self.read_lpr_sas(x,unique_id_df = unique_subjects,keep = ['DW_EK_KONTAKT','DW_EK_FORLOEB','PNR','DATO_START','C_INDM','C_PATTYPE','PRIORITET','KONTAKTTYPE']),list_of_files))
    
    """
    ascategory = ['C_INDM','C_PATTYPE','PRIORITET','KONTAKTTYPE']
    for df in adm_list:
      #print(df.memory_usage(deep=True))
      df[df.columns.intersection(ascategory)] = df[df.columns.intersection(ascategory)].astype('category')
      #print(df.memory_usage(deep=True))
    """
    
    return(adm_list)
  
  
  def map_adm_list2(self,list_of_files,unique_subjects):
    
    adm_list = list(map(lambda x: self.read_lpr_sas(x,unique_id_df = unique_subjects,keep = ['DW_EK_KONTAKT','PNR','DATO_START']),list_of_files))
    
    return(adm_list)
  
  def map_acute_data(self,list_of_files,adm_list,unique_subjects):
    
    
    ssr_data = pd.concat(list(map(lambda x: self.read_lpr_sas(
      x,
      lookup       = None,
      unique_id_df = unique_subjects,
      filter_fct   = dmf.filter_ssr_codes),list_of_files)))
    
    lpr_data = pd.concat(list(map(lambda x: dmf.filter_acute_codes(x),adm_list)))
    
    return(pd.concat([ssr_data,lpr_data]).drop_duplicates())
  
  
  def map_lmdb_data(self,list_of_files,unique_subjects):
    ssr_data = pd.concat(list(map(lambda x: self.read_lpr_sas(
      x,
      lookup       = None,
      unique_id_df = unique_subjects,
      filter_fct   = dmf.filter_atc_codes,
      #filter_list  = pd.concat([self.atc_codes,self.nmi_atc_codes])),list_of_files)))
      filter_list  = self.atc_codes),list_of_files)))
    return(ssr_data)
  
  
  def map_bef_data(self,list_of_files):
    bef_data = pd.concat(list(map(lambda x: self.read_lpr_sas(
      x,
      lookup     = None,
      filter_fct = dmf.filter_bef_by_age,
      start      = dt.datetime(2012,1,1),
      end        = dt.datetime(2022,12,31)),list_of_files)))
    return(bef_data)
  
  
  def map_diag_data(self,list_of_files,adm_list):
    diag_data = pd.concat(list(map(lambda x,y: self.read_lpr_sas(
      x,
      unique_id_df = y[["DW_EK_KONTAKT","PNR","DATO_START"]],
      match_by     = "DW_EK_KONTAKT",
      filter_fct   = dmf.filter_diag_codes,
      filter_list  = self.diag_codes),
        list_of_files,
        adm_list)))
    return(diag_data)
  
  
  def map_nmi_data(self,list_of_files,adm_list):
    diag_data = pd.concat(list(map(lambda x,y: self.read_lpr_sas(
      x,
      unique_id_df = y[["DW_EK_KONTAKT","PNR","DATO_START"]],
      match_by     = "DW_EK_KONTAKT",
      filter_fct   = dmf.filter_diag_codes,
      filter_list  = self.nmi_diag_codes),
        list_of_files,
        adm_list)))
    return(diag_data)
  
  def map_nmi_data2(self,list_of_files,unique_subjects):
    ssr_data = pd.concat(list(map(lambda x: self.read_lpr_sas(
      x,
      lookup       = None,
      unique_id_df = unique_subjects,
      filter_fct   = dmf.filter_atc_codes,
      filter_list  = self.nmi_atc_codes),list_of_files)))
    return(ssr_data)
  
  
  def map_proc_data(self,list_of_files1,list_of_files2,adm_list):
    
    which_adm = [False]*(len(adm_list)-len(list_of_files1)-1) + [True]*(len(list_of_files1)+1)
    which_adm[-2] = False
    
    print(list_of_files1)
    print("")
    print(list(compress(self.list_adm_files,which_adm)))
    print("")
    #print(list(compress(adm_list,which_adm)))
    #print("")
    
    proc1_data = pd.concat(list(map(lambda x,y: self.read_lpr_sas(
      x,
      unique_id_df = y[["PNR","DW_EK_KONTAKT"]],
      match_by     = "DW_EK_KONTAKT",
      filter_fct   = dmf.filter_proc_codes,
      filter_list  = self.proc_codes,
      by           = 'DW_EK_KONTAKT'),
        list_of_files1,
        list(compress(adm_list,which_adm)))))
    proc2_data = pd.concat(list(map(lambda x,y: self.read_lpr_sas(
      x,
      unique_id_df = y[["PNR","DW_EK_FORLOEB"]],
      match_by     = "DW_EK_FORLOEB",
      filter_fct   = dmf.filter_proc_codes,
      filter_list  = self.proc_codes,
      by           = 'DW_EK_FORLOEB'),
        [list_of_files1[-1]],
        [adm_list[-2]])))
    
    
    which_adm = [False]*(len(adm_list)-len(list_of_files2)-1) + [True]*(len(list_of_files2)+1)
    which_adm[-2] = False
    
    print(list_of_files2)
    print("")
    print(list(compress(self.list_adm_files,which_adm)))
    print("")
    
    proc3_data = pd.concat(list(map(lambda x,y: self.read_lpr_sas(
      x,
      unique_id_df = y[["PNR","DW_EK_KONTAKT"]],
      match_by     = "DW_EK_KONTAKT",
      filter_fct   = dmf.filter_proc_codes,
      filter_list  = self.proc_codes,
      by           ='DW_EK_KONTAKT'),
        list_of_files2,
        list(compress(adm_list,which_adm)))))
    proc4_data = pd.concat(list(map(lambda x,y: self.read_lpr_sas(
      x,
      unique_id_df = y[["PNR","DW_EK_FORLOEB"]],
      match_by     = "DW_EK_FORLOEB",
      filter_fct   = dmf.filter_proc_codes,
      filter_list  = self.proc_codes,
      by           ='DW_EK_FORLOEB'),
        [list_of_files2[-1]],
        [adm_list[-2]])))
    return(pd.concat([proc1_data,proc2_data,proc3_data,proc4_data]).drop_duplicates())
    
    
  


  def collect(self):
    
    bef_data = self.map_bef_data(self.list_bef_files)
    prs.pyreadstat.write_dta(bef_data,dst_path = self.tmp + "bef_data.dta")
    #bef_data,_ = prs.pyreadstat.read_dta(self.tmp + "bef_data.dta")
    #print(bef_data.head())
    unique_subjects = bef_data["PNR"].drop_duplicates()
    del bef_data
    
    lmdb_data = self.map_lmdb_data(self.list_lmdb_files,unique_subjects)
    prs.pyreadstat.write_dta(lmdb_data,dst_path = self.tmp + "lmdb_data.dta")
    del lmdb_data
    
    #adm_list = self.list_adm_files
    adm_list = self.map_adm_list(self.list_adm_files,unique_subjects)
    which_adm = [True]*len(adm_list)
    which_adm[-2] = False
    
    acute_data = self.map_acute_data(self.list_ssr_files,list(compress(adm_list,which_adm)),unique_subjects)
    prs.pyreadstat.write_dta(acute_data,dst_path = self.tmp + "acute_data.dta")
    del acute_data
    
    diag_data = self.map_diag_data(self.list_diag_files,list(compress(adm_list,which_adm)))
    #diag_data = diag_data.groupby(["DW_EK_KONTAKT","PNR","DATO_START"]).agg(dmf.first_non_zero).reset_index()
    prs.pyreadstat.write_dta(diag_data,dst_path = self.tmp + "diag_data.dta")
    del diag_data
    
    proc_data = self.map_proc_data(self.list_ube_files,self.list_opr_files,adm_list)
    prs.pyreadstat.write_dta(proc_data,dst_path = self.tmp + "proc_data.dta")
    del proc_data
    
    
  def reshape_dream(self,unique_subjects):
    slice_s = 200000
    offset = 0
    s = 1
    df = pd.DataFrame()
    while True:
      df_chunk,_ = prs.pyreadstat.read_sas7bdat(self.rawg + "dream202404.sas7bdat",row_limit=slice_s,row_offset=offset)
      if df_chunk.empty: break
      
      df_chunk = df_chunk.join(pd.DataFrame(unique_subjects).set_index("PNR"),on="PNR",how='inner',validate='1:1')
      
      columns = ['PNR'] + list(filter(lambda x: re.match(r'^y_[12]',x),df_chunk.columns))
      df_chunk = df_chunk[columns].set_index('PNR').stack().reset_index(name='status')
      df_chunk.loc[:,'status_dato'] = df_chunk['level_1'].apply(lambda x: dmf.date_from_week(x[2:6]) if int(x[2:4])<40 else dmf.date_from_week(x[2:6],century="19"))
      df_chunk = df_chunk[['PNR','status_dato','status']][df_chunk['status']!=""]
      
      df = pd.concat([df,df_chunk])
      print(". Slice: " + str(s))
      offset += slice_s
      s += 1
    
    prs.pyreadstat.write_dta(df,dst_path = self.tmp + "dream.dta")
  

  



if __name__ == "__main__":
  
  
  os.chdir("D:/Data/Workdata/Study1/Code/")
  
  rawg = "D:/Data/Rawdata/primary/"
  rawe = "D:/Data/Rawdata/external/"
  tmp = "../Tmpdata/"
  res = "../Results/"
  
  np.random.seed(1234)
  
  my_dc = dms.DataManagementSpecial(rawg,rawe,tmp,res,justtest=False)

  my_dc.collect()
  