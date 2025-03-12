import pandas as pd
import os
import re
import datetime as dt
from pandas.core.common import flatten


def match_unknown_index(
  data,
  exposurevar,
  subj_id,
  start_date,
  end_date,
  index_date,
  min_history_days,
  ncontrols=4,
  ncontrols_is_max=False,
  replacement=True,
  catvars=None,
  contvars=None,
  contvars_tol=None,
  tv_catvars=None,
  tv_suffix=None,
  reduce_contrls=False):
  
  return(
    match(
		  data             = data,
		  casevar          = exposurevar,
		  subj_id          = subj_id,
		  start_date       = start_date,
		  end_date         = end_date,
		  index_date       = index_date,
		  min_history_days = min_history_days,
		  ncontrols        = ncontrols,
		  ncontrols_is_max = ncontrols_is_max,
		  replacement      = replacement,
		  catvars          = catvars,
		  contvars         = contvars,
		  contvars_tol     = contvars_tol,
		  tv_catvars       = tv_catvars,
      tv_suffix        = tv_suffix,
		  reduce_contrls   = reduce_contrls,
		  match_type       = 'match_unknown_index'
	  )
  )


def risk_set_sampling(
  data,
  casevar,
  subj_id,
  start_date,
  end_date,
  ncontrols=4,
  ncontrols_is_max=False,
  replacement=True,
  catvars=None,
  contvars=None,
  contvars_tol=None,
  reduce_contrls=False):
  
  return(
    match(
		  data             = data,
		  casevar          = exposurevar,
		  subj_id          = subj_id,
		  start_date       = start_date,
		  end_date         = end_date,
		  ncontrols        = ncontrols,
		  ncontrols_is_max = ncontrols_is_max,
		  replacement      = replacement,
		  catvars          = catvars,
		  contvars         = contvars,
		  contvars_tol     = contvars_tol,
		  reduce_contrls   = reduce_contrls,
		  match_type       = 'risk_set_sampling'
	  )
  )
  
def match(
  data,
  casevar, # 1: case, 0: control
  subj_id,
  start_date,
  end_date,
  index_date=None,
  min_history_days=None, # Use timedelta
  ncontrols=4,
  ncontrols_is_max=False,
  replacement=True,
  catvars=None,
  contvars=None,
  contvars_tol=None, # Use timedelta if contvar is date
  tv_catvars=None,
  tv_suffix=None,
  reduce_contrls=False,
  match_type = 'risk_set_sampling'):
  
  # TODO:
  # Check subj_id is unique
  # check contvars and contvars_tol are same length
  # check check check
  
  matched = data[data[casevar]==1].copy().reset_index(drop=True)
  allcontrols = data.copy().reset_index(drop=True) # Including cases
  if match_type == 'match_unknown_index':
  	allcontrols = allcontrols[allcontrols[casevar]==0].reset_index(drop=True)
  
  ncases = len(matched)
  allcontrols['newcase'] = 0
  matched['newcase'] = 1
  matched['mid'] = pd.Series(range(0,ncases))
  matched['keep'] = True
  
  print(ncases)
  
  if reduce_contrls and (len(allcontrols) > (2*ncontrols*ncases)):
    randompick = pd.Series([False]*(len(allcontrols)-(2*ncontrols*ncases)) + [True]*(2*ncontrols*ncases)).sample(frac=1).reset_index(drop=True)
    allcontrols = allcontrols[randompick].reset_index(drop=True)
    
  
  which_contrls_base = pd.Series([True]*len(allcontrols))
  
  for j in range(0,ncases):
    
    which_contrls = which_contrls_base
    if match_type == 'match_unknown_index':
      which_contrls = which_contrls & (matched[index_date].iloc[j] < allcontrols[end_date]) & ((matched[index_date].iloc[j]-dt.timedelta(days=min_history_days)) > allcontrols[start_date])
    elif match_type == 'risk_set_sampling':
      which_contrls = which_contrls & (matched[end_date].iloc[j]-matched[start_date].iloc[j]) < (allcontrols[end_date]-allcontrols[start_date])
      
    if catvars is not None:
      for var in list(flatten([catvars])):
        which_contrls = which_contrls & (allcontrols[var] == matched[var].iloc[j])
    
    if contvars is not None:
      for var,tol in zip(list(flatten([contvars])),list(flatten([contvars_tol]))):
        which_contrls = which_contrls & (allcontrols[var] < (matched[var].iloc[j] + tol)) & (allcontrols[var] > (matched[var].iloc[j] - tol))
      
    if tv_catvars is not None:
      suffix = matched[tv_suffix].iloc[j]
      for var in list(flatten([tv_catvars])):
        which_contrls = which_contrls & (allcontrols[var+suffix] == matched[var+suffix].iloc[j])
    
    
    print(j,end="",flush=True) if j % 40 == 0 else print(".",end="",flush=True)
    
    
    npot_contrls = which_contrls.sum()
    cncontrols = ncontrols
    if not ncontrols_is_max:
      if npot_contrls < ncontrols:
        matched.loc[j,'keep'] = False
        continue
    else:
      if npot_contrls == 0:
        matched.loc[j,'keep'] = False
        continue
      elif npot_contrls < ncontrols:
        cncontrols = npot_contrls
    
    
    which_contrls = (which_contrls & which_contrls.sample(weights=which_contrls,n=cncontrols))
    
    picked = (allcontrols[which_contrls].reset_index(drop=True))
    picked['keep'] = True
    picked['mid'] = matched['mid'].iloc[j]
    if match_type == 'match_unknown_index':
      picked[index_date] = matched[index_date].iloc[j]
    matched = pd.concat([matched,picked])
    
    if not replacement:
      which_contrls_base = which_contrls_base & ~(which_contrls)
      
    
    
  print("")
  
  return(matched[matched['keep']])


if __name__ == "__main__":
  print("There is no main!")
