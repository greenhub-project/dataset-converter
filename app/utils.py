import pickle
import numpy as np
import pandas as pd
import pyarrow.parquet as pq


def typecast_ints(gl_int):
  if gl_int is not None:
    return gl_int.apply(pd.to_numeric, downcast='unsigned')
  return None


def typecast_floats(gl_float):
  if gl_float is not None:
    return gl_float.apply(pd.to_numeric, downcast='float')
  return None


def typecast_objects(gl_obj):
  if gl_obj is None:
    return None

  # convert object to category columns
  # when unique values < 50% of total
  converted_obj = pd.DataFrame()
  for col in gl_obj.columns:
    num_unique_values = len(gl_obj[col].unique())
    num_total_values = len(gl_obj[col])
    if num_total_values == 0:
      converted_obj.loc[:, col] = gl_obj[col]
    elif (num_unique_values / num_total_values) < 0.5:
      converted_obj.loc[:, col] = gl_obj[col].astype('category')
    else:
      converted_obj.loc[:, col] = gl_obj[col]
  return converted_obj


def cache_dtypes(df, ignored=[]):
  dtypes = df.drop(ignored, axis=1).dtypes
  dtypes_col = dtypes.index
  dtypes_type = [i.name for i in dtypes.values]
  return dict(zip(dtypes_col, dtypes_type))


def save_dtypes(dtypes, path):
  with open(path, 'wb') as handle:
    pickle.dump(dtypes, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_dtypes(path):
  with open(path, 'rb') as handle:
    return pickle.load(handle)


def save_df(df, path, compression='snappy', use_dictionary=True):
  """
  Save a pandas DataFrame to a parquet file
  """
  try:
    df.to_parquet(path, compression=compression,
                  use_dictionary=use_dictionary)
  except Exception as e:
    print(e)