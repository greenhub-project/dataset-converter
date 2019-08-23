#!/usr/bin/env python3

import os
import glob
import yaml
import importlib
import pkgutil
import numpy as np
import pandas as pd

from utils import save_df, typecast_ints, typecast_floats, \
  typecast_objects, save_dtypes, cache_dtypes


data_path = os.path.abspath('./data')


def downcast_df(df):
  # downcast integer columns
  print('Downcasting integer columns if exist')
  converted_int = typecast_ints(df.select_dtypes(include=['int']))

  # downcast float columns
  print('Downcasting float columns if exist')
  converted_float = typecast_floats(df.select_dtypes(include=['float']))

  # convert object columns to lowercase
  print('Converting object columns to categories when possible')
  df_obj = df.select_dtypes(include=['object'])
  df_obj = df_obj.apply(lambda x: x.str.strip())
  df_obj = df_obj.apply(lambda x: x.str.lower())

  # convert object to category columns
  # when unique values < 50% of total
  converted_obj = typecast_objects(df_obj)

  # transform optimized types
  df[converted_int.columns] = converted_int
  df[converted_float.columns] = converted_float
  df[converted_obj.columns] = converted_obj

  return df


def load_tasks(df, plugins, category):
  tasks = {}

  if plugins is not None:
    print('Loading [{}] plugins:'.format(category))

    if category in plugins:
      tasks = {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules('.')
        if name in plugins[category]
      }
      for task in plugins[category]:
        if task in tasks:
          print('Executing [{}] plugin'.format(task))
          df = tasks[task].apply(df)
    
    return df


def export_files(df, name):
  filename = name.split('.')[0]

  filepath = os.path.join(data_path, filename + '.dtypes.p')
  print('Creating dtypes file -> {}'.format(filepath))
  save_dtypes(cache_dtypes(df), filepath)

  filepath = os.path.join(data_path, filename + '.pk')
  print('Creating parquet file -> {}'.format(filepath))
  save_df(df, filepath)


def process_df(df, plugins, verbose=True):
  if verbose:
    df.info(memory_usage='deep')

  # Call before plugins
  df = load_tasks(df, plugins, 'before')

  df = downcast_df(df)

  if verbose:
    print('Memory usage after:')
    df.info(memory_usage='deep')

  return df


def load_single(name, usecols, parse_dates, plugins):
  filepath = os.path.join(data_path, name)
  print('Loading data file -> {}'.format(filepath))

  df = pd.read_csv(filepath, sep=';', usecols=usecols, 
                    parse_dates=parse_dates, low_memory=False, quoting=3)
  
  return process_df(df, plugins)


def load_multiple(name, usecols, parse_dates, chunksize, plugins):
  chunk_list = []  # append each chunk df here
  filepath = os.path.join(data_path, name)
  df_chunk = pd.read_csv(filepath, sep=';', usecols=usecols, parse_dates=parse_dates,
                          chunksize=chunksize, low_memory=False, quoting=3)
  step = 1
  sep = '==========================================='

  print('Loading data file in chunks -> {}'.format(filepath))

  # Each chunk is in df format
  for chunk in df_chunk:
    print('Performing chunk step[{}] {}'.format(step, sep))

    # perform data filtering
    chunk_filter = process_df(chunk, plugins)
    
    # Once the data filtering is done, append the chunk to list
    chunk_list.append(chunk_filter)

    step += 1

  print('Merging all processed chunks')
  # concat the list into dataframe 
  df_concat = pd.concat(chunk_list)

  return df_concat


def convert_df(params):
  df = None
  usecols = None
  plugins = None
  chunksize = None
  parse_dates = None

  print('Parsed arguments: {}'.format(params))

  if 'usecols' in params:
    usecols = params['usecols']
  if 'chunksize' in params:
    chunksize = params['chunksize']
  if 'parse_dates' in params:
    parse_dates = params['parse_dates']
  if 'plugins' in params:
    plugins = params['plugins']

  if chunksize is None:
    df = load_single(params['name'], usecols, parse_dates, plugins)
  else:
    df = load_multiple(params['name'], usecols, parse_dates, chunksize, plugins)

  # Call after plugins
  df = load_tasks(df, plugins, 'after')

  export_files(df, params['name'])


def main():
  try:
    files = [f for f in glob.glob(data_path + "**/*.yml", recursive=True)]

    for config in files:
      with open(config) as f:
        print('Loading config file -> {}'.format(config))
        convert_df(yaml.load(f, Loader=yaml.FullLoader))
  except Exception as e:
    print(e)

if __name__ == "__main__":
  main()
