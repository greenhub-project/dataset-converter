#!/usr/bin/env python3

import os
import glob
import yaml
import importlib
import pkgutil
import numpy as np
import pandas as pd

from errno import ENOENT

from utils import save_df, typecast_ints, typecast_floats, \
  typecast_objects, save_dtypes, cache_dtypes, compress_df


data_path = os.path.abspath('./data')
config_path = os.path.abspath('./config')
output_path = os.path.abspath('./output')


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
  if df_obj is not None:
    df_obj = df_obj.apply(lambda x: x.str.strip())
    df_obj = df_obj.apply(lambda x: x.str.lower())

  # convert object to category columns
  # when unique values < 50% of total
  converted_obj = typecast_objects(df_obj)

  # transform optimized types
  if converted_int is not None:
    df.loc[:, converted_int.columns] = converted_int
  if converted_float is not None:
    df.loc[:, converted_float.columns] = converted_float
  if converted_obj is not None:
    df.loc[:, converted_obj.columns] = converted_obj

  return df


def load_tasks(df, plugins, category):
  tasks = {}

  if plugins is not None:
    print('Loading [{}] plugins:'.format(category))

    if category in plugins:
      tasks = {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules(['.'])
        if name in plugins[category]
      }
      for task in plugins[category]:
        if task in tasks and hasattr(tasks[task], 'apply'):
          print('Executing [{}] plugin'.format(task))
          df = tasks[task].apply(df)

  return df


def export_files(df, options, page=None):
  filename = options['name'].split('.')[0]

  if options['partition']:
    filename += '.' + str(page)

  filepath = os.path.join(output_path, filename + '.dtypes.pickle')
  print('Creating dtypes file -> {}'.format(filepath))
  save_dtypes(cache_dtypes(df), filepath)

  filepath = os.path.join(output_path, filename + '.parquet')
  print('Creating parquet file -> {}'.format(filepath))
  save_df(df, filepath)


def compress_files(name):
  filename = name.split('.')[0]
  files = os.path.join(output_path, filename + '*.parquet')
  filepath = os.path.join(output_path, filename + '.parquet')

  print('Compressing parquet files -> {}'.format(filepath + '.7z'))
  compress_df(filepath + '.7z', files)

  files = os.path.join(output_path, filename + '*.dtypes.pickle')
  filepath = os.path.join(output_path, filename + '.dtypes.pickle')

  print('Compressing parquet files -> {}'.format(filepath + '.7z'))
  compress_df(filepath + '.7z', files)


def process_df(df, plugins, verbose=True):
  if verbose:
    print('Memory usage before:')
    df.info(memory_usage='deep')

  # Call before plugins
  df = load_tasks(df, plugins, 'before')

  df = downcast_df(df)

  if verbose:
    print('Memory usage after:')
    df.info(memory_usage='deep')

  return df


def load_single(options):
  filepath = os.path.join(data_path, options['name'])

  if not os.path.exists(filepath):
    raise IOError(ENOENT, 'File not found', filepath)

  print('Loading data file -> {}'.format(filepath))

  df = pd.read_csv(filepath, sep=options['sep'], usecols=options['usecols'],
                    parse_dates=options['parse_dates'], quoting=3)
  
  return process_df(df, options['plugins'])


def load_multiple(options):
  chunk_list = []  # append each chunk df here
  filepath = os.path.join(data_path, options['name'])

  if not os.path.exists(filepath):
    raise IOError(ENOENT, 'File not found', filepath)

  df_chunk = pd.read_csv(filepath, sep=options['sep'], usecols=options['usecols'],
                          parse_dates=options['parse_dates'], chunksize=options['chunksize'], quoting=3)
  step = 1
  hr = '==========================================='

  print('Loading data file in chunks -> {}'.format(filepath))

  # Each chunk is in df format
  for chunk in df_chunk:
    print('Performing chunk step[{}] {}'.format(step, hr))

    # perform data filtering
    chunk_filtered = process_df(chunk, options['plugins'])
    
    if not options['partition']:
      # Once the data filtering is done, append the chunk to list
      chunk_list.append(chunk_filtered)
    else:
      print('Exporting chunk to file')
      chunk_filtered = load_tasks(chunk_filtered, options['plugins'], 'after')
      export_files(chunk_filtered, options, step)

    step += 1

  if not options['partition']:
    print('Merging all processed chunks')
    # concat the list into dataframe
    df = None
    while chunk_list:
      df = pd.concat([df, chunk_list.pop(0)], ignore_index=True)

    return df


def convert_df(params):
  print('Parsed arguments: {}'.format(params))

  options = {
    'name': params['name'],
    'sep': ';',
    'chunksize': None,
    'compression': True,
    'partition': False,
    'usecols': None,
    'parse_dates': None,
    'plugins': None
  }

  df = None

  if 'sep' in params:
    options['sep'] = params['sep']
  if 'compression' in params:
    options['compression'] = params['compression']
  if 'partition' in params:
    options['partition'] = params['partition']
  if 'usecols' in params:
    options['usecols'] = params['usecols']
  if 'chunksize' in params:
    options['chunksize'] = params['chunksize']
  if 'parse_dates' in params:
    options['parse_dates'] = params['parse_dates']
  if 'plugins' in params:
    options['plugins'] = params['plugins']

  is_partitioned = (options['chunksize'] is not None) and options['partition']

  if is_partitioned:
    load_multiple(options)
  else:
    if options['chunksize'] is None:
      df = load_single(options)
    else:
      df = load_multiple(options)

    df = load_tasks(df, options['plugins'], 'after')
    export_files(df, options)

  if options['compression']:
    compress_files(options['name'])


def main():
  try:
    files = [f for f in glob.glob(config_path + "**/*.yml", recursive=True)]

    for config in files:
      with open(config) as f:
        print('Loading config file -> {}'.format(config))
        convert_df(yaml.load(f, Loader=yaml.FullLoader))
    
    print('Done!')
  except Exception as e:
    print(e)

if __name__ == "__main__":
  main()
