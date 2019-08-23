import numpy as np
import pandas as pd


def apply(df):
  # sorting
  df = df.sort_values(by=['device_id', 'timestamp'])

  # reset indexes
  df = df.reset_index(drop=True)

  # explicitly cast battery level to integer
  df_level = df.battery_level * 100
  converted_level = df_level.astype(np.uint8)
  df['battery_level'] = converted_level

  # filter out malformed records
  df = df[df.battery_level <= 100]

  return df