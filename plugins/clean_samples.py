import numpy as np
import pandas as pd


def apply(df):
  # sorting
  df = df.sort_values(by=['device_id', 'timestamp'])

  # reset indexes
  df = df.reset_index(drop=True)

  # explicitly cast battery level to integer
  df.loc[:, 'battery_level'] = df.battery_level * 100
  df.loc[:, 'battery_level'] = df.battery_level.astype(np.uint8)

  # filter out malformed records
  df = df[df.battery_level <= 100]

  return df