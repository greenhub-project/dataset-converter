import pandas as pd


def apply(df):
  # sorting
  df = df.sort_values(by=['device_id', 'timestamp'])

  # date filtering
  df = df[pd.Timestamp('2017-10-15') <= df.timestamp]

  return df