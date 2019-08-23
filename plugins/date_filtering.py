import pandas as pd


def apply(df):
  # date filtering
  df = df[pd.Timestamp('2017-10-15') <= df.timestamp]

  return df