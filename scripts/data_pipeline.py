from data_clean import Clean_df as clean
import pandas as pd
from geopy.distance import great_circle as GRC

df_order = pd.read_csv('../data/nb.csv')
df_driver = pd.read_csv('../data/driver_locations_during_request.csv')
df_driver.drop(columns=['created_at', 'updated_at'], inplace=True)


def compute_proximity(df):
    return df.apply(lambda row: GRC(
        (row['lat'], row['lng']), (row['origin_lat'], row['origin_lon'])).m, axis=1)


def fetch_clean_data(version='V2.0'):
    clean_df = clean(df_order)
    df_order_clean = clean_df.data_pipeline()
    df_order_clean.rename(columns={'trip_id': 'order_id'}, inplace=True)

    df = df_driver.merge(df_order_clean, how='inner', on='order_id')
    df_accepted = df[df.driver_action == 'accepted'].copy(deep=True)
    df_accepted.drop(columns=['driver_action'], inplace=True)

    # unfulfilled requests are known based on the duration time(less than 3 minutes)
    # or distance is less than 100 meters
    df_accepted['driver_proximity'] = compute_proximity(df_accepted)
    df_accepted['fulfilled'] = 1
    df_accepted.loc[(df_accepted.duration < 180) | (df_accepted.duration > 43200) | (
        df_accepted.distance < 100), 'fulfilled'] = 0

    return df_accepted


    
