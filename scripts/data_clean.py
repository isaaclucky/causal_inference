import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.preprocessing import MinMaxScaler, StandardScaler, Normalizer
from log_supp import App_Logger
from geopy.distance import great_circle as GRC
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import *
from custom_calendar import NigeriaCalendar
app_logger = App_Logger("../logs/data_cleaner.log").get_app_logger()





class Clean_df:
    def __init__(self, df: pd.DataFrame, deep=False) -> None:
        """
        Returns a DataCleaner Object with the passed DataFrame Data set as its own DataFrame
        Parameters
        ----------
        df:
            Type: pd.DataFrame
        Returns
        -------
        None
        """
        self.logger = App_Logger(
            "../logs/data_cleaner.log").get_app_logger()
        if (deep):
            self.df = df.copy(deep=True)
        else:
            self.df = df

    def get_numerical_columns(self) -> list:
        """
        Returns numerical column names
        """
        return self.df.select_dtypes(include='number').columns

    def drop_null_entries(self, subset=['trip_start_time']) -> pd.DataFrame:
        """
        Checks if there is a null entry in the dataset and removes them
        """
        self.df.dropna(subset=subset, axis=0, inplace=True)
        return self.df

    def convert_to_datetime(self, column: str) -> pd.DataFrame:
        """Convert column to datetime."""
        try:
            self.logger.info('Converting Column to Datetime')
            self.df[column] = pd.to_datetime(self.df[column])
            return self.df
        except Exception:
            self.logger.exception(
                'Failed to convert Column to Datetime')
            sys.exit(1)

    def label_encode(self, col_names: list) -> pd.DataFrame:
        """ Performs Label encoding of the given columns

        Parameters
        ------------
        df: Pandas DataFrame: dataframe to be computed
        Columns: list of columns
        Returns
        ------------
        The method returns a dataframe with label encoded categorical features
        """

        le = LabelEncoder()
        for col in col_names:
            self.df[col+'_labelEncoded'] = le.fit_transform(self.df[col])

        self.df.drop(columns=col_names, axis=1, inplace=True)
        return self.df

    def one_hot_encode(self, col_names: list) -> pd.DataFrame:
        """ Performs One hot encoding of the given columns

        Parameters
        ------------
        df: Pandas DataFrame: dataframe to be computed
        Columns: list of columns
        Returns
        ------------
        The method returns a dataframe with One-hot encoded categorical features
        """
        # ohe = OneHotEncoder(handle_unknown='ignore')

        return pd.get_dummies(self.df, columns=col_names)

    def drop_duplicate(self) -> pd.DataFrame:
        """Drop duplicate rows."""
        self.logger.info('Dropping duplicate row')
        self.df = self.df.drop_duplicates()
        self.logger.info('duplicate dropped')

        return self.df

    def compute_holidays(self, column='isHoliday'):
        NC = CustomBusinessDay(calendar=NigeriaCalendar())
        total = pd.date_range(start=self.df.trip_start_time.dt.date.unique(
        ).min(), end=self.df.trip_start_time.dt.date.unique().max())

        working = pd.date_range(start=self.df.trip_start_time.dt.date.unique().min(),
                                end=self.df.trip_start_time.dt.date.unique().max(), freq=NC)
        # df = pd.DataFrame(s, columns=['Date'])
        holiday = set(total.date)-set(working.date)

        self.df[column] = self.df.trip_start_time.dt.date.isin(holiday)*1

        return self.df



    def map_days(self, date_col):
        # Reshaping the time stamp representations
        self.df['year'] = self.df[date_col].dt.year
        self.df['month'] = self.df[date_col].dt.month
        self.df['isWeekDay'] = (self.df[date_col].dt.day_of_week < 5)*1
        self.df['hour'] = self.df[date_col].dt.hour

        return self.df

    def rename_cols(self):
        df_new = pd.DataFrame()
        cols = ['trip_id', 'trip_origin', 'trip_destination',
                'trip_start_time', 'trip_end_time']
        for i in range(len(cols)):
            df_new[cols[i]] = self.df.iloc[:, i]
        self.df = df_new

        return self.df

    def split_date_cols(self):
        origin = self.df["trip_origin"].str.split(",", n=1, expand=True)
        dest = self.df["trip_destination"].str.split(",", n=1, expand=True)
        self.df['origin_lat'] = origin[0]
        self.df['origin_lon'] = origin[1]
        self.df['dest_lat'] = dest[0]
        self.df['dest_lon'] = dest[1]
        self.df.drop(columns=['trip_origin', 'trip_destination'], inplace=True)
        return self.df

    def cast_location(self):
        self.df.dest_lat = self.df.dest_lat.astype(float)
        self.df.dest_lon = self.df.dest_lon.astype(float)
        self.df.origin_lat = self.df.origin_lat.astype(float)
        self.df.origin_lon = self.df.origin_lon.astype(float)

        return self.df

    def convert_to_datetime(self) -> pd.DataFrame:
        """Convert column to datetime."""
        try:

            self.df['trip_start_time'] = pd.to_datetime(self.df['trip_start_time'])
            self.df['trip_end_time'] = pd.to_datetime(self.df['trip_end_time'])
            return self.df
        except Exception:
            self.logger.exception(
                'Failed to convert Column to Datetime')
            sys.exit(1)

    def compute_abs_distance(self, origin_lat='origin_lat', origin_lon='origin_lon', dest_lat='dest_lat', dest_lon='dest_lon', name='distance'):
        self.df[name] = self.df.apply(lambda row: GRC(
            (row[origin_lat], row[origin_lon]), (row[dest_lat], row[dest_lon])).m, axis=1)

        return self.df

    def compute_avg_speed(self):
        self.df['speed'] = self.df['distance'] / self.df['duration']
        return self.df

    def duration_seconds(self):
        self.df['duration'] = (self.df['trip_end_time'] -
                               self.df['trip_start_time']).dt.total_seconds()

        return self.df

    def minmax_scaling(self) -> pd.DataFrame:
        """
        Returns dataframe with minmax scaled columns
        """
        scaller = MinMaxScaler()
        res = pd.DataFrame(
            scaller.fit_transform(
                self.df[self.get_numerical_columns(self.df)]), columns=self.get_numerical_columns(self.df)
        )
        return res

    def join_dataframes(self, df1, df2, on, how="inner"):
        """Join the two dataframes."""
        try:
            self.logger.info('Joining two Dataframes')
            return pd.merge(df1, df2, on=on)
        except Exception:
            self.logger.exception(
                'Failed to join two Dataframes')
            sys.exit(1)

    def normalizer(self) -> pd.DataFrame:
        """
        Returns dataframe with normalized columns
        """
        nrm = Normalizer()
        res = pd.DataFrame(
            nrm.fit_transform(
                self.df[self.get_numerical_columns(self.df)]), columns=self.get_numerical_columns(self.df)
        )
        return res

    def drop_unwanted_cols(self, columns: list) -> pd.DataFrame:
        """
        Drops columns which doesn't add to the model training
        ------------------
        columns:
            Type: list 
        Returns:
        ---------------
        pd.DataFrame
        """
        self.df.drop(columns=columns, axis=1, inplace=True)
        self.logger.info("Successfully droped unwanted columns")
        return self.df

    def remove_single_val_columns(self) -> pd.DataFrame:
        vc = self.df.nunique()
        col = []
        for i in len(v):
            if list(vc)[i] == 1:
                col.append(v.index[i])
        self.df.drop(columns=col, axis=1, inplace=True)
        return self.df

    def change_columns_type_to(self, cols: list, data_type: str) -> pd.DataFrame:
        """
        Returns a DataFrame where the specified columns data types are changed to the specified data type
        Parameters
        ----------
        cols:
            Type: list
        data_type:
            Type: str
        Returns
        -------
        pd.DataFrame
        """
        try:
            for col in cols:
                self.df[col] = self.df[col].astype(data_type)
        except:
            print('Failed to change columns type')
        self.logger.info(f"Successfully changed columns type to {data_type}")
        return self.df

    def data_pipeline(self) -> pd.DataFrame:
        """
        performs a pipiline of cleaning methods in the given dataframe
        """
        
        self.rename_cols()
        self.drop_duplicate()
        self.drop_null_entries()
        self.split_date_cols()
        self.cast_location()
        self.compute_abs_distance()
        self.convert_to_datetime()
        self.map_days('trip_start_time')
        self.duration_seconds()
        self.compute_holidays()
        self.compute_avg_speed()

        return self.df
