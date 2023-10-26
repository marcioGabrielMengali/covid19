import multiprocessing
from utils.Csv import Csv
from utils.Log import Log
from utils.Path import Path

class Transform:
    def __init__(self, raw_path, filename, format, separator, processed_path):
        self.raw_path = raw_path
        self.filename = filename
        self.format = format
        self.separator = separator
        self.processed_path = processed_path
        self.logger = Log.start_log()
        self.count = 0
        self.count_city = 0
        self.count_state = 0

    def get_data(self):
        """Funtion to return the raw data in chunks"""
        dfs = Csv.read_csv_chunks(
            chunksize=50000,
            filename=self.filename,
            format=self.format,
            path=self.raw_path,
            sep=self.separator
        )
        return dfs
    
    def drop_na(self, df, columns):
        """Function to drop Na values"""
        try:
            df.dropna(subset=columns, inplace=True)
        except Exception as e:
            self.logger.error(f'Error To Drop Na: {str(e)}')
    
    def set_datatype(self, df):
        """Function to change columns datatypes"""
        try:
            new_df = df.astype(
                dtype={
                    'city_ibge_code': 'int64',
                    'estimated_population': 'int64',
                    'estimated_population_2019': 'int64'
                }, copy=True
            )
            return new_df
        except Exception as e:
            self.logger.error(f'Error To change data types: {str(e)}')
    
    def drop_columns(self, df, columns):
        """Function to drop columns"""
        try:
            new_df = df.drop(columns, axis=1)
            return new_df
        except Exception as e:
            self.logger.error(f'Error To drop columns: {str(e)}')
    
    #Desenvolver
    def rename_columns(self, df, columns):
        """Function to rename columns"""
        try:
            df.rename(columns=columns, inplace=True)
        except Exception as e:
            self.logger.error(f'Error To rename columns: {str(e)}')

    def trasformations(self, df):
        """functions that call all the data transformations steps"""
        #first transformations
        self.drop_na(df, columns=['city_ibge_code'])
        new_df = self.set_datatype(df)
        city_df = new_df[new_df['city'].notna()]
        state_df = new_df[new_df['city'].isna()]
        state_df_trasnformed = self.drop_columns(state_df, columns=['city'])
        self.rename_columns(state_df_trasnformed, columns={'city_ibge_code': 'state_ibge_code'})
        return (city_df, state_df_trasnformed)


    def transform_data(self):
        """Main Function for transformation"""
        city_path = Path.make_path(f'{self.processed_path}\\covid_data_per_city.csv')
        state_path = Path.make_path(f'{self.processed_path}\\covid_data_per_state.csv')
        dfs = self.get_data()
        pool = multiprocessing.Pool()
        results = pool.map(self.trasformations, dfs)
        for city, state in results:
            #collect data
            self.count += city.shape[0] + state.shape[0]
            self.count_city += city.shape[0]
            self.count_state += state.shape[0]
            city.to_csv(
                city_path,
                sep=';',
                encoding='utf-8',
                header=None,
                index=False,
                mode='a'
            )
            state.to_csv(
                state_path,
                sep=';',
                encoding='utf-8',
                header=None,
                index=False,
                mode='a'
            )
        self.logger.info(f'Total Processed Rows: {self.count}')
        self.logger.info(f'Total City Rows: {self.count_city}')
        self.logger.info(f'Total State Rows: {self.count_state}')
        
        
        
