from utils.Path import Path
from utils.Log import Log

import pandas as pd

class Csv:
    @staticmethod
    def read_csv(path, filename, type, sep):
        """Function to read a csv using pandas"""
        try:
            file_path = Path.make_path(f'{path}\\{filename}.{type}')
            df = pd.read_csv(file_path, sep=sep, encoding='Utf-8')
            return df
        except Exception as e:
            logger = Log.start_log()
            logger.error(f'Error to read csv: {str(e)}')
    
    def read_csv_chunks(path, filename, format, sep, chunksize):
        """Function to read csv in chunks using pandas"""
        try:
            file_path = Path.make_path(f'{path}\\{filename}.{format}')
            dfs = pd.read_csv(file_path, sep=sep, encoding='Utf-8', chunksize=chunksize)
            return dfs
        except Exception as e:
            logger = Log.start_log()
            logger.error(f'Error to read csv: {str(e)}')
            
