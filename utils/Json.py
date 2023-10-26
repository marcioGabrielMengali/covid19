import json
import pandas as pd

from utils.Log import Log
from utils.Path import Path

class Json:
    @staticmethod
    def return_json_dict(path):
        """Function to return a json in a dict format"""
        try:
            with open(path, 'r') as file:
                json_dict = json.load(file)
            return json_dict
        except Exception as e:
            logger = Log.start_log()
            logger.error(f'Error to convert json into dict {str(e)}')
    
    def read_json_chunks(path, filename, type, chunksize):
        """Function to read a json file in chunks using pandas"""
        file_path = Path.make_path(f'{path}\\{filename}.{type}')
        dfs = pd.read_json(file_path, orient='records', encoding='Utf-8', chunksize=chunksize, lines=True)
        return dfs