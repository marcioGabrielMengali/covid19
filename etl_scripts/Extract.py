import json
import requests
import gzip
import shutil
import os

from utils.Log import Log
from utils.Path import Path

class Extract:
    def __init__(self, url, raw_data_path):
        self.url = url
        self.raw_data_path = raw_data_path
        self.logger = Log.start_log()

    def download_file(self):
        """Function to download the source file"""
        self.logger.info('Starting extract data')
        download_file_path = Path.make_path(f'{self.raw_data_path}\\covid.csv.gz')
        unzziped_file_path = Path.make_path(f'{self.raw_data_path}\\covid.csv')
        try:
            with requests.get(url=self.url, stream=True) as r:
                status_code = r.status_code
                if status_code == 200:
                    self.logger.info(f'Url status code {status_code}')
                    with open(download_file_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    with gzip.open(download_file_path, 'rb') as r:
                        with open(unzziped_file_path, 'wb') as w:
                            shutil.copyfileobj(r, w)
                    os.remove(download_file_path)
                    self.logger.info('Finished extract data')
                else:
                    self.logger.error(f'Error request data {status_code}')
        except Exception as e:
            self.logger.error(f'Error on download_file function {str(e)}')


    