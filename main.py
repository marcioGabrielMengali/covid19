import json

from utils.Path import Path
from utils.Log import Log
from utils.Json import Json

from etl_scripts.Extract import Extract
from etl_scripts.Transform import Transform
from etl_scripts.Load import Load



def main():
    logger = Log.start_log()
    logger.info('Starting ETL')
    config_path = Path.make_path('config\\config.json')
    configs = Json.return_json_dict(path=config_path)
    #Extract data phase
    extract = Extract(raw_data_path=configs['file_paths']['raw_data'], url=configs['data_source']['url'])
    extract.download_file()
    #Transform data phase
    transform = Transform(
        raw_path=configs['file_paths']['raw_data'],
        filename=configs['downloaded_data']['filename'],
        format=configs['downloaded_data']['format'],
        separator=configs['downloaded_data']['separator'],
        processed_path=configs['file_paths']['processed_data'],
    )
    transform.transform_data()
    #Load data phase
    for file_to_upload_config in configs['processed_data']:
        for file_to_upload in file_to_upload_config:
            load = Load(
                path=configs['file_paths']['processed_data'],
                filename=file_to_upload_config[file_to_upload]['filename'],
                format=file_to_upload_config[file_to_upload]['format'],
                sep=file_to_upload_config[file_to_upload]['sep'],
                columns=file_to_upload_config[file_to_upload]['columns'],
                database=configs['database']['database'],
                host=configs['database']['host'],
                user=configs['database']['user'],
                password=configs['database']['password'],
                port=configs['database']['port'],
                table=file_to_upload_config[file_to_upload]['table']
            )
            load.upload_file()
if __name__ == '__main__':
    main()



