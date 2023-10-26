from utils.Csv import Csv
from utils.Log import Log
from utils.Json import Json
from utils.Path import Path

import json
import concurrent.futures
#from sqlalchemy import create_engine, text
import psycopg2
from functools import partial
from io import StringIO



class Load:
    def __init__(self, path, filename, format, sep, columns, database, host, user, password, port, table):
        self.path = path
        self.filename = filename
        self.format = format
        self.sep = sep
        self.columns = columns
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.table = table
        self.logger = Log.start_log()
        self.insert = 0

    def create_connection(self):
        """Function to create the database engine"""
        try:
            conn = psycopg2.connect(
                dbname=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return conn
        except Exception as e:
            self.logger.error(f'Error to connect to database: {str(e)}')
        
    
    def create_table(self, cursor):
        """Function to create database table"""
        try:
            if self.table == 'covid_city':
                sql = f'''
                    DROP TABLE IF EXISTS {self.table}; 
                    CREATE TABLE public.{self.table}(
                        city TEXT NULL,
                        city_ibge_code BIGINT NULL,
                        date DATE NULL,
                        epidemiological_week INTEGER NULL,
                        estimated_population BIGINT NULL,
                        estimated_population_2019 BIGINT NULL,
                        is_last BOOLEAN NULL,
                        is_repeated BOOLEAN NULL,
                        last_available_confirmed INTEGER NULL,
                        last_available_confirmed_per_100k_inhabitants DECIMAL NULL,
                        last_available_date DATE,
                        last_available_death_rate DECIMAL,
                        last_available_deaths INTEGER,
                        order_for_place INTEGER,
                        place_type VARCHAR(10),
                        state VARCHAR(3),
                        new_confirmed INTEGER,
                        new_deaths INTEGER,
                        dt_upload TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP
                    );
                '''
            elif self.table == 'covid_state':
                sql = f'''
                    DROP TABLE IF EXISTS {self.table}; 
                    CREATE TABLE public.{self.table}(
                        state_ibge_code INTEGER,
                        date DATE,
                        epidemiological_week INTEGER,
                        estimated_population BIGINT,
                        estimated_population_2019 BIGINT,
                        is_last BOOLEAN,
                        is_repeated BOOLEAN,
                        last_available_confirmed INTEGER,
                        last_available_confirmed_per_100k_inhabitants DECIMAL,
                        last_available_date DATE,
                        last_available_death_rate DECIMAL,
                        last_available_deaths INTEGER,
                        order_for_place INTEGER,
                        place_type VARCHAR(10),
                        state VARCHAR(3),
                        new_confirmed INTEGER,
                        new_deaths INTEGER,
                        dt_upload TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP
                    );
                '''
            cursor.execute(sql)
            self.logger.info(f'Table Created: {self.table}')
        except Exception as e:
            self.logger.error(f'Error to create table: {str(e)}')
            exit()

    def upload_file(self):
        """Funtion that starts file upload"""
        self.logger.info('Starting upload data')
        dfs = Json.read_json_chunks(
            path=self.path,
            filename=self.filename,
            type=self.format,
            chunksize=10
        )
        conn = self.create_connection()
        cursor = conn.cursor()
        self.create_table(cursor=cursor)
        path = Path.make_path(file=f'{self.path}\\{self.filename}.{self.format}')
        try:
            with open(path, 'r', encoding='utf-8') as f:
                cursor.copy_from(f, self.table, sep=self.sep, null='',columns=self.columns)
            conn.commit()
        except Exception as e:
            self.logger.info(f'Erro to upload data: {str(e)}')
        

