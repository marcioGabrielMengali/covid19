import json
import logging.config

from utils.Path import Path

class Log:
    @staticmethod
    def start_log():
        """Function to start the log"""
        path = Path.make_path(file='config\\logging_config.json')
        print(path)
        with open(path, 'r') as config_file:
            config = json.load(config_file)
        logging.config.dictConfig(config)
        logger = logging.getLogger(__name__)
        return logger