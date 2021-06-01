import json
import os
from pathlib import Path

from model.db_details import DbDetails
from logger.db_logger import DBLogger


class JsonFileOperation:
    def __init__(self, file_name, transaction_id, db_details):
        self.file_name = file_name
        self.error_collection_name = 'error_log'
        self.general_collection_name = 'general_log'
        self.logger_db_details = DbDetails(user_name=db_details.user_name, password=db_details.password,
                                           db_type='', db_name=db_details.db_name + '_LOG', host='')
        self.logger = DBLogger(transaction_id, self.logger_db_details)

    def read_json_file(self):
        self.logger.log('in read_json_file()', self.general_collection_name)
        try:
            parent_path = Path(os.path.realpath('__file__')).parent
            f = open(os.path.join(parent_path, 'input_data', self.file_name), mode='r')
            records = json.load(f)
            return records
        except Exception as e:
            self.logger.log(f'error occurred in JsonFileOperation while reading json file {e}',
                            self.error_collection_name)
        finally:
            if f is not None:
                f.close()
