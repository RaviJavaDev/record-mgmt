import os
import csv
from pathlib import Path

from model.db_details import DbDetails
from logger.db_logger import DBLogger


class CsvFileOperations:
    def __init__(self, transaction_id, file_name, db_details):
        self.file_name = file_name
        self.error_collection_name = 'error_log'
        self.general_collection_name = 'general_log'
        self.logger_db_details = DbDetails(user_name=db_details.user_name, password=db_details.password,
                                           db_type='', db_name=db_details.db_name + '_LOG', host='')
        self.logger = DBLogger(transaction_id, self.logger_db_details)

    def write_data(self, records):
        try:
            self.logger.log('in write_data()', self.general_collection_name)
            parent_path = Path(os.path.realpath('__file__')).parent
            os.makedirs(os.path.join(parent_path, 'output_data'), exist_ok=True)
            f = open(os.path.join(parent_path, 'output_data', self.file_name), mode='w')
            csv_writer = csv.writer(f, delimiter=',')
            csv_writer.writerows(records)
            self.logger.log('out write_data()', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred in CsvFileOperations while creating csv file {e}', self.error_collection_name)
        finally:
            f.close()
