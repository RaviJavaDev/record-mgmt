from logger.file_logger import FileLogger
from util.mongo_operations import MongoOperation
from datetime import datetime


class DBLogger:
    def __init__(self, transaction_id, db_details):
        self.transaction_id = transaction_id
        self.logger = FileLogger()
        self.mongodb = MongoOperation(db_details)

    def log(self, message, collection_name):
        """
        This method saves message into collection.

        :param message: message to be saved.
        :param collection_name: collection name in mongo db.
        :return:
        """
        try:
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            log_message = {
                'Log_updated_date': now,
                'Log_update_time': current_time,
                'Log_message': message,
                'transaction_id': self.transaction_id
            }
            self.mongodb.save_single_record(log_message, collection_name)
        except Exception as e:
            self.logger.error(f'Error occurred in  DBLogger {e}')
            raise e


if __name__ == '__main__':
    db_logger = DBLogger()
    record = {'log_message': '1 record saved', 'transaction_id': 124}
    db_logger.log(record, 'transaction_log')
