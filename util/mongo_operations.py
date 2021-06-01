import pymongo
from logger.file_logger import FileLogger


class MongoOperation:
    """
    This class used to perform operation on MongoDB.
    """

    def __init__(self, db_details):
        try:
            self.logger = FileLogger()
            self.logger.debug('connecting to MongoDB Started')
            user_name = db_details.user_name
            password = db_details.password
            connection_url = f"mongodb+srv://{user_name}:{password}@projectscluster.2dm5i.mongodb.net/test?retryWrites=true&w=majority"
            self.client = pymongo.MongoClient(connection_url)
            self.db_name = db_details.db_name
            self.logger.debug('connecting to MongoDB Finished')
        except Exception as e:
            self.logger.error(f'Error occurred while connection Mongo Db {e}')
            raise e

    def get_db(self):
        """
        This method returns database object.

        :return: db object
        """
        self.logger.info('in get_db()')
        try:
            return self.client[self.db_name]
        except Exception as e:
            self.logger.error(f'Error occurred while getting client {e}')

    def save_single_record(self, record, collection_name):
        """
        This method saves single record.

        :param record:
        :param collection_name:

        :return: saved record id
        """
        try:
            self.logger.info('in save_single_record()')
            collection = self.get_db()[collection_name]
            record_id = collection.insert_one(record)
            self.logger.info('out save_single_record()')
            return record_id
        except Exception as e:
            self.logger.error(f'Error occurred while saving single record {e}')

    def save_multiple_records(self, records, collection_name):
        """
        This method saves multiple record.

        :param records:
        :param collection_name:
        """

        try:
            self.logger.info('in save_multiple_records()')
            collection = self.get_db()[collection_name]
            record_ids = collection.insert_many(records)
            self.logger.info('out save_multiple_records()')
            return record_ids
        except Exception as e:
            self.logger.error(f'Error occurred while saving multiple records {e}')

    def get_record(self, collection_name, filter):
        """
        This method returns single record.

        :param collection_name:
        :param filter:

        :return: record
        """

        try:
            self.logger.info('in get_record()')
            collection = self.get_db()[collection_name]
            record = collection.find_one(filter)
            self.logger.info('in get_record()')
            return record
        except Exception as e:
            self.logger.error(f'Error occurred while getting records {e}')

    def update_record(self, collection_name, update_record, update_condition):
        """
        This method updates record.

        :param collection_name:
        :param update_record:
        :param update_condition:
        """
        try:
            self.logger.info('in update_record()')
            collection = self.get_db()[collection_name]
            collection.update_one(update_condition, {"$set": update_record})
            self.logger.info('out update_record()')
        except Exception as e:
            self.logger.error(f'Error occurred while updating record {e}')

    def delete_record(self, collection_name, delete_condition):
        """
        This method deletes record.

        :param collection_name:
        :param delete_condition:
        """
        try:
            self.logger.info('in delete_record()')
            collection = self.get_db()[collection_name]
            collection.delete_one(delete_condition)
            self.logger.info('out delete_record()')
        except Exception as e:
            self.logger.error(f'Error occurred while deleting record {e}')

    def get_records(self, collection_name,num_records):
        """
        This method return multiple record.

        :param collection_name:

        :return:list of record
        """

        try:
            self.logger.info('in get_records()')
            collection = self.get_db()[collection_name]
            records = collection.find().limit(num_records)
            self.logger.info('out get_records()')
            return records
        except Exception as e:
            self.logger.error(f'Error occurred while getting record {e}')


if __name__ == '__main__':
    mongo_operation = MongoOperation()
    record = {'log_message': 'record saved', 'transaction_id': 12345}
    # mongo_operation.save_single_record(record, 'transaction_log')
    records = mongo_operation.get_records('transaction_log')
    for record in records:
        print(record)
