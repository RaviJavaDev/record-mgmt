from model.db_details import DbDetails
from logger.db_logger import DBLogger
from util.cassandra_operations import CassandraOperations
from util.mongo_operations import MongoOperation
from util.my_sql_operations import MySqlOperation


class RecordMgmt:
    def __init__(self, transaction_id, db_details):
        self.collection_name = ''
        self.error_collection_name = 'error_log'
        self.general_collection_name = 'general_log'
        self.logger_db_details = DbDetails(user_name=db_details.user_name, password=db_details.password,
                                           db_type='', db_name=db_details.db_name + '_LOG', host='')
        self.logger = DBLogger(transaction_id, self.logger_db_details)
        self.db_type = db_details.db_type
        if self.db_type == 'MYSQL':
            self.my_sql_db_opr = MySqlOperation(transaction_id, db_details, self.logger_db_details)
        elif self.db_type == 'CASSANDRA':
            self.cassandra_opr = CassandraOperations(transaction_id=transaction_id, keyspace_name=db_details.db_name,
                                                     logger_db_details=self.logger_db_details)
        else:
            self.mongo_opr = MongoOperation(db_details)

    def create_database(self, db_name):
        """
        This method creates database.

        :param db_name: Database name.
        :return:
        """
        try:
            self.logger.log(f'{db_name} database creation started...', self.general_collection_name)
            if self.db_type == 'MYSQL':
                self.my_sql_db_opr.create_database(database_name=db_name)
            elif self.db_type == 'CASSANDRA':
                self.cassandra_opr.create_keyspace(keyspace_name=db_name)
            self.logger.log(f'{db_name} database creation completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating {db_name} database {e}', self.error_collection_name)
            raise e

    def create_table(self, table_name, columns):

        """
        This method creates table.

        :param table_name: Table name to create table.
        :param columns: list of column in table.
        :return:
        """
        try:
            self.logger.log('Creating table started...', self.general_collection_name)
            if self.db_type == 'MYSQL':
                self.my_sql_db_opr.create_table(table_name, columns)
            elif self.db_type == 'CASSANDRA':
                self.cassandra_opr.create_table(table_name=table_name, columns=columns)
            elif self.db_type == 'MONGODB':
                pass
            self.logger.log('Creating table completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def save_record(self, table_name, dct_obj):
        """
        This method saves record.

        :param table_name: Table name .
        :param dct_obj: dictionary to save.
        :return:
        """
        try:
            self.logger.log('Inserting record started...', self.general_collection_name)
            columns = ",".join([key for key in dct_obj.keys()])
            record = tuple([key for key in dct_obj.values()])
            if self.db_type == 'MYSQL':
                self.my_sql_db_opr.save_record(table_name=table_name, columns=columns, record=record)
            elif self.db_type == 'CASSANDRA':
                self.cassandra_opr.save_record(table_name=table_name, columns=columns, record=record)
            elif self.db_type == 'MONGODB':
                self.mongo_opr.save_single_record(record=dct_obj, collection_name=table_name)
            self.logger.log('Inserting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def save_multiple_record(self, table_name, dict_list):
        """
        This method saves multiple records.

        :param table_name: Table name .
        :param dict_list: dictionary list to save.
        :return:
        """
        try:
            self.logger.log('Inserting record started...', self.general_collection_name)
            if self.db_type == 'MYSQL':
                for dct_obj in dict_list:
                    columns = ",".join([key for key in dct_obj.keys()])
                    record = tuple([key for key in dct_obj.values()])
                    self.my_sql_db_opr.save_record(table_name=table_name, columns=columns,
                                                   record=record)
            elif self.db_type == 'CASSANDRA':
                for dct_obj in dict_list:
                    columns = ",".join([key for key in dct_obj.keys()])
                    record = tuple([key for key in dct_obj.values()])
                    self.cassandra_opr.save_record(table_name=table_name, columns=columns, record=record)
            elif self.db_type == 'MONGODB':
                self.mongo_opr.save_multiple_records(records=dict_list, collection_name=table_name)
            self.logger.log('Inserting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def update_record(self, table_name, update_condition, new_value):
        """
        This method updates records.

        :param table_name:Table name.
        :param update_condition:condition to update record.
        :param new_value:new value to update.
        :return:
        """
        try:
            self.logger.log('update_record() started...', self.general_collection_name)
            if self.db_type == 'MYSQL':
                records = []
                for key in new_value.keys():
                    records.append((key + '=' + '%s'))
                update_column = ",".join(records)

                update_record = tuple([key for key in new_value.values()])

                update_condition = [key + '=' + str(value) for key, value in update_condition.items()]
                update_condition = ','.join(update_condition)

                self.my_sql_db_opr.update_record(table_name=table_name, update_column=update_column,
                                                 update_record=update_record,
                                                 update_condition=update_condition)
            elif self.db_type == 'CASSANDRA':
                records = []
                for key in new_value.keys():
                    records.append((key + '=' + '%s'))
                update_column = ",".join(records)

                update_record = tuple([key for key in new_value.values()])

                update_condition = [key + '=' + str(value) for key, value in update_condition.items()]
                update_condition = ','.join(update_condition)

                self.cassandra_opr.update_record(table_name=table_name, update_column=update_column,
                                                 update_record=update_record,
                                                 update_condition=update_condition)
            elif self.db_type == 'MONGODB':
                self.mongo_opr.update_record(collection_name=table_name, update_record=new_value,
                                             update_condition=update_condition)

            self.logger.log('update_record() completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while updating record {e}', self.error_collection_name)
            raise e

    def delete_record(self, table_name, delete_condition):
        """
        This method deletes record.

        :param table_name: Table name.
        :param delete_condition: Condition to delete record.
        :return:
        """
        try:
            self.logger.log('delete_record_() started...', self.general_collection_name)
            if self.db_type == 'MYSQL':
                delete_condition = [key + '=' + str(value) for key, value in delete_condition.items()]
                delete_condition = ','.join(delete_condition)

                self.my_sql_db_opr.delete_record(table_name=table_name,
                                                 delete_condition=delete_condition)
            elif self.db_type == 'CASSANDRA':
                delete_condition = [key + '=' + str(value) for key, value in delete_condition.items()]
                delete_condition = ','.join(delete_condition)
                self.cassandra_opr.delete_record(table_name=table_name,
                                                 delete_condition=delete_condition)
            elif self.db_type == 'MONGODB':
                self.mongo_opr.delete_record(collection_name=table_name, delete_condition=delete_condition)

            self.logger.log('delete_record_by_id() completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while getting record {e}', self.error_collection_name)
            raise e

    def get_records(self, table_name, num_records):
        """
        This method retrieve n records.

        :param table_name: Table name.
        :param num_records: number of records to fetch.
        :return:
        """
        try:
            self.logger.log('getting record started...', self.general_collection_name)
            if self.db_type == 'MYSQL':
                records = self.my_sql_db_opr.get_records(table_name=table_name, num_records=num_records)
            elif self.db_type == 'CASSANDRA':
                records = self.cassandra_opr.get_records(table_name=table_name, num_records=num_records)
            elif self.db_type == 'MONGODB':
                mongo_records = self.mongo_opr.get_records(collection_name=table_name, num_records=num_records)
                records = []
                for rec in mongo_records:
                    records.append(([rec for rec in rec.values()])[1:])
            response = []
            for record in records:
                response.append(record)
            self.logger.log('getting record completed...', self.general_collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while getting record {e}', self.error_collection_name)
            raise e
        return response
