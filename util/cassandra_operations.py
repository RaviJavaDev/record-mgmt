from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from logger.db_logger import DBLogger


class CassandraOperations:
    def __init__(self, transaction_id, logger_db_details, keyspace_name, cluster_type='local'):
        self.cluster_type = cluster_type
        self.logger = DBLogger(transaction_id, logger_db_details)
        self.collection_name = 'db_log'
        self.error_collection_name = 'error_log'
        self.keyspace_name = keyspace_name
        self.session = self.__get_session()

    def __get_session(self):
        try:
            if self.cluster_type == 'cloud':
                cloud_config = {
                    'secure_connect_bundle': 'C:\Ravis\Data Science\Practice\Cassandra\secure-connect-Test.zip'
                }
                auth_provider = PlainTextAuthProvider('ihMLPIrUYsFpBGhDPFPPNTzP',
                                                      'XT9.cqTK4mTAyPwM.mp6-1B1IxJP1sCYW9QPPjhTb_z-K,H0IUHIE7+hO9A2xwJnosBEt9tFBId0dF9qU7xxL+-dF86JLKREjDgik0b5YlIBWq7Ssy-SmHt07igw.stL')
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                session_cloud = cluster.connect()
                return session_cloud
            else:
                cluster = Cluster()
                session_local = cluster.connect()
                return session_local
        except Exception as e:
            self.logger.log(f'error occurred while connecting mysql database {e}', self.error_collection_name)
            raise e

    def create_keyspace(self, keyspace_name):
        """
        This method create keyspace.

        :param keyspace_name:
        :return:
        """
        try:
            query = "CREATE KEYSPACE IF NOT EXISTS " + ' ' + keyspace_name + " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3}"
            self.session.execute(query)
            self.logger.log(f'created keyspace successfully. query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating keyspace {e}', self.error_collection_name)
            raise e

    def create_table(self, table_name, columns):
        try:
            column_query = ''
            for key, value in columns.items():
                column_query += f'{key} {value}, '
            query = f'CREATE TABLE IF NOT EXISTS {self.keyspace_name}.{table_name} ({column_query})'
            query = query[:len(query) - 3] + ')'
            print(query)
            self.session.execute(query)
            self.logger.log(f'creating table successfully. query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def save_record(self, table_name, columns, record):
        try:
            parameter = '%s,' * len(record)
            parameter = parameter[:len(parameter) - 1]
            query = "INSERT INTO {}.{} ({}) VALUES({})".format(self.keyspace_name, table_name, columns, parameter)
            self.session.execute(query, record)
            self.logger.log(f'inserted record successfully. query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while saving record in table {e}', self.error_collection_name)
            raise e

    def update_record(self, table_name, update_column, update_record, update_condition):
        try:
            query = f"UPDATE {self.keyspace_name}.{table_name} SET {update_column} WHERE {update_condition}"
            self.logger.log(f'update query: {query}', self.collection_name)
            self.session.execute(query, update_record)
            self.logger.log(f'record updated successfully. query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while updating record in table {e}', self.error_collection_name)
            raise e

    def delete_record(self, table_name, delete_condition):
        try:
            query = f"DELETE FROM {self.keyspace_name}.{table_name}  WHERE {delete_condition}"
            self.logger.log(f'delete query: {query}', self.collection_name)
            self.session.execute(query)
            self.logger.log(f'record deleted successfully. query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while updating record in table {e}', self.error_collection_name)
            raise e

    def get_records(self, table_name,num_records):
        try:
            query = f'SELECT * FROM {self.keyspace_name}.{table_name} LIMIT {num_records}'
            records = self.session.execute(query)
            self.logger.log(f'fetched record successfully. query: {query}', self.collection_name)
            return records
        except Exception as e:
            self.logger.log(f'error occurred while getting record from table {e}', self.error_collection_name)
            raise e
