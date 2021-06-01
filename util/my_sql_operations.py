import mysql.connector as connection

from logger.db_logger import DBLogger


class MySqlOperation:
    def __init__(self, transaction_id, db_details, logger_db_details):
        host = db_details.host
        user_name = db_details.user_name
        password = db_details.password
        self.db_name = db_details.db_name
        self.collection_name = 'db_log'
        self.error_collection_name = 'error_log'
        self.logger = DBLogger(transaction_id, logger_db_details)
        try:
            self.conn = connection.connect(host=host, user=user_name, passwd=password, use_pure=True)
        except Exception as e:
            self.logger.log(f'error occurred while connecting mysql database {e}', self.error_collection_name)
            raise e

    def create_database(self, database_name):
        """
        This method creates database.

        :param database_name: database name.
        :return:
        """
        try:
            cursor = self.conn.cursor()
            query = f'CREATE DATABASE IF NOT EXISTS {database_name}'
            cursor.execute(query)
            self.logger.log(f'created database query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating database {e}', self.error_collection_name)
            raise e

    def create_table(self, table_name, columns):
        """
        This method creates table.

        :param table_name:T able name.
        :param columns: Columns names in table.
        :return:
        """
        try:
            cursor = self.conn.cursor()
            column_query = ''
            for key, value in columns.items():
                column_query += f'{key} {value}, '
            query = f'CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({column_query})'
            self.logger.log(f'creating table started query: {query}', self.collection_name)
            query = query[:len(query) - 3] + ')'
            cursor.execute(query)
            self.logger.log(f'creating table finished query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while creating table {e}', self.error_collection_name)
            raise e

    def save_record(self, table_name, columns, record):
        """
        This method saves record.

        :param table_name: Table name.
        :param columns:
        :param record:
        :return:
        """
        try:
            cursor = self.conn.cursor()
            parameter = '%s,' * len(record)
            parameter = parameter[:len(parameter) - 1]
            query = "INSERT INTO {}.{} ({}) VALUES({})".format(self.db_name, table_name, columns, parameter)
            self.logger.log(f'insert query: {query}', self.collection_name)
            cursor.execute(query, record)
            self.conn.commit()
            self.logger.log(f'inserted record successfully. query: {query}', self.collection_name)
        except Exception as e:
            self.logger.log(f'error occurred while saving record in table {e}', self.error_collection_name)
            raise e

    def update_record(self, table_name, update_column, update_record, update_condition):
        """
        This method update record.

        :param table_name:
        :param update_column:
        :param update_record:
        :param update_condition:
        :return:
        """
        try:
            cursor = self.conn.cursor()
            query = f"UPDATE {self.db_name}.{table_name} SET {update_column} WHERE {update_condition}"
            self.logger.log(f'update query: {query}', self.collection_name)
            cursor.execute(query, update_record)
            self.logger.log(f'record updated successfully. query: {query}', self.collection_name)
            self.conn.commit()
        except Exception as e:
            self.logger.log(f'error occurred while updating record in table {e}', self.error_collection_name)
            raise e

    def delete_record(self, table_name, delete_condition):
        """
        This method delete record.

        :param table_name:
        :param delete_condition:
        :return:
        """
        try:
            cursor = self.conn.cursor()
            query = f"DELETE FROM {self.db_name}.{table_name}  WHERE {delete_condition}"
            self.logger.log(f'delete query: {query}', self.collection_name)
            cursor.execute(query)
            self.logger.log(f'record deleted successfully. query: {query}', self.collection_name)
            self.conn.commit()
        except Exception as e:
            self.logger.log(f'error occurred while updating record in table {e}', self.error_collection_name)
            raise e

    def get_records(self, table_name,num_records):
        """
        This method gets multiple records.
        :param table_name:
        :param num_records:
        :return:
        """
        try:
            cursor = self.conn.cursor()
            query = f'SELECT * FROM {self.db_name}.{table_name} LIMIT {num_records}'
            self.logger.log(f'fetch query: {query}', self.collection_name)
            cursor.execute(query)
            records = cursor.fetchall()
            self.logger.log(f'fetched record query: {query}', self.collection_name)
            return records
        except Exception as e:
            self.logger.log(f'error occurred while getting record from table {e}', self.error_collection_name)
            raise e


if __name__ == '__main__':
    my_sql_opr = MySqlOperation('123432')
    db_name = 'EMP_MGMT_DB'
    my_sql_opr.create_database(db_name)
    columns = {'EMP_ID': 'INT', 'EMP_NAME': 'VARCHAR(40)', 'AGE': 'INT'}
    table_name = 'EMPLOYEE_HIST'
    my_sql_opr.create_table(db_name, table_name, columns)

    columns = []
    values = []
    emp = {'EMP_ID': 1, 'EMP_NAME': 'Ravi', 'AGE': 32}
    columns = ",".join([key for key in emp.keys()])
    record = tuple([key for key in emp.values()])
    my_sql_opr.save_record(db_name, table_name, columns, record)
