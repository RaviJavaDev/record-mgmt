import json
import uuid

from flask import Flask, jsonify, request, render_template

from model.db_details import DbDetails
from service.record_mgmt import RecordMgmt
from util.csv_file_oprations import CsvFileOperations
from util.json_file_operations import JsonFileOperation

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])  # To render Homepage
def home_page():
    return render_template('index.html')


@app.route('/api/create-db', methods=['POST'])
def create_db():
    """
    This api creates database.
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            if request.content_type == 'application/x-www-form-urlencoded':
                db_type = request.form['db_type']
                host = request.json['host']
                user_name = request.json['user_name']
                password = request.json['password']
                db_name = request.json['db_name']
            else:
                db_type = request.json['db_type']
                host = request.json['host']
                user_name = request.json['user_name']
                password = request.json['password']
                db_name = request.json['db_name']
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            record_mgmt.create_database(db_name)
            # resp = Response(js_dump, status=200,
            #                mimetype='application/json')
            return jsonify({'Status': 'SUCCESS', 'message': f'Database {db_name} created successfully in {db_type}.'})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/form/create-db', methods=['POST'])
def create_db_form():
    """
    This api creates database.
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.form['db_type']
            host = request.form['host']
            user_name = request.form['user_name']
            password = request.form['password']
            db_name = request.form['db_name']
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            record_mgmt.create_database(db_name)
            return render_template('results.html', result=f'Database {db_name} created successfully in {db_type}.')
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/api/create-table', methods=['POST'])
def create_table():
    """
    This api create tables.
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.json['db_type']
            host = request.json['host']
            user_name = request.json['user_name']
            password = request.json['password']
            db_name = request.json['db_name']
            table_name = request.json['table_name']
            columns = request.json['columns']
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            record_mgmt.create_table(table_name=table_name, columns=columns)
            return jsonify({'Status': 'SUCCESS', 'message': f'Table {table_name} created successfully in {db_name}.'})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/form/create-table', methods=['POST'])
def create_table_form():
    """
    This api create tables.
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.form['db_type']
            host = request.form['host']
            user_name = request.form['user_name']
            password = request.form['password']
            db_name = request.form['db_name']
            table_name = request.form['table_name']
            columns = json.loads(request.form['columns'])
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            record_mgmt.create_table(table_name=table_name, columns=columns)
            return render_template('results.html', result=f'Table {table_name} created successfully in {db_name}.')
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/api/save-record', methods=['POST'])
def save_record():
    """
    This api saves record.\n
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.json['db_type']
            host = request.json['host']
            user_name = request.json['user_name']
            password = request.json['password']
            db_name = request.json['db_name']
            table_name = request.json['table_name']
            dct_obj = request.json['dct_obj']
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            record_mgmt.save_record(table_name, dct_obj)
            return jsonify({'Status': 'SUCCESS', 'message': f'Record saved successfully in {db_name}.{table_name}.'})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/form/save-record', methods=['POST'])
def save_record_form():
    """
    This api saves record.\n
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.form['db_type']
            host = request.form['host']
            user_name = request.form['user_name']
            password = request.form['password']
            db_name = request.form['db_name']
            table_name = request.form['table_name']
            dct_obj = json.loads(request.form['dct_obj'])
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            record_mgmt.save_record(table_name, dct_obj)
            return render_template('results.html', result=f'Record saved successfully in {db_name}.{table_name}.')
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/api/upload-records', methods=['POST'])
def upload_records():
    """
    This api saves multiple records.\n
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.json['db_type']
            host = request.json['host']
            user_name = request.json['user_name']
            password = request.json['password']
            db_name = request.json['db_name']
            table_name = request.json['table_name']
            file_name = request.json['file_name']
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            json_file_operation = JsonFileOperation(transaction_id=transaction_id, file_name=file_name,
                                                    db_details=db_details)
            records = json_file_operation.read_json_file()
            record_mgmt = RecordMgmt(transaction_id=transaction_id, db_details=db_details)
            record_mgmt.save_multiple_record(table_name=table_name, dict_list=records)
            return jsonify({'Status': 'SUCCESS', 'message': 'Records saved successfully.'})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/form/upload-records', methods=['POST'])
def upload_records_form():
    """
    This api saves multiple records.\n
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.form['db_type']
            host = request.form['host']
            user_name = request.form['user_name']
            password = request.form['password']
            db_name = request.form['db_name']
            table_name = request.form['table_name']
            file_name = request.form['file_name']
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            json_file_operation = JsonFileOperation(transaction_id=transaction_id, file_name=file_name,
                                                    db_details=db_details)
            records = json_file_operation.read_json_file()
            record_mgmt = RecordMgmt(transaction_id=transaction_id, db_details=db_details)
            record_mgmt.save_multiple_record(table_name=table_name, dict_list=records)
            return render_template('results.html', result='Records saved successfully.')
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/api/update-record', methods=['POST'])
def update_record():
    """
     This api update the record.\n
     :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.json['db_type']
            host = request.json['host']
            user_name = request.json['user_name']
            password = request.json['password']
            db_name = request.json['db_name']
            table_name = request.json['table_name']
            filter_criteria = request.json['filter_criteria']
            new_value = request.json['new_value']
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            record_mgmt.update_record(table_name=table_name, filter_criteria=filter_criteria,
                                      new_value=new_value)
            return jsonify({'Status': 'SUCCESS', 'message': 'Record updated successfully.'})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/form/update-record', methods=['POST'])
def update_record_form():
    """
     This api update the record.\n
     :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.form['db_type']
            host = request.form['host']
            user_name = request.form['user_name']
            password = request.form['password']
            db_name = request.form['db_name']
            table_name = request.form['table_name']
            filter_criteria = json.loads(request.form['filter_criteria'])
            new_value = json.loads(request.form['new_value'])
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            record_mgmt.update_record(table_name=table_name, filter_criteria=filter_criteria,
                                      new_value=new_value)
            return render_template('results.html', result='Record updated successfully.')
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/api/delete-record', methods=['POST'])
def delete_record():
    """
    This api deletes record.
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.json['db_type']
            host = request.json['host']
            user_name = request.json['user_name']
            password = request.json['password']
            db_name = request.json['db_name']
            table_name = request.json['table_name']
            filter_criteria = request.json['filter_criteria']
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            record_mgmt.delete_record(table_name=table_name, filter_criteria=filter_criteria)
            return jsonify({'Status': 'SUCCESS', 'message': 'Record deleted successfully.'})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/form/delete-record', methods=['POST'])
def delete_record_form():
    """
    This api deletes record.
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.form['db_type']
            host = request.form['host']
            user_name = request.form['user_name']
            password = request.form['password']
            db_name = request.form['db_name']
            table_name = request.form['table_name']
            filter_criteria = json.loads(request.form['filter_criteria'])
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            record_mgmt.delete_record(table_name=table_name, filter_criteria=filter_criteria)
            return render_template('results.html', result='Record deleted successfully.')
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/api/download-records', methods=['POST'])
def download_records():
    """
    This api download records.
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.json['db_type']
            host = request.json['host']
            user_name = request.json['user_name']
            password = request.json['password']
            db_name = request.json['db_name']
            table_name = request.json['table_name']
            num_records = request.json['num_records']
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            csv_operation = CsvFileOperations(transaction_id, f'{table_name}_{num_records}.csv', db_details=db_details)
            records = record_mgmt.get_records(table_name=table_name, num_records=num_records)
            csv_operation.write_data(records)
            return jsonify({'Status': 'SUCCESS', 'message': 'file successfully downloaded to location ' +
                                                            f"/output_data/{table_name}_{num_records}.csv",
                            'no. of records: ': f'{len(records)}'})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/form/download-records', methods=['POST'])
def download_records_form():
    """
    This api download records.
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_type = request.form['db_type']
            host = request.form['host']
            user_name = request.form['user_name']
            password = request.form['password']
            db_name = request.form['db_name']
            table_name = request.form['table_name']
            num_records = request.form['num_records']
            db_details = DbDetails(db_type=db_type, db_name=db_name, host=host, user_name=user_name, password=password)
            record_mgmt = RecordMgmt(transaction_id, db_details)
            csv_operation = CsvFileOperations(transaction_id, f'{table_name}_{num_records}.csv', db_details=db_details)
            records = record_mgmt.get_records(table_name=table_name, num_records=num_records)
            csv_operation.write_data(records)
            return render_template('results.html',
                                   result=f'file successfully downloaded to location /output_data/{table_name}_{num_records}.csv')
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


if __name__ == '__main__':
    app.run()
