class RequestParamHelper:

    def get_request_parameters(self,request):
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
        return db_name, db_type, host, password, user_name
