from flask import Blueprint, render_template, request
from sdbms.app.service.builder import QueryBuilder
from sdbms.core._manager import DbManager
from sdbms.core._parser import QueryParser, CommandError

"""
Web api that builds the query and sends it to the query manager.

Each route will represent a different CRUD operation and for each operation you will need
to send a form with the certain data, and as result we will receive a html page.

1.'/' we will display the menu that will give you all the hiperlinks for each operation.

2.'/select' page used for building the select query. Some fields of this form ar optional.
As a result, you will receive a page witch contains the selected rows.

3.'/insert' page used for building the insert query. For this one you will need to press the 
'+' button and fill the fields. As a response, you will receive a success message or an exception if something went wrong

4.'/delete' page use for deleting a row. As a response you will receive success or expcetion.

5.'/update' page used for updating a row. You will need to fill the labels, conditions and values.
As a response you will receive a successs message or an exception.
"""

root_path = "/Users/cernescustefan/Documents/Facultate/db"

main_api = Blueprint('main', __name__,
                     template_folder='templates')


@main_api.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@main_api.route('/select', methods=['GET'])
def select():
    return render_template('select.html')

@main_api.route('/insert', methods=['GET'])
def insert():
    return render_template('insert.html')

@main_api.route('/delete', methods=['GET'])
def delete():
    return render_template('delete.html')

@main_api.route('/update', methods=['GET'])
def update():
    return render_template('update.html')


@main_api.route('/result', methods=['POST', 'GET'])
def result():
    if request.method == 'POST':
        result = request.form
        queryBuilder = QueryBuilder()
        set_db = queryBuilder.use_db(result)
        query = queryBuilder.build_select(result)
        print(result)
        db_manager = DbManager(root_path)
        parser = QueryParser()
        cmd = parser.parse(set_db)
        rv = cmd.execute(db_manager)
        cmd = parser.parse(query)
        rv = cmd.execute(db_manager)

    context = list(rv)
    return render_template('result.html', context=context)

@main_api.route('/insertResult', methods=['POST', 'GET'])
def insertResult():
    if request.method == 'POST':
        result = request.form
        queryBuilder = QueryBuilder()
        set_db = queryBuilder.use_db(result)
        query = queryBuilder.build_insert(result)
        print(result)
        assert result
        db_manager = DbManager(root_path)
        parser = QueryParser()
        cmd = parser.parse(set_db)
        rv = cmd.execute(db_manager)
        cmd = parser.parse(query)
        rv = cmd.execute(db_manager)

    return render_template('insertResutl.html')


@main_api.route('/deleteResult', methods=['POST', 'GET'])
def deleteResult():
    if request.method == 'POST':
        result = request.form
        queryBuilder = QueryBuilder()
        set_db = queryBuilder.use_db(result)
        query = queryBuilder.build_delete(result)
        print(result)
        assert result
        db_manager = DbManager(root_path)
        parser = QueryParser()
        cmd = parser.parse(set_db)
        rv = cmd.execute(db_manager)
        cmd = parser.parse(query)
        rv = cmd.execute(db_manager)

    return render_template('deleteResult.html')

@main_api.route('/updateResult', methods=['POST', 'GET'])
def updateResult():
    if request.method == 'POST':
        result = request.form
        queryBuilder = QueryBuilder()
        set_db = queryBuilder.use_db(result)
        print(result)
        assert result
        query = queryBuilder.build_update(result)
        db_manager = DbManager(root_path)
        parser = QueryParser()
        cmd = parser.parse(set_db)
        rv = cmd.execute(db_manager)
        cmd = parser.parse(query)
        rv = cmd.execute(db_manager)

    return render_template('updateResult.html')
