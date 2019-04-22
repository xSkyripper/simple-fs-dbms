from flask import Blueprint, render_template, request
from sdbms.app.service.builder import QueryBuilder
from sdbms.core.manager import DbManager
from sdbms.core.parser import QueryParser, CommandError

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
        query = queryBuilder.build_update(result)
        db_manager = DbManager(root_path)
        parser = QueryParser()
        cmd = parser.parse(set_db)
        rv = cmd.execute(db_manager)
        cmd = parser.parse(query)
        rv = cmd.execute(db_manager)

    return render_template('updateResult.html')
