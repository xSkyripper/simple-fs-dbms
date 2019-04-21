from flask import Blueprint, render_template


main_api = Blueprint('main', __name__,
                     template_folder='templates')

@main_api.route('/', methods=['GET'])
def index():
    context = {
        'some_list': [1, 2, 3, 4, 5],
        'some_string': 'Hello world',
        'some_int': 12345,
    }
    return render_template('index.html', context=context)
