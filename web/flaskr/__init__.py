import os

from flask import Flask, request, render_template
from flask import send_from_directory


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # a simple page that says hello
    @app.route('/<path:filename>')
    def serve_static(filename):
        return render_template(filename)
    
    @app.route('/',methods=['GET','POST'])
    def home():
        if request.method=='GET':
            return render_template("sql_tool.html")
        else:
            query = request.form["query"]
            print(query)
            return render_template("sql_tool.html")


    # @app.route('/submit_query',methods=['GET','POST'])
    # def submit_query():
    #     print(request.form)
    #     message = request.form["query"]
    #     print(message)
    #     return render_template('plan_history.html')
    
    @app.route('/hello')
    def hello():
        return 'Hello, World!'

    @app.route('/postmethod', methods = ['POST'])
    def postmethod():
        data = request.form['text']
        print (data)


    return app