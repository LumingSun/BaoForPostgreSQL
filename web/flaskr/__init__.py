import os

from flask import Flask, request, render_template
from flask import send_from_directory
from .backend import run_query, optimize_query

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

    # @app.after_request
    # def after_request(resp):
    #     resp.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,session_id')
    #     resp.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS,HEAD')
    #     # 这里不能使用add方法，否则会出现 The 'Access-Control-Allow-Origin' header contains multiple values 的问题
    #     resp.headers['Access-Control-Allow-Origin'] = '*'
    #     return resp

    @app.route('/pg_run', methods=['POST'])
    def run_with_pg():
        sql = request.values['sql']
        print ("run with postgresql")
        status = run_query(sql, bao_select=False, bao_reward=False)
        return {}

    @app.route('/deepo_run', methods=['POST'])
    def optimize_with_deepo():
        sql = request.values['sql']
        print ("optimized with deepo")
        # status = run_query(sql, bao_select=True, bao_reward=True)
        optimize_query(sql)
        return {}
    
    return app