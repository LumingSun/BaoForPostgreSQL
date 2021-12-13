## requirements:
```
pip install scikit-learn numpy joblib
pip install torch==1.5.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install psycopg2-binary
```




```
# in bao_server directory, start server:
run mian.py

# in web directory run following commands:
export FLASK_APP=flaskr
export FLASK_ENV=development
flask run --port=5002 --host=0.0.0.0
```