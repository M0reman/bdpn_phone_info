import os
from flask import Flask

app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))

# TMP_FOLDER = f'{basedir}/static/tmp/'
TMP_FOLDER = f'{basedir}\\static\\tmp\\'

app.config['TMP_FOLDER'] = TMP_FOLDER

from igp import routes
