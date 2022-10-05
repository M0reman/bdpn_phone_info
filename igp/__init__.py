import os
from flask import Flask
from sys import platform

app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))

""" 
Определение версии ОС для правильной поставновки слешей
"""
if platform == "linux" or platform == "linux2":
    TMP_FOLDER = f'{basedir}/static/tmp/'
elif platform == "darwin":
    TMP_FOLDER = f'{basedir}/static/tmp/'
elif platform == "win32":
    TMP_FOLDER = f'{basedir}\\static\\tmp\\'

app.config['TMP_FOLDER'] = TMP_FOLDER

from igp import routes
