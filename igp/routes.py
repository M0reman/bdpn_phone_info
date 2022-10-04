from igp import app
from igp.init_functions import update_def_data, update_tz_data, update_zniis_data, get_real_info
from flask import request


@app.route('/update-def', methods=['GET'])
def update_def():
    result = update_def_data()
    return result


@app.route('/update-tz', methods=['GET'])
def update_tz():
    result = update_tz_data()
    return result


@app.route('/update-zniis', methods=['GET'])
def update_zniis():
    result = update_zniis_data()
    return result


@app.route('/get-phone-info', methods=['GET'])
def get_phone_info():
    result = get_real_info(request.args.get('phone'))
    return result

