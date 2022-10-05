from igp.settings import API_KEY, ZNIIS_HOST, ZNIIS_PORT, ZNIIS_LOGIN, ZNIIS_PWD, ZNIIS_FILE_DIR
import dask.dataframe as dd
import pandas as pd
import requests
import urllib3
from igp import app
import os
import googlemaps
from datetime import datetime
import re
import pysftp
from zipfile import ZipFile
import pytz
import time

gmaps = googlemaps.Client(key=API_KEY)


def update_def_data():
    """
    Загрузка файла DEF-9xx.csv с
    """
    urllib3.disable_warnings()
    url_csv = "https://opendata.digital.gov.ru/downloads/DEF-9xx.csv"
    r = requests.get(url_csv, verify=False)

    if r.status_code == 200:
        try:
            with open(f"{app.config['TMP_FOLDER']}DEF-9xx.csv", 'wb') as file:
                file.write(r.content)
            return os.path.basename(f"{app.config['TMP_FOLDER']}DEF-9xx.csv")
        except Exception as error:
            return error
    else:
        return r.status_code


def get_location(region):
    """
    Получение коррдинат региона
    """
    g = gmaps.geocode(region, language='ru-RU')
    if re.search(r'Крым', region):
        country = "Россия"
    else:
        country = g[0]["address_components"][-1]["long_name"]

    lat = g[0]["geometry"]["location"]["lat"]
    long = g[0]["geometry"]["location"]["lng"]

    return lat, long, country


def get_timezone(loc):
    """
    Получение временной зоны по координатам
    """
    now = datetime.now()
    gt = gmaps.timezone((loc[0], loc[1]), datetime.timestamp(now), language='ru-RU')
    return gt['timeZoneId']


def update_tz_data():
    """
    Формирование файла tz.csv
    """
    data = pd.read_csv(f"{app.config['TMP_FOLDER']}DEF-9xx.csv", delimiter=';')
    df = data.copy()
    df_region = df[['Регион']]
    df_region = df_region.drop_duplicates()

    timezone = []
    loc = []
    country = []
    for _, row in df_region.iterrows():
        location = get_location(row[0])
        tz = get_timezone(location)
        timezone.append(tz)
        loc.append(f'{location[0]}, {location[1]}')
        country.append(location[2])
        # print('Регион: %s\nКоординаты: %s, %s\nTimezone: %s\nСтрана: %s' % (row[0], location[0], location[1], tz, location[2]))

    df_region['Timezone'] = timezone
    df_region['Координаты'] = loc
    df_region['Страна'] = country

    df_region.to_csv(f"{app.config['TMP_FOLDER']}tz.csv", sep=';', index=False)

    return os.path.basename(f"{app.config['TMP_FOLDER']}tz.csv")


def update_zniis_data():
    """
    Загрузка файла DEF-9xx.csv с
    """
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    try:
        conn = pysftp.Connection(host=ZNIIS_HOST, port=ZNIIS_PORT, username=ZNIIS_LOGIN, password=ZNIIS_PWD,
                                 cnopts=cnopts)
        print("Соединение установлено")
    except:
        print('Ошибка при подключении к серверу')

    current_time = datetime.now().strftime('%Y-%m-%d')
    result = {}
    dir_data = conn.listdir_attr(ZNIIS_FILE_DIR)
    for att_file in dir_data:
        date_file = datetime.fromtimestamp(att_file.st_mtime, pytz.timezone("Europe/Moscow")).strftime('%Y-%m-%d')
        if current_time == date_file:
            conn.get(ZNIIS_FILE_DIR + att_file.filename, app.config['TMP_FOLDER'] + att_file.filename)
            with ZipFile(app.config['TMP_FOLDER'] + att_file.filename, 'r') as zip_file:
                zip_file.extractall(app.config['TMP_FOLDER'])

            os.remove(app.config['TMP_FOLDER'] + att_file.filename)

            result = {"filename": att_file.filename,
                      "date": att_file.st_mtime,
                      "size": att_file.st_size}
    return result


try:
    data_phone = pd.read_csv(f"{app.config['TMP_FOLDER']}DEF-9xx.csv", delimiter=';')
except:
    update_def_data()
    data_phone = pd.read_csv(f"{app.config['TMP_FOLDER']}DEF-9xx.csv", delimiter=';')

try:
    data_tz = pd.read_csv(f"{app.config['TMP_FOLDER']}tz.csv", delimiter=';')
except:
    update_tz_data()
    data_tz = pd.read_csv(f"{app.config['TMP_FOLDER']}tz.csv", delimiter=';')

try:
    for file in os.listdir(app.config['TMP_FOLDER']):
        if re.search('Port_All_New_' + datetime.now().strftime('%Y%m%d'), file):
            filename = file
    data_pdpn = dd.read_csv(app.config['TMP_FOLDER'] + filename)
except:
    update_zniis_data()
    for file in os.listdir(app.config['TMP_FOLDER']):
        if re.search('Port_All_New_' + datetime.now().strftime('%Y%m%d'), file):
            filename = file
    data_pdpn = dd.read_csv(app.config['TMP_FOLDER'] + filename)


def get_bdpn_info(phone):
    """
    Получение оператора связи из БДПН
    """
    result = data_pdpn.loc[data_pdpn['Number'] == int(phone), "OrgName"].compute().values[0]
    result = re.match(r'(.+)([А-Я][А-Я][А-Я])', result)

    return result[0]


def info(region):
    """
    Получение информации о регионе из файла tz.csv
    """
    for _, row in data_tz.iterrows():
        if region == row[0]:
            return row[1], row[3]


def get_real_info(phone):
    """
    Получение информации о номере телефона
    """
    start_time = time.time()
    prefix = int(phone[-10:-7])
    part = int(phone[-7:])

    for _, row in data_phone.iterrows():
        r = row[0]
        if prefix == row[0]:
            if part >= row[1] and part <= row[2]:
                try:
                    realop = get_bdpn_info(phone)
                    result_info = {
                        "number": phone,
                        "prefix": prefix,
                        "part": part,
                        "operator": realop,
                        "region": row[5],
                        "country": info(row[5])[1],
                        "Timezone": info(row[5])[0],
                        "ExTime": time.time() - start_time
                    }
                except:
                    result_info = {
                        "number": phone,
                        "prefix": prefix,
                        "part": part,
                        "operator": row[4],
                        "region": row[5],
                        "country": info(row[5])[1],
                        "Timezone": info(row[5])[0],
                        "ExTime": time.time() - start_time
                    }
    return result_info
