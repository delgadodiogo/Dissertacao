import requests
import time
import hmac
import hashlib
from urllib.request import urlopen
import json
import urllib.request
import datetime
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import configparser
import webbrowser
import schedule
import time

json_list = []
config = None

global apikey
global API_SECRET
global station_id
global historic_data
global start_timestamp
global t

#Get values from WeatherLinkCloud 
#Get WeatherLink v2 API Key and API Secret https://www.weatherlink.com/account
#Once on the account page click Generate v2 Key

def get_config():
    """
    Get config variables values
    """
    #
    # GLOBAL VARIABLES
    #
    # [ADMIN]
    global ADMIN_EMAIL


    # [InfluxDB]
    global INFLUXDB_ADDRESS
    global INFLUXDB_PORT
    global INFLUXDB_DATABASE
    global INFLUXDB_TIME_PRECISION


    # Instantiate config
    config_parser = configparser.ConfigParser()
    config_parser.read('WeatherLink.ini')

    # [ADMIN]
    ADMIN_EMAIL = config_parser.get('ADMIN', 'Admin_email')


    # [InfluxDB]
    INFLUXDB_ADDRESS = config_parser.get('InfluxDB', 'InfluxDB_Adress')
    INFLUXDB_PORT = config_parser.get('InfluxDB', 'InfluxDB_Port')
    INFLUXDB_DATABASE = config_parser.get('InfluxDB', 'InfluxDB_Database')
    INFLUXDB_TIME_PRECISION = config_parser.get('InfluxDB', 'InfluxDB_Time_Precision')




def getCurrentData():

    global apikey
    global API_SECRET
    global station_id
    global historic_data
    global start_timestamp
    global t

    #Get current weather conditions
    #t = int(time.time())
    t = int(time.time())
    message2 = "api-key%sstation-id%st%s" %(apikey, station_id, t)
    signature2 = hmac.new(bytes(API_SECRET , 'latin-1'), msg = bytes(message2 , 'latin-1'), digestmod = hashlib.sha256).hexdigest()
    URL = 'https://api.weatherlink.com/v2/current/%s?api-key=%s&t=%s&api-signature=%s' %(station_id, apikey, str(t), signature2)
    try:
        request = requests.get(URL, timeout=3)
        #connected to internet
        if (historic_data == 1):
            getHistoricData()
            historic_data = 0
        response = urllib.request.urlopen(URL)
        data_json = json.loads(response.read())
        savetoInflux(data_json)
        start_timestamp = t
    except (requests.ConnectionError, requests.Timeout) as exception:
        historic_data = 1
        print("No internet connection.")



def getHistoricData():

    global api_key
    global API_SECRET
    global station_id
    global historic_data
    global start_timestamp
    global t

    t = int(time.time()) 
    end_timestamp = t


    #colocar na influx para todos os timestamps do start-time ao endtime

    if (end_timestamp - start_timestamp > 300):
        message3 = "api-key%send-timestamp%sstart-timestamp%sstation-id%st%s" %(apikey, end_timestamp, start_timestamp, station_id, t)
        signature3 = hmac.new(bytes(API_SECRET , 'latin-1'), msg = bytes(message3 , 'latin-1'), digestmod = hashlib.sha256).hexdigest()
        URL2 = 'https://api.weatherlink.com/v2/historic/%s?api-key=%s&t=%s&start-timestamp=%s&end-timestamp=%s&api-signature=%s' %(station_id, apikey, str(t), start_timestamp, end_timestamp, signature3)
        response = urllib.request.urlopen(URL2)
        data_json = json.loads(response.read())
        print(data_json)  


def main():

    global apikey
    global API_SECRET
    global station_id
    global historic_data

    apikey = "wlnqtipr3ssd4jzqjpecphtvajpiefuv"   
    API_SECRET = 'urkd214z8a6nxkvpqpgxmoieljrsuuao' 
    t = int(time.time())
    historic_data = 0

    #calculate the api signature with HMAC SHA-256 algorithm with the concatenated string as the message and the API Secret as HMAC secret key

    message = "api-key%st%s" %(apikey, t)
    signature = hmac.new(bytes(API_SECRET , 'latin-1'), msg = bytes(message , 'latin-1'), digestmod = hashlib.sha256).hexdigest()

    URL = 'https://api.weatherlink.com/v2/stations?api-key=%s&t=%s&api-signature=%s' %(apikey, str(t), signature)
    response = urllib.request.urlopen(URL)
    data_json = json.loads(response.read())
    station_id = data_json["stations"][0]["station_id"]


    get_config()
    schedule.every(10).seconds.do(getCurrentData)


    while True:
        schedule.run_pending()
        #getCurrentData()

    #getHistoricData()


def savetoInflux(dataw):

    global t
    print(dataw)
    temp = dataw["sensors"][4]["data"][0]["temp"]
    solar_radiation = dataw["sensors"][4]["data"][0]["solar_rad"]
    uv_index = dataw["sensors"][4]["data"][0]["uv_index"]
    wind_speed = dataw["sensors"][4]["data"][0]["wind_speed_avg_last_1_min"]
    wind_dir = dataw["sensors"][4]["data"][0]["wind_dir_last"]
    rain_size = dataw["sensors"][4]["data"][0]["rain_rate_last_mm"]
    ut = dataw["sensors"][0]["data"][0]["ts"]
    bar_absolute = dataw["sensors"][3]["data"][0]["bar_absolute"]
    temp_in = dataw["sensors"][2]["data"][0]["temp_in"]
    hum_in = dataw["sensors"][2]["data"][0]["hum_in"]
    wind_chill = dataw["sensors"][4]["data"][0]["wind_chill"]

    temp_In = round((5 / 9) * (temp_in - 32))
    temp_Ex = round((5 / 9) * (temp - 32))
    wind_Chill = round((5 / 9) * (wind_chill - 32))

    print(t)
    
    print("Hey")
    print(wind_speed)

    global json_list
    json_body = {
        'measurement': 'WeatherLink_Live',
        'tags': {
            'location': 'IGUP',
            'instrument': 'WeatherLink',
            'latitude': '41.136111',
            'longitude': '8.6025000',
            'altitude': '93.515'
        },
        'time': t,
        'fields': {
            'temp_Ex': int(temp_Ex),
            'temp_In': int(temp_In),
            'solar_radiation': int(solar_radiation),
            'uv index': int(uv_index),
            'wind_speed': int(wind_speed),
            'wind_dir': int(wind_dir),
            'rain_size': int(rain_size),
            'bar_absolute': int(bar_absolute),
            'hum_in': int(hum_in),
            'wind_chill': int(wind_Chill)
        },
    } 

    json_list.append(json_body)

    influxdb_client = InfluxDBClient(host=INFLUXDB_ADDRESS, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE)
    influxdb_client.create_database(INFLUXDB_DATABASE)


    influxdb_client.write_points(json_list, time_precision=INFLUXDB_TIME_PRECISION, protocol='json')

    #results = influxdb_client.query('select rain_size from WeatherLink_Live;')
    #print ("Result: {0}".format(results))


if __name__ == "__main__":
    main() 


