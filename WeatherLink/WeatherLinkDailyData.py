import requests
import time
import hmac
import hashlib
from urllib.request import urlopen
import json
import urllib.request
import datetime
from datetime import datetime, timedelta
from datetime import date
from influxdb import InfluxDBClient
import configparser
import webbrowser
import schedule
import time
from calendar import monthrange
from dateutil.relativedelta import relativedelta


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


def valuesToInflux():
    global start_timestamp
    global end_timestamp
    global api_key
    global station_id
    global API_SECRET
    global response
    global URL2
    global count
    global count2
    global signature3
    global t

    t = int(time.time())

    #generate the URL to get the values from start-timestamp to the end-timestamp

    if (end_timestamp - start_timestamp >= 300):
        message3 = "api-key%send-timestamp%sstart-timestamp%sstation-id%st%s" %(apikey, end_timestamp, start_timestamp, station_id, t)
        signature3 = hmac.new(bytes(API_SECRET , 'latin-1'), msg = bytes(message3 , 'latin-1'), digestmod = hashlib.sha256).hexdigest()
        URL2 = 'https://api.weatherlink.com/v2/historic/%s?api-key=%s&t=%s&start-timestamp=%s&end-timestamp=%s&api-signature=%s' %(station_id, apikey, str(t), start_timestamp, end_timestamp, signature3)
        try: response = urllib.request.urlopen(URL2)
    
        except urllib.error.URLError as e:
            time.sleep(3600)
            count+=1
            failed()


        data_json = json.loads(response.read())
        savetoInflux(data_json)
        count2 = count
        count = 0


def getHistoricData():

    global api_key
    global station_id
    global start_timestamp
    global end_timestamp
    global t
    global count
    global count2
    global date
    i = 0

    #calculate the start_timestamp and end_timestamp from the previous day in order to get the data
    t = int(time.time())
    current_time = datetime.now() 
    date = datetime.strptime(date, "%d/%m/%Y - %H:%M:%S")
    start_timestamp = round(datetime.timestamp(date) + 3600*count2)
    end_timestamp = round(start_timestamp + 86400 + 3600*count)

    count2 = 0
    valuesToInflux()


def failed():
    global response
    global count
    global URL2 
    global start_timestamp
    global end_timestamp
    global apikey
    global station_id
    global t
    global signature3

    #in case there is a failure in the call to the WeatherLinkApi we try again later increasing the end_timestamp until we succeed
    end_timestamp = round(start_timestamp + 86400 + 3600*count)

    message3 = "api-key%send-timestamp%sstart-timestamp%sstation-id%st%s" %(apikey, end_timestamp, start_timestamp, station_id, t) 
    signature3 = hmac.new(bytes(API_SECRET, 'latin-1'), msg = bytes(message3, 'latin-1'), digestmod = hashlib.sha256).hexdigest()
    URL2 = 'https://api.weatherlink.com/v2/historic/%s?api-key=%s&t=%s&start-timestamp=%s&end-timestamp=%s&api-signature=%s' %(station_id, apikey, str(t), start_timestamp, end_timestamp, signature3)
    try: response = urllib.request.urlopen(URL2)

    except urllib.error.URLError as e:
        print("failed")
        time.sleep(3600)
        count += 1
        failed()

def last_day_of_month(date_value):
    return date_value.replace(day = monthrange(date_value.year, date_value.month)[1])

def main():

    global apikey
    global API_SECRET
    global station_id
    global historic_data
    global x
    global URL
    global response
    global date
    global count
    global count2

    count = 0
    count2 = 0


    apikey = "wlnqtipr3ssd4jzqjpecphtvajpiefuv"   
    API_SECRET = 'urkd214z8a6nxkvpqpgxmoieljrsuuao' 
    t = int(time.time())
    current_time = datetime.now()

    #calculate the date of the last day and save it in date in order to get the values
    if current_time.day == 1:
        last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
        date = last_day_of_prev_month.month + "/" + str(current_time.month) + "/" + str(current_time.year) + " - " + "00:00:00"
    if current_time.day > 10 and current_time.month > 9:
        date = str(current_time.day-1) + "/" + str(current_time.month) + "/" + str(current_time.year) + " - " + "00:00:00"
    elif current_time.day <= 10 and current_time.month > 9:
        date = "0" + str(current_time.day-1) + "/" + str(current_time.month) + "/" + str(current_time.year) + " - " + "00:00:00"  
    elif current_time.day > 10 and current_time.month <= 9:
        date = str(current_time.day-1) + "/" + "0" + str(current_time.month) + "/" + str(current_time.year) + " - " + "00:00:00" 
    elif current_time.day <= 10 and current_time.month <= 9:
        date = "0" + str(current_time.day-1) + "/" + "0" + str(current_time.month) + "/" + str(current_time.year) + " - " + "00:00:00"



    #calculate the api signature with HMAC SHA-256 algorithm with the concatenated string as the message and the API Secret as HMAC secret key

    message = "api-key%st%s" %(apikey, t)
    signature = hmac.new(bytes(API_SECRET , 'latin-1'), msg = bytes(message , 'latin-1'), digestmod = hashlib.sha256).hexdigest()

    URL = 'https://api.weatherlink.com/v2/stations?api-key=%s&t=%s&api-signature=%s' %(apikey, str(t), signature)

    response = urllib.request.urlopen(URL)
    data_json = json.loads(response.read())
    station_id = data_json["stations"][0]["station_id"]
    get_config()

    #At the end of the day, get the historical data
    schedule.every().day.at("00:00").do(getHistoricData)

    while True:
        schedule.run_pending()  


def makeInt(x):
    if x != None:
        x = int(x)
        return x
    else:
        return x

def converter(x):
    if x != None:
        x = round((5 / 9) * (x - 32))
        return x
    else:
        return x


def savetoInflux(dataw):

    #Filter the Json structure received as dataw and save the values in the database InfluxDB.

    global start_timestamp
    global count
    i = 0
    print("save")

    influxdb_client = InfluxDBClient(host=INFLUXDB_ADDRESS, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE)
    influxdb_client.create_database(INFLUXDB_DATABASE)

    while i < (288 + 12*count):

        #IGUPINT

        temp_In = converter(makeInt(dataw["sensors"][2]["data"][i]["temp_in_last"]))
        temp_In_Hi = converter(makeInt(dataw["sensors"][2]["data"][i]["temp_in_hi"]))
        temp_In_Lo = converter(makeInt(dataw["sensors"][2]["data"][i]["temp_in_lo"]))
        hum_In = makeInt(dataw["sensors"][2]["data"][i]["hum_in_last"])
        hum_In_Hi = makeInt(dataw["sensors"][2]["data"][i]["hum_in_hi"])
        hum_In_Lo = makeInt(dataw["sensors"][2]["data"][i]["hum_in_lo"])
        dew_Point_In = converter(makeInt(dataw["sensors"][2]["data"][i]["dew_point_in"]))
        internal_Heat_Index = converter(makeInt(dataw["sensors"][2]["data"][i]["heat_index_in"]))
        bar_Absolute = makeInt(dataw["sensors"][3]["data"][i]["bar_absolute"])
        bar_Hi = makeInt(dataw["sensors"][3]["data"][i]["bar_hi"])
        bar_Lo = makeInt(dataw["sensors"][3]["data"][i]["bar_lo"])
        bar_Sea_Level = makeInt(dataw["sensors"][3]["data"][i]["bar_sea_level"])


        #IGUPEXT

        temp_Ex = converter(makeInt(dataw["sensors"][4]["data"][i]["temp_avg"]))
        temp_Lo = converter(makeInt(dataw["sensors"][4]["data"][i]["temp_lo"]))
        temp_Hi = converter(makeInt(dataw["sensors"][4]["data"][i]["temp_hi"]))
        Wp = makeInt(dataw["sensors"][4]["data"][i]["wind_speed_avg"])
        Wphi = makeInt(dataw["sensors"][4]["data"][i]["wind_speed_hi"])
        Wdir = makeInt(dataw["sensors"][4]["data"][i]["wind_dir_of_prevail"])
        Wdirhi = makeInt(dataw["sensors"][4]["data"][i]["wind_speed_hi_dir"])
        wind_Chill = converter(makeInt(dataw["sensors"][4]["data"][i]["wind_chill_last"]))
        wind_Chill_Low = converter(makeInt(dataw["sensors"][4]["data"][i]["wind_chill_lo"]))
        rain_Size = makeInt(dataw["sensors"][4]["data"][i]["rainfall_mm"])
        rain_Rate_Hi = makeInt(dataw["sensors"][4]["data"][i]["rain_rate_hi_mm"])
        solar_Radiation = makeInt(dataw["sensors"][4]["data"][i]["solar_rad_avg"])
        solar_Radiation_Hi = makeInt(dataw["sensors"][4]["data"][i]["solar_rad_hi"])
        uv_Index = makeInt(dataw["sensors"][4]["data"][i]["uv_index_avg"])
        uv_Index_Hi = makeInt(dataw["sensors"][4]["data"][i]["uv_index_hi"])
        uv_Dose = makeInt(dataw["sensors"][4]["data"][i]["uv_dose"])
        heating_Days = makeInt(dataw["sensors"][4]["data"][i]["heating_degree_days"])
        cooling_Days = makeInt(dataw["sensors"][4]["data"][i]["cooling_degree_days"])


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
            'time': start_timestamp,
            'fields': {
                #IGUPINT
                'temp_In': temp_In,
                'temp_In_Hi': temp_In_Hi,
                'temp_In_Lo': temp_In_Lo,
                'hum_in': hum_In,
                'hum_in_hi': hum_In_Hi,
                'hum_in_lo': hum_In_Lo,
                'dew_Point_In': dew_Point_In,
                'internal_Heat_Index': internal_Heat_Index,
                'bar_absolute': bar_Absolute,
                'bar_hi': bar_Hi,
                'bar_lo': bar_Lo,
                'bar_sea_level': bar_Sea_Level,


                #IGUPEXT
                'temp_Ex': temp_Ex,
                'temp_Lo': temp_Lo,
                'temp_Hi': temp_Hi,
                'wind_speed': Wp,
                'wind_speed_hi': Wphi,
                'wind_dir': Wdir,
                'wind_dir_hi': Wdirhi,
                'wind_chill': wind_Chill,
                'wind_chill_low': wind_Chill_Low,
                'rain_size': rain_Size,
                'rain_rate_hi': rain_Rate_Hi,
                'solar_radiation': solar_Radiation,
                'solar_radiation_hi': solar_Radiation_Hi,
                'heating_days': heating_Days,
                'cooling_days': cooling_Days
            },
        } 
        i+=1
        start_timestamp+=300

        json_list.append(json_body)
        influxdb_client.write_points(json_list, time_precision=INFLUXDB_TIME_PRECISION, protocol='json')



if __name__ == "__main__":
    main() 


