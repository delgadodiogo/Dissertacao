import requests
import time
import hmac
import hashlib
from urllib.request import urlopen
import json
import urllib.request
import datetime
from datetime import datetime, timedelta, date
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


def valuesToInflux():
    global start_timestamp
    global end_timestamp
    global api_key
    global station_id
    global API_SECRET
    global response
    global URL2

    t = int(time.time())

    if (end_timestamp - start_timestamp >= 300):
        message3 = "api-key%send-timestamp%sstart-timestamp%sstation-id%st%s" %(apikey, end_timestamp, start_timestamp, station_id, t)
        signature3 = hmac.new(bytes(API_SECRET , 'latin-1'), msg = bytes(message3 , 'latin-1'), digestmod = hashlib.sha256).hexdigest()
        URL2 = 'https://api.weatherlink.com/v2/historic/%s?api-key=%s&t=%s&start-timestamp=%s&end-timestamp=%s&api-signature=%s' %(station_id, apikey, str(t), start_timestamp, end_timestamp, signature3)
        try: response = urllib.request.urlopen(URL2)
    
        except urllib.error.URLError as e:
            print ("failed2")
            #time.sleep(3600)
            #failed(URL2)


        data_json = json.loads(response.read())
        #print(data_json)
        savetoInflux(data_json)


def getHistoricData():

    global api_key
    global API_SECRET
    global station_id
    global historic_data
    global start_timestamp
    global end_timestamp
    global t
    global count
    global count2
    count = 0
    count2 = 0
    i = 0

    #if there is a failure in the internet connection, wait for 1 hour that day and then try again
    hostname = "google.com"
    response = os.system("ping -c 1 " + hostname)
    if response == 0:
        print("is up!")
    else:
        print("is down!")
        time.sleep(3600)
        count+=1
        failed()

    t = int(time.time())
    current_time = datetime.now() 
    date = str(current_time.day-1) + "/" + str(current_time.month) + "/" + str(current_time.year) + " - " + "00:00:00" 
    #date = "05/08/2021 - 00:00:00"
    date = datetime.strptime(date, "%d/%m/%Y - %H:%M:%S")

    #start_timestamp corresponds to day before at midninght + hours of values read of new day
    start_timestamp = round(datetime.timestamp(date) + 3600*count2)

    #end_timestamp corresponds to the day before + any hours delayed in the connectivity
    end_timestamp = round(start_timestamp + 86400 + 3600*count)

    count2 = count


    valuesToInflux()


def failed():
    global response
    global count

    hostname = "google.com"
    response = os.system("ping -c 1 " + hostname)
    if response == 0:
        print("is up!")
    else:
        print("is down!")
        time.sleep(3600)
        count+=1
        failed()

def main():

    global apikey
    global API_SECRET
    global station_id
    global historic_data
    global x
    global URL
    global response

    apikey = "wlnqtipr3ssd4jzqjpecphtvajpiefuv"   
    API_SECRET = 'urkd214z8a6nxkvpqpgxmoieljrsuuao' 
    t = int(time.time()) 

    #calculate the api signature with HMAC SHA-256 algorithm with the concatenated string as the message and the API Secret as HMAC secret key

    message = "api-key%st%s" %(apikey, t)
    signature = hmac.new(bytes(API_SECRET , 'latin-1'), msg = bytes(message , 'latin-1'), digestmod = hashlib.sha256).hexdigest()

    URL = 'https://api.weatherlink.com/v2/stations?api-key=%s&t=%s&api-signature=%s' %(apikey, str(t), signature)

    x = 30
    try: response = urllib.request.urlopen(URL)
    
    except urllib.error.URLError as e:
        print("internet fail")
        time.sleep(x*2)
        failed(URL)


    data_json = json.loads(response.read())
    station_id = data_json["stations"][0]["station_id"]
    get_config()
    getHistoricData()

"""     schedule.every().day.at("00:00").do(getHistoricData)

    while True:
        schedule.run_pending()  """



def savetoInflux(dataw):

    global start_timestamp
    i = 0
    print("save")
    #print(dataw)

    influxdb_client = InfluxDBClient(host=INFLUXDB_ADDRESS, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE)
    influxdb_client.create_database(INFLUXDB_DATABASE)

    while i < (288 + 12*count):

        #IGUPINT

        temp_in = dataw["sensors"][2]["data"][i]["temp_in_last"]
        temp_in_hi = dataw["sensors"][2]["data"][i]["temp_in_hi"]
        temp_in_lo = dataw["sensors"][2]["data"][i]["temp_in_lo"]
        hum_in = dataw["sensors"][2]["data"][i]["hum_in_last"]
        hum_in_hi = dataw["sensors"][2]["data"][i]["hum_in_hi"]
        hum_in_lo = dataw["sensors"][2]["data"][i]["hum_in_lo"]
        dew_point_in = dataw["sensors"][2]["data"][i]["dew_point_in"]
        internal_heat_index = dataw["sensors"][2]["data"][i]["heat_index_in"]
        bar_absolute = dataw["sensors"][3]["data"][i]["bar_absolute"]
        bar_hi = dataw["sensors"][3]["data"][i]["bar_hi"]
        bar_lo = dataw["sensors"][3]["data"][i]["bar_lo"]
        bar_sea_level = dataw["sensors"][3]["data"][i]["bar_sea_level"]


        temp_In = round((5 / 9) * (temp_in - 32))
        temp_In_Hi = round((5 / 9) * (temp_in_hi - 32))
        temp_In_Lo = round((5 / 9) * (temp_in_lo - 32)) 
        dew_Point_In = round((5 / 9) * (dew_point_in - 32)) 
        Internal_heat_index = round((5 / 9) * (internal_heat_index - 32))


        #IGUPEXT

        temp_ex = dataw["sensors"][4]["data"][i]["temp_avg"]
        temp_lo = dataw["sensors"][4]["data"][i]["temp_lo"]
        temp_hi = dataw["sensors"][4]["data"][i]["temp_hi"]
        wp = dataw["sensors"][4]["data"][i]["wind_speed_avg"]
        wphi = dataw["sensors"][4]["data"][i]["wind_speed_hi"]
        wdir = dataw["sensors"][4]["data"][i]["wind_dir_of_prevail"]
        wdirhi = dataw["sensors"][4]["data"][i]["wind_speed_hi_dir"]
        wind_chill = dataw["sensors"][4]["data"][i]["wind_chill_last"]
        wind_chill_low = dataw["sensors"][4]["data"][i]["wind_chill_lo"]
        rain_size = dataw["sensors"][4]["data"][i]["rainfall_mm"]
        rain_rate_hi = dataw["sensors"][4]["data"][i]["rain_rate_hi_mm"]
        solar_radiation = dataw["sensors"][4]["data"][i]["solar_rad_avg"]
        solar_radiation_hi = dataw["sensors"][4]["data"][i]["solar_rad_hi"]
        uv_index = dataw["sensors"][4]["data"][i]["uv_index_avg"]
        uv_index_hi = dataw["sensors"][4]["data"][i]["uv_index_hi"]
        uv_dose = dataw["sensors"][4]["data"][i]["uv_dose"]
        heating_days = dataw["sensors"][4]["data"][i]["heating_degree_days"]
        cooling_days = dataw["sensors"][4]["data"][i]["cooling_degree_days"]


        temp_Ex = round((5 / 9) * (temp_ex - 32))
        temp_Lo = round((5 / 9) * (temp_lo - 32))
        temp_Hi = round((5 / 9) * (temp_hi - 32))
        wind_Chill = round((5 / 9) * (wind_chill - 32))
        wind_Chill_low = round((5 / 9) * (wind_chill_low - 32))

        global json_list

        if wind_chill != None:
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
                    'temp_In': int(temp_In),
                    'temp_In_Hi': int(temp_In_Hi),
                    'temp_In_Lo': int(temp_In_Lo),
                    'hum_in': int(hum_in),
                    'hum_in_hi': int(hum_in_hi),
                    'hum_in_lo': int(hum_in_lo),
                    'dew_Point_In': int(dew_Point_In),
                    'bar_absolute': int(bar_absolute),
                    'bar_hi': int(bar_hi),
                    'bar_lo': int(bar_lo),


                    #IGUPEXT
                    'temp_Ex': int(temp_Ex),
                    'temp_Lo': int(temp_Lo),
                    'temp_Hi': int(temp_Hi),
                    'wind_speed': int(wp),
                    'wind_speed_hi': int(wphi),
                    'wind_dir': int(wdir),
                    'wind_dir_hi': int(wdirhi),
                    'wind_chill': int(wind_Chill),
                    'wind_chill_low': int(wind_Chill_low),
                    'rain_size': int(rain_size),
                    'rain_rate_hi': int(rain_rate_hi),
                    'solar_radiation': int(solar_radiation),
                    'solar_radiation_hi': int(solar_radiation_hi),
                    'heating_days': int(heating_days),
                    'cooling_days': int(cooling_days)
                },
            } 
        i+=1
        start_timestamp+=300

        json_list.append(json_body)
        influxdb_client.write_points(json_list, time_precision=INFLUXDB_TIME_PRECISION, protocol='json')
    
    count = 0



if __name__ == "__main__":
    main() 


