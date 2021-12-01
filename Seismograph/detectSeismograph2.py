import os
import sys
import time
import string
import datetime
import json
import configparser 
from influxdb import InfluxDBClient
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

json_list = []
config = None
var = 26

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

    global flag
    flag = 0

    config_parser = configparser.ConfigParser()
    config_parser.read('SeismographConfiguration.ini')

    # [ADMIN]
    ADMIN_EMAIL = config_parser.get('ADMIN', 'Admin_email')


    # [InfluxDB]
    INFLUXDB_ADDRESS = config_parser.get('InfluxDB', 'InfluxDB_Adress')
    INFLUXDB_PORT = config_parser.get('InfluxDB', 'InfluxDB_Port')
    INFLUXDB_DATABASE = config_parser.get('InfluxDB', 'InfluxDB_Database')
    INFLUXDB_TIME_PRECISION = config_parser.get('InfluxDB', 'InfluxDB_Time_Precision')

class MonitorFolder(FileSystemEventHandler):
    FILE_SIZE=0

    def on_created(self, event):
        #print(event.src_path, event.event_type)
        global var
        print("hellofdd\n")
        if os.path.exists('eof'):
            print("exists")
            var = "" + str(var) + ""
            os.system("./drf2txt 04" + var + "21_000000 99999999")
            time.sleep(2)
            txtfile = "04" + str(var) + "21_000000.txt"
            var = int(var) + 1
            os.system("rm eof")
            valuesToDatabase(txtfile)
        #valuesToDatabase(filename[0])
        #self.checkFolderSize(event.src_path)

    #def on_modified(self, event):
        #print(event.src_path, event.event_type)
        #self.checkFolderSize(event.src_path)
    
    def on_deleted(self, event):
        print(event.src_path, event.event_type)


def valuesToDatabase(filename):
    #Save the values from the text file into the database
    global influxdb_client
    filepath = filename
    
    with open(filepath,'r') as f:
        lines = f.readlines()

    month = lines[0][12:14]
    day = lines[0][15:17]
    year = lines[0][18:20]

    hour = lines[0][21:23]
    min = lines[0][24:26]
    sec = lines[0][27:29]

    date_string = month + "/" + day + "/20" + year + " " + hour + ":" + min + ":" + sec
    date = datetime.datetime.strptime(date_string, "%m/%d/%Y %H:%M:%S")
    timestamp = datetime.datetime.timestamp(date)
    timestamp = timestamp * 1000

    with open(filepath,'r') as f:
        for line in lines[4:]:
            if line:
                if line != "\n":
                    print("here")
                    json_body = parse(line, timestamp)
                    timestamp = timestamp + 10
                    store_in_db(json_body)


def main():
    global i
    global influxdb_client
    global var
    i = 0

    # Get variables values from INI file
    get_config()

    # Instantiate a connection to the InfluxDB
    influxdb_client = InfluxDBClient(host=INFLUXDB_ADDRESS, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE)
    influxdb_client.create_database(INFLUXDB_DATABASE)

    src_path = "/home/up201504848/test"
    
    event_handler=MonitorFolder()
    observer = Observer()
    observer.schedule(event_handler, path=src_path, recursive=True)
    print("Monitoring started")
    observer.start()
    try:
        while(True):
           time.sleep(1)
           
    except KeyboardInterrupt:
            observer.stop()
            observer.join()



def parse(line, timestamp):
    """
    Parse the received message

    :param message: Received message
    """
    

    value = [0 for i in range(6)] 
    value = line.split(",")
    value[5] = value[5].rstrip("\n")
    #print(value)

    json_body = generate_json(value, timestamp)
    return json_body


def generate_json(value, ut):
    """
    Build JSON to store in DB

    :param value: X, Y and Z component for each type of seismograph (LP and SP)
    :param ut: Unix-Timestamp
    """
    global json_list
    json_body = {
            "measurement": "Seismograph_Data",
            "tags": {
                "Long-Period_Instrument": "XPTO_LP",
                "Short-Period_Instrument": "XPTO_SP",
                "Measurement": "u"
            },
            "time":int(ut),
            "fields": {
                "LP_X": int(value[0]),
                "LP_Y": int(value[1]),
                "LP_Z": int(value[2]),
                "SP_X": int(value[3]),
                "SP_Y": int(value[4]),
                "SP_Z": int(value[5])
            }
        }

    json_body = json.dumps(json_body)

    return json_body


def store_in_db(message):
    """
    Store in the database
    """
    global json_list
    global influxdb_client
    global i

    json_obj = json.loads(message)
    json_list.append(json_obj)


    # Save measurement in the database
    i+=1
    print(i)
    influxdb_client.write_points(json_list, time_precision=INFLUXDB_TIME_PRECISION, protocol='json')
    json_list = []


if __name__ == '__main__':
    main()
