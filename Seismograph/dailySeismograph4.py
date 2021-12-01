import os
import time
import schedule
import json
from datetime import datetime
from datetime import timedelta
import logging
import logging.handlers
import paho.mqtt.client as mqtt
from configparser import ConfigParser
import socket
import tqdm
from urllib.request import urlopen
import urllib3
import subprocess


json_list = []
config = None

broker_client = ''
flush_offline_data = 0
flag_connected = 0
logger = ''
logger_offline = ''
flag_first_connection = 0
txtfile = ""



def parse_offline_data():
    """
    Parse and publish data gathered while connection with the broker was down
    """
    global OFFLINE_LOG_FILE
    global MQTT_TOPIC

    file_path = OFFLINE_LOG_FILE
    with open(file_path) as fp:
        line = fp.readline()
        while line:
            broker_client.publish(MQTT_TOPIC, line)
            line = fp.readline()


def get_config():
    """
    Get config variables values
    """   
    # [MQTT]
    global BROKER_ADDRESS
    global BROKER_PORT
    global BROKER_USERNAME
    global BROKER_PASSWORD
    global MQTT_CLIENTID
    global MQTT_TOPIC
    global QOS
    
    # [LOG]
    global OFFLINE_LOG_FILE
    global LOCAL_DATA_STORAGE_FILE
    
    config_parser = ConfigParser()
    config_parser.read('SeismographConfiguration.ini')
    
    
    # [MQTT]
    BROKER_ADDRESS = config_parser.get('MQTT', 'Broker_Address')
    BROKER_PORT = config_parser.getint('MQTT', 'Broker_Port')
    BROKER_USERNAME = config_parser.get('MQTT', 'Broker_Username')
    BROKER_PASSWORD = config_parser.get('MQTT', 'Broker_Password')
    MQTT_CLIENTID = config_parser.get('MQTT', 'MQTT_ClientID')
    MQTT_TOPIC = config_parser.get('MQTT', 'MQTT_Topic')
    QOS = config_parser.getint('MQTT', 'QoS')
    
    # [LOG]
    OFFLINE_LOG_FILE = config_parser.get('LOG', 'Offline_Log_File')
    LOCAL_DATA_STORAGE_FILE = config_parser.get('LOG', 'Local_Data_Storage_File')




def failed():
    global sys
    global x
    global t

    time.sleep(t)
    z = os.system("scp -P 24204 " + sys + " eof up201504848@ssh.alunos.dcc.fc.up.pt:~/test")
    
    if z != 0:
        print("failed")
        failed()
        t = t*2


def job():
    #get daily files with test binary files
    global var
    global x
    global t
    global sys
    global txtfile
    t = 60

    var = "" + str(var) + ""
    print(var)
    sys = f"sys1.{x}.dat"
    print(sys)
    os.system("cp " + sys + " ../WinSDR_data/data")
    time.sleep(1)
    os.system("./drf2txt 04" + var + "21_000000 99999999")
    txtfile = "04" + var + "21_000000.txt"
    var = int(var) + 1
    x = x + 1

    z = os.system("scp -P 24204 " + sys + " eof up201504848@ssh.alunos.dcc.fc.up.pt:~/test")
    
    if z != 0:
        failed()



def job2():

    global logger_offline
    global logger
    global flush_offline_data
    global MQTT_TOPIC
    global QOS
    global flag_connected
    global flag_first_connection
    global txtfile

    #get daily files from Seismograph
    previous_date = datetime.today() - timedelta(days=1)


    year = str(previous_date.year)
    if previous_date.month > 0 and previous_date.month < 10: 
        month = "0" + str(previous_date.month)
    else:
        month = str(previous_date.month)

    if previous_date.day > 0 and previous_date.day < 10: 
        day = "0" + str(previous_date.day)
    else:
        day = str(previous_date.day)

    var = month + day + year[2:]
    print(var)
    #os.system("./drf2txt " + var + "_000000 99999999")
    txtfile = var + "_000000.txt"

    with open(txtfile,'r') as f:
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

    with open(txtfile,'r') as f:
        for line in lines[4:]:
            if line:
                if line != "\n":
                    json_body = parse(line, timestamp)
                    timestamp = timestamp + 10
                    if flag_connected == 1:
                        if flush_offline_data == 1:
                            print("here")
                            parse_offline_data()
                            flush_offline_data = 0
                        time.sleep(0.01)
                        result = broker_client.publish(MQTT_TOPIC, json_body, qos=QOS)
                        logger.info(json_body)
                    else:
                        # Local storage
                        logger_offline.info(json_body)
                        logger.info(json_body)



def main():
    global var
    global x

    i = 1
    filepath = "/mnt/c/WinSDR_data"
    date = "20210426"
    sys = f"sys1.{date}.dat"
    x = int(date)
    var = 26


    get_config()


    #job()
    schedule.every().day.at("00:00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)



def initialize_logger():
    """
    Initialize logger
    """
    global logger
    global LOCAL_DATA_STORAGE_FILE
    print(LOCAL_DATA_STORAGE_FILE)
    log_handler = logging.handlers.TimedRotatingFileHandler(LOCAL_DATA_STORAGE_FILE, when='midnight')

    log_formatter = logging.Formatter('%(message)s')
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger('MyLogger')
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)



if __name__ ==  "__main__":
    main()
