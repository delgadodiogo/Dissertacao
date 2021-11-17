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


json_list = []
config = None

broker_client = ''
flush_offline_data = 0
flag_connected = 0
logger = ''
logger_offline = ''
flag_first_connection = 0
txtfile = ""


def on_connect(client, userdata, flags, rc):
    """
    Callback function for connection with the broker

    :param Client client: The client instance for this callback
    :param dict userdata: The private user data as set in Client() or user_data_set()
    :param dict flags: Response flags sent by the broker
    :param int rc: The connection result
    :return:
    """
    global flag_connected
    global flag_first_connection
    print('Client connected with the Broker.')
    #flag_connected = 1

    """ 
    if flag_first_connection == 1:
        flag_first_connection = 0
        print('yoooooooooooooooooooooooooooo Mr White biatch')
    else:
        print('yoooooooooooooooooooooooooooo biatch') """

    if rc == 0:
        flag_connected = 1
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", rc)

    #ParseOfflineData()


def on_disconnect(client, userdata, rc):
    """
    Callback function for disconnection with the broker

    :param Client client: The client instance for this callback
    :param dict userdata: The private user data as set in Client() or user_data_set()
    :param int rc: The connection result
    :return:
    """
    global flag_connected
    global flush_offline_data
    flush_offline_data = 1
    flag_connected = 0
    set_local_store()
    print('Client disconnected with the Broker.')


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




def waitforInternet():
    x = 60
    hostname = "google.com"
    response = os.system("ping -c 1 " + hostname)
    if response == 0:
        print("is up!")
        sendfile()
    else:
        print("is down!")
        time.sleep(x)
        waitforInternet()


def sendfile():

    global txtfile
    global filesize
    global BUFFER_SIZE
    global s
    global host
    global port
    global SEPARATOR

    # create the client socket
    s = socket.socket()

    print(f"[+] Connecting to {host}:{port}")
    try:
        s.connect((host, port))
    except OSError:
        waitforInternet()
    print("[+] Connected.")

    # send the filename and filesize
    s.send(f"{txtfile}{SEPARATOR}{filesize}".encode())

    # start sending the file
    progress = tqdm.tqdm(range(filesize), f"Sending {txtfile}", unit="B", unit_scale=True, unit_divisor=1024)
    with open(txtfile, "rb") as f:
        while True:
            try:
                # read the bytes from the file
                bytes_read = f.read(BUFFER_SIZE)
                if not bytes_read:
                    # file transmitting is done
                    break
                # we use sendall to assure transimission in 
                # busy networks
                s.sendall(bytes_read)
                # update the progress bar
                progress.update(len(bytes_read))

            #In the case there is a failure in connectivity
            except ConnectionResetError:
                print("ConnectionError\n")
                #s.close()
                time.sleep(60)
                s.sendall(bytes_read)
                waitforInternet()

            except BrokenPipeError:
                print("BrokenPipeError\n")
                s.close()
                time.sleep(60)
                sendfile()



def job():
    #get daily files with test binary files
    global var
    global x
    global txtfile
    global filesize
    global BUFFER_SIZE
    global s
    global host
    global port
    global SEPARATOR


    var = "" + str(var) + ""
    print(var)
    sys = f"sys1.{x}.dat"
    print(sys)
    os.system("cp " + sys + " ../WinSDR_data/data")
    time.sleep(1)
    os.system("./drf2txt 04" + var + "21_000000 99999999")
    x += 1
    txtfile = "04" + var + "21_000000.txt"
    var = int(var) + 1

    #send filetxt to server
    SEPARATOR = "<SEPARATOR>"
    BUFFER_SIZE = 4096 # send 4096 bytes each time step

    # the ip address or hostname of the server, the receiver
    host = "192.168.1.204"
    # the port, let's use 5001
    port = 5001
    # get the file size
    filesize = os.path.getsize(txtfile)


    

    sendfile()
    

    # close the socket

    s.close()



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
    date = "20210425"
    sys = f"sys1.{date}.dat"
    x = int(date)
    var = 25


    get_config()

    job()
    # schedule.every().day.at("20:25").do(job)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)



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


def initialize_broker_connection():
    """
    Connect with the broker
    """
    global broker_client
    global BROKER_ADDRESS
    global BROKER_PORT
    global MQTT_CLIENTID
    global BROKER_USERNAME
    global BROKER_PASSWORD
        
    broker_client = mqtt.Client(MQTT_CLIENTID, clean_session=False)
    broker_client.username_pw_set(username=BROKER_USERNAME, password=BROKER_PASSWORD)

    broker_client.on_connect = on_connect
    broker_client.on_disconnect = on_disconnect
    broker_client.connect(BROKER_ADDRESS, BROKER_PORT)
    broker_client.loop_start()


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


if __name__ ==  "__main__":
    main()
