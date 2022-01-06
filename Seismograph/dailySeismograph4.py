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

txtfile = ""




def get_config():
    """
    Get config variables values
    """   
    #Load variables in SeismographConfiguration.ini
    # [LOG]
    global OFFLINE_LOG_FILE
    global LOCAL_DATA_STORAGE_FILE
    
    config_parser = ConfigParser()
    config_parser.read('SeismographConfiguration.ini')
    



def failed():
    global sys
    global x
    global t

    #If the scp command fails to send the binary file to the server, try again until it succeeds
    time.sleep(t)
    z = os.system("scp -P 24204 " + sys + " eof up201504848@ssh.alunos.dcc.fc.up.pt:~/test")
    
    if z != 0:
        print("failed")
        failed()
        t = t*2

""" 
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
        failed() """



def job2():

    global logger_offline
    global logger
    global flush_offline_data
    global flag_connected
    global flag_first_connection
    global txtfile
    global sys
    global t

    t = 60

    #get daily files from Seismograph
    #build the command for the binary file of the previous day into sys variable
    previous_date = datetime.today() - timedelta(days=1)
    filepath = "/mnt/c/WinSDR_data"

    year = str(previous_date.year)
    if previous_date.month > 0 and previous_date.month < 10:
        month = "0" + str(previous_date.month)
    else:
        month = str(previous_date.month)

    if previous_date.day > 0 and previous_date.day < 10:
        day = "0" + str(previous_date.day)
    else:
        day = str(previous_date.day)

    sys = year + month + day
    sys = filepath + "sys1." + sys + ".dat"

    #send the binary file to the server up201504848@ssh.alunos.fc.up.pt
    z = os.system("scp -P 24204 " + sys + " eof up201504848@ssh.alunos.dcc.fc.up.pt:~/test")

    if z != 0:
        failed()



def main():

    get_config()

    #execute function every day at midnight to get the values
    schedule.every().day.at("00:00").do(job2)

    while True:
        schedule.run_pending()
        time.sleep(1)



if __name__ ==  "__main__":
    main()
