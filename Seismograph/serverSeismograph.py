import datetime
import time
import json
from influxdb import InfluxDBClient
import paho.mqtt.client as mqtt
import configparser


json_list = []
config = None

def get_config():
    """
    Get config variables values
    """
    #
    # GLOBAL VARIABLES
    #
    # [ADMIN]
    global ADMIN_EMAIL

    # [MQTT]
    global BROKER_ADDRESS
    global BROKER_PORT
    global BROKER_USERNAME
    global BROKER_PASSWORD
    global MQTT_CLIENTID
    global MQTT_TOPIC
    global QOS

    # [InfluxDB]
    global INFLUXDB_ADDRESS
    global INFLUXDB_PORT
    global INFLUXDB_DATABASE
    global INFLUXDB_TIME_PRECISION

    global flag
    flag = 0
    # Instantiate config
    config_parser = configparser.ConfigParser()
    config_parser.read('SeismographConfiguration.ini')

    # [ADMIN]
    ADMIN_EMAIL = config_parser.get('ADMIN', 'Admin_email')

    # [MQTT]
    BROKER_ADDRESS = config_parser.get('MQTT', 'Broker_Address')
    BROKER_PORT = config_parser.getint('MQTT', 'Broker_Port')
    BROKER_USERNAME = config_parser.get('MQTT', 'Broker_Username')
    BROKER_PASSWORD = config_parser.get('MQTT', 'Broker_Password')
    MQTT_CLIENTID = config_parser.get('MQTT', 'MQTT_ClientID')
    MQTT_TOPIC = config_parser.get('MQTT', 'MQTT_Topic')
    QOS = config_parser.getint('MQTT', 'QoS')

    # [InfluxDB]
    INFLUXDB_ADDRESS = config_parser.get('InfluxDB', 'InfluxDB_Adress')
    INFLUXDB_PORT = config_parser.get('InfluxDB', 'InfluxDB_Port')
    INFLUXDB_DATABASE = config_parser.get('InfluxDB', 'InfluxDB_Database')
    INFLUXDB_TIME_PRECISION = config_parser.get('InfluxDB', 'InfluxDB_Time_Precision')


def on_connect(client, userdata, flags, rc):
    """
    Callback function for connection with the broker

    :param Client client: The client instance for this callback
    :param dict userdata: The private user data as set in Client() or user_data_set()
    :param dict flags: Response flags sent by the broker
    :param int rc: The connection result
    :return:
    """
    print('Connected With The Broker.')
    client.subscribe(MQTT_TOPIC, qos=QOS)


def on_message_from_seismograph(client, userdata, message):
    """
    Callback function for magnetometer message

    :param Client client: The client instance for this callback
    :param dict userdata: The private user data as set in Client() or user_data_set()
    :param MQTTMessage message: Message
    """
    global flag
    flag = flag + 1
    print(flag)
    print('Message Receieved from Seismograph: ' + message.payload.decode())
    store_in_db(message.payload.decode())


def on_message(client, userdata, message):
    """
    Callback function for other messages message

    :param Client client: The client instance for this callback
    :param dict userdata: The private user data as set in Client() or user_data_set()
    :param MQTTMessage message: Message
    """
    print('Message Recieved from Others: ' + message.payload.decode())


def on_log(client, userdata, level, buf):
    """
    Callback function for logs

    :param Client client: The client instance for this callback
    :param dict userdata: The private user data as set in Client() or user_data_set()
    :param dict level: The severity of the message
    :param str buf: The message itself
    """
    print('log: ' + buf)


def main():
    """
    Main function
    """
    # Get variables values from INI file
    get_config()

    # Connect with the broker
    initialize_broker_connection()


def initialize_broker_connection():
    """
    Connect with the broker
    """
    global client
    client = mqtt.Client(client_id=MQTT_CLIENTID, clean_session=False)  # Create new MQTT client instance
    client.username_pw_set(username=BROKER_USERNAME, password=BROKER_PASSWORD)  # Set credentials
    client.on_connect = on_connect   # Define connect callback function
    client.on_message = on_message   # Define message callback function
    try:
        client.connect(BROKER_ADDRESS, BROKER_PORT)  # Connect to the broker
    except:
        print('ERROR: Error while connecting with the broker.')
        raise Exception('BrokerConnectionError')
    
    client.subscribe(MQTT_TOPIC, qos=QOS)  # Subscribe to topic
    client.message_callback_add(MQTT_TOPIC, on_message_from_seismograph)  # Define magnetometer message callback function
    client.loop_forever()


def store_in_db(message):
    """
    Store in the database
    """
    global json_list

    json_obj = json.loads(message)
    json_list.append(json_obj)

    # Instantiate a connection to the InfluxDB
    influxdb_client = InfluxDBClient(host=INFLUXDB_ADDRESS, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE)
    influxdb_client.create_database(INFLUXDB_DATABASE)

    # Save measurement in the database
    #print('Write points: {0}'.format(json_list))
    influxdb_client.write_points(json_list, time_precision=INFLUXDB_TIME_PRECISION, protocol='json')
    json_list = []

    #results = influxdb_client.query('select LP_X from Seismograph_Data;')
    #print ("Result: {0}".format(results))

if __name__ == '__main__':
    main()

