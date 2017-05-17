'''
Created on May 3, 2017
@author: tyler_tracey
'''

import time
import json

import mysql.connector
from iothub_client import IoTHubClient, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubError
from iothub_client_sample import send_confirmation_callback

class VirtualDeviceObject(object):
    
    DEVICE_ID = "[DEVICE_ID]"
    IOT_CONFIG = "[IOT_CONFIG]"
    DATABASE_CONFIG = "[DATABASE_CONFIG]"
    
    CONNECTION_STRING = "[CONNECTION_STRING]"
    
    DB_USER = "[DB_USER]"
    DB_PASS = "[DB_PASS]"
    DB_HOST = "[DB_HOST]"
    DB_NAME = "[DB_NAME]"
    
    DB_TABLE = ""
    DB_COLUMN = ""
    
    DB_DATA_TABLE = ""
    DB_DATA_TIME = ""
    DB_DATA_DIRECION = ""
    DB_DATA_LANE = ""
    DB_DATA_LENGTH = ""
    DB_DATA_SPEED = ""
    DB_DATA_INTERVALNO = ""
    DB_DATA_LOCATIONNO = ""
      
    LAST_SYNC = "[LAST_SYNC]"
    LANE_COUNT = 0
    DIRECTION = 0
    LOCATION_NUMBER = 0
    LAST_TIMESTAMP = 0
    HOLD_TIMESTAMP = 0
    
    FOUND = False
    
    TIMEOUT = 241000
    MINIMUM_POLLING_TIME = 9
    MESSAGE_TIMEOUT = 10000
    SEND_CALLBACKS = 0
    INTERVAL = 5

    DATA = []
    TOTAL_TARGETS = "Waiting for sync..."
    MAX_SPEED = "Waiting for sync..."
    MIN_SPEED = "Waiting for sync..."
    MEAN_SPEED = "Waiting for sync..."
    
    
    PROTOCOL = IoTHubTransportProvider.MQTT
    PROTOCOL_HTTP = IoTHubTransportProvider.HTTP
    
    def __init__(self, DID):
       
        global DEVICE_ID
        global IOT_CONFIG
        global DATABASE_CONFIG
        global CONNECTION_STRING
        global DB_HOST
        global DB_NAME
        global DB_USER
        global DB_PASS
        global DB_TABLE
        global DB_COLUMN
        global DB_DATA_TABLE
        global DB_DATA_TIME
        global DB_DATA_DIRECION
        global DB_DATA_LANE
        global DB_DATA_LENGTH
        global DB_DATA_SPEED
        global DB_DATA_INTERVALNO
        global DB_DATA_LOCATIONNO
        global FOUND

        self.DEVICE_ID = DID
        self.DB_DEVICE_ID = DID[6:]
        
        with open('settings/devices.json') as json_data_file:
            data = json.load(json_data_file)
            for field in data['registered_devices']:
                if 'device_id' in field and field['device_id'] == self.DEVICE_ID:
                    self.FOUND = True
                    self.IOT_CONFIG = field['iot_config']
                    self.DATABASE_CONFIG = field['database_config']
                    self.PRIMARY_KEY = "fnJFDPbSXxAMOMGkl984YbN2nFmhGPK2p5h49CrNs9I="
                    
            if self.FOUND == False:
                print ("NO REGISTERED DEVICE WITH THAT ID")
                return
                    
                    
                    
        with open('settings/database.json') as json_data_file:
            data = json.load(json_data_file)
            for field in data['tetryon_config']:
                if 'db_tag' in field and field['db_tag'] == self.DATABASE_CONFIG:
                    self.DB_HOST = field['db_host']
                    self.DB_NAME = field['db_name']
                    self.DB_USER = field['db_user']
                    self.DB_PASS = field['db_pass']
                    self.DB_TABLE = field['db_table']
                    self.DB_COLUMN = field['db_column']
                    self.DB_DATA_TABLE = field['db_data_table']
                    self.DB_DATA_TIME = field['db_data_time']
                    self.DB_DATA_DIRECTION = field['db_data_direction']
                    self.DB_DATA_LANE = field['db_data_lane']
                    self.DB_DATA_LENGTH = field['db_data_lenth']
                    self.DB_DATA_SPEED = field['db_data_speed']
                    self.DB_DATA_INTERVALNO = field['db_data_intervalNum']
                    self.DB_DATA_LOCATIONNO = field['db_data_locationNum']
                    
        self.CONNECTION_STRING = ("HostName=ATSPOC.azure-devices.net;DeviceId="+self.DEVICE_ID+";SharedAccessKey="+self.PRIMARY_KEY).__str__()
        
        self.initialize_values()
        
    #Confirmation that the data was received by the IoT  
    def send_confirmation_callback(self, message, result, user_context):
        global SEND_CALLBACKS
        print ( "Confirmation[%d] received for message with result = %s" % (user_context, result) )
        map_properties = message.properties()
        print ( "    message_id: %s" % message.message_id )
        print ( "    correlation_id: %s" % message.correlation_id )
        key_value_pair = map_properties.get_internals()
        print ( "    Properties: %s" % key_value_pair )
        self.SEND_CALLBACKS += 1
        print ( "    Total calls confirmed: %d" % self.SEND_CALLBACKS )
        self.LAST_SYNC = time.strftime("%d/%m/%Y")+" -- "+time.strftime("%H:%M:%S")
        print ( "    Last_IOT_Callback: " + self.LAST_SYNC )
       
    def iothub_client_init(self):
        # prepare iothub client
        client = IoTHubClient(self.CONNECTION_STRING, self.PROTOCOL_HTTP)

        # HTTP specific settings
        if client.protocol == IoTHubTransportProvider.HTTP:
            client.set_option("timeout", self.TIMEOUT)
            client.set_option("MinimumPollingTime", self.MINIMUM_POLLING_TIME)

            # set the time until a message times out
            client.set_option("messageTimeout", self.MESSAGE_TIMEOUT)

        # to enable MQTT logging set to 1
        #if client.protocol == IoTHubTransportProvider.MQTT:
            #client.set_option("logtrace", 0)

        #client.set_message_callback(
            #receive_message_callback, RECEIVE_CONTEXT)
        
        return client
    
    def initialize_values(self):
        
        try:
            db = mysql.connector.connect(user=self.DB_USER,
                             password=self.DB_PASS,
                             host=self.DB_HOST,
                             database=self.DB_NAME)
            
            cursor = db.cursor()
            
            sql_locno = "SELECT "+self.DB_DATA_LOCATIONNO+" FROM "+self.DB_TABLE+" WHERE "+self.DB_COLUMN+"='"+self.DB_DEVICE_ID.__str__()+"'"
            cursor.execute(sql_locno)
            loc = cursor.fetchone()
            self.LOCATION_NUMBER = loc[0]
            
            
            sql_time_start = "SELECT MAX("+self.DB_DATA_TIME+") FROM "+self.DB_DATA_TABLE+" WHERE "+self.DB_DATA_LOCATIONNO+"="+self.LOCATION_NUMBER.__str__()

            cursor.execute(sql_time_start)
            time = cursor.fetchone()
            if time[0].__str__() == 'None':
                self.LAST_TIMESTAMP = 0
                self.HOLD_TIMESTAMP = 0
            else:
                self.LAST_TIMESTAMP = time[0].__str__()
                self.HOLD_TIMESTAMP = time[0].__str__()
                
            self.update_json()
            self.iothub_try()
            
        except IoTHubError as iothub_error:
            print ( "Unexpected error %s from IoTHub" % iothub_error )
            return
        except KeyboardInterrupt:
            print ( "IoTHubClient sample stopped" )
            
    def update_json(self):
        
        with open('settings/devices.json') as json_data_file:
            data = json.load(json_data_file)
            for i in xrange(len(data['registered_devices'])):
                if data['registered_devices'][i]['device_id'] == self.DEVICE_ID:
                    data['registered_devices'][i]['device_status'] = 'ACTIVE'
                    
        with open('settings/devices.json', "w" ) as json_data:
            json.dump(data, json_data)
        
        
    
    #Connects to database, pulls information, updates virtual device data
    def check_for_updates(self):
        try:
            db = mysql.connector.connect(user=self.DB_USER,
                             password=self.DB_PASS,
                             host=self.DB_HOST,
                             database=self.DB_NAME)
            
            cursor = db.cursor()
            
            time_check = "SELECT MAX("+self.DB_DATA_TIME+") FROM "+self.DB_DATA_TABLE+" WHERE "+self.DB_DATA_LOCATIONNO+"='"+self.LOCATION_NUMBER.__str__()+"'"
            cursor.execute(time_check)
            time_cb = cursor.fetchone()
            if time_cb[0].__str__() == 'None':
                self.HOLD_TIMESTAMP = 0
            else:
                self.HOLD_TIMESTAMP = time_cb[0].__str__()
            
            print( "Queried Timestamp:\t"+self.HOLD_TIMESTAMP.__str__() )
            print( "Last Updated Timestamp:\t"+self.LAST_TIMESTAMP.__str__() )
            
            if self.HOLD_TIMESTAMP > self.LAST_TIMESTAMP:
                pull_data = "SELECT "+self.DB_DATA_SPEED+" FROM "+self.DB_DATA_TABLE+" WHERE "+self.DB_DATA_LOCATIONNO+"="+self.LOCATION_NUMBER.__str__()+" AND _Time>"+self.LAST_TIMESTAMP.__str__()+" AND _Time<="+self.HOLD_TIMESTAMP.__str__()+" ORDER BY "+self.DB_DATA_SPEED+" ASC" 
                cursor.execute(pull_data)
                data = cursor.fetchall()
                
                self.DATA = list(map(sum, data))
                print ( self.DATA )
            
                self.LAST_TIMESTAMP = self.HOLD_TIMESTAMP
                self.HOLD_TIMESTAMP = 0
                
                self.calculate()
                
                return True
            
            
        except IoTHubError as iothub_error:
            print ( "Unexpected error %s from IoTHub" % iothub_error )
            return
        except KeyboardInterrupt:
            print ( "IoTHubClient sample stopped" )
                
    def calculate(self):
            
        self.TOTAL_TARGETS = len(self.DATA)
        self.MAX_SPEED = max(self.DATA)
        self.MIN_SPEED = min(self.DATA)
        self.MEAN_SPEED = (sum(self.DATA) / self.TOTAL_TARGETS)
            
              
    def iothub_try(self):
        try:
            client = self.iothub_client_init()
            message_counter = 0
            while True:
                is_update = self.check_for_updates()
                if is_update == True:
                    print ( "TTRRUUEE" )
                    
                    j = { "Total Cars: " : self.TOTAL_TARGETS.__str__(), "Average Speed: " : self.MEAN_SPEED.__str__(), "Max Speed: " : self.MAX_SPEED.__str__(), "Min Speed: " : self.MIN_SPEED.__str__(), "Raw Data: " : self.DATA.__str__() } 
                    msg_txt_formatted = json.dumps(j)
                    
                    # messages can be encoded as string or bytearray
                    print ( msg_txt_formatted )
                    if (message_counter & 1) == 1:
                        message = IoTHubMessage(bytearray(msg_txt_formatted, 'utf8'))
                        client.send_event_async(message, send_confirmation_callback, message_counter)
                    else:
                        message = IoTHubMessage(msg_txt_formatted)
                        message.message_id = "message_%d" % message_counter
                        message.correlation_id = "correlation_%d" % message_counter
                        client.send_event_async(message, send_confirmation_callback, message_counter)
                        is_update = False
                else:      
                    status = client.get_send_status()
                    print ( "Send status: %s" % status )
                    time.sleep(10)
                    message_counter += 1
                        
        except IoTHubError as iothub_error:
            print ( "Unexpected error %s from IoTHub" % iothub_error )
            return
        except KeyboardInterrupt:
            print ( "IoTHubClient sample stopped" )
        
#[REGISTERED DEVICE ID]
if __name__ == '__main__':
    device = VirtualDeviceObject("::VD::0000179df19f")
     
     


    