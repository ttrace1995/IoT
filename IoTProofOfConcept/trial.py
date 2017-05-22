'''
Created on May 17, 2017
@author: tyler
'''
import time
import json
import mysql.connector
import logging
logging.basicConfig(filename='f.log',level=logging.DEBUG)
from iothub_client import IoTHubClient, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubError
from iothub_client_sample import send_confirmation_callback
    
SEND_CALLBACKS = 0

class TVD(object):
    
    def __init__(self):
        self.tetryon_initialize_connect()
        self.initialize_json()
        self.start_cycle()
    
    def tetryon_initialize_connect(self):
        #Initializes the Tetryon database values from JSON file
        with open('settings/tetryon.json') as json_data_file:
            data = json.load(json_data_file)
            for field in data['tetryon_config']:
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
                
        with open('settings/ATSPOC') as json_data_file:
            data = json.load(json_data_file)
            for field in data['iot_config']:
                self.HOST_NAME = field['host_name']
                
        self.PROTOCOL = IoTHubTransportProvider.MQTT
        self.PROTOCOL_HTTP = IoTHubTransportProvider.HTTP
        self.TIMEOUT = 241000
        self.MINIMUM_POLLING_TIME = 9
        self.MESSAGE_TIMEOUT = 10000
        #self.SEND_CALLBACKS = 0
        self.INTERVAL = 5
            

    def initialize_json(self):
    
        with open('settings/tetryon_devices') as json_data_file:
            devices = json.load(json_data_file)
            #for field in data['registered_devices']:
            for i in xrange(len(devices['registered_devices'])):
                #tetryon_sid = devices['registered_devices'][i]['device_id'][6:]
                #print ( tetryon_sid )
            
                try:
                    db = mysql.connector.connect(user=self.DB_USER,
                             password=self.DB_PASS,
                             host=self.DB_HOST,
                             database=self.DB_NAME)
            
                    cursor = db.cursor()
                    
                    sql_time_start = "SELECT MAX("+self.DB_DATA_TIME+") FROM "+self.DB_DATA_TABLE+" WHERE "+self.DB_DATA_LOCATIONNO+"="+devices['registered_devices'][i]['loc_num']

                    cursor.execute(sql_time_start)
                    time = cursor.fetchone()
                    #print (time)
                    
                    devices['registered_devices'][i]['timestamp'] = time[0].__str__()
                    
                    with open('settings/tetryon_devices', 'w') as json_data_file:
                        json_data_file.write(json.dumps(devices))
                    
                    
                
                except KeyboardInterrupt:
                    print ( "IoTHubClient sample stopped" )
                    
        
    def start_cycle(self):
        
        logging.info('started listening...')
        
        while True:
            
            time.sleep(20)
            
            with open('settings/tetryon_devices') as json_data_file:
                devices = json.load(json_data_file)
                #for field in data['registered_devices']:
                for i in xrange(len(devices['registered_devices'])):
                    iot_id = devices['registered_devices'][i]['device_id']
                    #tetryon_sid = iot_id[6:]
                    #print ( tetryon_sid )
            
                    try:
                        db = mysql.connector.connect(user=self.DB_USER,
                            password=self.DB_PASS,
                            host=self.DB_HOST,
                            database=self.DB_NAME)
            
                        cursor = db.cursor()
                        
                        
                        sql_tet_time = "SELECT MAX("+self.DB_DATA_TIME+") FROM "+self.DB_DATA_TABLE+" WHERE "+self.DB_DATA_LOCATIONNO+"="+devices['registered_devices'][i]['loc_num']
                        cursor.execute(sql_tet_time)
                        t = cursor.fetchone()
                        
                        tetryon_time = t[0].__str__()
                        #print ( tetryon_time )
                        json_time = devices['registered_devices'][i]['timestamp']
                        #print ( json_time )
                        
                        
                        if tetryon_time == 'None':
                            #time.sleep(10)
                            pass
                        
                        elif int(tetryon_time) > int(json_time):
        
                            pull_data = "SELECT "+self.DB_DATA_SPEED+" FROM "+self.DB_DATA_TABLE+" WHERE "+self.DB_DATA_LOCATIONNO+"="+devices['registered_devices'][i]['loc_num']+" AND _Time>"+json_time+" AND _Time<="+tetryon_time+" ORDER BY "+self.DB_DATA_TIME+" ASC" 
                            cursor.execute(pull_data)
                            first_data = cursor.fetchall()
                            raw_data = list(map(sum, first_data))
                            #print ( raw_data )
                            
                            total_cars = len(raw_data)
                            max_speed = max(raw_data)
                            min_speed = min(raw_data)
                            mean_speed = (sum(raw_data) / total_cars)
                            
                            data = { "DeviceId" : iot_id, "RawData" : raw_data.__str__(), "TotalCars" : total_cars.__str__(), "AverageSpeed" : mean_speed.__str__(), "MaxSpeed" : max_speed.__str__(), "MinSpeed" : min_speed.__str__() }
                            
                            connection_string = ("HostName="+self.HOST_NAME+".azure-devices.net;DeviceId="+iot_id+";SharedAccessKey="+devices['registered_devices'][i]['device_primary_key']).__str__()
                            #print ( connection_string )
                            
                            self.send_to_iot(data, devices['registered_devices'][i]['device_id'], connection_string)
            
                            devices['registered_devices'][i]['timestamp'] = tetryon_time
                            with open('settings/tetryon_devices', 'w') as json_data_file:
                                json_data_file.write(json.dumps(devices))
                                
                            #time.sleep(10)
                                
                        else:
                            logging.info('no updates')
                            
                            #time.sleep(10)
                        
                            
            
                    except KeyboardInterrupt:
                        print ( "IoTHubClient sample stopped" )
                        
                        
    def send_to_iot(self, data, did, connection):
        
        try:
            client = self.iothub_client_init(connection)
            message_counter = 0
            
            msg_txt_formatted = json.dumps(data)
            # messages can be encoded as string or bytearray
            #print ( msg_txt_formatted )
            if (message_counter & 1) == 1:
                message = IoTHubMessage(bytearray(msg_txt_formatted, 'utf8'))
                client.send_event_async(message, send_confirmation_callback, message_counter)
                time.sleep(10)
                logging.info('sent new message to iot')
            else:
                message = IoTHubMessage(msg_txt_formatted)
                message.message_id = "message_%d" % message_counter
                message.correlation_id = "correlation_%d" % message_counter
                client.send_event_async(message, send_confirmation_callback, message_counter)
                time.sleep(10)
                logging.info('sent new message to iot')
        
        except IoTHubError as iothub_error:
            print ( "Unexpected error %s from IoTHub" % iothub_error )
            return
        except KeyboardInterrupt:
            print ( "IoTHubClient sample stopped" )
        
                        
    def iothub_client_init(self, connection):
        # prepare iothub client
        client = IoTHubClient(connection, self.PROTOCOL_HTTP)

        # HTTP specific settings
        if client.protocol == IoTHubTransportProvider.HTTP:
            client.set_option("timeout", self.TIMEOUT)
            client.set_option("MinimumPollingTime", self.MINIMUM_POLLING_TIME)

            # set the time until a message times out
            client.set_option("messageTimeout", self.MESSAGE_TIMEOUT)

        # to enable MQTT logging set to 1
        #if client.protocol == IoTHubTransportProvider.MQTT:
            #client.set_option("logtrace", 1)

        #client.set_message_callback(
            #receive_message_callback, RECEIVE_CONTEXT)
        
        return client 
    
    #Confirmation that the data was received by the IoT  
    def send_confirmation_callback(self, message, result, user_context):
        global SEND_CALLBACKS
        #print ( "Confirmation[%d] received for message with result = %s" % (user_context, result) )
        #map_properties = message.properties()
        #print ( "    message_id: %s" % message.message_id )
        #print ( "    correlation_id: %s" % message.correlation_id )
        #key_value_pair = map_properties.get_internals()
        #print ( "    Properties: %s" % key_value_pair )
        self.SEND_CALLBACKS += 1
        #print ( "    Total calls confirmed: %d" % self.SEND_CALLBACKS )     
            
                
            
            
if __name__ == '__main__':
    ok = TVD()