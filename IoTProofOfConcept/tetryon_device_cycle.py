'''
Created on May 17, 2017
@author: tyler_tracey
@version: 2.0
'''
import time
import json
import mysql.connector
from iothub_client import IoTHubClient, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubError
from iothub_client_sample import send_confirmation_callback
from iothub_service_client import IoTHubRegistryManager, IoTHubRegistryManagerAuthMethod

from iothub_registrymanager_sample import print_device_info

    

class TVD(object):
    
    def __init__(self):
        
        self.tetryon_initialize_connect()
        #self.check_registration()
        self.initialize_json()
        #self.start_cycle()
    
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
                
        #Initializes the IOT hub settings       
        with open('settings/ATSPOC') as json_data_file:
            data = json.load(json_data_file)
            for field in data['iot_config']:
                self.HOST_NAME = field['host_name']
                self.CONNECTION_STRING = field['pk_connection_string']
        
        #Defines the protocols and other values necessary to transfer data to the iothub      
        self.PROTOCOL = IoTHubTransportProvider.MQTT
        self.PROTOCOL_HTTP = IoTHubTransportProvider.HTTP
        self.TIMEOUT = 241000
        self.MINIMUM_POLLING_TIME = 9
        self.MESSAGE_TIMEOUT = 10000
        self.SEND_CALLBACKS = 0
        self.INTERVAL = 5
        
    def check_registration(self):
        
        registered_ids = []
        
        with open('settings/tetryon_devices') as json_data_file:
            data = json.load(json_data_file)
            for field in data['registered_devices']:
                registered_id = field['device_id'][6:]
                registered_ids.append(registered_id)
                 
        try:
            tetryon_ids = []
            formatted_tetryon_ids = []
            
            db = mysql.connector.connect(user=self.DB_USER,
                    password=self.DB_PASS,
                    host=self.DB_HOST,
                    database=self.DB_NAME)
            
            cursor = db.cursor()
            sql = "SELECT "+self.DB_COLUMN+" FROM "+self.DB_TABLE
            cursor.execute(sql)
            tetryon_ids = cursor.fetchall()
            
            for i in xrange(len(tetryon_ids)):
                tetryon_ids[i] = ''.join(tetryon_ids[i])
            
            diff = list(set(formatted_tetryon_ids) - set(registered_ids))
            
            if diff == []:
                print ("No new devices found")
            else:
                print ("hello")
                self.register_new(diff)
            
        except KeyboardInterrupt:
            print ( "IoTHubClient sample stopped" )
            
    def register_new(self, devices):
        
        for i in xrange(len(devices)): 
            
            iothub_registry_manager = IoTHubRegistryManager(self.CONNECTION_STRING)
    
            try:
                ID = "::VD::"+devices[i].__str__()
                iothub_registry_manager = IoTHubRegistryManager(self.CONNECTION_STRING)
                auth_method = IoTHubRegistryManagerAuthMethod.SHARED_PRIVATE_KEY
                new_device = iothub_registry_manager.create_device(ID, "", "", auth_method)
        
                self.write_json_device(ID, new_device)
        
            except IoTHubError as iothub_error:
                print ( "Unexpected error {0}".format(iothub_error) )
                return
            except KeyboardInterrupt:
                print ( "iothub_createdevice stopped" )
                
    def write_json_device(self, d_id, new_device):
    
        with open('settings/tetryon_devices') as json_data:
            data = json.load(json_data)
        
        data['registered_devices'].append({
            'device_id': new_device.deviceId.__str__(),
            'device_primary_key': new_device.primaryKey.__str__(),
            'device_secondary_key': new_device.secondaryKey.__str__(),
            'device_connection_state': new_device.connectionState.__str__(),
            'device_iot_status': new_device.status.__str__(),
            'device_last_activity_time': new_device.lastActivityTime.__str__(),
            'device_ctdmc': new_device.cloudToDeviceMessageCount.__str__(),
            'device_isManaged': new_device.isManaged.__str__(),
            'device_authMethod': new_device.authMethod.__str__(),
            'database_config': 'tetryon',
            'iot_config': 'iothub',
            'device_status': 'PENDING APPROVAL',
            'timestamp' : '0',
            'loc_num' : '0'
            })
    
        with open('settings/tetryon_devices', "w" ) as json_data:
            json.dump(data, json_data) 
        
            print_device_info("Created Device", new_device)
            self.initialize_json()
        
        
    def print_device_info(self, title, iothub_device):
        print ( title + ":" )
        print ( "iothubDevice.deviceId                    = {0}".format(iothub_device.deviceId) )
        print ( "iothubDevice.primaryKey                  = {0}".format(iothub_device.primaryKey) )
        print ( "iothubDevice.secondaryKey                = {0}".format(iothub_device.secondaryKey) )
        print ( "iothubDevice.connectionState             = {0}".format(iothub_device.connectionState) )
        print ( "iothubDevice.status                      = {0}".format(iothub_device.status) )
        print ( "iothubDevice.lastActivityTime            = {0}".format(iothub_device.lastActivityTime) )
        print ( "iothubDevice.cloudToDeviceMessageCount   = {0}".format(iothub_device.cloudToDeviceMessageCount) )
        print ( "iothubDevice.isManaged                   = {0}".format(iothub_device.isManaged) )
        print ( "iothubDevice.authMethod                  = {0}".format(iothub_device.authMethod) )
        print ( "" )   
                    
            
    #This method updates the 'timestamp' field for each device stored
    #in the local json file so that the initial timestamp is the latest timestamp
    def initialize_json(self):
        
        #Cycle through every json device and update its timestamp based on the value pulled from tetryon
        with open('settings/tetryon_devices') as json_data_file:
            devices = json.load(json_data_file)
            for i in xrange(len(devices['registered_devices'])):
            
                try:
                    db = mysql.connector.connect(user=self.DB_USER,
                             password=self.DB_PASS,
                             host=self.DB_HOST,
                             database=self.DB_NAME)
                    
                    cursor = db.cursor()
                    
                    sql_locno = "SELECT "+self.DB_DATA_LOCATIONNO+" FROM "+self.DB_TABLE+" WHERE "+self.DB_COLUMN+"='"+devices['registered_devices'][i]['device_id'][6:]+"'"
                    cursor.execute(sql_locno)
                    loc = sum(cursor.fetchone()).__str__()
                    
                    devices['registered_devices'][i]['loc_num'] = loc
                    
                    
                    sql_time_start = "SELECT MAX("+self.DB_DATA_TIME+") FROM "+self.DB_DATA_TABLE+" WHERE "+self.DB_DATA_LOCATIONNO+"="+devices['registered_devices'][i]['loc_num']
                    cursor.execute(sql_time_start)
                    time = cursor.fetchone()
                    
                    devices['registered_devices'][i]['timestamp'] = time[0].__str__()
                    
                    
                    
                    with open('settings/tetryon_devices', 'w') as json_data_file:
                        json_data_file.write(json.dumps(devices))
                        
                    self.start_cycle()
                    
                    
                
                except KeyboardInterrupt:
                    print ( "IoTHubClient sample stopped" )
                    
    #This method cycles through the tetryon database and checks if there is a more recent
    #timestamp than the one currently held in the json object associated with that device.
    #if so, pull the most recent data, and send it to the IOT hub, otherwise, continue to 
    #cycle through   
    def start_cycle(self):
        
        while True:
            
            self.check_registration()
            
            with open('settings/tetryon_devices') as json_data_file:
                devices = json.load(json_data_file)
                #for field in data['registered_devices']:
                for i in xrange(len(devices['registered_devices'])):
                    iot_id = devices['registered_devices'][i]['device_id']
            
                    try:
                        db = mysql.connector.connect(user=self.DB_USER,
                            password=self.DB_PASS,
                            host=self.DB_HOST,
                            database=self.DB_NAME)
            
                        cursor = db.cursor()
                        
                        sql_tet_time = "SELECT MAX("+self.DB_DATA_TIME+") FROM "+self.DB_DATA_TABLE+" WHERE "+self.DB_DATA_LOCATIONNO+"="+devices['registered_devices'][i]['loc_num']
                        cursor.execute(sql_tet_time)
                        time = cursor.fetchone()
                        
                        tetryon_time = time[0].__str__()

                        json_time = devices['registered_devices'][i]['timestamp']
                        
                        
                        if tetryon_time == 'None':
                            print ( "nothing" )
                    
                        elif int(tetryon_time) > int(json_time):
        
                            pull_data = "SELECT "+self.DB_DATA_SPEED+" FROM "+self.DB_DATA_TABLE+" WHERE "+self.DB_DATA_LOCATIONNO+"="+devices['registered_devices'][i]['loc_num']+" AND _Time>"+json_time+" AND _Time<="+tetryon_time+" ORDER BY "+self.DB_DATA_TIME+" ASC" 
                            cursor.execute(pull_data)
                            first_data = cursor.fetchall()
                            raw_data = list(map(sum, first_data))

                            
                            total_cars = len(raw_data)
                            max_speed = max(raw_data)
                            min_speed = min(raw_data)
                            mean_speed = (sum(raw_data) / total_cars)
                            
                            data = { "DeviceId" : iot_id, "RawData" : raw_data.__str__(), "TotalCars" : total_cars.__str__(), "AverageSpeed" : mean_speed.__str__(), "MaxSpeed" : max_speed.__str__(), "MinSpeed" : min_speed.__str__() }
                            
                            connection_string = ("HostName="+self.HOST_NAME+".azure-devices.net;DeviceId="+iot_id+";SharedAccessKey="+devices['registered_devices'][i]['device_primary_key']).__str__()
 
                            
                            self.send_to_iot(data, iot_id, connection_string)
            
                            devices['registered_devices'][i]['timestamp'] = tetryon_time
                            with open('settings/tetryon_devices', 'w') as json_data_file:
                                json_data_file.write(json.dumps(devices))
                                
                        else:
                            print ( "Waiting for update....")
                        
                    except KeyboardInterrupt:
                        print ( "IoTHubClient sample stopped" )
                        
    #This method sends data coming from a specific device through to the iot hub
    #given the proper connection string and data for that device                   
    def send_to_iot(self, data, did, connection):
        
        try:
            client = self.iothub_client_init(connection)
            message_counter = 0
            
            msg_txt_formatted = json.dumps(data)

            print ( msg_txt_formatted )
            if (message_counter & 1) == 1:
                message = IoTHubMessage(bytearray(msg_txt_formatted, 'utf8'))
                client.send_event_async(message, send_confirmation_callback, message_counter)
                time.sleep(2)
            else:
                message = IoTHubMessage(msg_txt_formatted)
                message.message_id = "message_%d" % message_counter
                message.correlation_id = "correlation_%d" % message_counter
                client.send_event_async(message, send_confirmation_callback, message_counter)
                time.sleep(2)
        
        except IoTHubError as iothub_error:
            print ( "Unexpected error %s from IoTHub" % iothub_error )
            return
        except KeyboardInterrupt:
            print ( "IoTHubClient sample stopped" )
        
    #This method initializes the client  with either a HTTP protocol or a MQTT protocol                   
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
    
    #Confirmation that the data was received by the IoT, print message callback  
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
            
       
if __name__ == '__main__':
    run = TVD()
    
    