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


def start_cycle():
    tetryon_initialize_connect()
    iot_initialize()
    update_device_json()
    #run()


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
            
        try:    
            TETRYON_CONNECTION = mysql.connector.connect(user=self.DB_USER,
                            password=self.DB_PASS,
                            host=self.DB_HOST,
                            database=self.DB_NAME)
            
            self.TETRYON_CURSOR = TETRYON_CONNECTION.cursor()
        
        except KeyboardInterrupt:
            print ( "IoTHubClient sample stopped" )
            
def iot_initialize(self):
    
    with open('settings/iot.json') as json_data_file:
        data = json.load(json_data_file)
        for field in data['iot_config']:
            self.HOST_NAME = field['host_name']
            
def update_device_json(self):
    
    with open('settings/tetryon_devices.json') as json_data_file:
        devices = json.load(json_data_file)
        #for field in data['registered_devices']:
        for i in xrange(len(devices['registered_devices'])):
            
            iot_device = devices['registered_devices'][i]['device_id']
            tetryon_device = iot_device[:6]
            
            print (iot_device)
            
            sql_time_start = "SELECT MAX("+self.DB_DATA_TIME+") FROM "+self.DB_DATA_TABLE+" WHERE "+self.DB_DATA_LOCATIONNO+"="+devices['registered_devices'][i]['loc_num']

            self.TETRYON_CURSOR.execute(sql_time_start)
            time = self.TETRYON_CURSOR.fetchone()
            
if __name__ == '__main__':
    start_cycle()
    
    
    
            
    
                
                
                
                
                
                
                