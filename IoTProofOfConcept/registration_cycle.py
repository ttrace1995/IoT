'''
Created on May 8, 2017
@author: tyler
This class cycles through the Tetryon database to 
check if they have been registered yet with IOT, if so, check status,
if not, register them
'''

import json
import time
import mysql.connector
from iothub_service_client import IoTHubRegistryManager, IoTHubRegistryManagerAuthMethod
from iothub_service_client import IoTHubError
from iothub_registrymanager_sample import print_device_info


DB_USER = "[DB_USER]"
DB_PASS = "[DB_PASS]"
DB_HOST = "[DB_HOST]"
DB_NAME = "[DB_NAME]"
DB_TYPE = "[DB TYPE]"
DB_TABLE = "[TTC]"
DB_COLUMN = "[CTC]"

DB_CONNECTION = "[DB CONNECTION]"

ID_LIST = "[ID_LIST]"


def pull_tetryon_json_data():
    
    global DB_NAME
    global DB_HOST
    global DB_USER
    global DB_PASS
    global DB_TABLE
    global DB_COLUMN
    
    with open('settings/database.json') as json_data_file:
        data = json.load(json_data_file)
        for field in data['tetryon_config']:
            DB_HOST = field['db_host']
            DB_NAME = field['db_name']
            DB_USER = field['db_user']
            DB_PASS = field['db_pass']
            DB_TABLE = field['db_table']
            DB_COLUMN = field['db_column']
            
            print('Databse Tag:\t' + field['db_tag'])
            print('Databse Name:\t' + DB_NAME)
            print('Databse User:\t' + DB_USER)
            print('Databse Pass:\t' + DB_PASS)
            print('Databse Host:\t' + DB_HOST)
            print('Databse Table:\t' + DB_TABLE)
            print('Databse Column:\t' + DB_COLUMN+"\n************************************\n")
            
            
            if field['db_tag'] == 'tetryon':
                db_connect_tetryon()
            
def db_connect_tetryon():
    
    global DB_CONNECTION
    
    try:
        DB_CONNECTION = mysql.connector.connect(user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST,
                            database=DB_NAME)
        
        query_devices()
        
    #THESE ARE NOT HANDLED PROPERLY  
    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )
        
        
def query_devices():
    
    global ID_LIST
    
    try:
        cursor = DB_CONNECTION.cursor()
        IDs = "SELECT "+DB_COLUMN+" FROM "+DB_TABLE
        cursor.execute(IDs)
        ID_LIST = cursor.fetchall()
        
        cycle_IDs()
    
    #THESE ARE NOT HANDLED PROPERLY 
    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )
        
def cycle_IDs():
    
    REGISTERED_LIST = ['holder']
    
    with open('settings/devices.json') as json_data_file:
            data = json.load(json_data_file)
            for field in data['registered_devices']:
                registered_id = field['device_id']
                REGISTERED_LIST.append(registered_id)
    
    for i, a in enumerate(ID_LIST):
        for j, b in enumerate(REGISTERED_LIST):
                if REGISTERED_LIST[j][6:] == ''.join(ID_LIST[i]):
                    pull_status(REGISTERED_LIST[j])
                    break
                elif REGISTERED_LIST[j][6:] != ''.join(ID_LIST[i]) and REGISTERED_LIST.__len__() == (j+1):
                    print("NEW DEVICE DISCOVERED: "+ ''.join(ID_LIST[i])+"\n")
                    #get_device("::VD::"+''.join(ID_LIST[i])
                    create_new_device(''.join(ID_LIST[i]))
                    
                    
def pull_status(d_id):
    
    with open('settings/devices.json') as json_data_file:
            data = json.load(json_data_file)
            for field in data['registered_devices']:
                if 'device_id' in field and field['device_id'] == d_id:
                    print ("ALREADY SEEN DEVICE: "+d_id)
                    print field['device_status']+"*************\n"
                    
def create_new_device(d_id):
    
    with open('settings/iot.json') as json_data_file:
        data = json.load(json_data_file)
        for field in data['iot_config']:
            CONNECTION_STRING = field['pk_connection_string'].__str__()
            
    iothub_registry_manager = IoTHubRegistryManager(CONNECTION_STRING)
    
    try:
        ID = "::VD::"+d_id.__str__()
        iothub_registry_manager = IoTHubRegistryManager(CONNECTION_STRING)
        auth_method = IoTHubRegistryManagerAuthMethod.SHARED_PRIVATE_KEY
        new_device = iothub_registry_manager.create_device(ID, "", "", auth_method)
        
        write_json_device(ID, new_device)
        
    except IoTHubError as iothub_error:
        print ( "Unexpected error {0}".format(iothub_error) )
        return
    except KeyboardInterrupt:
        print ( "iothub_createdevice stopped" )
        
        
def write_json_device(d_id, new_device):
    
    with open('settings/devices.json') as json_data:
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
        'device_status': 'PENDING APPROVAL'
        })
    
    with open('settings/devices.json', "w" ) as json_data:
        json.dump(data, json_data) 
        
    print_device_info("Created Device", new_device)
    time.sleep(3)
        
        
def print_device_info(title, iothub_device):
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
                    
if __name__ == '__main__':
    pull_tetryon_json_data()
    
