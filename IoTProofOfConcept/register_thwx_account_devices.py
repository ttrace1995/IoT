'''
Created on May 19, 2017
@author: tyler
'''
import json
import time
import mysql.connector
import requests
from iothub_service_client import IoTHubRegistryManager, IoTHubRegistryManagerAuthMethod
from iothub_service_client import IoTHubError
from iothub_registrymanager_sample import print_device_info

ID_LIST = []
ACCOUNT = "ACCOUNT"
headers = {'Content-Type':'application/json', 'Accept':'application/json', 'appKey':'777fce0a-e1af-4f47-9608-91c2812887a9'}

def pull_thwx_data(account):
  
    #print(self.ACCOUNT)
    url = "http://trafficloud.alltrafficsolutions.com/Thingworx/Things/"+account+"/Services/ListAccountDevices?appKey=777fce0a-e1af-4f47-9608-91c2812887a9";
    #print url 
    r = requests.post(url, headers=headers, data={}) 
    #print ("here2")
    if r.status_code == 200:
        temp = json.loads(r.content)
        #print ( json.dumps(temp, indent=4, sort_keys=True) )
        for i in xrange(len(temp['rows'])):
            print ( temp['rows'][i]['EntityName'] )
            ID_LIST.append(temp['rows'][i]['EntityName'])
    
    
        print ID_LIST
        cycle_IDs()
        
def cycle_IDs():
    REGISTERED_LIST = ['holder']
    
    with open('settings/JJdevices.json') as json_data_file:
            data = json.load(json_data_file)
            if data['registered_devices'] == []:
                register_all()
                return
              
            for field in data['registered_devices']:
                registered_id = field['device_id']
                REGISTERED_LIST.append(registered_id)
    
    for i, a in enumerate(ID_LIST):
        for j, b in enumerate(REGISTERED_LIST):
                if REGISTERED_LIST[j][6:] == ''.join(ID_LIST[i]):
                    print ("Already Registered")
                elif REGISTERED_LIST[j][6:] != ''.join(ID_LIST[i]) and REGISTERED_LIST.__len__() == (j+1):
                    print("NEW DEVICE DISCOVERED: "+ ''.join(ID_LIST[i])+"\n")
                    create_new_device(''.join(ID_LIST[i]))

                    
def register_all():
    
    for i, a in enumerate(ID_LIST):
        holder = ''.join(ID_LIST[i])
        create_new_device(holder)

                    
def create_new_device(d_id):
    
    with open('settings/TylersHub') as json_data_file:
        data = json.load(json_data_file)
        for field in data['iot_config']:
            CONNECTION_STRING = field['pk_connection_string'].__str__()
            
    iothub_registry_manager = IoTHubRegistryManager(CONNECTION_STRING)
    
    try:
        ID = d_id.__str__()
        iothub_registry_manager = IoTHubRegistryManager(CONNECTION_STRING)
        auth_method = IoTHubRegistryManagerAuthMethod.SHARED_PRIVATE_KEY
        new_device = iothub_registry_manager.create_device(ID, "", "", auth_method)
        print ("here")
        write_json_device(ID, new_device)
        
    except IoTHubError as iothub_error:
        print ( "Unexpected error {0}".format(iothub_error) )
        return
    except KeyboardInterrupt:
        print ( "iothub_createdevice stopped" )
        
        
def write_json_device(d_id, new_device):
    
    with open('settings/JJdevices.json') as json_data:
        data = json.load(json_data)
    
    data['registered_devices'].append({
        'id': new_device.deviceId.__str__(),
        'pk': new_device.primaryKey.__str__()
        })
    
    with open('settings/JJdevices.json', "w" ) as json_data:
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
    pull_thwx_data("ATS.Account.6217")
    
    