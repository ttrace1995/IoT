'''
Created on May 9, 2017
@author: tyler
'''

import json
from iothub_service_client import IoTHubRegistryManager

def find_device(DEVICE_ID):
    
    with open('settings/devices.json') as json_data_file:
            data1 = json.load(json_data_file)
            #for field in data['registered_devices']:
            for i in xrange(len(data1['registered_devices'])):
                if data1['registered_devices'][i]['device_id'] == DEVICE_ID:
                    print ("FOUND DEVICE")
                    with open('settings/iot.json') as json_data_file:
                        data2 = json.load(json_data_file)
                        for field in data2['iot_config']:
                            CONNECTION_STRING = field['pk_connection_string'].__str__()
                            iothub_registry_manager = IoTHubRegistryManager(CONNECTION_STRING)
                            delete_device(iothub_registry_manager, DEVICE_ID)
                            return
                            
                elif data1['registered_devices'][i]['device_id'] != DEVICE_ID and (i+1) == len(data1['registered_devices']):
                    print("NO DEVICE FOUND")
                    

def delete_device(iothrm, DID):
    
    print ( "Deleting Device... " + DID )
    iothrm.delete_device(DID)
    
    with open('settings/devices.json') as json_data:
        data3 = json.load(json_data)
        for i in xrange(len(data3['registered_devices'])):
            if data3['registered_devices'][i]['device_id'] == DID:
                del data3['registered_devices'][i]
                    
                with open('settings/devices.json', "w" ) as json_data:
                    json.dump(data3, json_data)  
                
                    print ( "DEVICE DELETED" )
                    return
                
                
#print("NO REGISTERED DEVICES")
    
if __name__ == '__main__':
    find_device("::VD::0000179df19f")
    
    