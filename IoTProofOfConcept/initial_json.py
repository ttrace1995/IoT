'''
Created on May 8, 2017
@author: tyler
'''
import json

def write_databse_json():
    
    Ddata = {}  
    Ddata['tetryon_config'] = []  
    Ddata['tetryon_config'].append({  
        'db_tag': 'tetryon',
        'db_user': 'tcjdbc',
        'db_pass': '6wu4T4mzJLJaBHsX',
        'db_host': '10.150.9.1',
        'db_name': 'speedlaneDb',
        'db_table': 'speedlanes',
        'db_column': 'sid',
        'db_data_table':'targets',
        'db_data_time':'_Time',
        'db_data_direction':'_Direction',
        'db_data_lane':'_Lane',
        'db_data_lenth':'_Length',
        'db_data_speed':'_Speed',
        'db_data_intervalNum':'intervalNo',
        'db_data_locationNum':'locationNo'
    })

    with open('settings/database.json', 'w') as outfile:  
        json.dump(Ddata, outfile)
        
def write_IOT_json():
    
    IOTdata = {}
    IOTdata['iot_config'] = []
    IOTdata['iot_config'].append({
        'iot_tag': 'iothub',
        'host_name': 'IOTExampleHub',
        'access_policy_name': 'iothubowner',
        'primary_key': 'g8wqO2ofL8T5tvBq5SQevhYN0pdMMmZPqn1lcA7ll68=',
        'secondary_key': '2OOxQp3Rg/0D8V3osyOAlEBazA6VCmkUFUgE28gg6SY=',
        'pk_connection_string': 'HostName=IOTExampleHub.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=g8wqO2ofL8T5tvBq5SQevhYN0pdMMmZPqn1lcA7ll68=',
        'sk_ocnnection_string': 'HostName=IOTExampleHub.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=2OOxQp3Rg/0D8V3osyOAlEBazA6VCmkUFUgE28gg6SY='
        })
    
    with open('settings/iot.json', 'w') as outfile:  
        json.dump(IOTdata, outfile)
        

        
def initialize_devices_json():
    
    data = {}
    data['registered_devices'] = []
    with open('settings/devices.json', "w" ) as outfile:
        json.dump(data, outfile)
    
        
        
if __name__ == '__main__':
    write_databse_json()
    write_IOT_json()
    initialize_devices_json()
    

    
    
    
    
    