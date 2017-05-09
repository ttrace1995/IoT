'''
Created on May 8, 2017

@author: tyler
'''
from ConfigParser import SafeConfigParser

def create_iotinfo_config():
    
    config = SafeConfigParser()
    config.read('settings.txt')
    config.add_section('iot_hub')
    config.set('iot_hub', 'host_name', 'IOTExampleHub')
    config.set('iot_hub', 'access_policy_name', 'iothubowner')
    config.set('iot_hub', 'pk', 'g8wqO2ofL8T5tvBq5SQevhYN0pdMMmZPqn1lcA7ll68=')
    config.set('iot_hub', 'sk', '2OOxQp3Rg/0D8V3osyOAlEBazA6VCmkUFUgE28gg6SY=')
    config.set('iot_hub', 'pk_connection_string', 'HostName=IOTExampleHub.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=g8wqO2ofL8T5tvBq5SQevhYN0pdMMmZPqn1lcA7ll68=')
    config.set('iot_hub', 'sk_connection_string', 'HostName=IOTExampleHub.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=2OOxQp3Rg/0D8V3osyOAlEBazA6VCmkUFUgE28gg6SY=')
    with open('settings.txt', 'w') as f:
        config.write(f)
        
def create_database_config():
    
    config = SafeConfigParser()
    config.read('settings.txt')
    config.add_section('tetryon_db')
    config.set('tetryon_db', 'db_user', 'tcjdbc')
    config.set('tetryon_db', 'db_pass', '6wu4T4mzJLJaBHsX')
    config.set('tetryon_db', 'db_host', '10.150.9.1')
    config.set('tetryon_db', 'db_name', 'speedlaneDb')

    with open('settings.txt', 'w') as f:
        config.write(f)
        
def device_config():
    
    config = SafeConfigParser()
    config.read('settings.txt')
    config.add_section('config')
    config.set('config', 'last_check', 'tcjdbc')
    config.set('config', 'last_successful_sync', '6wu4T4mzJLJaBHsX')
    config.set('config', 'db_host', '10.150.9.1')
    config.set('config', 'db_name', 'speedlaneDb')

    with open('settings.txt', 'w') as f:
        config.write(f)
    
    
    
    
if __name__ == '__main__':
    create_iotinfo_config()
    create_database_config()