'''
Created on May 3, 2017
@author: tyler_tracey
'''

import time
import json
import requests
from iothub_client import IoTHubClient, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubError
from iothub_client_sample import send_confirmation_callback

class VirtualDeviceObject(object):
    
    DEVICE_ID = "[DEVICE_ID]"
    IOT_CONFIG = "[IOT_CONFIG]"
    
    hold = 0
    
    headers = {'Content-Type':'application/json', 'Accept':'application/json', 'appKey':'d3c92c06-166e-4adf-af65-3be398663260'}
    
    CONNECTION_STRING = "[CONNECTION_STRING]"
    
    ACCOUNT = ""
      
    LAST_SYNC = "[LAST_SYNC]"
    
    check = False

    TIMEOUT = 241000
    MINIMUM_POLLING_TIME = 9
    MESSAGE_TIMEOUT = 10000
    SEND_CALLBACKS = 0
    INTERVAL = 5
    
    JSON_DATA = ""
    
    PROTOCOL = IoTHubTransportProvider.MQTT
    PROTOCOL_HTTP = IoTHubTransportProvider.HTTP
    
    def __init__(self, account):
        
        global ACCOUNT
        global DEVICE_ID
        global IOT_CONFIG
        global CONNECTION_STRING
        global FOUND
        self.ACCOUNT = account
                    
        with open('settings/iot.json') as json_data_file:
            data = json.load(json_data_file)
            for field in data['iot_config']:
                if field['iot_tag'] == self.IOT_CONFIG:
                    self.HOST_NAME = field['host_name']
        
        #self.iothub_try()
        self.pull_account_devices()
        
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
       
    def iothub_client_init(self, connection):
        # prepare iothub client
        client = IoTHubClient(connection, self.PROTOCOL_HTTP)

        # HTTP specific settings
        if client.protocol == IoTHubTransportProvider.HTTP:
            client.set_option("timeout", self.TIMEOUT)
            client.set_option("MinimumPollingTime", self.MINIMUM_POLLING_TIME)

            # set the time until a message times out
            client.set_option("messageTimeout", self.MESSAGE_TIMEOUT)
        
        return client
    
#*****************************************************************************************************

    def pull_account_devices(self):
        global check
        #print(self.ACCOUNT)
        url = "http://trafficloud.alltrafficsolutions.com/Thingworx/Things/"+self.ACCOUNT+"/Services/ListAccountDevices?appKey=d3c92c06-166e-4adf-af65-3be398663260";
        #print url 
        r = requests.post(url, headers=self.headers, data={}) 
        #print ("here2")
        if r.status_code == 200:
            temp = json.loads(r.content)
            #print ( json.dumps(temp, indent=4, sort_keys=True) )
            for i in xrange(len(temp['rows'])):
                print ( temp['rows'][i]['EntityName'] )
                #print ( temp['rows'][i]['LastCommunication'] )
                #print ( temp['rows'][i]['PhysicalOwner'] )
                
                
                #self.list_recent_moves( temp['rows'][i]['EntityName'] )
        
        else:
            print('ERROR  failed on ', url, ' with code ',
            r.status_code, 'data', r.content)
     
    def list_recent_moves(self, entity_name):
        url = "http://trafficloud.alltrafficsolutions.com/Thingworx/Things/"+entity_name+"/Services/GetSiteAtDate?appKey=d3c92c06-166e-4adf-af65-3be398663260"; 
        data = { }
        data_json = json.dumps( data )
        r = requests.post(url, headers=self.headers, data=data_json)

        if r.status_code == 200:
            temp = json.loads(r.content)
            #print temp
            if temp['rows'] == []:
                print ( "NO DATA" )
                print ( "\n" )
                return
            else:
                
                current_site = temp['rows'][0]['result']
                print ( current_site )
                
                print( current_site )
                print ( "\n" )
                self.pull_device_history(current_site, entity_name)
                #print ( json.dumps(latest, indent=4, sort_keys=True) )
        else:
            print('ERROR  failed on ', url, ' with code ',
            r.status_code, 'data', r.content)
             
    def pull_device_history(self, current_site, entity_name):
        url = "http://trafficloud.alltrafficsolutions.com/Thingworx/Things/ATS.HistoryTrafficLog/Services/QueryStreamData?appKey=d3c92c06-166e-4adf-af65-3be398663260";
        data = { "maxItems" : "1", "query" : { "filters": { "type" : "LIKE", "fieldName" : "DeviceThingName", "value" : entity_name
                                } }, "source" : current_site }
        data_json = json.dumps(data)
        r = requests.post(url, data=data_json, headers=self.headers) 
        if r.status_code == 200:
            temp = json.loads(r.content)
            self.JSON_DATA = json.dumps(temp, indent=4, sort_keys=True)
            if self.hold < 13:
                print( entity_name )
                print (self.JSON_DATA)
                print self.hold
                con = self.create_connection(entity_name)
                self.iothub_try(self.JSON_DATA, con)
                self.hold += 1
            else:
                self.hold = 0
                self.pull_account_devices()
        else:
            print('ERROR  failed on ', url, ' with code ',
                    r.status_code, 'data', r.content)
            
    def create_connection(self, name):
        
        with open('settings/JJdevices.json') as json_data_file:
            devices = json.load(json_data_file)
            for i in xrange(len(devices['registered_devices'])):
                if devices['registered_devices'][i]['id'] == name:
                    pk = devices['registered_devices'][i]['pk']
        
        connection = ("HostName=TylerHub.azure-devices.net;DeviceId="+name+";SharedAccessKey="+pk).__str__()
        
        return connection
    
    
#************************************************************************************** 
              
              
    def iothub_try(self, json, connection):
        try:
            client = self.iothub_client_init(connection)
            message_counter = 0
            
                
            msg_txt_formatted = json
            # messages can be encoded as string or bytearray
            #print ( msg_txt_formatted )
                    
            if (message_counter & 1) == 1:
                message = IoTHubMessage(bytearray(msg_txt_formatted, 'utf8'))
            else:
                message = IoTHubMessage(msg_txt_formatted)
                # optional: assign ids
                message.message_id = "message_%d" % message_counter
                message.correlation_id = "correlation_%d" % message_counter

                client.send_event_async(message, send_confirmation_callback, message_counter)

            status = client.get_send_status()
            print ( "Send status: %s" % status )
            time.sleep(5)
            message_counter += 1
            
                        
        except IoTHubError as iothub_error:
            print ( "Unexpected error %s from IoTHub" % iothub_error )
            return
        except KeyboardInterrupt:
            print ( "IoTHubClient sample stopped" )
        
#[REGISTERED DEVICE ID] + Account name
if __name__ == '__main__':
    device = VirtualDeviceObject("ATS.Account.6217")
     
     


    