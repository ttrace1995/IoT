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
    
    headers = {'Content-Type':'application/json', 'Accept':'application/json', 'appKey':'4637bf60-7e1a-42e5-8315-8551ac04ee08'}
    
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
    
    def __init__(self, DID, account):
        
        global ACCOUNT
        global DEVICE_ID
        global IOT_CONFIG
        global CONNECTION_STRING
        global FOUND

        self.DEVICE_ID = DID
        self.DB_DEVICE_ID = DID[6:]
        self.ACCOUNT = account
                    
        with open('settings/iot.json') as json_data_file:
            data = json.load(json_data_file)
            for field in data['iot_config']:
                if field['iot_tag'] == self.IOT_CONFIG:
                    self.HOST_NAME = field['host_name']
                    
                    
        self.CONNECTION_STRING = "HostName=ATSPOC.azure-devices.net;DeviceId=ATS.Device.18970713000130;SharedAccessKey=wA7tXDVeDfv8rVTI4ukJv97ElEguVhSclGhh6Tq64O0="
        
        self.iothub_try()
        
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
        
        return client
    
#*****************************************************************************************************
  
    def pull_account_devices(self):
        global check
        print("HERE")
        url = "http://tcsb.alltrafficsolutions.com/Thingworx/Things/"+self.ACCOUNT+"/Services/GetAccountDevices?appKey=4637bf60-7e1a-42e5-8315-8551ac04ee08"; 
        r = requests.post(url, headers=self.headers, data={}) 
        if r.status_code == 200:
            temp = json.loads(r.content)
            #print ( json.dumps(temp, indent=4, sort_keys=True) )
            for i in xrange(len(temp['rows'])):
                if temp['rows'][i]['name'] == self.DEVICE_ID:
                    print ( temp['rows'][i]['name'] )
                    self.list_recent_moves()
        
        else:
            print('ERROR  failed on ', url, ' with code ',
            r.status_code, 'data', r.content)
     
    def list_recent_moves(self):
        url = "http://tcsb.alltrafficsolutions.com/Thingworx/Things/ATS.HistorySite/Services/ListRecentMoves?appKey=4637bf60-7e1a-42e5-8315-8551ac04ee08"; 
        data = { "DeviceThingName" : self.DEVICE_ID, "SearchStartTime" : "0" }
        data_json = json.dumps( data )
        r = requests.post(url, headers=self.headers, data=data_json)
        if r.status_code == 200:
            temp = json.loads(r.content)
            if temp['rows'] == []:
                print ( "NO DATA" )
                    
                self.check = False
                
            latest = max(temp['rows'], key=lambda ev: ev['timestamp'])
            current_site = latest['NewSite']
            print( current_site )
            self.pull_device_history(current_site)
            #print ( json.dumps(latest, indent=4, sort_keys=True) )
        else:
            print('ERROR  failed on ', url, ' with code ',
            r.status_code, 'data', r.content)
             
    def pull_device_history(self, current_site):
        url = "http://tcsb.alltrafficsolutions.com/Thingworx/Things/ATS.HistoryTrafficLog/Services/QueryStreamData?appKey=4637bf60-7e1a-42e5-8315-8551ac04ee08";
        data = { "maxItems" : "1", "query" : { "filters": { "type" : "LIKE", "fieldName" : "DeviceThingName", "value" : self.DEVICE_ID
                                } }, "source" : current_site }
        data_json = json.dumps(data)
        r = requests.post(url, data=data_json, headers=self.headers) 
        if r.status_code == 200:
            temp = json.loads(r.content)
            self.JSON_DATA = json.dumps(temp, indent=4, sort_keys=True)
            print self.JSON_DATA
            self.check = True
        else:
            print('ERROR  failed on ', url, ' with code ',
                    r.status_code, 'data', r.content)
    
    
    
    
#************************************************************************************** 
    
    
              
    def iothub_try(self):
        try:
            client = self.iothub_client_init()
            message_counter = 0
            while True:
                
                self.pull_account_devices()
                
                if self.check == True:
                
                    msg_txt_formatted = self.JSON_DATA
                    # messages can be encoded as string or bytearray
                    print ( msg_txt_formatted )
                    
                    if (message_counter & 1) == 1:
                        message = IoTHubMessage(bytearray(msg_txt_formatted, 'utf8'))
                    else:
                        message = IoTHubMessage(msg_txt_formatted)
                        # optional: assign ids
                        message.message_id = "message_%d" % message_counter
                        message.correlation_id = "correlation_%d" % message_counter

                        client.send_event_async(message, send_confirmation_callback, message_counter)
                        
                        self.check = False
                        time.sleep(15)
    
                else:
                    print ( "OH NO" )    
                    status = client.get_send_status()
                    print ( "Send status: %s" % status )
                    time.sleep(15)
                    message_counter += 1
                        
        except IoTHubError as iothub_error:
            print ( "Unexpected error %s from IoTHub" % iothub_error )
            return
        except KeyboardInterrupt:
            print ( "IoTHubClient sample stopped" )
        
#[REGISTERED DEVICE ID] + Account name
if __name__ == '__main__':
    device = VirtualDeviceObject("ATS.Device.18970713000130", "ATS.Account.9252")
     
     


    