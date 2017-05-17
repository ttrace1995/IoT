'''
Created on May 11, 2017
@author: tyler_tracey
'''

import requests
import json

headers = {'Content-Type':'application/json', 'Accept':'application/json', 'appKey':'4637bf60-7e1a-42e5-8315-8551ac04ee08'}
    
def pull_account_devices(account_name):
    
    url = "http://tcsb.alltrafficsolutions.com/Thingworx/Things/"+account_name+"/Services/GetAccountDevices?appKey=4637bf60-7e1a-42e5-8315-8551ac04ee08"; 
    
    r = requests.post(url, headers=headers, data={}) 

    if r.status_code == 200:
        temp = json.loads(r.content)
        #print ( json.dumps(temp, indent=4, sort_keys=True) )
        for i in xrange(len(temp['rows'])):
            print ( temp['rows'][i]['name'] )
            list_recent_moves(temp['rows'][i]['name'])
        
    else:
        print('ERROR  failed on ', url, ' with code ',
        r.status_code, 'data', r.content)
        
        
def list_recent_moves(device_id):
    
    url = "http://tcsb.alltrafficsolutions.com/Thingworx/Things/ATS.HistorySite/Services/ListRecentMoves?appKey=4637bf60-7e1a-42e5-8315-8551ac04ee08"; 
    
    
    data = { "DeviceThingName" : device_id, "SearchStartTime" : "0" }
    data_json = json.dumps( data )
    
    r = requests.post(url, headers=headers, data=data_json) 

    if r.status_code == 200:
        temp = json.loads(r.content)
        if temp['rows'] == []:
            print ( "NO DATA" )
            return
        latest = max(temp['rows'], key=lambda ev: ev['timestamp'])
        current_site = latest['NewSite']
        print( current_site )
        pull_device_history(device_id, current_site)
        #print ( json.dumps(latest, indent=4, sort_keys=True) )
        
    else:
        print('ERROR  failed on ', url, ' with code ',
        r.status_code, 'data', r.content)
             
        
def pull_device_history(device_id, current_site):
    
    url = "http://tcsb.alltrafficsolutions.com/Thingworx/Things/ATS.HistoryTrafficLog/Services/QueryStreamData?appKey=4637bf60-7e1a-42e5-8315-8551ac04ee08";
    
    data = { "maxItems" : "1", "query" : { "filters": { "type" : "LIKE", "fieldName" : "DeviceThingName", "value" : device_id
                                     } }, "source" : current_site }
    
    data_json = json.dumps(data)
    
    r = requests.post(url, data=data_json, headers=headers) 
    
    if r.status_code == 200:
        temp = json.loads(r.content)
        print ( json.dumps(temp, indent=4, sort_keys=True) )
        
    else:
        print('ERROR  failed on ', url, ' with code ',
        r.status_code, 'data', r.content)
        
    
      
if __name__ == '__main__':
    pull_account_devices("ATS.Account.9252")
    


    
