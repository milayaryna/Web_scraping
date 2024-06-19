import requests
from wsgiref.handlers import format_date_time
from time import mktime
from datetime import datetime
import hmac
from hashlib import sha1
import base64
import pandas as pd
import os
import time
import json

# ================================變數設定區================================
app_id = ####
app_key = ####
all_param = {
                 'bus':{'save_path':r'Data\bus',    #該類資料欲存放檔案路徑
                        'param':{ '台北市':{'city_name':'Taipei','save_filename':'台北市_bus.csv'},  # city_name:API要輸入的縣市名稱參數  save_filename:存檔檔案名稱
                                  '新北市':{'city_name':'NewTaipei','save_filename':'新北市_bus.csv'},
                                  '桃園市':{'city_name':'Taoyuan','save_filename':'桃園市_bus.csv'},
                                  '台中市':{'city_name':'Taichung','save_filename':'台中市_bus.csv'},
                                  '台南市':{'city_name':'Tainan','save_filename':'台南市_bus.csv'},
                                  '高雄市':{'city_name':'Kaohsiung','save_filename':'高雄市_bus.csv'},
                                  '新竹市':{'city_name':'Hsinchu','save_filename':'新竹市_bus.csv'},
                                  '新竹縣':{'city_name':'HsinchuCounty','save_filename':'新竹縣_bus.csv'}
                                }
                        },
                 'bike':{'save_path':r'Data\bike',
                         'param':{ '台北市':{'city_name':'Taipei','save_filename':'台北市_bike.csv'},
                                   '新北市':{'city_name':'NewTaipei','save_filename':'新北市_bike.csv'},
                                   '桃園市':{'city_name':'Taoyuan','save_filename':'桃園市_bike.csv'},
                                   '台中市':{'city_name':'Taichung','save_filename':'台中市_bike.csv'},
                                   '台南市':{'city_name':'Tainan','save_filename':'台南市_bike.csv'},
                                   '高雄市':{'city_name':'Kaohsiung','save_filename':'高雄市_bike.csv'},
                                   '新竹市':{'city_name':'Hsinchu','save_filename':'新竹市_bike.csv'},
                                 }
                        },
                 'MRT':{'save_path':r'Data\MRT',
                        'param':{'台北捷運':{'name':'TRTC','save_filename':'雙北_MRT.csv'},
                                 '淡海輕軌':{'name':'NTDLRT','save_filename':'雙北_MRT輕軌.csv'},
                                 '桃園捷運':{'name':'TYMC','save_filename':'桃園_MRT.csv'},
                                 '高雄捷運':{'name':'KRTC','save_filename':'高雄_MRT.csv'},
                                 '高雄輕軌':{'name':'KLRT','save_filename':'高雄_MRT輕軌.csv'}
                                }
                       },
                 'train':{'save_path':r'Data\train',
                          'param':{'save_filename':'台灣_TRAIN.csv'}}
            }
# ============================================================================
class Auth():

    def __init__(self, app_id, app_key):
        self.app_id = app_id
        self.app_key = app_key

    def get_auth_header(self):
        content_type = 'application/x-www-form-urlencoded'
        grant_type = 'client_credentials'

        return{
            'content-type' : content_type,
            'grant_type' : grant_type,
            'client_id' : self.app_id,
            'client_secret' : self.app_key
            }
            
class data():
    def __init__(self, app_id, app_key, auth_response):
        self.app_id = app_id
        self.app_key = app_key
        self.auth_response = auth_response

    def get_data_header(self):
        auth_JSON = json.loads(self.auth_response.text)
        access_token = auth_JSON.get('access_token')

        return{
            'authorization': 'Bearer '+access_token
        }

#獲取各縣市所有巴士站點資訊
def get_bus(type_param):
    param = type_param['param']
    s_path = type_param['save_path']
    if not os.path.exists(s_path):
        os.makedirs(s_path)
    for city,city_param in param.items():
        print('   Processing Bus...{}'.format(city))
        URL = 'https://tdx.transportdata.tw/api/basic/v2/Bus/Stop/City/{}?&$format=JSON'.format(city_param['city_name'])
        time.sleep(12)
        res = requests.get(URL,headers= d.get_data_header())
        jd = res.json()
        all_stops = []
        
        for stop in jd:
            StopUID = stop.get('StopUID')
            AuthorityID = stop.get('AuthorityID')
            City = stop.get('City')
            StopName = stop['StopName']['Zh_tw']
            lat = float(stop['StopPosition']['PositionLat'])
            lng = float(stop['StopPosition']['PositionLon'])
            all_stops.append({'StopUID':StopUID,'AuthorityID':AuthorityID,'City':City,'StopName':StopName,'lat':lat,'lng':lng})
        df_all_stops = pd.DataFrame(all_stops)
        df_all_stops.to_csv(os.path.join(s_path,city_param['save_filename']),index=False,encoding='utf_8_sig')
        print('   File Saved Bus {}'.format(city))
    print('='*50)
    
#獲取全國所有火車站點資訊
def get_train(type_param):
    print('   Processing Train...')
    param = type_param['param']
    s_path = type_param['save_path']
    if not os.path.exists(s_path):
        os.makedirs(s_path)
    url = 'https://tdx.transportdata.tw/api/basic/v2/Rail/TRA/Station?$format=JSON'
    time.sleep(12)
    res = requests.get(url,headers=d.get_data_header())
    jd = res.json()
    
    all_stops = []
    for stop in jd:
        StationID = stop.get('StationID')
        StationName = stop.get('StationName')['Zh_tw']
        try:
            lat = float(stop['StationPosition']['PositionLat'])
            lng = float(stop['StationPosition']['PositionLon'])
            StationAddress = stop.get('StationAddress')
            StationClass = int(stop.get('StationClass'))
            all_stops.append({'StationID':StationID,'StationName':StationName,'lat':lat,'lng':lng,'StationAddress':StationAddress,'StationClass':StationClass})
        except:
            pass
    df_all_stops = pd.DataFrame(all_stops)
    df_all_stops.to_csv(os.path.join(s_path,param['save_filename']),index=False,encoding='utf_8_sig')
    print('   File Saved Train')
    print('='*50)
    
#獲取各縣市所有腳踏車站點資訊
def get_bike(type_param):
    param = type_param['param']
    s_path = type_param['save_path']
    if not os.path.exists(s_path):
        os.makedirs(s_path)
    for city,city_param in param.items():
        print('   Processing Bike...{}'.format(city))
        URL = 'https://tdx.transportdata.tw/api/basic/v2/Bike/Station/City/{}?$format=JSON'.format(city_param['city_name'])
       
        time.sleep(12)
        res = requests.get(URL,headers=d.get_data_header())

        jd = res.json()
        
        all_stops = []
        for stop in jd:
            StationUID = stop.get('StationUID')
            StationID = stop.get('StationID')
            StationName = stop.get('StationName')['Zh_tw']
            StationAddress = stop.get('StationAddress')['Zh_tw']
            BikesCapacity = stop.get('BikesCapacity')
            lat = stop.get('StationPosition')['PositionLat']
            lng = stop.get('StationPosition')['PositionLon']
            all_stops.append({'StationUID':StationUID,'StationID':StationID,'StationName':StationName,'StationAddress':StationAddress,'BikesCapacity':BikesCapacity,'lat':lat,'lng':lng})
        df_all_stops = pd.DataFrame(all_stops)
        df_all_stops.to_csv(os.path.join(s_path,city_param['save_filename']),index=False,encoding='utf_8_sig')
        print('   File Saved Bike {}'.format(city))
    print('='*50)
    
#獲取各捷運系統站點資訊
def get_MRT(type_param):
    param = type_param['param']
    s_path = type_param['save_path']
    if not os.path.exists(s_path):
        os.makedirs(s_path)
    for city,city_param in param.items():
        print('   Processing MRT...{}'.format(city))
        URL = 'https://tdx.transportdata.tw/api/basic/v2/Rail/Metro/Station/{}?$format=JSON'.format(city_param['name'])
        time.sleep(12)
        res = requests.get(URL,headers=d.get_data_header())
        jd = res.json()
        all_stops = []
        for stop in jd:
            StationUID = stop['StationUID']
            StationID = stop.get('StationID')
            StationName = stop.get('StationName')['Zh_tw']
            StationAddress = stop.get('StationAddress')
            BikeAllowOnHoliday = stop.get('BikeAllowOnHoliday')
            lat = stop.get('StationPosition')['PositionLat']
            lng = stop.get('StationPosition')['PositionLon']
            all_stops.append({'StationUID':StationUID,'StationID':StationID,'StationName':StationName,'StationAddress':StationAddress,'BikeAllowOnHoliday':BikeAllowOnHoliday,'lat':lat,'lng':lng})
        df_all_stops = pd.DataFrame(all_stops)
        df_all_stops.to_csv(os.path.join(s_path,city_param['save_filename']),index=False,encoding='utf_8_sig')
        print('   File Saved MRT {}'.format(city))
    
if __name__ == '__main__':
    auth_url="https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
    print('Crawling MOTC...')
    a = Auth(app_id, app_key)
    auth_response = requests.post(auth_url, a.get_auth_header())
    print(auth_response)
    d = data(app_id, app_key, auth_response)
    get_bus(all_param['bus'])
    get_bike(all_param['bike'])
    get_train(all_param['train'])
    get_MRT(all_param['MRT'])
    
    print('Done Crawling MOTC！')
    print('='*85)
