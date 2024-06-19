import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import geopandas as gpd
from shapely.geometry import Point
import time
import random
from datetime import datetime
from transform_time import transform_time

# 抓取指定縣市之所有鄉鎮市區名稱 county_name:str,縣市名稱
def get_all_town(county_name):
    res1 = requests.post('https://emap.pcsc.com.tw/EMapSDK.aspx',data={'commandid':'GetTown','cityid':county_dic[county_name]})
    # soup1 = bs(res1.text,'lxml')
    soup1 = bs(res1.text,'xml')

    return [t.text for t in soup1.select('TownName')]

# 抓取指定縣市所有門市 county_name:str,縣市名稱
def get_county_location(county_name):
    columns = ['縣市','行政區','分店名稱','x','y','telnum','faxnum','address']
    df = pd.DataFrame(columns=columns)
    for town in get_all_town(county_name):
        print('   Processing...{}{}'.format(county_name,town))
        data = {
            'commandid': 'SearchStore',
            'city': county_name,
            'town': town}

        time.sleep(random.randint(8,12))
        # 抓取指定鄉鎮市區所有門市
        res2 = requests.post('https://emap.pcsc.com.tw/EMapSDK.aspx',data=data)
        soup2 = bs(res2.text,'xml')

        for store in soup2.select('GeoPosition'):
            store_name = store.find('POIName').text.strip()
            x = float(store.find('X').text.strip())/10e5
            y =float(store.find('Y').text.strip())/10e5
            telnum = store.find('Telno').text.strip()
            faxnum = store.find('FaxNo').text.strip()
            address = store.find('Address').text.strip()
            df = df.append(pd.Series([county_name,town,store_name,x,y,telnum,faxnum,address],index=columns),ignore_index=True)
    return df
    
if __name__ == '__main__':
    print('Crawling 7-11...')
    start_time = datetime.now()
    start_str = datetime.strftime(start_time,'%Y-%m-%d-%H:%M:%S')
    print('Start Time：',start_str)
    
    county_dic = {'台北市':'01', '基隆市':'02','新北市':'03','桃園市':'04','新竹市':'05','新竹縣':'06','苗栗縣':'07','台中市':'08',
                  '彰化縣':'10','南投縣':'11','雲林縣':'12','嘉義市':'13','嘉義縣':'14','台南市':'15','高雄市':'17','屏東縣':'19',
                  '宜蘭縣':'20','花蓮縣':'21','台東縣':'22','澎湖縣':'23','金門縣':'25','連江縣':'24'}
                  

    list_df_county = []
    for county in county_dic:
        list_df_county.append(get_county_location(county))
    df = pd.concat(list_df_county)
    df = df.rename(columns={'分店名稱':'store_name','address':'store_address','x':'lng','y':'lat'})

    df.to_csv(r'Data\7_11.csv',index=False,encoding='utf_8_sig')
    
    end_time = datetime.now()
    end_str = datetime.strftime(end_time,'%Y-%m-%d-%H:%M:%S')
    print('End Time：',end_str)
    
    t = (end_time-start_time)
    print(transform_time(t))
    
    #記錄執行時間
    with open(r'Data\商家_time_records.txt','a') as f:
        f.write('\n=============================================')
        f.write('\n7-11')
        f.write('\n--------------')
        f.write('\nStart Time：{}'.format(start_str))
        f.write('\nEnd Time  ：{}'.format(end_str))
        f.write('\nTotal Time：{}'.format(transform_time(t)))
        f.write('\n=============================================\n')
    print('Done Crawling 7-11！')
    print('='*85)
