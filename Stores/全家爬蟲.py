import requests

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import urllib
import json
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import random
import re
from datetime import datetime
from transform_time import transform_time

def get_all_county():
    res1 = requests.get('https://www.family.com.tw/Marketing/storemap/',headers = headers)  #url Albert Lee Edit
    soup = bs(res1.text,'html.parser')
    time.sleep(5)
    print(soup)
    all_county = [c.text for c in soup.find('div',{'id':'map_right'}).find_all('a',onclick=re.compile(r'showAdminArea'))]
    return all_county
    
def get_all_town(county):
    res2 = requests.get('https://api.map.com.tw/net/familyShop.aspx?searchType=ShowTownList&type=&city={}&fun=storeTownList&key=6F30E8BF706D653965BDE302661D1241F8BE9EBC'.format(urllib.parse.quote(county)),headers=headers)
    jd2 = json.loads(res2.text.replace('\r\n','').replace(' ','').replace('storeTownList(','')[:-1])
    all_town = [c.get('town') for c in jd2]
    return all_town
    
def get_town_store(county,town):
    res3 = requests.get('https://api.map.com.tw/net/familyShop.aspx?searchType=ShopList&type=&city={}&area={}&road=&fun=showStoreList&key=6F30E8BF706D653965BDE302661D1241F8BE9EBC'.format(urllib.parse.quote(county),urllib.parse.quote(town)),headers=headers)
    jd3 = json.loads(res3.text.replace('\r\n','').replace(' ','').replace('showStoreList(','')[:-1])
    columns = ['store_name','county','town','tel','lat','lng','store_address']
    df = pd.DataFrame(columns=columns)
    for store in jd3:
        name = store['NAME']
        tel = store['TEL']
        lat = float(store['py'])
        lng = float(store['px'])
        address = store['addr']
        s = pd.Series([name,county,town,tel,lat,lng,address],index=columns)
        df = df.append(s,ignore_index=True)
    return df

if __name__ == '__main__': 
    # headers = {'User-Agent': 'Mozilla/5.0'}
    print('Crawling 全家...')
    start_time = datetime.now()
    start_str = datetime.strftime(start_time,'%Y-%m-%d-%H:%M:%S')
    print('Start Time：',start_str)
    
    headers = {'Referer': 'https://www.family.com.tw/Marketing/storemap/?',    #url Albert Lee Edit
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36'}
              
    li = []
    for county in get_all_county():
        for town in get_all_town(county):
            print('   Processing...{}{}'.format(county,town))
            try:
                li.append(get_town_store(county,town))
            except:
                continue
            time.sleep(random.randint(3,8))
    df = pd.concat(li)
    df.to_csv(r'Data\全家.csv',index=False,encoding='utf_8_sig')
    
    end_time = datetime.now()
    end_str = datetime.strftime(end_time,'%Y-%m-%d-%H:%M:%S')
    print('End Time：',end_str)
    
    t = (end_time-start_time)
    print(transform_time(t))
    
    with open(r'Data\商家_time_records.txt','a') as f:
        f.write('\n=============================================')
        f.write('\n全家')
        f.write('\n--------------')
        f.write('\nStart Time：{}'.format(start_str))
        f.write('\nEnd Time  ：{}'.format(end_str))
        f.write('\nTotal Time：{}'.format(transform_time(t)))
        f.write('\n=============================================\n')
    print('Done Crawling 全家！')
    print('='*85)
