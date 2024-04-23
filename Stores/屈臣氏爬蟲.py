import requests
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from bs4 import BeautifulSoup as bs
import re
import pandas as pd
import time
from datetime import datetime
import random
import json
from transform_time import transform_time
import os
import sys
sys.path.append(r'..\..')
from selenium import webdriver
from selenium.webdriver.common.by import By

# =====參數=====
driver_path = '../../CHROME_DRIVER/chromedriver_116.exe'
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'}
options = webdriver.ChromeOptions()
options.binary_location = 'C:/Users/sinopacDAD/Chrome測試版/chrome-win64/chrome.exe'

# # =======取出所有縣市==========
def get_county():
    county_url = 'https://api.watsons.com.tw/api/v2/wtctw/stores/townsAndDistricts?isCceOrCc=false&lang=zh_TW&curr=TWD'
    driver = webdriver.Chrome(driver_path, options=options)
    res = driver.get(county_url)
    html_str = driver.page_source
    driver.quit()
    
    soup = bs(html_str, 'html.parser')
    pre_tag = soup.find('pre')
    json_str = pre_tag.get_text()

    data = json.loads(json_str)

    rows = []
    for county, areas in data.items():
        for area in areas:
            rows.append({'county': county, 'area': area})

    df = pd.DataFrame(rows)
    county_list = list(set(df['county']))
    return county_list

# =======每次查詢不同縣市=======
def get_store(county):
    print(county)
    current_page = 0
    total_pages = 1  # 初始化總頁數為1

    while current_page < total_pages:
        print('Processing...{}...頁數{}'.format(county,current_page))
        url = f'https://api.watsons.com.tw/api/v2/wtctw/stores/watStores?currentPage={current_page}&pageSize=20&region={county}&isCceOrCc=false&fields=FULL&lang=zh_TW&curr=TWD'
        driver = webdriver.Chrome(driver_path, options=options)
        driver.get(url)
        t = random.randint(2,8)
        time.sleep(t)
        html_str = driver.page_source
        driver.quit()

        soup = bs(html_str, 'html.parser')

        # 獲取當前頁面的店家資訊
        store_tags = soup.find_all('stores')
        for store in store_tags:
            displayName = store.find('displayname').text
            displayAddress3 = store.find('displayaddress3').text
            displayAddress4 = store.find('displayaddress4').text
            displayAddress1 = store.find('displayaddress1').text
            latitude = store.find('latitude').text
            longitude = store.find('longitude').text

            stores_data.append({
                'store_name': displayName,
                'county': displayAddress3,
                'town': displayAddress4,
                'store_address': displayAddress1,
                'lat': latitude,
                'lng': longitude
            })

        # 獲取總頁數
        total_pages_tag = soup.find('pagination').find('totalpages')
        total_pages = int(total_pages_tag.text)

        current_page += 1

    return stores_data



if __name__ == '__main__':
    print('Crawling 屈臣氏...')
    start_time = datetime.now()
    start_str = datetime.strftime(start_time,'%Y-%m-%d-%H:%M:%S')
    print('Start Time：',start_str)
    
    county_list = get_county()
    stores_data = []
    for county in county_list:
        get_store(county)
        
    stores_df = pd.DataFrame(stores_data)
    stores_df.to_csv(r'Data\屈臣氏_test.csv', index=False, encoding='utf_8_sig')

    # =====紀錄=====
    end_time = datetime.now()
    end_str = datetime.strftime(end_time,'%Y-%m-%d-%H:%M:%S')
    print('End Time：',end_str)
    
    t = (end_time-start_time)
    print(transform_time(t))
    
    with open(r'Data\商家_time_records.txt','a') as f:
        f.write('\n=============================================')
        f.write('\n屈臣氏')
        f.write('\n--------------')
        f.write('\nStart Time：{}'.format(start_str))
        f.write('\nEnd Time  ：{}'.format(end_str))
        f.write('\nTotal Time：{}'.format(transform_time(t)))
        f.write('\n=============================================\n')
    print('Done Crawling 屈臣氏！')
    print('='*85)
