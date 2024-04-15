import requests
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import time
import random
from datetime import datetime
from transform_time import transform_time
import json


if __name__ == '__main__':
    print('Crawling 全聯...')
    start_time = datetime.now()
    start_str = datetime.strftime(start_time,'%Y-%m-%d-%H:%M:%S')
    print('Start Time：',start_str)
    
    #====參數====
    #url
    url = 'https://www.pxmart.com.tw/api/stores'
    #headers
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36', 'content-type':'application/json;charset=UTF-8'}

    #抓取資料:
    response = requests.post(url, headers=headers)
    data_json = json.loads(response.text)
    
    #轉換成DataFrame並存成CSV檔案
    df = pd.json_normalize(data_json['data'])
    df.to_csv(r'Data\全聯.csv',index=False,encoding='utf_8_sig')
    
    end_time = datetime.now()
    end_str = datetime.strftime(end_time,'%Y-%m-%d-%H:%M:%S')
    print('End Time：',end_str)
    
    t = (end_time-start_time)
    print(transform_time(t))
    
    with open(r'Data\商家_time_records.txt','a') as f:
        f.write('\n=============================================')
        f.write('\n全聯')
        f.write('\n--------------')
        f.write('\nStart Time：{}'.format(start_str))
        f.write('\nEnd Time  ：{}'.format(end_str))
        f.write('\nTotal Time：{}'.format(transform_time(t)))
        f.write('\n=============================================\n')
    print('Done Crawling 全聯！')
    print('='*85)
