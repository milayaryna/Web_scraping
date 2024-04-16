import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import re
import os
import sys
sys.path.append(r'..\..')
from geocode import geocode_dataframe

from selenium import webdriver
from selenium.webdriver.common.by import By
import time


# 將臺取代為台
def ReplaceTai(string):
    return string.replace('臺','台')

# 將括號中文字刪除
def TakeOffBracket(string):
    return re.sub(r'\(.*\)','',string)

# 將句號刪除
def TakeOffPeriod(string):
    return re.sub('。','',string)

# 將地址擷取至"號"，後面不要
def SplitToNumber(string):
    if '號' in string:
        return string.split('號')[0]+'號'
    else:
        return string

# 部分地址門牌號碼多個(如16、18號)，擷取到第一個數字
def SplitNumber(string):
    try:
        if not '、' in string:
            return string
        else:
            reg = re.compile(r'\d+(-\d+)?(、\d+(-\d+)?)*號')
            return re.sub(reg,'',string)+reg.search(string).group().split('、')[0]+'號'
    except:
        return string



def crawler(driver_path, url):
    options = webdriver.ChromeOptions()
    options.binary_location = 'C:/Users/sinopacDAD/Chrome測試版/chrome-win64/chrome.exe'
    # prefs = {'profile.default_content_setting_values.automatic_downloads':1, 'profile.default_content_settings.popups':0, 'download.default_directory':out_path}
    # options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(driver_path, options=options)

    #Visit the website
    driver.get(url)
    time.sleep(3)
    tbl = driver.find_element_by_css_selector("div#table > table").get_attribute('outerHTML')
    # print(tbl)
    df  = pd.read_html(tbl)
    
    df = df[0].iloc[:,1:-1]
    print(df)
    return df




if __name__ == '__main__':  
	# 設定瀏覽器檔案下載目的地路
	# out_path = os.path.abspath(f'./output/{date_str}')
    driver_path = '../../CHROME_DRIVER/chromedriver_116.exe'
    url = 'https://www.post.gov.tw/post/internet/I_location/index_all.html'
    print('Crawling Post...')
    df_posts = crawler(driver_path, url)
    # df_posts = pd.DataFrame(df_posts.iloc[1:,:], columns=df_posts.columns)
    # df_posts = df_posts.rename(columns=dict(zip([i for i in range(len(df_posts.columns))],df_posts.iloc[0:1,:].values[0])))
    new_header = df_posts.iloc[0]
    df_posts = df_posts.iloc[1:,:]
    df_posts.columns = new_header
    # print(df_posts)

    df_posts['局名'] = df_posts['局名'].str.split('\n').str[1]

    df_posts['局址'] = df_posts['局址'].apply(ReplaceTai)
    df_posts['局址'] = df_posts['局址'].apply(TakeOffBracket)
    df_posts['局址'] = df_posts['局址'].apply(TakeOffPeriod)
    df_posts['局址'] = df_posts['局址'].apply(SplitToNumber)
    df_posts['局址'] = df_posts['局址'].apply(SplitNumber)

    df_posts = df_posts.reset_index(drop=True)
    with open('lookup_table_path.txt') as f:
        lookup_table_path = f.read()
    print('   Geocoding Address...')
    # geocode
    df_posts = geocode_dataframe(df_posts,'局址',lookup_table_path,from_file='郵局')     
    df_posts.to_csv(os.path.join('Data','Post_All.csv'),encoding='utf_8_sig',index=False)
    print('Done Crawling Post！')
    print('='*85)
