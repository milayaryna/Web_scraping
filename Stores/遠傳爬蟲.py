from selenium import webdriver
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import random
from datetime import datetime
from transform_time import transform_time
import sys
sys.path.append(r'..\..')
from geocode import geocode_dataframe
sys.path.append(r'..')
# import check_driver_version

def get_county_dic(driver):
    #點選搜尋方式並選取"縣市、路名、店名"
    driver.find_element_by_css_selector(".with-arrow > .fui-dropdown-item > span").click()
    driver.find_element_by_css_selector(".menu-wrapper > .fui-item:nth-child(1) > .content").click()
    #店選"請選擇縣市"
    driver.find_element_by_css_selector(".filter-city > .fui-dropdown:nth-child(1) > .fui-dropdown-item > span").click()
    #取出所有縣市
    all_county = [ele.text for ele in driver.find_elements_by_css_selector(".d-block .fui-item > .content")]
    dic = dict(zip(all_county,range(1,len(all_county)+1)))
    #再把"請點選縣市點掉"
    driver.find_element_by_css_selector(".filter-city > .fui-dropdown:nth-child(1) > .fui-dropdown-item > span").click()
    return dic
    
def get_county_stores(driver,county,county_id):
    #店選"請選擇縣市"
    time.sleep(random.randint(4,6))
    driver.find_element_by_css_selector(".filter-city > .fui-dropdown:nth-child(1) > .fui-dropdown-item > span").click()
    driver.find_element_by_css_selector(".d-block .fui-item:nth-child({}) > .content".format(county_id)).click()
    time.sleep(10)
    
    soup = bs(driver.page_source,'lxml')
    all_stores = soup.find('div',{'class':'searchbox'}).find_all('div',{'class':'list'})
    li = []
    for store in all_stores:
        store_county = county
        store_name = store.find('h4').text
        store_address = store.find('p').text
        li.append({'store_county':store_county,'store_name':store_name,'store_address':store_address})
    df = pd.DataFrame(li)
    return df
    
if __name__ == '__main__':
    print('Crawling 遠傳電信...')
    start_time = datetime.now()
    start_str = datetime.strftime(start_time,'%Y-%m-%d-%H:%M:%S')
    print('Start Time：',start_str)
    
    ############################################################
    options = webdriver.ChromeOptions()
    options.binary_location = 'C:/Users/sinopacDAD/Chrome測試版/chrome-win64/chrome.exe'
    driver_path = '../../CHROME_DRIVER/chromedriver_116.exe'
    ############################################################
    # DC = check_driver_version.DriverCheck(chrome_path,chrome_driver_path)
    # driver_path_to_use = DC.check_chromedriver_version()
    
    driver = webdriver.Chrome(driver_path, chrome_options=options)
    driver.implicitly_wait(5)
    driver.get('https://ecare.fetnet.net/DigService/help-center/store')
    time.sleep(5)
    
    all_county_dic = get_county_dic(driver)
    
    li_df = []
    for county,county_id in all_county_dic.items():
        print('   Processing...{}'.format(county))
        li_df.append(get_county_stores(driver,county,county_id))
    total_df = pd.concat(li_df,ignore_index=True)
    driver.close()
    
    total_df.reset_index(drop=True,inplace=True)
    with open('lookup_table_path.txt') as f:
        lookup_table_path = f.read()
    print('   Geocoding Address...')
    total_df = geocode_dataframe(total_df,'store_address',lookup_table_path,from_file='遠傳電信')
    total_df.to_csv(r'Data\遠傳電信.csv',index=False,encoding='utf_8_sig')
    
    
    end_time = datetime.now()
    end_str = datetime.strftime(end_time,'%Y-%m-%d-%H:%M:%S')
    print('End Time：',end_str)
    
    t = (end_time-start_time)
    print(transform_time(t))
    
    with open(r'Data\商家_time_records.txt','a') as f:
        f.write('\n=============================================')
        f.write('\n遠傳電信')
        f.write('\n--------------')
        f.write('\nStart Time：{}'.format(start_str))
        f.write('\nEnd Time  ：{}'.format(end_str))
        f.write('\nTotal Time：{}'.format(transform_time(t)))
        f.write('\n=============================================\n')
    print('Done Crawling 遠傳電信！')
    print('='*85)
