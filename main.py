import subprocess
import os
from tkinter import E

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import json
import requests
import re
from bs4 import BeautifulSoup as bs
# import fake_useragent as fuaF
import time
import random
import check_driver_version
from selenium import webdriver
import json
from bs4 import BeautifulSoup as bs

# ==============ATM資料================================================
def f1():
    
    try:
        if  record.loc['ATM資料','統計時間'][:7] !=  this_month or pd.notnull(record.loc['ATM資料','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'ATM資料'))
            subprocess.check_call(f'python 爬蟲_ATM位置.py',shell=True)
            record.loc['ATM資料','上次統計時間'] = record.loc['ATM資料','統計時間']
            record.loc['ATM資料','程式更新時間'] = today_str
            record.loc['ATM資料','統計時間'] = today_str
            record.loc['ATM資料','本月是否更新'] = 1
            record.loc['ATM資料','報錯'] = None
        else:
            if record.loc['ATM資料','統計時間'][:7] == this_month:
                record.loc['ATM資料','本月是否更新'] = 1
            else:
                record.loc['ATM資料','本月是否更新'] = 0
    except Exception as e:
        record.loc['ATM資料','報錯'] = [e]
        record.loc['ATM資料','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ==============ATM資料 end============================================
    

# ===============MOTC==================================================
def f2():
    
    try:
        if  record.loc['MOTC','統計時間'][:7] !=  this_month or pd.notnull(record.loc['MOTC','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'MOTC'))
            subprocess.check_call(f'python 爬蟲_MOTC.py',shell=True)
            record.loc['MOTC','上次統計時間'] = record.loc['MOTC','統計時間']
            record.loc['MOTC','本月是否更新'] = 1
            record.loc['MOTC','程式更新時間'] = today_str
            
            record.loc['MOTC','統計時間'] = today_str
            record.loc['MOTC','報錯'] = None

        else:
            if record.loc['MOTC','統計時間'][:7] == this_month:
                record.loc['MOTC','本月是否更新'] = 1
            else:
                record.loc['MOTC','本月是否更新'] = 0
    except Exception as e:
        record.loc['MOTC','報錯'] = [e]
        record.loc['MOTC','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ===============MOTC end==============================================


# ===============OSM===================================================
def f3():
    
    try:
        if  record.loc['OSM','統計時間'][:7] !=  this_month or pd.notnull(record.loc['OSM','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'OSM'))
            subprocess.check_call(f'python 爬蟲_osm.py',shell=True)
            record.loc['OSM','上次統計時間'] = record.loc['OSM','統計時間']
            record.loc['OSM','統計時間'] = today_str
            record.loc['OSM','本月是否更新'] = 1
            record.loc['OSM','程式更新時間'] = today_str
            record.loc['OSM','報錯'] = None

        else:
            if record.loc['OSM','統計時間'][:7] == this_month:
                record.loc['OSM','本月是否更新'] = 1
            else:
                record.loc['OSM','本月是否更新'] = 0
    except Exception as e:
        record.loc['OSM','報錯'] = [e]
        record.loc['OSM','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ===============OSM end===============================================


# ===============人口資料==============================================
def f4():
    
    try:

        if  int(datetime.datetime.strftime(datetime.datetime.today(),'%Y/%m/%d')[-2:]) <= 10:
            today = datetime.datetime.today()- relativedelta(months=2)
            today_str = datetime.datetime.strftime(today,'%Y/%m/%d')
            this_year_month_roc = str(int(today_str[:4]) - 1911)+today_str[5:7]
        else:
            today = datetime.datetime.today()- relativedelta(months=1)
            today_str = datetime.datetime.strftime(today,'%Y/%m/%d')
            this_year_month_roc = str(int(today_str[:4]) - 1911)+today_str[5:7]

        record_period_population = record.loc['人口資料']['統計時間']
        if pd.isnull(record_period_population):
            record_period_population = 0
        else:
            record_period_population = int(record_period_population)
            
        if int(this_year_month_roc) > record_period_population or pd.notnull(record.loc['人口資料','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'人口資料'))
            time.sleep(random.randint(60,90)) 
            subprocess.check_call('python 爬蟲_人口.py',shell=True)
            
            record.loc['人口資料','統計時間'] = record.loc['人口資料','上次統計時間'] 
            record.loc['人口資料','統計時間'] = this_year_month_roc
            record.loc['人口資料','本月是否更新'] = 1
            record.loc['人口資料','報錯'] = None
            
        else:
            if record.loc['人口資料','統計時間'] == this_year_month_roc:
                record.loc['人口資料','本月是否更新'] = 1
            else:
                record.loc['人口資料','本月是否更新'] = 0
                
    except Exception as e:
        record.loc['人口資料','報錯'] = [e]
        record.loc['人口資料','本月是否更新'] = 0
    today = datetime.datetime.today()
    record.loc['人口資料','程式更新時間'] = today_str
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ===============人口資料 end==========================================


# ===============房價==================================================
def f5():
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get('https://plvr.land.moi.gov.tw/Download_ajax_active',headers=headers)
        soup = bs(res.text,'lxml')
        file_date_regex = re.compile(r'資料內容：登記日期 (\d+)年(\d+)月(\d+)日至 (\d+)年(\d+)月(\d+)日之買賣案件')
        # description_text = soup.find('table').find('td',{'colspan':'3'}).find('span').text
        description_text = soup.find('span', class_ = "text-danger").text
        search_res = file_date_regex.search(description_text)
        latest_date = datetime.datetime(int(search_res.group(4))+1911,int(search_res.group(5)),int(search_res.group(6)))
        record_date = record.loc['房價','統計時間']
        if (pd.isnull(record_date)) or (datetime.datetime.strptime(record_date,'%Y/%m/%d') < latest_date) or pd.notnull(record.loc['房價','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'房價'))
            subprocess.check_call(f'python 爬蟲_房價.py',shell=True)

            record.loc['房價','上次統計時間'] = record.loc['房價','統計時間']
            record.loc['房價','統計時間'] = datetime.datetime.strftime(latest_date,'%Y/%m/%d')
            record.loc['房價','本月是否更新'] = 1
            record.loc['房價','報錯'] = None

        else:
            if record.loc['房價','統計時間'][:7] == this_month:
                record.loc['房價','本月是否更新'] = 1

            else:
                record.loc['房價','本月是否更新'] = 0

    except Exception as e:
        record.loc['房價','報錯'] = [e]
        record.loc['房價','本月是否更新'] = 0
    record.loc['房價','程式更新時間'] = today_str
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ===============房價 end==============================================


# ===============所得資料============================================== 2021/07/20 Albert完成
def f6():    
    try:
        if  record.loc['所得資料','統計時間'][:7] !=  this_month or pd.notnull(record.loc['所得資料','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'所得資料'))
            subprocess.check_call(f'python income.py',shell=True)
            
            year_number = ''
            for file in os.listdir(os.getcwd()+r'\Data'):
                regex = re.compile(r'.*年度綜合所得稅申報核定統計專冊.*')
                match = regex.search(file)
                if match:
                    year_number = match[0][:3]
            
            record.loc['所得資料','上次統計時間'] = record.loc['所得資料','統計時間']
            record.loc['所得資料','統計時間'] = year_number
            record.loc['所得資料','本月是否更新'] = 1
            record.loc['所得資料','程式更新時間'] = today_str
            record.loc['所得資料','報錯'] = None

        else:
            if record.loc['所得資料','統計時間'][:7] == this_month:
                record.loc['所得資料','本月是否更新'] = 1
            else:
                record.loc['所得資料','本月是否更新'] = 0
    except Exception as e:
        record.loc['所得資料','報錯'] = [e]
        record.loc['所得資料','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ===============所得資料 end==========================================


# ===============金融機構==============================================
def f7():
    
    try:
        if  record.loc['金融機構','統計時間'][:7] !=  this_month or pd.notnull(record.loc['金融機構','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'金融機構'))
            subprocess.check_call(f'python 爬蟲_金融機構.py',shell=True)
            record.loc['金融機構','上次統計時間'] = record.loc['金融機構','統計時間']
            record.loc['金融機構','統計時間'] = today_str
            record.loc['金融機構','程式更新時間'] = today_str
            record.loc['金融機構','本月是否更新'] = 1
            record.loc['金融機構','報錯'] = None

        else:
            if record.loc['金融機構','統計時間'][:7] == this_month:
                record.loc['金融機構','本月是否更新'] = 1
            else:
                record.loc['金融機構','本月是否更新'] = 0
    except Exception as e:
        record.loc['金融機構','報錯'] = [e]
        record.loc['房價','本月是否更新'] = 0
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ===============金融機構 end==========================================

# ===============商家=================================================
def f8():
    
    try:
        if  record.loc['商家_7-11','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_7-11','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_7-11.py',shell=True)
            record.loc['商家_7-11','上次統計時間'] = record.loc['商家_7-11','統計時間']
            record.loc['商家_7-11','統計時間'] = today_str
            record.loc['商家_7-11','本月是否更新'] = 1
            record.loc['商家_7-11','程式更新時間'] = today_str
            record.loc['商家_7-11','報錯'] = None
        else:
            if record.loc['商家_7-11','統計時間'][:7] == this_month:
                record.loc['商家_7-11','本月是否更新'] = 1
            else:
                record.loc['商家_7-11','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_7-11','報錯'] = [e]
        record.loc['商家_7-11','本月是否更新'] = 0
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')

def f9():    
    try:    
        if  record.loc['商家_中華電信','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_中華電信','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_中華電信.py',shell=True)
            record.loc['商家_中華電信','上次統計時間'] = record.loc['商家_中華電信','統計時間']
            record.loc['商家_中華電信','統計時間'] = today_str
            record.loc['商家_中華電信','本月是否更新'] = 1
            record.loc['商家_中華電信','程式更新時間'] = today_str
            record.loc['商家_中華電信','報錯'] = None

        else:
            if record.loc['商家_中華電信','統計時間'][:7] == this_month:
                record.loc['商家_中華電信','本月是否更新'] = 1
            else:
                record.loc['商家_中華電信','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_中華電信','報錯'] = [e]
        record.loc['商家_中華電信','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
    

def f10():
    try:    
        if  record.loc['商家_全家','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_全家','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_全家.py',shell=True)
            record.loc['商家_全家','上次統計時間'] = record.loc['商家_全家','統計時間']
            record.loc['商家_全家','統計時間'] = today_str
            record.loc['商家_全家','本月是否更新'] = 1
            record.loc['商家_全家','程式更新時間'] = today_str
            record.loc['商家_全家','報錯'] = None

        else:
            if record.loc['商家_全家','統計時間'][:7] == this_month:
                record.loc['商家_全家','本月是否更新'] = 1
            else:
                record.loc['商家_全家','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_全家','報錯'] = [e]
        record.loc['商家_全家','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')

def f11():
    
    try:
        if  record.loc['商家_全聯','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_全聯','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_全聯.py',shell=True) #20240415 mila_edit
            record.loc['商家_全聯','上次統計時間'] = record.loc['商家_全聯','統計時間']
            record.loc['商家_全聯','統計時間'] = today_str
            record.loc['商家_全聯','本月是否更新'] = 1
            record.loc['商家_全聯','程式更新時間'] = today_str
            record.loc['商家_全聯','報錯'] = None

        else:
            if record.loc['商家_全聯','統計時間'][:7] == this_month:
                record.loc['商家_全聯','本月是否更新'] = 1
            else:
                record.loc['商家_全聯','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_全聯','報錯'] = [e]
        record.loc['商家_全聯','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
    
# def f12():   
#     try:
#         if  record.loc['商家_亞太電信','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_亞太電信','報錯']):
#             os.chdir(os.path.join(crawler_base_path,'商家'))
#             subprocess.check_call(f'python 爬蟲_亞太電信.py',shell=True)
#             record.loc['商家_亞太電信','上次統計時間'] = record.loc['商家_亞太電信','統計時間']
#             record.loc['商家_亞太電信','統計時間'] = today_str
#             record.loc['商家_亞太電信','本月是否更新'] = 1
#             record.loc['商家_亞太電信','程式更新時間'] = today_str
#             record.loc['商家_亞太電信','報錯'] = None

#         else:
#             if record.loc['商家_亞太電信','統計時間'][:7] == this_month:
#                 record.loc['商家_亞太電信','本月是否更新'] = 1
#             else:
#                 record.loc['商家_亞太電信','本月是否更新'] = 0
#     except Exception as e:
#         record.loc['商家_亞太電信','報錯'] = [e]
#         record.loc['商家_亞太電信','本月是否更新'] = 0
    
#     os.chdir(crawler_base_path)
#     record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
        
def f12():   
    try:    
        if  record.loc['商家_家樂福','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_家樂福','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_家樂福.py',shell=True)
            record.loc['商家_家樂福','上次統計時間'] = record.loc['商家_家樂福','統計時間']
            record.loc['商家_家樂福','統計時間'] = today_str
            record.loc['商家_家樂福','本月是否更新'] = 1
            record.loc['商家_家樂福','程式更新時間'] = today_str
            record.loc['商家_家樂福','報錯'] = None

        else:
            if record.loc['商家_家樂福','統計時間'][:7] == this_month:
                record.loc['商家_家樂福','本月是否更新'] = 1
            else:
                record.loc['商家_家樂福','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_家樂福','報錯'] = [e]
        record.loc['商家_家樂福','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
    
def f13():   
    try:    
        if  record.loc['商家_康是美','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_康是美','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_康是美.py',shell=True)
            record.loc['商家_康是美','上次統計時間'] = record.loc['商家_康是美','統計時間']
            record.loc['商家_康是美','統計時間'] = today_str
            record.loc['商家_康是美','本月是否更新'] = 1
            record.loc['商家_康是美','程式更新時間'] = today_str
            record.loc['商家_康是美','報錯'] = None

        else:
            if record.loc['商家_康是美','統計時間'][:7] == this_month:
                record.loc['商家_康是美','本月是否更新'] = 1
            else:
                record.loc['商家_康是美','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_康是美','報錯'] = [e]
        record.loc['商家_康是美','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
    
def f14():    
    try:
        if  record.loc['商家_屈臣氏','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_屈臣氏','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_屈臣氏.py',shell=True)
            record.loc['商家_屈臣氏','上次統計時間'] = record.loc['商家_屈臣氏','統計時間']
            record.loc['商家_屈臣氏','統計時間'] = today_str
            record.loc['商家_屈臣氏','本月是否更新'] = 1
            record.loc['商家_屈臣氏','程式更新時間'] = today_str
            record.loc['商家_屈臣氏','報錯'] = None

        else:
            if record.loc['商家_屈臣氏','統計時間'][:7] == this_month:
                record.loc['商家_屈臣氏','本月是否更新'] = 1
            else:
                record.loc['商家_屈臣氏','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_屈臣氏','報錯'] = [e]
        record.loc['商家_屈臣氏','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')

#要Geocoding
    
def f15():
    try:
        if  record.loc['商家_OK便利商店','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_OK便利商店','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_OK便利商店.py',shell=True)
            record.loc['商家_OK便利商店','上次統計時間'] = record.loc['商家_OK便利商店','統計時間']
            record.loc['商家_OK便利商店','統計時間'] = today_str
            record.loc['商家_OK便利商店','本月是否更新'] = 1
            record.loc['商家_OK便利商店','程式更新時間'] = today_str
            record.loc['商家_OK便利商店','報錯'] = None
            
        else:
            if record.loc['商家_OK便利商店','統計時間'][:7] == this_month:
                record.loc['商家_OK便利商店','本月是否更新'] = 1
            else:
                record.loc['商家_OK便利商店','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_OK便利商店','報錯'] = [e]
        record.loc['商家_OK便利商店','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
    
def f16():
    try:    
        if  record.loc['商家_大潤發','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_大潤發','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_大潤發.py',shell=True)
            record.loc['商家_大潤發','上次統計時間'] = record.loc['商家_大潤發','統計時間']
            record.loc['商家_大潤發','本月是否更新'] = 1
            record.loc['商家_大潤發','程式更新時間'] = today_str
            record.loc['商家_大潤發','統計時間'] = today_str
            record.loc['商家_大潤發','報錯'] = None

        else:
            if record.loc['商家_大潤發','統計時間'][:7] == this_month:
                record.loc['商家_大潤發','本月是否更新'] = 1
            else:
                record.loc['商家_大潤發','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_大潤發','報錯'] = [e]
        record.loc['商家_大潤發','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
    
def f17():
    try:    
        if  record.loc['商家_台灣大哥大','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_台灣大哥大','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_台灣大哥大.py',shell=True)
            record.loc['商家_台灣大哥大','上次統計時間'] = record.loc['商家_台灣大哥大','統計時間']
            record.loc['商家_台灣大哥大','本月是否更新'] = 1
            record.loc['商家_台灣大哥大','程式更新時間'] = today_str
            record.loc['商家_台灣大哥大','統計時間'] = today_str
            record.loc['商家_台灣大哥大','報錯'] = None

        else:
            if record.loc['商家_台灣大哥大','統計時間'][:7] == this_month:
                record.loc['商家_台灣大哥大','本月是否更新'] = 1
            else:
                record.loc['商家_台灣大哥大','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_台灣大哥大','報錯'] = [e]
        record.loc['商家_台灣大哥大','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')

def f18():    
    try:    
        if  record.loc['商家_頂好','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_頂好','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_頂好.py',shell=True)
            record.loc['商家_頂好','上次統計時間'] = record.loc['商家_頂好','統計時間']
            record.loc['商家_頂好','本月是否更新'] = 1
            record.loc['商家_頂好','程式更新時間'] = today_str
            record.loc['商家_頂好','統計時間'] = today_str
            record.loc['商家_頂好','報錯'] = None

        else:
            if record.loc['商家_頂好','統計時間'][:7] == this_month:
                record.loc['商家_頂好','本月是否更新'] = 1
            else:
                record.loc['商家_頂好','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_頂好','報錯'] = [e]
        record.loc['商家_頂好','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')

def f19():    
    try:    
        if  record.loc['商家_萊爾富','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_萊爾富','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_萊爾富.py',shell=True)
            record.loc['商家_萊爾富','上次統計時間'] = record.loc['商家_萊爾富','統計時間']
            record.loc['商家_萊爾富','本月是否更新'] = 1
            record.loc['商家_萊爾富','程式更新時間'] = today_str
            record.loc['商家_萊爾富','統計時間'] = today_str
            record.loc['商家_萊爾富','報錯'] = None

        else:
            if record.loc['商家_萊爾富','統計時間'][:7] == this_month:
                record.loc['商家_萊爾富','本月是否更新'] = 1
            else:
                record.loc['商家_萊爾富','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_萊爾富','報錯'] = [e]
        record.loc['商家_萊爾富','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')

def f20():
    try:    
        if  record.loc['商家_遠傳電信','統計時間'][:7] !=  this_month or pd.notnull(record.loc['商家_遠傳電信','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'商家'))
            subprocess.check_call(f'python 爬蟲_遠傳電信.py',shell=True)
            record.loc['商家_遠傳電信','上次統計時間'] = record.loc['商家_遠傳電信','統計時間']
            record.loc['商家_遠傳電信','本月是否更新'] = 1
            record.loc['商家_遠傳電信','程式更新時間'] = today_str
            record.loc['商家_遠傳電信','統計時間'] = today_str
            record.loc['商家_遠傳電信','報錯'] = None
            
        else:
            if record.loc['商家_遠傳電信','統計時間'][:7] == this_month:
                record.loc['商家_遠傳電信','本月是否更新'] = 1
            else:
                record.loc['商家_遠傳電信','本月是否更新'] = 0
    except Exception as e:
        record.loc['商家_遠傳電信','報錯'] = [e]
        record.loc['商家_遠傳電信','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ===============商家 end=============================================


# ===============郵局=================================================
def f21():
    
    try:
        if  record.loc['郵局','統計時間'][:7] !=  this_month or pd.notnull(record.loc['郵局','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'郵局'))
            subprocess.check_call(f'python 爬蟲_郵局.py',shell=True)
            record.loc['郵局','上次統計時間'] = record.loc['郵局','統計時間']
            record.loc['郵局','本月是否更新'] = 1
            record.loc['郵局','程式更新時間'] = today_str
            record.loc['郵局','統計時間'] = today_str
            record.loc['郵局','報錯'] = None

        else:
            if record.loc['郵局','統計時間'][:7] == this_month:
                record.loc['郵局','本月是否更新'] = 1
            else:
                record.loc['郵局','本月是否更新'] = 0
    except Exception as e:
        record.loc['郵局','報錯'] = [e]
        record.loc['郵局','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ===============郵局 end=============================================


# ===============學校=================================================(Albert20210720修改完成)
def f22():
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}

        web = requests.get('https://depart.moe.edu.tw/ed4500/News.aspx?n=5A930C32CC6C3818&sms=91B3AAE8C6388B96', headers=headers) #Albert修改
        soup = bs(web.text, 'lxml') #Albert修改
        latest_edu_shcool_web = soup.find('a',{'id':'ContentPlaceHolder1_gvIndex_lnkTitle_0'})['href'] #Albert修改 #取得最新學年度的資料連結

        res = requests.get('https://depart.moe.edu.tw/ed4500/'+latest_edu_shcool_web ,headers=headers)
        soup = bs(res.text,'lxml') 
        ##抓取最新學年度
        latest_update_string = soup.find('title').text.replace('\r','').replace('\n','').replace('\t','')
        latest_update_year = int(latest_update_string[:3])
        ##抓取上版日期
        date_regex = re.compile('上版日期：(\d+-\d+-\d+)')
        latest_update_date = date_regex.search(soup.find('td',string=date_regex).text).group(1)
        y,m,d = map(int,latest_update_date.split('-'))
        y += 1911
        latest_update_date = datetime.datetime(y,m,d)
        latest_update_date_str = datetime.datetime.strftime(latest_update_date,'%Y/%m/%d')

        record_latest_date = record.loc['學校','統計時間']
        if pd.isnull(record_latest_date):
            record_latest_date = latest_update_date - datetime.timedelta(days=1)
        else:
            record_latest_date = datetime.datetime.strptime(record_latest_date,'%Y/%m/%d')
        if latest_update_date > record_latest_date or pd.notnull(record.loc['學校','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'學校'))
            subprocess.check_call(f'python 爬蟲_學校.py --year {latest_update_year}',shell=True)
            record.loc['學校','統計時間'] = latest_update_date_str
            record.loc['學校','本月是否更新'] = 1
            record.loc['學校','報錯'] = None
        else:
            if record.loc['學校','統計時間'][:7] == this_month:
                record.loc['學校','本月是否更新'] = 1
            else:
                record.loc['學校','本月是否更新'] = 0
    except Exception as e:
        record.loc['學校','報錯'] = [e]
        record.loc['學校','本月是否更新'] = 0
    record.loc['學校','程式更新時間'] = today_str
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ===============學校 end=============================================


# ===============醫療機構資料=========================================
def f23():
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get('https://dep.mohw.gov.tw/DOMA/cp-4926-54415-106.html',headers = headers)
        soup = bs(res.text,'lxml')
            
        update_time_regex = re.compile(r'更新時間：(\d+-\d+-\d+)')
        update_time_text = soup.find('section',{'id':'ContentPage'}).find('ul',{'class':'info'}).find_all('li')[-1].text
        update_time_str = update_time_regex.search(update_time_text).group(1)
        y,m,d = map(int,update_time_str.split('-'))
        y += 1911
        update_time = datetime.datetime(y,m,d)
        update_time_str = datetime.datetime.strftime(update_time,'%Y/%m/%d')

        record_latest_date_medical = record.loc['醫療機構資料','統計時間']
        if pd.isnull(record_latest_date_medical):
            record_latest_date_medical = update_time-datetime.timedelta(days=1)
        else:
            record_latest_date_medical = datetime.datetime.strptime(record_latest_date_medical,'%Y/%m/%d')
        if update_time > record_latest_date_medical or pd.notnull(record.loc['醫療機構資料','報錯']):
            os.chdir(os.path.join(crawler_1st_path,'醫療機構資料'))
            time.sleep(random.randint(20,30))
            subprocess.check_call(f'python 爬蟲_醫療機構之基本資料.py',shell=True)
            record.loc['醫療機構資料','統計時間'] = update_time_str
            record.loc['醫療機構資料','本月是否更新'] = 1
            record.loc['醫療機構資料','報錯'] = None
        else:
            if record.loc['醫療機構資料','統計時間'][:7] == this_month:
                record.loc['醫療機構資料','本月是否更新'] = 1
            else:
                record.loc['醫療機構資料','本月是否更新'] = 0
    except Exception as e:
        record.loc['醫療機構資料','報錯'] = [e]
        record.loc['醫療機構資料','本月是否更新'] = 0
    record.loc['醫療機構資料','程式更新時間'] = today_str
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')
# ===============醫療機構資料 end=====================================

# ===============二期爬蟲_五大超商====================================

def f24():
    try:    
        if  record.loc['五大超商','統計時間'][:7] !=  this_month or pd.notnull(record.loc['五大超商','報錯']):
            os.chdir(os.path.join(crawler_2nd_path,'五大超商'))
            print('****開始運下載五大超商資料****')
            subprocess.check_call(f'python convenience_store_downloader.py',shell=True) #下載
            print('****開始進行五大超商經緯度轉換****')
            subprocess.check_call(f'conv_store_geocoding.py',shell=True)
            record.loc['五大超商','上次統計時間'] = record.loc['五大超商','統計時間']
            record.loc['五大超商','統計時間'] = today_str
            record.loc['五大超商','本月是否更新'] = 1
            record.loc['五大超商','程式更新時間'] = today_str
            record.loc['五大超商','報錯'] = None

        else:
            if record.loc['五大超商','統計時間'][:7] == this_month:
                record.loc['五大超商','本月是否更新'] = 1
            else:
                record.loc['五大超商','本月是否更新'] = 0
    except Exception as e:
        record.loc['五大超商','報錯'] = [e]
        record.loc['五大超商','本月是否更新'] = 0
    
    os.chdir(crawler_base_path)
    record.to_csv('爬蟲資料更新紀錄.csv',encoding='utf_8_sig')

# ===============二期爬蟲_五大超商 end================================


if __name__ == '__main__':
    #設定路徑====================================================
    crawler_base_path = os.getcwd() 
    crawler_1st_path = os.getcwd() + '/一期爬蟲/'
    crawler_2nd_path = os.getcwd() + '/二期爬蟲/'

    #設定時間參數====================================================
    today_str = datetime.datetime.strftime(datetime.datetime.today(),'%Y/%m/%d')
    this_month = today_str[:7]
    # this_month = '202402'
    this_year_roc = str(int(today_str[:4]) - 1911)
    this_year_month_roc = str(int(today_str[:4]) - 1911)+today_str[5:7]
    # this_year_month_roc = '11302'
    print(this_month)
    today = datetime.datetime.strptime(today_str,"%Y/%m/%d")
    record = pd.read_csv('爬蟲資料更新紀錄.csv',encoding='utf-8',index_col='資料名稱')
    #新增一欄
    # record['報錯'] = None
    # record['上次統計時間'] = None

    ############################################################
    chrome_path = r'C:\Users\sinopacDAD\Chrome測試版\chrome-win64\chrome.exe'
    chrome_driver_path = r'CHROME_DRIVER'
    ############################################################
    # DC = check_driver_version.DriverCheck(chrome_path,chrome_driver_path)
    # driver_path_to_use = DC.check_chromedriver_version()

    print("爬蟲城市編號: 1(ATM),2(MOTC),3(OSM),4(人口),5(房價),6(所得),7(金融機構),8(7-11),9(中華電信),10(全家),11(全聯),12(家樂福),13(康是美),14(屈臣氏),15(OK),16(大潤發),17(台哥大),18(頂好),19(萊爾富),20(遠傳電信),21(郵局),22(學校),23(醫療機構),24(五大超商_二期爬蟲)")

    s = int(input("請輸入起始編號(默認1)") or "1")
    e = int(input("請輸入結束編號(默認24)") or "24")
    e = e+1

    for i in range(s, e):
        print(i)
        fn = locals()['f{}'.format(i)]
        fn()
